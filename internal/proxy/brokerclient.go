package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	brokerproxypb "github.com/ronlv10/substrate-ws-poc/proto/brokerproxy/v1"
)

const egressTimeout = 30 * time.Second

// BrokerClient maintains the proxy's connection to the always-on broker. The
// stream dies on every suspend (the peer outlives the checkpoint); Run
// redials with backoff and re-announces, and the restore detector calls
// Redial for an immediate reconnect instead of waiting out keepalive.
type BrokerClient struct {
	core        *Core
	addr        string
	identityDir string
	log         *slog.Logger

	mu       sync.Mutex
	stream   brokerproxypb.BrokerProxy_SessionClient
	cancel   context.CancelFunc
	corrID   uint64
	inflight map[uint64]chan *brokerproxypb.EgressResp
	wake     chan struct{} // restore detector pokes this to cut short a backoff sleep
}

func NewBrokerClient(core *Core, addr, identityDir string, log *slog.Logger) *BrokerClient {
	bc := &BrokerClient{
		core:        core,
		addr:        addr,
		identityDir: identityDir,
		log:         log,
		inflight:    map[uint64]chan *brokerproxypb.EgressResp{},
		wake:        make(chan struct{}, 1),
	}
	core.SetEgress(bc.egress)
	core.SetOnAck(bc.sendAck)
	return bc
}

// Run dials the broker and keeps one Session stream alive, reconnecting with
// backoff forever. It never returns.
func (bc *BrokerClient) Run() {
	conn, err := grpc.NewClient(bc.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		bc.log.Error("local-proxy: broker dial setup failed", slog.Any("error", err))
		os.Exit(1)
	}
	client := brokerproxypb.NewBrokerProxyClient(conn)

	backoff := time.Second
	for {
		err := bc.session(client)
		bc.log.Info("local-proxy: broker session ended, reconnecting",
			slog.Any("error", err), slog.Duration("backoff", backoff))
		backoff = bc.waitBeforeReconnect(backoff)
	}
}

// waitBeforeReconnect blocks until the next reconnect should happen and returns
// the backoff for the following wait. A wake — the restore detector flagging a
// checkpoint/restore — reconnects immediately and resets the backoff, so a
// resume never waits out a backoff inflated by pre-suspend failures. Otherwise
// it sleeps the current backoff and grows it (capped), the ordinary path for a
// genuine broker outage.
func (bc *BrokerClient) waitBeforeReconnect(backoff time.Duration) time.Duration {
	select {
	case <-bc.wake:
		return time.Second
	case <-time.After(backoff):
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		return backoff
	}
}

func (bc *BrokerClient) session(client brokerproxypb.BrokerProxyClient) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Session(ctx)
	if err != nil {
		return err
	}

	atespace, name, err := bc.identity()
	if err != nil {
		return err
	}
	token := bc.core.AppToken()
	if token == "" {
		token = os.Getenv("SLACK_APP_TOKEN")
	}
	announce := &brokerproxypb.Announce{
		Atespace:     atespace,
		Name:         name,
		AppToken:     token,
		LastAckedSeq: bc.core.LastContiguousAcked(),
	}
	if err := stream.Send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Announce{Announce: announce}}); err != nil {
		return err
	}
	bc.log.Info("local-proxy: announced to broker",
		slog.String("actor", atespace+"/"+name),
		slog.Uint64("last_acked_seq", announce.LastAckedSeq))

	bc.mu.Lock()
	bc.stream, bc.cancel = stream, cancel
	bc.mu.Unlock()
	defer func() {
		bc.mu.Lock()
		if bc.stream == stream {
			bc.stream, bc.cancel = nil, nil
		}
		for id, ch := range bc.inflight {
			close(ch)
			delete(bc.inflight, id)
		}
		bc.mu.Unlock()
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		switch m := msg.Msg.(type) {
		case *brokerproxypb.BrokerMsg_Event:
			if err := bc.core.Deliver(m.Event.Seq, m.Event.Payload); err != nil {
				bc.log.Warn("local-proxy: dropping undeliverable event", slog.Any("error", err))
			}
		case *brokerproxypb.BrokerMsg_EgressResp:
			bc.mu.Lock()
			ch := bc.inflight[m.EgressResp.CorrId]
			delete(bc.inflight, m.EgressResp.CorrId)
			bc.mu.Unlock()
			if ch != nil {
				ch <- m.EgressResp
			}
		}
	}
}

// Redial tears down the current stream and wakes the reconnect loop so it
// reconnects immediately. Called on restore detection: the old stream's peer
// state is gone, and both waiting for keepalive to notice and sitting out a
// stale backoff cost the whole wake latency.
func (bc *BrokerClient) Redial() {
	bc.mu.Lock()
	cancel := bc.cancel
	bc.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case bc.wake <- struct{}{}: // non-blocking: cut short a backoff sleep
	default:
	}
}

// send serializes stream writes (Announce is sent before the stream is
// published, ack/egress after — gRPC allows one concurrent sender).
func (bc *BrokerClient) send(msg *brokerproxypb.ProxyMsg) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if bc.stream == nil {
		return fmt.Errorf("broker stream not connected")
	}
	return bc.stream.Send(msg)
}

// sendAck is best-effort: a lost ack is re-covered by last_acked_seq on the
// next Announce, and the broker's redelivery is de-duplicated by the agent.
func (bc *BrokerClient) sendAck(seq uint64) {
	if err := bc.send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Ack{Ack: &brokerproxypb.Ack{Seq: seq}}}); err != nil {
		bc.log.Warn("local-proxy: ack not sent", slog.Uint64("seq", seq), slog.Any("error", err))
	}
}

func (bc *BrokerClient) egress(method, path string, header http.Header, body []byte) (*EgressResult, error) {
	var hdrs []*brokerproxypb.Header
	for k, vv := range header {
		hdrs = append(hdrs, &brokerproxypb.Header{Name: k, Values: vv})
	}
	ch := make(chan *brokerproxypb.EgressResp, 1)
	bc.mu.Lock()
	bc.corrID++
	id := bc.corrID
	bc.inflight[id] = ch
	bc.mu.Unlock()

	err := bc.send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Egress{Egress: &brokerproxypb.Egress{
		CorrId: id, Method: method, Path: path, Headers: hdrs, Body: body,
	}}})
	if err != nil {
		bc.mu.Lock()
		delete(bc.inflight, id)
		bc.mu.Unlock()
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("broker stream closed mid-egress")
		}
		h := http.Header{}
		for _, hd := range resp.Headers {
			for _, v := range hd.Values {
				h.Add(hd.Name, v)
			}
		}
		return &EgressResult{Status: int(resp.Status), Header: h, Body: resp.Body}, nil
	case <-time.After(egressTimeout):
		bc.mu.Lock()
		delete(bc.inflight, id)
		bc.mu.Unlock()
		return nil, fmt.Errorf("egress timed out after %s", egressTimeout)
	}
}

// identity reads the actor identity fresh from the per-resume mount. Never
// cache it: reads taken before a restore (the golden-snapshot era) report the
// golden template's identity.
func (bc *BrokerClient) identity() (atespace, name string, err error) {
	read := func(f string) (string, error) {
		b, err := os.ReadFile(filepath.Join(bc.identityDir, f))
		return strings.TrimSpace(string(b)), err
	}
	if atespace, err = read("atespace"); err != nil {
		return "", "", err
	}
	if name, err = read("actor-id"); err != nil {
		return "", "", err
	}
	return atespace, name, nil
}
