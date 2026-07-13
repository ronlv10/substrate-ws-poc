package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	brokerproxypb "github.com/ronlv10/substrate-ws-poc/proto/brokerproxy/v1"
)

// dialSession serves srv over bufconn and opens one Session stream.
func dialSession(t *testing.T, srv *Server) brokerproxypb.BrokerProxy_SessionClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	brokerproxypb.RegisterBrokerProxyServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	stream, err := brokerproxypb.NewBrokerProxyClient(conn).Session(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func announce(t *testing.T, stream brokerproxypb.BrokerProxy_SessionClient, atespace, name string, lastAcked uint64) {
	t.Helper()
	err := stream.Send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Announce{Announce: &brokerproxypb.Announce{
		Atespace: atespace, Name: name, AppToken: "xapp-test", LastAckedSeq: lastAcked,
	}}})
	if err != nil {
		t.Fatal(err)
	}
}

// newTestServer wires a Server whose sessions read from a fake Slack conn and
// whose egress forwards to apiBase.
func newTestServer(t *testing.T, apiBase string) (*Server, *fakeSlackConn, *fakeResumer) {
	t.Helper()
	conn := newFakeSlackConn()
	resumer := &fakeResumer{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry(resumer, &fakeSuspender{}, nil, 0, 0, log)
	srv := &Server{reg: reg, httpClient: http.DefaultClient, apiBase: apiBase, log: log}

	// Pre-create the session and drive its read loop from the fake conn, so
	// EnsureStarted (which would dial real Slack) finds it already started.
	sess := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	sess.mu.Lock()
	sess.stop = make(chan struct{})
	stop := sess.stop
	sess.mu.Unlock()
	go sess.readSlackUntilClose(conn, stop)
	t.Cleanup(func() { close(stop); close(conn.frames) })
	return srv, conn, resumer
}

func TestSessionAnnounceEventAckRoundTrip(t *testing.T) {
	srv, slackConn, _ := newTestServer(t, "https://slack.com")
	stream := dialSession(t, srv)
	announce(t, stream, "demo", "echo-1", 0)

	slackConn.frames <- eventFrame("e1")
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	ev := msg.GetEvent()
	if ev == nil || ev.Seq != 1 {
		t.Fatalf("expected Event seq=1, got %+v", msg)
	}
	var env struct {
		EnvelopeID string `json:"envelope_id"`
	}
	if err := json.Unmarshal(ev.Payload, &env); err != nil || env.EnvelopeID != "e1" {
		t.Fatalf("payload should be the original envelope: %s (err=%v)", ev.Payload, err)
	}

	stream.Send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Ack{Ack: &brokerproxypb.Ack{Seq: 1}}})
	waitFor(t, "buffer drain", func() bool {
		s := srv.reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.buffer) == 0
	})
}

func TestSessionResendsUnackedAfterReconnect(t *testing.T) {
	srv, slackConn, _ := newTestServer(t, "https://slack.com")
	stream1 := dialSession(t, srv)
	announce(t, stream1, "demo", "echo-1", 0)

	slackConn.frames <- eventFrame("e1")
	slackConn.frames <- eventFrame("e2")
	for i := 0; i < 2; i++ {
		if _, err := stream1.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	stream1.CloseSend()

	// Suspend/resume: fresh stream, proxy acked only seq 1.
	stream2 := dialSession(t, srv)
	announce(t, stream2, "demo", "echo-1", 1)
	msg, err := stream2.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev := msg.GetEvent(); ev == nil || ev.Seq != 2 {
		t.Fatalf("expected re-send of seq=2 only, got %+v", msg)
	}
}

func TestSessionRejectsMissingAnnounce(t *testing.T) {
	srv, _, _ := newTestServer(t, "https://slack.com")
	stream := dialSession(t, srv)
	stream.Send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Ack{Ack: &brokerproxypb.Ack{Seq: 1}}})
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream without Announce should be rejected")
	}
}

func TestGoldenAnnounceGetsNoSlackSession(t *testing.T) {
	srv, _, resumer := newTestServer(t, "https://slack.com")
	stream := dialSession(t, srv)
	announce(t, stream, "ate-golden", "some-uuid", 0)

	time.Sleep(100 * time.Millisecond)
	srv.reg.mu.Lock()
	_, exists := srv.reg.sessions[ActorRef{Atespace: "ate-golden", Name: "some-uuid"}]
	srv.reg.mu.Unlock()
	if exists || resumer.resumes() != 0 {
		t.Fatalf("golden announce created state: session=%v resumes=%d", exists, resumer.resumes())
	}
}

func TestEgressRelayRoundTrip(t *testing.T) {
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat.postMessage" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"echoed":%q}`, body)
	}))
	defer slack.Close()

	srv, _, _ := newTestServer(t, slack.URL)
	stream := dialSession(t, srv)
	announce(t, stream, "demo", "echo-1", 0)

	err := stream.Send(&brokerproxypb.ProxyMsg{Msg: &brokerproxypb.ProxyMsg_Egress{Egress: &brokerproxypb.Egress{
		CorrId: 42, Method: "POST", Path: "/api/chat.postMessage",
		Headers: []*brokerproxypb.Header{{Name: "Content-Type", Values: []string{"application/json"}}},
		Body:    []byte(`{"channel":"C1"}`),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	resp := msg.GetEgressResp()
	if resp == nil || resp.CorrId != 42 || resp.Status != 200 {
		t.Fatalf("unexpected egress response: %+v", msg)
	}
}
