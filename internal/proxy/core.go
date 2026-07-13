// Package proxy is the local egress proxy that runs inside the actor image,
// co-resident with the agent. It impersonates Slack for its single loopback
// client (a stock Bolt Socket Mode app) and relays events and Web API calls to
// the always-on egress broker.
//
// Delivery contract: the loopback socket survives checkpoint/restore, but the
// stock Slack client still reconnects after every restore (its pong-staleness
// check uses the wall clock, which jumps). An event written during that churn
// window is silently dropped, so the proxy buffers every event until the agent
// acks it and only writes once the current connection has proven itself with a
// heartbeat ping. Unacked events are re-sent, with their original envelope
// ids, after each re-attach.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// EgressResult is the broker's (or standalone stub's) answer to an agent Web
// API call.
type EgressResult struct {
	Status int
	Header http.Header
	Body   []byte
}

// EgressFunc relays an agent Web API request upstream.
type EgressFunc func(method, path string, header http.Header, body []byte) (*EgressResult, error)

// Core owns the agent-facing connection state and the unacked-event buffer.
// One Core serves exactly one co-resident agent.
type Core struct {
	log    *slog.Logger
	egress EgressFunc
	onAck  func(seq uint64) // reports an agent ack upstream; nil in standalone

	mu       sync.Mutex
	appToken string
	agent    *websocket.Conn
	agentSeq int  // attach counter, labels log lines
	ready    bool // current attach has heartbeated
	pending  []pendingEvent
	seqByID  map[string]uint64
	maxAcked uint64
}

type pendingEvent struct {
	seq uint64
	raw []byte
}

func NewCore(log *slog.Logger) *Core {
	return &Core{log: log, seqByID: map[string]uint64{}}
}

func (c *Core) SetEgress(f EgressFunc)      { c.egress = f }
func (c *Core) SetOnAck(f func(seq uint64)) { c.onAck = f }

// SetAppToken records the token captured from apps.connections.open.
func (c *Core) SetAppToken(tok string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tok != "" && tok != c.appToken {
		c.appToken = tok
		c.log.Info("local-proxy: captured app token")
	}
}

func (c *Core) AppToken() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appToken
}

// AgentQuiescent drives /readyz: the agent is attached and has heartbeated on
// the current connection. Readiness gates checkpoints (the golden snapshot and
// ResumeActor's wait both fire on it), and a Node process frozen mid-startup
// SIGILLs on restore — the first heartbeat only happens well past that.
// In-flight Web API calls deliberately do not block readiness: a retrying
// client would hold it hostage and deadlock golden-snapshot creation.
func (c *Core) AgentQuiescent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent != nil && c.ready
}

// WaitQuiescent blocks until AgentQuiescent holds or ctx is done. The broker
// announce waits on this: the broker starts its idle-suspend clock when the
// proxy announces, so announcing before the agent has settled would let the
// broker checkpoint a still-starting Node process, which SIGILLs on restore.
func (c *Core) WaitQuiescent(ctx context.Context) error {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		if c.AgentQuiescent() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// LastContiguousAcked is the resume point for the broker Announce: every event
// with seq <= this value has been acked by the agent.
func (c *Core) LastContiguousAcked() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) > 0 {
		return c.pending[0].seq - 1
	}
	return c.maxAcked
}

// Deliver buffers one event (raw Socket Mode envelope) and writes it
// immediately if the current agent connection is heartbeat-ready.
func (c *Core) Deliver(seq uint64, raw []byte) error {
	env, err := slack.DecodeEnvelope(raw)
	if err != nil {
		return fmt.Errorf("local-proxy: undeliverable event seq=%d: %w", seq, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, pendingEvent{seq: seq, raw: raw})
	c.seqByID[env.EnvelopeID] = seq
	if c.agent != nil && c.ready {
		return c.writeLocked(c.agent, raw, seq)
	}
	c.log.Info("local-proxy: event buffered until agent heartbeat",
		slog.Uint64("seq", seq), slog.Bool("attached", c.agent != nil))
	return nil
}

// Attach installs a freshly upgraded agent connection; delivery stays held
// until MarkReady.
func (c *Core) Attach(conn *websocket.Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != nil {
		// Expected: the stock client reconnects after every restore.
		c.log.Info("local-proxy: agent reconnected, replacing previous connection")
		_ = c.agent.Close()
	}
	c.agent = conn
	c.agentSeq++
	c.ready = false
	return c.agentSeq
}

// Detach clears conn if it is still current.
func (c *Core) Detach(conn *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent == conn {
		c.agent = nil
		c.ready = false
	}
}

// MarkReady flushes the buffer on the first heartbeat of the given attach.
// Bolt starts heartbeating only once it is dispatching; anything written
// earlier can land in the reconnect churn and be silently dropped.
func (c *Core) MarkReady(conn *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != conn || c.ready {
		return
	}
	c.ready = true
	if len(c.pending) > 0 {
		c.log.Info("local-proxy: agent heartbeat, flushing buffered events",
			slog.Int("count", len(c.pending)))
	}
	for _, p := range c.pending {
		if err := c.writeLocked(conn, p.raw, p.seq); err != nil {
			c.log.Warn("local-proxy: flush failed", slog.Uint64("seq", p.seq), slog.Any("error", err))
			return
		}
	}
}

// OnAgentAck drops the buffered event and reports it upstream.
func (c *Core) OnAgentAck(envelopeID string) {
	c.mu.Lock()
	seq, ok := c.seqByID[envelopeID]
	if !ok {
		c.mu.Unlock()
		c.log.Warn("local-proxy: ack for unknown envelope", slog.String("envelope_id", envelopeID))
		return
	}
	delete(c.seqByID, envelopeID)
	for i, p := range c.pending {
		if p.seq == seq {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			break
		}
	}
	if seq > c.maxAcked {
		c.maxAcked = seq
	}
	onAck := c.onAck
	c.mu.Unlock()

	c.log.Info("local-proxy: agent acked event", slog.Uint64("seq", seq))
	if onAck != nil {
		onAck(seq)
	}
}

// Egress relays an agent Web API call via the configured upstream.
func (c *Core) Egress(method, path string, header http.Header, body []byte) (*EgressResult, error) {
	if c.egress == nil {
		return nil, fmt.Errorf("local-proxy: no egress path configured")
	}
	return c.egress(method, path, header, body)
}

// writeLocked writes one frame to the agent. Callers hold c.mu, which also
// serializes writers (gorilla permits one at a time; control frames use
// WriteControl, which is concurrency-safe).
func (c *Core) writeLocked(conn *websocket.Conn, raw []byte, seq uint64) error {
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		return err
	}
	c.log.Info("local-proxy: event delivered to agent", slog.Uint64("seq", seq))
	return nil
}

// decodeAck extracts the envelope_id from an agent frame; non-ack frames yield "".
func decodeAck(raw []byte) string {
	var a struct {
		EnvelopeID string `json:"envelope_id"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return ""
	}
	return a.EnvelopeID
}
