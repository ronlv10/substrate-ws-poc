// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package proxy is the local egress proxy that runs INSIDE the actor image,
// co-resident with the agent. It impersonates Slack for its single loopback
// client (a stock Bolt Socket Mode app) and relays events and Web API calls
// to the always-on egress broker.
//
// Delivery contract, established by the phase-0 spikes: the loopback socket
// survives checkpoint/restore, but the stock Slack client still reconnects
// after every restore (its pong-staleness check uses the wall clock, which
// jumps). An event written during that churn window is silently dropped, so
// the proxy buffers every event until the agent acks it and only writes when
// the CURRENT connection has proven itself with a heartbeat ping. Undelivered
// events are re-sent, with their original envelope ids, after each re-attach.
package proxy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/ronlv10/substrate-ws-poc/internal/socketmode"
)

// EgressResult is the broker's (or standalone stub's) answer to an agent Web
// API call.
type EgressResult struct {
	Status int
	Header http.Header
	Body   []byte
}

// EgressFunc relays an agent Web API request. Implementations: the broker
// client (relay over the Session stream) or the standalone stub.
type EgressFunc func(method, path string, header http.Header, body []byte) (*EgressResult, error)

// Core owns the agent-facing connection state and the undelivered-event
// buffer. One Core serves exactly one co-resident agent.
type Core struct {
	log    *slog.Logger
	egress EgressFunc
	// onAck reports an agent ack upstream (broker Ack{seq}); nil in standalone.
	onAck func(seq uint64)

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

// SetEgress and SetOnAck wire the upstream side (broker client or standalone).
func (c *Core) SetEgress(f EgressFunc)     { c.egress = f }
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

// AgentAttached reports whether an agent WS is currently attached; it drives
// /readyz. Gating readiness on attachment does double duty: ResumeActor's
// gate waits out the local reconnect churn, and the golden checkpoint cannot
// fire while the agent is mid-startup (a Node process frozen mid-V8-boot
// SIGILLs on restore).
func (c *Core) AgentAttached() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.agent != nil
}

// LastContiguousAcked is the resume point for the broker Announce: every
// event with seq <= this value has been acked by the agent.
func (c *Core) LastContiguousAcked() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) > 0 {
		return c.pending[0].seq - 1
	}
	return c.maxAcked
}

// Deliver buffers one event (raw Socket Mode envelope, original envelope_id)
// and writes it immediately if the current agent connection is heartbeat-ready.
func (c *Core) Deliver(seq uint64, raw []byte) error {
	env, err := socketmode.DecodeEnvelope(raw)
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

// Attach installs a freshly upgraded agent connection. Delivery stays held
// until MarkReady (the first heartbeat ping on THIS connection).
func (c *Core) Attach(conn *websocket.Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent != nil {
		// The stock client reconnects after every restore; the replaced
		// connection is the dead-to-it survivor. Expected, not an error.
		c.log.Info("local-proxy: agent reconnected, replacing previous connection")
		_ = c.agent.Close()
	}
	c.agent = conn
	c.agentSeq++
	c.ready = false
	return c.agentSeq
}

// Detach clears conn if it is still current (read loop ended).
func (c *Core) Detach(conn *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.agent == conn {
		c.agent = nil
		c.ready = false
	}
}

// MarkReady flushes the buffer on the first heartbeat of the given attach.
// Bolt reaches connected:ready and starts heartbeating only after it is
// dispatching; anything written earlier can land in the churn window and be
// silently dropped (observed in the phase-0 spike).
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

// OnAgentAck consumes an ack frame from the agent: drop the buffered event
// and report it upstream.
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

// writeLocked writes one frame to the agent; callers hold c.mu (which also
// serializes writers, satisfying gorilla's one-writer rule; control frames
// use WriteControl, which is safe concurrently).
func (c *Core) writeLocked(conn *websocket.Conn, raw []byte, seq uint64) error {
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		return err
	}
	c.log.Info("local-proxy: event delivered to agent", slog.Uint64("seq", seq))
	return nil
}

// decodeAck extracts the envelope_id from an agent frame; non-ack frames
// (empty id) are ignored by the caller.
func decodeAck(raw []byte) string {
	var a struct {
		EnvelopeID string `json:"envelope_id"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return ""
	}
	return a.EnvelopeID
}
