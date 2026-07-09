// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/socketmode"
)

// actorSink is the broker's write side of the actor-facing Socket Mode
// WebSocket. It is an interface so the session's buffering/delivery logic can be
// unit-tested without a real WebSocket.
type actorSink interface {
	// WriteFrame sends one raw Socket Mode frame to the actor.
	WriteFrame(b []byte) error
}

// helloFrame is the constant hello a Slack Socket Mode server sends on connect.
var helloFrame = mustJSON(socketmode.Envelope{Type: socketmode.TypeHello, NumConnections: 1})

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Registry owns one session per actor. A session holds that actor's persistent
// Slack connection, its captured token, and its event buffer, keyed by
// ActorRef — so each Slack connection maps to exactly one actor and inbound
// events route unambiguously.
type Registry struct {
	resumer   Resumer
	suspender Suspender
	dialer    SlackDialer
	log       *slog.Logger

	// deliverDelay holds buffered events for this long after an actor
	// (re)connects before delivering them, giving a client that reconnects with
	// churn (Bolt after a checkpoint/restore) time to settle onto a stable
	// connection first. The timer resets on each reconnect. Zero = deliver
	// immediately.
	deliverDelay time.Duration

	// idleGrace is how long the actor's broker-facing connection may be quiet in
	// both directions (no delivered event, no ack, no forwarded API call;
	// keepalive pings excluded) before the broker suspends it. Zero disables
	// broker-driven suspend.
	idleGrace time.Duration

	mu       sync.Mutex
	sessions map[ActorRef]*session
}

// NewRegistry builds a Registry.
func NewRegistry(resumer Resumer, suspender Suspender, dialer SlackDialer, deliverDelay, idleGrace time.Duration, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{
		resumer:      resumer,
		suspender:    suspender,
		dialer:       dialer,
		deliverDelay: deliverDelay,
		idleGrace:    idleGrace,
		log:          log,
		sessions:     make(map[ActorRef]*session),
	}
}

// GetOrCreate returns the session for ref, creating it if necessary.
func (r *Registry) GetOrCreate(ref ActorRef) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[ref]
	if !ok {
		s = &session{ref: ref, reg: r, freshIDs: make(map[string]uint64)}
		r.sessions[ref] = s
	}
	return s
}

// Lookup returns the existing session for ref, or nil if none exists (unlike
// GetOrCreate it never creates one — used by the passthrough to touch a session
// only if the actor is actually connected).
func (r *Registry) Lookup(ref ActorRef) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[ref]
}

// session coordinates one actor's Slack connection and message delivery.
type session struct {
	ref ActorRef
	reg *Registry

	mu       sync.Mutex
	appToken string
	started  bool
	closed   bool
	stop     chan struct{}

	sink       actorSink         // current actor-facing connection; nil when suspended/disconnected
	resuming   bool              // a resume is in flight (or the actor is booting) — don't trigger another
	buffer     []pendingEvent    // events awaiting the actor's ack; redelivered on every (re)connect
	nextKey    uint64            // per-event internal key generator
	freshIDs   map[string]uint64 // freshly minted actor-facing envelope_id -> event key
	settled    bool              // true once the post-attach settle delay has elapsed
	deliverTmr *time.Timer       // fires when the connection is considered settled

	// Broker-driven idle suspend. idleTmr fires idleGrace after the last activity
	// (delivery, ack, or forwarded API call) and suspends the actor from outside.
	// inFlight counts outstanding actor->Slack forwards (chat.postMessage) so we
	// never suspend mid-send; suspending guards against a double suspend.
	idleTmr    *time.Timer
	inFlight   int
	suspending bool

	// sinkWrite serializes writes to sink (gorilla permits one writer at a time).
	sinkWrite sync.Mutex
}

// noteActivity resets the idle-suspend timer. Called on every real actor<->broker
// exchange (event delivered, ack received, API call forwarded) — but NOT on
// keepalive pings, so a quiet-but-connected actor still ages out and suspends.
func (s *session) noteActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked()
}

// armIdleLocked (re)arms the idle timer unless the actor is disconnected, a
// suspend is already in flight, a forward is in flight, or idle suspend is off.
// Caller holds mu.
func (s *session) armIdleLocked() {
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
	if s.sink == nil || s.suspending || s.inFlight > 0 || s.reg.idleGrace <= 0 {
		return
	}
	s.idleTmr = time.AfterFunc(s.reg.idleGrace, s.onIdle)
}

// onIdle suspends the actor after idleGrace of no activity, from the OUTSIDE.
func (s *session) onIdle() {
	s.mu.Lock()
	if s.sink == nil || s.suspending || s.inFlight > 0 {
		s.mu.Unlock()
		return
	}
	s.suspending = true
	ref := s.ref
	s.mu.Unlock()

	s.reg.log.Info("egress-broker: actor idle; suspending from broker",
		slog.String("actor", ref.String()), slog.Duration("idle", s.reg.idleGrace))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.reg.suspender.Suspend(ctx, ref); err != nil {
		s.reg.log.Warn("egress-broker: suspending actor failed; will retry after next idle",
			slog.String("actor", ref.String()), slog.Any("err", err))
		s.mu.Lock()
		s.suspending = false
		s.armIdleLocked()
		s.mu.Unlock()
	}
}

// beginForward marks an actor->Slack forward in progress (chat.postMessage), so
// idle suspend holds off until it completes. endForward re-arms.
func (s *session) beginForward() {
	s.mu.Lock()
	s.inFlight++
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
	s.mu.Unlock()
}
func (s *session) endForward() {
	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.armIdleLocked()
	s.mu.Unlock()
}

// pendingEvent is a Slack event held until the actor acknowledges it. It is kept
// (not dropped on write) so it survives a client that opens several connections
// or reconnects mid-startup (e.g. Bolt): the event is redelivered on each
// attach and only removed once the actor acks it.
//
// key is an internal id. Each delivery stamps a FRESH actor-facing envelope_id
// (mapped back to key via session.freshIDs) so that a client which dedupes by
// envelope_id — Bolt does — does not silently drop a redelivery as a duplicate.
// The actor de-dupes echoes by the Slack message ts instead.
type pendingEvent struct {
	key uint64
	raw []byte // original Slack envelope (its envelope_id is rewritten per delivery)
}

// EnsureStarted records the app-level token and, on first call, starts the
// persistent Slack read loop. Called from the actor's apps.connections.open.
func (s *session) EnsureStarted(appToken string) {
	s.mu.Lock()
	s.appToken = appToken
	if s.started || s.closed {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.stop = make(chan struct{})
	stop := s.stop
	s.mu.Unlock()

	go s.runSlack(stop)
}

// runSlack maintains the persistent Slack Socket Mode connection: it (re)dials,
// reads envelopes, filters connection-management traffic, acks real events to
// Slack immediately, and hands them to the session for delivery.
func (s *session) runSlack(stop <-chan struct{}) {
	backoff := time.Second
	for {
		select {
		case <-stop:
			return
		default:
		}

		s.mu.Lock()
		token := s.appToken
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		conn, err := s.reg.dialer.Dial(ctx, token)
		cancel()
		if err != nil {
			s.reg.log.Warn("egress-broker: dialing Slack failed; will retry",
				slog.String("actor", s.ref.String()), slog.Any("err", err))
			if !sleepOrStop(stop, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		s.reg.log.Info("egress-broker: persistent Slack connection established", slog.String("actor", s.ref.String()))
		backoff = time.Second

		s.readSlackUntilClose(conn, stop)
		conn.Close()

		select {
		case <-stop:
			return
		default:
		}
	}
}

// readSlackUntilClose reads envelopes until the connection errors or stop fires.
func (s *session) readSlackUntilClose(conn SlackConn, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		env, raw, err := conn.Read()
		if err != nil {
			s.reg.log.Info("egress-broker: Slack connection closed; will reconnect",
				slog.String("actor", s.ref.String()), slog.Any("err", err))
			return
		}

		switch {
		case env.IsConnectionManagement():
			// hello / disconnect: keepalive/lifecycle traffic. Never wake the
			// actor. A disconnect asks us to reconnect, which the caller does by
			// returning; hello is informational.
			s.reg.log.Debug("egress-broker: ignoring Slack connection-management frame",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
			if env.Type == socketmode.TypeDisconnect {
				return
			}
		case env.IsEvent():
			// Ack to Slack immediately (within the ~3s window) so it is not
			// redelivered, then buffer + deliver to the actor.
			if err := conn.Ack(env.EnvelopeID); err != nil {
				s.reg.log.Warn("egress-broker: acking Slack event failed",
					slog.String("actor", s.ref.String()), slog.Any("err", err))
			}
			s.reg.log.Info("egress-broker: real Slack event received; delivering to actor",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
			s.onEvent(raw)
		default:
			s.reg.log.Debug("egress-broker: unrecognized Slack frame, ignoring",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
		}
	}
}

// onEvent buffers a real event and delivers it to the actor, resuming the actor
// first if it is suspended. The event stays buffered until the actor acks it.
func (s *session) onEvent(raw []byte) {
	frame := append([]byte(nil), raw...) // copy: gorilla reuses read buffers

	s.mu.Lock()
	pe := pendingEvent{key: s.nextKey, raw: frame}
	s.nextKey++
	s.buffer = append(s.buffer, pe)
	sink := s.sink
	needResume := sink == nil && !s.resuming
	if needResume {
		s.resuming = true // held until the actor attaches (or resume errors)
	}
	var frames [][]byte
	if sink != nil && s.settled {
		frames = [][]byte{s.stampLocked(pe)}
	}
	s.mu.Unlock()

	switch {
	case frames != nil:
		// Actor is connected and settled: deliver this new event now.
		s.writeToSink(sink, frames)
		s.noteActivity()
	case sink != nil:
		// Connected but still within the post-attach settle window: leave it
		// buffered; the settle timer will deliver it.
	case needResume:
		go s.resumeActor()
	}
}

// stampLocked rewrites an event's envelope_id to a fresh unique id, records the
// mapping (fresh id -> event key) so a later ack can be matched back, logs the
// (original, fresh) pair, and returns the frame to send. Caller holds mu.
//
// Fresh ids per delivery stop a client that dedupes by envelope_id (Bolt) from
// dropping a redelivery as a duplicate; the actor de-dupes echoes by message ts.
func (s *session) stampLocked(e pendingEvent) []byte {
	if s.freshIDs == nil {
		s.freshIDs = make(map[string]uint64)
	}
	fresh := randID()
	s.freshIDs[fresh] = e.key
	orig, _ := socketmode.DecodeEnvelope(e.raw)
	s.reg.log.Info("egress-broker: delivering event to actor",
		slog.String("actor", s.ref.String()),
		slog.String("slack_envelope_id", orig.EnvelopeID),
		slog.String("actor_envelope_id", fresh))
	return rewriteEnvelopeID(e.raw, fresh)
}

// buildEventFramesLocked returns one freshly-stamped frame per buffered event
// (no hello). Caller holds mu.
func (s *session) buildEventFramesLocked() [][]byte {
	frames := make([][]byte, 0, len(s.buffer))
	for _, e := range s.buffer {
		frames = append(frames, s.stampLocked(e))
	}
	return frames
}

// writeToSink writes frames to sink in order, under the write lock, bailing if
// the sink is no longer current or a write fails. Undelivered events remain in
// the buffer and are redelivered on the next attach.
func (s *session) writeToSink(sink actorSink, frames [][]byte) {
	s.sinkWrite.Lock()
	defer s.sinkWrite.Unlock()
	for _, f := range frames {
		s.mu.Lock()
		current := s.sink == sink
		s.mu.Unlock()
		if !current {
			return
		}
		if err := sink.WriteFrame(f); err != nil {
			s.reg.log.Warn("egress-broker: delivering to actor failed; will redeliver on reconnect",
				slog.String("actor", s.ref.String()), slog.Any("err", err))
			return
		}
	}
}

// Ack removes an event from the buffer once the actor acknowledges it. The actor
// acks the fresh, per-delivery envelope_id, which maps back to the event key.
func (s *session) Ack(actorEnvelopeID string) {
	if actorEnvelopeID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.freshIDs[actorEnvelopeID]
	if !ok {
		return
	}
	for i, e := range s.buffer {
		if e.key == key {
			s.buffer = append(s.buffer[:i], s.buffer[i+1:]...)
			break
		}
	}
	// Drop every fresh id that pointed at this (now-acked) event.
	for fid, k := range s.freshIDs {
		if k == key {
			delete(s.freshIDs, fid)
		}
	}
	s.armIdleLocked() // an ack is real actor->broker activity
}

// randID returns a short random hex id for actor-facing envelope ids.
func randID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// rewriteEnvelopeID returns raw with its top-level "envelope_id" replaced,
// preserving all other fields. Falls back to raw if it is not a JSON object.
func rewriteEnvelopeID(raw []byte, fresh string) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw
	}
	idJSON, err := json.Marshal(fresh)
	if err != nil {
		return raw
	}
	m["envelope_id"] = idJSON
	out, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return out
}

// resumeActor asks substrate to resume the actor. ResumeActor blocks until the
// actor's readyz returns 200; the actor then reconnects and Attach flushes the
// buffer. resuming stays set until Attach clears it, preventing duplicate
// resumes in the window between resume completing and the actor reconnecting.
func (s *session) resumeActor() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.reg.log.Info("egress-broker: resuming suspended actor to deliver event", slog.String("actor", s.ref.String()))
	if err := s.reg.resumer.Resume(ctx, s.ref); err != nil {
		s.reg.log.Error("egress-broker: resuming actor failed", slog.String("actor", s.ref.String()), slog.Any("err", err))
		s.mu.Lock()
		s.resuming = false // allow a later event to retry
		s.mu.Unlock()
	}
}

// Attach registers the actor's Socket Mode connection, sends the hello frame,
// and (re)delivers every buffered, un-acked event. Called when an actor
// (re)connects — including when a client like Bolt opens a fresh connection
// mid-startup, which is why delivery is redone here rather than assumed done.
func (s *session) Attach(sink actorSink) {
	s.mu.Lock()
	s.sink = sink
	s.resuming = false
	s.settled = false
	if s.deliverTmr != nil {
		s.deliverTmr.Stop()
		s.deliverTmr = nil
	}
	delay := s.reg.deliverDelay
	buffered := len(s.buffer)
	s.mu.Unlock()

	// Send hello immediately so the client can run its handshake; hold events
	// until it signals ready via its first heartbeat (see MarkReady). If there
	// is nothing buffered, mark settled now so future events flow immediately.
	s.writeToSink(sink, [][]byte{helloFrame})

	if buffered == 0 {
		// Nothing to hold; mark settled so subsequent events flow immediately.
		s.deliverBufferedNow(sink, "attach")
		return
	}
	// Hold buffered events until the client's first heartbeat (MarkReady) — its
	// connected:ready signal. delay>0 arms an optional fallback in case a client
	// never heartbeats; delay==0 means heartbeat-only (no fallback).
	s.reg.log.Info("egress-broker: holding buffered events until the client's first heartbeat",
		slog.String("actor", s.ref.String()), slog.Duration("fallback", delay), slog.Int("buffered", buffered))
	if delay > 0 {
		s.mu.Lock()
		s.deliverTmr = time.AfterFunc(delay, func() { s.deliverBufferedNow(sink, "fallback-timeout") })
		s.mu.Unlock()
	}
}

// MarkReady is called when the actor's connection proves it is established —
// its first Socket Mode heartbeat ping, which a client (Bolt) starts only once
// it reaches connected:ready. That is the real "handshake complete" signal, so
// we deliver buffered events on it instead of guessing with a timer. Called on
// every heartbeat, but delivery is idempotent per attach.
func (s *session) MarkReady(sink actorSink) {
	s.deliverBufferedNow(sink, "heartbeat")
}

// deliverBufferedNow marks the connection settled and delivers all buffered
// events (freshly stamped) to sink, if it is still the current connection and
// has not already been settled for this attach. Idempotent per attach, so it
// logs (and delivers) at most once per connection. trigger names what caused
// delivery (heartbeat / fallback / attach) for observability.
func (s *session) deliverBufferedNow(sink actorSink, trigger string) {
	s.mu.Lock()
	if s.sink != sink || s.settled {
		s.mu.Unlock()
		return
	}
	s.settled = true
	if s.deliverTmr != nil {
		s.deliverTmr.Stop()
		s.deliverTmr = nil
	}
	frames := s.buildEventFramesLocked()
	s.mu.Unlock()

	if len(frames) > 0 {
		s.reg.log.Info("egress-broker: connection established; delivering buffered events to actor",
			slog.String("actor", s.ref.String()), slog.Int("count", len(frames)), slog.String("trigger", trigger))
	}
	s.writeToSink(sink, frames)
	// Attaching/delivering is activity: start the idle countdown. A bootstrap
	// connection with nothing buffered lands here too, so an actor that just
	// opened its Slack connection with no work also ages out and gets suspended.
	s.noteActivity()
}

// Detach clears the actor connection if it is still the current one. Also fires
// when the broker suspends the actor and the checkpoint tears down its socket, so
// it resets the idle-suspend state.
func (s *session) Detach(sink actorSink) {
	s.mu.Lock()
	if s.sink == sink {
		s.sink = nil
		s.settled = false
		s.suspending = false
		s.inFlight = 0
		if s.deliverTmr != nil {
			s.deliverTmr.Stop()
			s.deliverTmr = nil
		}
		if s.idleTmr != nil {
			s.idleTmr.Stop()
			s.idleTmr = nil
		}
	}
	s.mu.Unlock()
}

// Close stops the persistent Slack loop.
func (s *session) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.stop != nil {
		close(s.stop)
	}
	s.mu.Unlock()
}

func sleepOrStop(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(d time.Duration) time.Duration {
	const max = 30 * time.Second
	d *= 2
	if d > max {
		return max
	}
	return d
}
