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
	"log/slog"
	"sync"
	"time"
)

// eventSink is the broker's write side toward an actor's local proxy — one
// Session stream. An interface so the session's buffering/delivery logic is
// unit-testable without gRPC.
type eventSink interface {
	// SendEvent delivers one buffered event (raw Slack envelope, original
	// envelope_id) with its sequence number.
	SendEvent(seq uint64, raw []byte) error
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

	// idleGrace is how long the actor's proxy stream may be quiet in both
	// directions (no delivered event, no ack, no relayed API call; gRPC
	// keepalives excluded) before the broker suspends the actor. Zero disables
	// broker-driven suspend.
	idleGrace time.Duration

	mu       sync.Mutex
	sessions map[ActorRef]*session
}

// NewRegistry builds a Registry.
func NewRegistry(resumer Resumer, suspender Suspender, dialer SlackDialer, idleGrace time.Duration, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{
		resumer:   resumer,
		suspender: suspender,
		dialer:    dialer,
		idleGrace: idleGrace,
		log:       log,
		sessions:  make(map[ActorRef]*session),
	}
}

// GetOrCreate returns the session for ref, creating it if necessary.
func (r *Registry) GetOrCreate(ref ActorRef) *session {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[ref]
	if !ok {
		s = &session{ref: ref, reg: r, nextSeq: 1}
		r.sessions[ref] = s
	}
	return s
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

	sink     eventSink      // current proxy stream; nil when suspended/disconnected
	resuming bool           // a resume is in flight (or the actor is booting) — don't trigger another
	buffer   []pendingEvent // events awaiting the proxy's ack, in seq order
	nextSeq  uint64

	// Broker-driven idle suspend. idleTmr fires idleGrace after the last activity
	// (delivery, ack, or relayed API call) and suspends the actor from outside.
	// inFlight counts outstanding actor->Slack relays (chat.postMessage) so we
	// never suspend mid-send; suspending guards against a double suspend.
	idleTmr    *time.Timer
	inFlight   int
	suspending bool

	// sinkWrite serializes deliveries so re-attach flushes and new events never
	// interleave out of order.
	sinkWrite sync.Mutex
}

// pendingEvent is a Slack event held until the actor's proxy acknowledges it
// (which the proxy does only once the agent acked it end-to-end). The envelope
// is delivered verbatim — original envelope_id — because the proxy owns
// redelivery timing; the agent additionally de-dupes by message ts.
type pendingEvent struct {
	seq uint64
	raw []byte
}

// noteActivity resets the idle-suspend timer. Called on every real
// actor<->broker exchange (event delivered, ack received, API call relayed) —
// but NOT on keepalives, so a quiet-but-connected actor still ages out.
func (s *session) noteActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked()
}

// armIdleLocked (re)arms the idle timer unless the actor is disconnected, a
// suspend is already in flight, a relay is in flight, or idle suspend is off.
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

// beginForward marks an actor->Slack relay in progress (chat.postMessage), so
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

// EnsureStarted records the app-level token and, on first call, starts the
// persistent Slack read loop. Called from the proxy's Announce.
func (s *session) EnsureStarted(appToken string) {
	s.mu.Lock()
	if appToken != "" {
		s.appToken = appToken
	}
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
			// hello / disconnect: lifecycle traffic that must never wake the
			// actor. slack-go reconnects on its own, so just drop it.
			s.reg.log.Debug("egress-broker: ignoring Slack connection-management frame",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
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

// onEvent buffers a real event and delivers it to the actor's proxy, resuming
// the actor first if it is suspended. The event stays buffered until acked.
func (s *session) onEvent(raw []byte) {
	frame := append([]byte(nil), raw...) // copy: the reader may reuse buffers

	s.mu.Lock()
	pe := pendingEvent{seq: s.nextSeq, raw: frame}
	s.nextSeq++
	s.buffer = append(s.buffer, pe)
	sink := s.sink
	needResume := sink == nil && !s.resuming
	if needResume {
		s.resuming = true // held until the proxy attaches (or resume errors)
	}
	s.mu.Unlock()

	switch {
	case sink != nil:
		s.writeToSink(sink, []pendingEvent{pe})
		s.noteActivity()
	case needResume:
		go s.resumeActor()
	}
}

// writeToSink delivers events in order, bailing if the sink is no longer
// current or a send fails. Undelivered events remain in the buffer and are
// re-sent on the next Attach.
func (s *session) writeToSink(sink eventSink, events []pendingEvent) {
	s.sinkWrite.Lock()
	defer s.sinkWrite.Unlock()
	for _, e := range events {
		s.mu.Lock()
		current := s.sink == sink
		s.mu.Unlock()
		if !current {
			return
		}
		if err := sink.SendEvent(e.seq, e.raw); err != nil {
			s.reg.log.Warn("egress-broker: delivering to proxy failed; will re-send on reconnect",
				slog.String("actor", s.ref.String()), slog.Uint64("seq", e.seq), slog.Any("err", err))
			return
		}
	}
}

// Ack removes an event from the buffer once the proxy reports the agent
// handled it.
func (s *session) Ack(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.buffer {
		if e.seq == seq {
			s.buffer = append(s.buffer[:i], s.buffer[i+1:]...)
			break
		}
	}
	s.armIdleLocked() // an ack is real actor->broker activity
}

// resumeActor asks substrate to resume the actor. ResumeActor blocks until the
// actor's readyz returns 200 — which the proxy answers only once its agent is
// attached — and the proxy then redials and announces, which flushes the
// buffer via Attach. resuming stays set until Attach clears it, preventing
// duplicate resumes in the window between resume completing and the proxy
// reconnecting.
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

// Attach registers the proxy's stream, drops everything the proxy already
// acked (lastAcked, from its Announce), and re-sends the remaining buffer in
// order. Delivery is immediate: the proxy holds events locally until its agent
// heartbeats, so the broker no longer second-guesses client readiness.
func (s *session) Attach(sink eventSink, lastAcked uint64) {
	s.mu.Lock()
	kept := s.buffer[:0]
	for _, e := range s.buffer {
		if e.seq > lastAcked {
			kept = append(kept, e)
		}
	}
	s.buffer = kept
	s.sink = sink
	s.resuming = false
	pending := append([]pendingEvent(nil), s.buffer...)
	s.mu.Unlock()

	if len(pending) > 0 {
		s.reg.log.Info("egress-broker: proxy attached; re-sending unacked events",
			slog.String("actor", s.ref.String()), slog.Int("count", len(pending)),
			slog.Uint64("last_acked_seq", lastAcked))
	}
	s.writeToSink(sink, pending)
	// Attaching is activity: start the idle countdown, so an actor that woke
	// with no work also ages out and suspends again.
	s.noteActivity()
}

// Detach clears the proxy stream if it is still the current one. Also fires
// when the broker suspends the actor and the checkpoint tears down the stream,
// so it resets the idle-suspend state.
func (s *session) Detach(sink eventSink) {
	s.mu.Lock()
	if s.sink == sink {
		s.sink = nil
		s.suspending = false
		s.inFlight = 0
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
