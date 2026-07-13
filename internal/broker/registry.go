// Package broker is the egress broker runtime: it holds each actor's persistent
// Slack connection across suspend/resume, serves the gRPC session protocol to
// the actor's local proxy, resumes the actor on an inbound event, and suspends
// it again when idle.
package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// eventSink is the write side toward an actor's local proxy (one Session
// stream), an interface so delivery can be tested without gRPC.
type eventSink interface {
	SendEvent(seq uint64, raw []byte) error
}

// Registry owns one session per actor, keyed by ActorRef.
type Registry struct {
	resumer   Resumer
	suspender Suspender
	dialer    *slack.Dialer
	log       *slog.Logger

	// idleGrace bounds how long the actor's stream may be quiet (excluding
	// keepalives) before the broker suspends it; zero disables broker-driven suspend.
	idleGrace time.Duration

	// handlingGrace bounds how long the broker holds idle-suspend after the actor
	// acks an inbound event, giving the agent time to produce its reply. Socket
	// Mode acks on receipt, long before the agent responds, so without this the
	// broker would checkpoint the actor mid-handle; zero disables the hold.
	handlingGrace time.Duration

	mu       sync.Mutex
	sessions map[ActorRef]*session
}

func NewRegistry(resumer Resumer, suspender Suspender, dialer *slack.Dialer, idleGrace, handlingGrace time.Duration, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{
		resumer:       resumer,
		suspender:     suspender,
		dialer:        dialer,
		idleGrace:     idleGrace,
		handlingGrace: handlingGrace,
		log:           log,
		sessions:      make(map[ActorRef]*session),
	}
}

func (r *Registry) GetOrCreate(ref ActorRef) *session {
	r.mu.Lock()
	s, ok := r.sessions[ref]
	if !ok {
		s = &session{ref: ref, reg: r, nextSeq: 1}
		r.sessions[ref] = s
	}
	r.mu.Unlock()
	return s
}

// session coordinates one actor's Slack connection and message delivery.
type session struct {
	ref ActorRef
	reg *Registry

	mu       sync.Mutex
	appToken string
	closed   bool
	stop     chan struct{} // non-nil once the persistent Slack loop is started

	sink     eventSink      // current proxy stream; nil when suspended/disconnected
	resuming bool           // a resume is in flight; don't trigger another
	buffer   []pendingEvent // events awaiting the proxy's ack, in seq order
	nextSeq  uint64

	idleTmr    *time.Timer
	inFlight   int // outstanding actor->Slack relays; never suspend mid-send
	suspending bool

	// handling marks the actor as processing an event it acked receipt of but has
	// not yet replied to. It bridges the gap between Socket Mode's on-receipt ack
	// and the agent's much-later reply; released when the agent starts relaying
	// its reply or handlingGrace elapses.
	handling    bool
	handlingTmr *time.Timer

	sinkWrite sync.Mutex // serializes deliveries so flushes and new events stay ordered
}

// pendingEvent is a Slack event held until the actor's proxy acks it (which it
// does only once the agent acked end-to-end). The envelope is delivered
// verbatim — the proxy owns redelivery timing, and the agent de-dupes by ts.
type pendingEvent struct {
	seq uint64
	raw []byte
}

func (s *session) noteActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked()
}

// armIdleLocked (re)arms the idle timer unless the actor is disconnected, a
// suspend or relay is in flight, an event is still unacked, or idle suspend is
// off. Suspending with a buffered event would checkpoint the actor mid-handle
// and redeliver on the next resume — a loop if the handler is slower than the
// idle grace. Caller holds mu.
func (s *session) armIdleLocked() {
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
	if s.sink == nil || s.suspending || s.inFlight > 0 || len(s.buffer) > 0 || s.handling || s.reg.idleGrace <= 0 {
		return
	}
	s.idleTmr = time.AfterFunc(s.reg.idleGrace, s.onIdle)
}

func (s *session) onIdle() {
	s.mu.Lock()
	if s.sink == nil || s.suspending || s.inFlight > 0 || len(s.buffer) > 0 || s.handling {
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

// beginForward/endForward bracket an actor->Slack relay so idle suspend holds
// off until it completes.
func (s *session) beginForward() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Do NOT clear the handling hold here: the agent's first egress is often an
	// early Slack call (auth.test, conversations.info) made before it thinks, not
	// the reply. Releasing on it would let the actor suspend during the model
	// call that follows — the reply produces no egress to hold on. The hold runs
	// its full grace instead; inFlight additionally holds across each relay.
	s.inFlight++
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
}
func (s *session) endForward() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.armIdleLocked()
}

// beginHandlingLocked starts (or extends) the processing hold after the actor
// acks an inbound event, so idle-suspend waits for the agent's reply rather than
// firing during Socket Mode's ack-before-handle gap. Caller holds mu.
func (s *session) beginHandlingLocked() {
	if s.reg.handlingGrace <= 0 {
		return
	}
	s.handling = true
	if s.handlingTmr != nil {
		s.handlingTmr.Stop()
	}
	s.handlingTmr = time.AfterFunc(s.reg.handlingGrace, s.onHandlingExpire)
}

// clearHandlingLocked releases the processing hold. Caller holds mu.
func (s *session) clearHandlingLocked() {
	s.handling = false
	if s.handlingTmr != nil {
		s.handlingTmr.Stop()
		s.handlingTmr = nil
	}
}

// onHandlingExpire caps the processing hold so an event the agent never replies
// to (e.g. an ignored message) still lets the actor suspend.
func (s *session) onHandlingExpire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.handling {
		return
	}
	s.handling = false
	s.handlingTmr = nil
	s.armIdleLocked()
}

// EnsureStarted records the app token and, on first call, starts the persistent
// Slack read loop.
func (s *session) EnsureStarted(appToken string) {
	s.mu.Lock()
	if appToken != "" {
		s.appToken = appToken
	}
	if s.stop != nil || s.closed {
		s.mu.Unlock()
		return
	}
	s.stop = make(chan struct{})
	stop := s.stop
	s.mu.Unlock()

	go s.runSlack(stop)
}

// runSlack maintains the persistent Slack connection: (re)dial, read, buffer +
// ack real events, hand them to the session.
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

		conn, err := s.reg.dialer.Dial(token)
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

		started := time.Now()
		s.readSlackUntilClose(conn, stop)
		conn.Close()

		select {
		case <-stop:
			return
		default:
		}

		// slack-go connects asynchronously, so a bad token or unreachable Slack
		// surfaces as a short-lived connection, not a dial error. Back off on
		// those too.
		if time.Since(started) < 30*time.Second {
			if !sleepOrStop(stop, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
		} else {
			backoff = time.Second
		}
	}
}

func (s *session) readSlackUntilClose(conn slack.Conn, stop <-chan struct{}) {
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
		case env.Type == slack.TypeHello || env.Type == slack.TypeDisconnect:
			// Lifecycle traffic that must never wake the actor; slack-go reconnects.
			s.reg.log.Debug("egress-broker: ignoring Slack connection-management frame",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
		case env.Type == slack.TypeEventsAPI:
			// Buffer first, then ack: only tell Slack we have the event once it is
			// captured. Buffering is a fast in-memory append (resuming the actor is
			// async), so we still ack well within Slack's ~3s redelivery window.
			s.reg.log.Info("egress-broker: real Slack event received; delivering to actor",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
			s.onEvent(raw)
			if err := conn.Ack(env.EnvelopeID); err != nil {
				s.reg.log.Warn("egress-broker: acking Slack event failed",
					slog.String("actor", s.ref.String()), slog.Any("err", err))
			}
		default:
			// Connection noise and unsupported payload types (slash_commands,
			// interactive — out of scope for this PoC).
			s.reg.log.Debug("egress-broker: ignoring Slack frame",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
		}
	}
}

// onEvent buffers a real event and delivers it, resuming the actor first if it
// is suspended. The event stays buffered until acked.
func (s *session) onEvent(raw []byte) {
	frame := append([]byte(nil), raw...) // copy: the reader may reuse buffers

	s.mu.Lock()
	pe := pendingEvent{seq: s.nextSeq, raw: frame}
	s.nextSeq++
	s.buffer = append(s.buffer, pe)
	sink := s.sink
	needResume := sink == nil && !s.resuming
	if needResume {
		s.resuming = true
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

// writeToSink delivers events in order, bailing if the sink is no longer current
// or a send fails; undelivered events stay buffered for the next Attach.
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

// Ack drops a buffered event once the proxy reports the agent handled it.
func (s *session) Ack(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.buffer {
		if e.seq == seq {
			s.buffer = append(s.buffer[:i], s.buffer[i+1:]...)
			// Acking is receipt, not completion: hold suspend until the agent
			// replies or the processing grace elapses.
			s.beginHandlingLocked()
			break
		}
	}
	s.armIdleLocked() // an ack is real actor->broker activity
}

// resumeActor asks substrate to resume the actor; Resume blocks until readyz is
// 200, which the proxy answers only once its agent is settled. resuming stays
// set until Attach clears it, preventing duplicate resumes in the window between
// resume completing and the proxy reconnecting.
func (s *session) resumeActor() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	s.reg.log.Info("egress-broker: resuming suspended actor to deliver event", slog.String("actor", s.ref.String()))
	if err := s.reg.resumer.Resume(ctx, s.ref); err != nil {
		s.reg.log.Error("egress-broker: resuming actor failed", slog.String("actor", s.ref.String()), slog.Any("err", err))
		s.mu.Lock()
		s.resuming = false // allow a later event to retry
		s.mu.Unlock()
	}
}

// Attach registers the proxy's stream, drops everything the proxy already acked
// (lastAcked, from its Announce), and re-sends the rest in order. Delivery is
// immediate: the proxy holds events locally until its agent is ready.
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
	// Attaching is activity: an actor that woke with no work also ages out.
	s.noteActivity()
}

// Detach clears the proxy stream if it is still the current one. Also fires when
// a suspend checkpoint tears the stream down, so it resets idle-suspend state.
func (s *session) Detach(sink eventSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sink != sink {
		return
	}
	s.sink = nil
	s.suspending = false
	s.inFlight = 0
	s.clearHandlingLocked()
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
}

// Close stops the persistent Slack loop.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.clearHandlingLocked()
	if s.stop != nil {
		close(s.stop)
	}
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
