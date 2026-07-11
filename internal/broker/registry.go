// Package broker is the egress broker runtime: it terminates the actor's TLS to
// Slack, holds each actor's persistent Slack connection across suspend/resume,
// resumes the actor on an inbound event, and suspends it again when idle.
package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// actorSink is the write side of the actor-facing Socket Mode WebSocket, an
// interface so delivery can be tested without a real socket.
type actorSink interface {
	WriteFrame(b []byte) error
}

var helloFrame = mustJSON(slack.Envelope{Type: slack.TypeHello, NumConnections: 1})

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Registry owns one session per actor, keyed by ActorRef.
type Registry struct {
	resumer   Resumer
	suspender Suspender
	dialer    *slack.Dialer
	log       *slog.Logger

	// idleGrace bounds how long the actor's connection may be quiet (excluding
	// keepalives) before the broker suspends it; zero disables broker-driven suspend.
	idleGrace time.Duration

	mu       sync.Mutex
	sessions map[ActorRef]*session
}

func NewRegistry(resumer Resumer, suspender Suspender, dialer *slack.Dialer, idleGrace time.Duration, log *slog.Logger) *Registry {
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

func (r *Registry) GetOrCreate(ref ActorRef) *session {
	r.mu.Lock()
	s, ok := r.sessions[ref]
	if !ok {
		s = &session{ref: ref, reg: r, freshIDs: make(map[string]uint64)}
		r.sessions[ref] = s
	}
	r.mu.Unlock()
	return s
}

// Lookup returns the existing session for ref, or nil — used by the passthrough
// to touch a session only when the actor is connected.
func (r *Registry) Lookup(ref ActorRef) *session {
	r.mu.Lock()
	s := r.sessions[ref]
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

	sink     actorSink         // current actor connection; nil when suspended/disconnected
	resuming bool              // a resume is in flight; don't trigger another
	buffer   []pendingEvent    // events awaiting the actor's ack; redelivered on every (re)connect
	nextKey  uint64            // per-event internal key generator
	freshIDs map[string]uint64 // per-delivery envelope_id -> event key
	settled  bool              // true once the actor's connection has signaled ready

	idleTmr    *time.Timer
	inFlight   int // outstanding actor->Slack forwards; never suspend mid-send
	suspending bool

	sinkWrite sync.Mutex // gorilla permits one writer at a time
}

func (s *session) noteActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armIdleLocked()
}

// armIdleLocked (re)arms the idle timer unless the actor is disconnected, a
// suspend or forward is in flight, or idle suspend is off. Caller holds mu.
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

// beginForward/endForward bracket an actor->Slack forward so idle suspend holds
// off until it completes.
func (s *session) beginForward() {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// pendingEvent is a Slack event held until the actor acks it, so it survives a
// client that reconnects mid-startup (Bolt). Each delivery stamps a fresh
// envelope_id so a client that dedupes by it (Bolt) does not drop a redelivery;
// the actor de-dupes echoes by Slack message ts instead.
type pendingEvent struct {
	key uint64
	raw []byte
}

// EnsureStarted records the app token and, on first call, starts the persistent
// Slack read loop.
func (s *session) EnsureStarted(appToken string) {
	s.mu.Lock()
	s.appToken = appToken
	if s.stop != nil || s.closed {
		s.mu.Unlock()
		return
	}
	s.stop = make(chan struct{})
	stop := s.stop
	s.mu.Unlock()

	go s.runSlack(stop)
}

// runSlack maintains the persistent Slack connection: (re)dial, read, ack real
// events to Slack, and hand them to the session.
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
			// This PoC bridges only events_api. Other payload-bearing Socket Mode
			// frames (slash_commands, interactive) would be handled the same way but
			// fall through to default below, since the echo bot doesn't use them.
			//
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
			// Connection noise (connecting, errors) and unsupported payload types
			// (slash_commands, interactive — out of scope for this PoC).
			s.reg.log.Debug("egress-broker: ignoring Slack frame",
				slog.String("actor", s.ref.String()), slog.String("type", env.Type))
		}
	}
}

// onEvent buffers a real event and delivers it, resuming the actor first if it
// is suspended. The event stays buffered until acked.
func (s *session) onEvent(raw []byte) {
	frame := append([]byte(nil), raw...) // copy: gorilla reuses read buffers

	s.mu.Lock()
	pe := pendingEvent{key: s.nextKey, raw: frame}
	s.nextKey++
	s.buffer = append(s.buffer, pe)
	sink := s.sink
	needResume := sink == nil && !s.resuming
	if needResume {
		s.resuming = true
	}
	var frames [][]byte
	if sink != nil && s.settled {
		frames = [][]byte{s.stampLocked(pe)}
	}
	s.mu.Unlock()

	switch {
	case frames != nil:
		s.writeToSink(sink, frames)
		s.noteActivity()
	case sink != nil:
		// Connected but still settling; the settle path will deliver it.
	case needResume:
		go s.resumeActor()
	}
}

// stampLocked rewrites an event's envelope_id to a fresh id, records the mapping
// so a later ack matches back, and returns the frame to send. Caller holds mu.
func (s *session) stampLocked(e pendingEvent) []byte {
	if s.freshIDs == nil {
		s.freshIDs = make(map[string]uint64)
	}
	fresh := randID()
	s.freshIDs[fresh] = e.key
	orig, _ := slack.DecodeEnvelope(e.raw)
	s.reg.log.Info("egress-broker: delivering event to actor",
		slog.String("actor", s.ref.String()),
		slog.String("slack_envelope_id", orig.EnvelopeID),
		slog.String("actor_envelope_id", fresh))
	return rewriteEnvelopeID(e.raw, fresh)
}

// buildEventFramesLocked returns one freshly-stamped frame per buffered event.
// Caller holds mu.
func (s *session) buildEventFramesLocked() [][]byte {
	frames := make([][]byte, 0, len(s.buffer))
	for _, e := range s.buffer {
		frames = append(frames, s.stampLocked(e))
	}
	return frames
}

// writeToSink writes frames in order, bailing if the sink is no longer current
// or a write fails; undelivered events stay buffered for the next attach.
func (s *session) writeToSink(sink actorSink, frames [][]byte) {
	s.sinkWrite.Lock()
	defer s.sinkWrite.Unlock()
	for _, f := range frames {
		if !s.isCurrentSink(sink) {
			return
		}
		if err := sink.WriteFrame(f); err != nil {
			s.reg.log.Warn("egress-broker: delivering to actor failed; will redeliver on reconnect",
				slog.String("actor", s.ref.String()), slog.Any("err", err))
			return
		}
	}
}

// isCurrentSink reports whether sink is still the session's active connection.
func (s *session) isCurrentSink(sink actorSink) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink == sink
}

// Ack drops the acked event from the buffer. The actor acks the fresh
// per-delivery envelope_id, which maps back to the event key.
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
	for fid, k := range s.freshIDs {
		if k == key {
			delete(s.freshIDs, fid)
		}
	}
	s.armIdleLocked() // an ack is real activity
}

func randID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// rewriteEnvelopeID replaces the top-level envelope_id, preserving other fields;
// returns raw unchanged if it is not a JSON object.
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

// resumeActor asks substrate to resume the actor. Resume blocks until readyz is
// green; resuming stays set until Attach clears it, preventing duplicate resumes.
func (s *session) resumeActor() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.reg.log.Info("egress-broker: resuming suspended actor to deliver event", slog.String("actor", s.ref.String()))
	if err := s.reg.resumer.Resume(ctx, s.ref); err != nil {
		s.reg.log.Error("egress-broker: resuming actor failed", slog.String("actor", s.ref.String()), slog.Any("err", err))
		s.mu.Lock()
		s.resuming = false
		s.mu.Unlock()
	}
}

// Attach registers the actor's connection and sends hello. Buffered events are
// held until the client signals ready (MarkReady); redelivery is redone here
// because a client like Bolt opens a fresh connection mid-startup.
func (s *session) Attach(sink actorSink) {
	s.mu.Lock()
	s.sink = sink
	s.resuming = false
	s.settled = false
	buffered := len(s.buffer)
	s.mu.Unlock()

	s.writeToSink(sink, [][]byte{helloFrame})

	if buffered == 0 {
		s.deliverBufferedNow(sink, "attach") // nothing to hold; mark settled now
		return
	}
	s.reg.log.Info("egress-broker: holding buffered events until the client's first heartbeat",
		slog.String("actor", s.ref.String()), slog.Int("buffered", buffered))
}

// MarkReady delivers buffered events on the actor's first Socket Mode heartbeat —
// its connected:ready signal. Called on every heartbeat; delivery is idempotent
// per attach.
func (s *session) MarkReady(sink actorSink) {
	s.deliverBufferedNow(sink, "heartbeat")
}

// deliverBufferedNow marks the connection settled and delivers buffered events
// once per attach.
func (s *session) deliverBufferedNow(sink actorSink, trigger string) {
	s.mu.Lock()
	if s.sink != sink || s.settled {
		s.mu.Unlock()
		return
	}
	s.settled = true
	frames := s.buildEventFramesLocked()
	s.mu.Unlock()

	if len(frames) > 0 {
		s.reg.log.Info("egress-broker: connection established; delivering buffered events to actor",
			slog.String("actor", s.ref.String()), slog.Int("count", len(frames)), slog.String("trigger", trigger))
	}
	s.writeToSink(sink, frames)
	s.noteActivity() // attaching counts as activity, so an idle actor still ages out
}

// Detach clears the actor connection if it is still current, resetting idle state.
// Also fires when a suspend checkpoint tears down the socket.
func (s *session) Detach(sink actorSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sink != sink {
		return
	}
	s.sink = nil
	s.settled = false
	s.suspending = false
	s.inFlight = 0
	if s.idleTmr != nil {
		s.idleTmr.Stop()
		s.idleTmr = nil
	}
}

func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
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
