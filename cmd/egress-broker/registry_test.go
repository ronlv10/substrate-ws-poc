package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// --- test doubles ---

type fakeFrame struct {
	env slack.Envelope
	raw []byte
}

type fakeSlackConn struct {
	frames []fakeFrame
	idx    int
	acks   []string
}

func (f *fakeSlackConn) Read() (slack.Envelope, []byte, error) {
	if f.idx >= len(f.frames) {
		return slack.Envelope{}, nil, io.EOF // ends readSlackUntilClose
	}
	fr := f.frames[f.idx]
	f.idx++
	return fr.env, fr.raw, nil
}

func (f *fakeSlackConn) Ack(id string) error {
	f.acks = append(f.acks, id)
	return nil
}

func (f *fakeSlackConn) Close() error { return nil }

type fakeResumer struct {
	mu    sync.Mutex
	calls []ActorRef
	done  chan ActorRef
}

func (r *fakeResumer) Resume(_ context.Context, ref ActorRef) error {
	r.mu.Lock()
	r.calls = append(r.calls, ref)
	r.mu.Unlock()
	if r.done != nil {
		r.done <- ref
	}
	return nil
}

func (r *fakeResumer) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

type fakeSink struct {
	mu     sync.Mutex
	frames [][]byte
}

func (s *fakeSink) WriteFrame(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frames = append(s.frames, append([]byte(nil), b...))
	return nil
}

func (s *fakeSink) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	copy(out, s.frames)
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestSession(resumer Resumer) *session {
	reg := NewRegistry(resumer, nil, nil, 0, discardLogger())
	return &session{ref: ActorRef{Atespace: "demo", Name: "echo-1"}, reg: reg}
}

func eventFrame(id string) fakeFrame {
	raw := []byte(`{"type":"events_api","envelope_id":"` + id + `","payload":{"type":"event_callback"}}`)
	env, _ := slack.DecodeEnvelope(raw)
	return fakeFrame{env: env, raw: raw}
}

// --- tests ---

// Slack keepalive/lifecycle frames must never ack to Slack or wake the actor.
func TestReadSlackIgnoresConnectionManagement(t *testing.T) {
	resumer := &fakeResumer{}
	s := newTestSession(resumer)
	conn := &fakeSlackConn{frames: []fakeFrame{
		{env: slack.Envelope{Type: slack.TypeHello, NumConnections: 1}},
	}}

	s.readSlackUntilClose(conn, make(chan struct{}))

	if len(conn.acks) != 0 {
		t.Errorf("acked %v to Slack for a hello frame; want no acks", conn.acks)
	}
	if resumer.callCount() != 0 {
		t.Errorf("resumed the actor for a hello frame; want no resume")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buffer) != 0 {
		t.Errorf("buffered %d frames for a hello; want 0", len(s.buffer))
	}
}

// A real event received while the actor is suspended must be acked to Slack,
// buffered, and trigger exactly one resume.
func TestRealEventWhileSuspendedResumesActor(t *testing.T) {
	resumer := &fakeResumer{done: make(chan ActorRef, 4)}
	s := newTestSession(resumer)
	conn := &fakeSlackConn{frames: []fakeFrame{eventFrame("env-1"), eventFrame("env-2")}}

	s.readSlackUntilClose(conn, make(chan struct{}))

	// Both events acked to Slack immediately.
	if len(conn.acks) != 2 || conn.acks[0] != "env-1" || conn.acks[1] != "env-2" {
		t.Errorf("acks = %v, want [env-1 env-2]", conn.acks)
	}
	// A resume was triggered.
	select {
	case <-resumer.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the actor to be resumed")
	}
	// Both events are buffered awaiting the actor's reconnect.
	s.mu.Lock()
	bufLen := len(s.buffer)
	s.mu.Unlock()
	if bufLen != 2 {
		t.Errorf("buffered %d events, want 2", bufLen)
	}
}

// Attaching an actor sends hello and (re)delivers the buffered events in order.
// The events stay buffered until acked, so they survive a reconnect.
func TestAttachRedeliversBufferedEventsUntilAcked(t *testing.T) {
	resumer := &fakeResumer{done: make(chan ActorRef, 4)}
	s := newTestSession(resumer)
	conn := &fakeSlackConn{frames: []fakeFrame{eventFrame("env-1"), eventFrame("env-2")}}
	s.readSlackUntilClose(conn, make(chan struct{}))
	<-resumer.done // let the resume goroutine run

	sink := &fakeSink{}
	s.Attach(sink)

	// Attach sends only hello; events are held until the client signals ready.
	if got := sink.snapshot(); len(got) != 1 {
		t.Fatalf("after attach (pre-heartbeat) actor received %d frames, want 1 (hello only)", len(got))
	}
	// The first heartbeat releases the buffered events.
	s.MarkReady(sink)

	frames := sink.snapshot()
	if len(frames) != 3 {
		t.Fatalf("actor received %d frames, want 3 (hello + 2 events)", len(frames))
	}
	if got, _ := slack.DecodeEnvelope(frames[0]); got.Type != slack.TypeHello {
		t.Errorf("first frame type = %q, want hello", got.Type)
	}
	// Delivered events carry FRESH, distinct envelope ids (not Slack's originals),
	// so a client that dedupes by envelope_id won't drop a redelivery.
	e1, _ := slack.DecodeEnvelope(frames[1])
	e2, _ := slack.DecodeEnvelope(frames[2])
	if e1.Type != slack.TypeEventsAPI || e2.Type != slack.TypeEventsAPI {
		t.Errorf("delivered frames are not both events: %q, %q", e1.Type, e2.Type)
	}
	if e1.EnvelopeID == "" || e2.EnvelopeID == "" || e1.EnvelopeID == e2.EnvelopeID {
		t.Errorf("expected two distinct fresh envelope ids, got %q and %q", e1.EnvelopeID, e2.EnvelopeID)
	}
	if e1.EnvelopeID == "env-1" || e2.EnvelopeID == "env-2" {
		t.Errorf("envelope ids should be freshly minted, not Slack's originals")
	}

	// resuming cleared on attach; events remain buffered (un-acked).
	s.mu.Lock()
	if s.resuming {
		t.Error("resuming flag still set after attach")
	}
	if len(s.buffer) != 2 {
		t.Errorf("buffer has %d events, want 2 still pending ack", len(s.buffer))
	}
	s.mu.Unlock()

	// A reconnect (new sink) redelivers the still-un-acked events (on its
	// heartbeat) with NEW fresh ids.
	sink2 := &fakeSink{}
	s.Attach(sink2)
	s.MarkReady(sink2)
	redelivered := sink2.snapshot()
	if len(redelivered) != 3 {
		t.Fatalf("reconnect redelivered %d frames, want 3 (hello + 2 unacked events)", len(redelivered))
	}

	// Acking the fresh ids from the latest delivery clears the buffer.
	r1, _ := slack.DecodeEnvelope(redelivered[1])
	r2, _ := slack.DecodeEnvelope(redelivered[2])
	s.Ack(r1.EnvelopeID)
	s.Ack(r2.EnvelopeID)
	s.mu.Lock()
	if len(s.buffer) != 0 {
		t.Errorf("buffer has %d events after acking both, want 0", len(s.buffer))
	}
	s.mu.Unlock()

	sink3 := &fakeSink{}
	s.Attach(sink3)
	s.MarkReady(sink3)
	if got := sink3.snapshot(); len(got) != 1 {
		t.Errorf("after acks, attach+ready delivered %d frames, want 1 (hello only)", len(got))
	}
}

// An event that arrives while the actor is already attached is delivered
// immediately without a resume.
func TestEventWhileAttachedDeliversWithoutResume(t *testing.T) {
	resumer := &fakeResumer{}
	s := newTestSession(resumer)
	sink := &fakeSink{}
	s.Attach(sink) // hello only

	conn := &fakeSlackConn{frames: []fakeFrame{eventFrame("env-9")}}
	s.readSlackUntilClose(conn, make(chan struct{}))

	if resumer.callCount() != 0 {
		t.Errorf("resumed the actor though it was attached; want no resume")
	}
	frames := sink.snapshot()
	// hello + the event (delivered with a fresh envelope id).
	if len(frames) != 2 {
		t.Fatalf("actor received %d frames, want 2 (hello + event)", len(frames))
	}
	e, _ := slack.DecodeEnvelope(frames[1])
	if e.Type != slack.TypeEventsAPI {
		t.Errorf("delivered frame type = %q, want an event", e.Type)
	}
	if e.EnvelopeID == "" || e.EnvelopeID == "env-9" {
		t.Errorf("delivered envelope_id = %q, want a freshly minted id", e.EnvelopeID)
	}
}

// Registry keys sessions per actor.
func TestRegistryGetOrCreateIsPerActor(t *testing.T) {
	reg := NewRegistry(&fakeResumer{}, nil, nil, 0, discardLogger())
	a1 := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	a1b := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	a2 := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-2"})
	if a1 != a1b {
		t.Error("GetOrCreate returned different sessions for the same actor")
	}
	if a1 == a2 {
		t.Error("GetOrCreate returned the same session for different actors")
	}
}
