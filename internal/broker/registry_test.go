package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// --- test doubles ---

// fakeSlackConn feeds scripted frames to readSlackUntilClose.
type fakeSlackConn struct {
	frames chan []byte
	acked  chan string
}

func newFakeSlackConn() *fakeSlackConn {
	return &fakeSlackConn{frames: make(chan []byte, 16), acked: make(chan string, 16)}
}

func (f *fakeSlackConn) Read() (slack.Envelope, []byte, error) {
	raw, ok := <-f.frames
	if !ok {
		return slack.Envelope{}, nil, io.EOF
	}
	env, err := slack.DecodeEnvelope(raw)
	return env, raw, err
}
func (f *fakeSlackConn) Ack(id string) error { f.acked <- id; return nil }
func (f *fakeSlackConn) Close() error        { return nil }

type fakeResumer struct {
	mu    sync.Mutex
	count int
}

func (r *fakeResumer) Resume(ctx context.Context, ref ActorRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return nil
}
func (r *fakeResumer) resumes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

type fakeSuspender struct {
	mu    sync.Mutex
	count int
}

func (s *fakeSuspender) Suspend(ctx context.Context, ref ActorRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return nil
}
func (s *fakeSuspender) suspends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// fakeSink records delivered seqs.
type fakeSink struct {
	mu     sync.Mutex
	events []uint64
	fail   bool
}

func (f *fakeSink) SendEvent(seq uint64, raw []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return fmt.Errorf("sink failed")
	}
	f.events = append(f.events, seq)
	return nil
}
func (f *fakeSink) delivered() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.events...)
}

func eventFrame(id string) []byte {
	return []byte(fmt.Sprintf(`{"type":"events_api","envelope_id":%q,"payload":{"type":"event_callback"}}`, id))
}

// newTestSession wires a session and drives its Slack read loop from a fake
// connection (the real loop is started by EnsureStarted, which needs a dialer).
func newTestSession(t *testing.T, idleGrace time.Duration) (*session, *fakeSlackConn, *fakeResumer, *fakeSuspender) {
	t.Helper()
	conn := newFakeSlackConn()
	resumer := &fakeResumer{}
	suspender := &fakeSuspender{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// handlingGrace disabled here so the existing idle/ack tests keep their
	// ack-then-suspend semantics; the hold has its own test below.
	reg := NewRegistry(resumer, suspender, nil, idleGrace, 0, log)
	s := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	stop := make(chan struct{})
	go s.readSlackUntilClose(conn, stop)
	t.Cleanup(func() { close(stop); close(conn.frames) })
	return s, conn, resumer, suspender
}

// newHandlingSession builds a session with both idle and handling grace set, to
// exercise the post-ack processing hold.
func newHandlingSession(t *testing.T, idleGrace, handlingGrace time.Duration) (*session, *fakeSlackConn, *fakeSuspender) {
	t.Helper()
	conn := newFakeSlackConn()
	suspender := &fakeSuspender{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry(&fakeResumer{}, suspender, nil, idleGrace, handlingGrace, log)
	s := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	stop := make(chan struct{})
	go s.readSlackUntilClose(conn, stop)
	t.Cleanup(func() { close(stop); close(conn.frames) })
	return s, conn, suspender
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestKeepaliveFramesNeverResume(t *testing.T) {
	s, conn, resumer, _ := newTestSession(t, 0)
	conn.frames <- []byte(`{"type":"hello","num_connections":1}`)
	conn.frames <- []byte(`{"type":"disconnect","reason":"warning"}`)
	time.Sleep(100 * time.Millisecond)
	if got := resumer.resumes(); got != 0 {
		t.Fatalf("connection-management frames triggered %d resumes", got)
	}
	s.mu.Lock()
	buffered := len(s.buffer)
	s.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("keepalives buffered as events: %d", buffered)
	}
}

func TestEventWhileDetachedBuffersAcksSlackAndResumesOnce(t *testing.T) {
	s, conn, resumer, _ := newTestSession(t, 0)
	conn.frames <- eventFrame("e1")
	conn.frames <- eventFrame("e2")

	waitFor(t, "slack acks", func() bool { return len(conn.acked) == 2 })
	waitFor(t, "buffering", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.buffer) == 2
	})
	waitFor(t, "one resume", func() bool { return resumer.resumes() == 1 })
	time.Sleep(50 * time.Millisecond)
	if got := resumer.resumes(); got != 1 {
		t.Fatalf("expected exactly one resume, got %d", got)
	}
}

func TestAttachDropsAckedAndResendsRestInOrder(t *testing.T) {
	s, conn, _, _ := newTestSession(t, 0)
	for i := 1; i <= 3; i++ {
		conn.frames <- eventFrame(fmt.Sprintf("e%d", i))
	}
	waitFor(t, "buffering", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.buffer) == 3
	})

	// The proxy already acked seq 1 before its last suspend.
	sink := &fakeSink{}
	s.Attach(sink, 1)
	got := sink.delivered()
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("expected re-send of [2 3], got %v", got)
	}

	// New events flow immediately while attached.
	conn.frames <- eventFrame("e4")
	waitFor(t, "live delivery", func() bool { return len(sink.delivered()) == 3 })
	if got := sink.delivered(); got[2] != 4 {
		t.Fatalf("expected seq 4 delivered live, got %v", got)
	}
}

func TestAckRemovesFromBuffer(t *testing.T) {
	s, conn, _, _ := newTestSession(t, 0)
	sink := &fakeSink{}
	s.Attach(sink, 0)
	conn.frames <- eventFrame("e1")
	waitFor(t, "delivery", func() bool { return len(sink.delivered()) == 1 })

	s.Ack(1)
	s.mu.Lock()
	buffered := len(s.buffer)
	s.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("acked event still buffered: %d", buffered)
	}

	sink2 := &fakeSink{}
	s.Attach(sink2, 0)
	if got := sink2.delivered(); len(got) != 0 {
		t.Fatalf("acked event re-sent: %v", got)
	}
}

func TestFailedSendKeepsEventForNextAttach(t *testing.T) {
	s, conn, _, _ := newTestSession(t, 0)
	sink := &fakeSink{fail: true}
	s.Attach(sink, 0)
	conn.frames <- eventFrame("e1")
	waitFor(t, "buffering", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.buffer) == 1
	})

	sink2 := &fakeSink{}
	s.Attach(sink2, 0)
	if got := sink2.delivered(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected re-send of [1], got %v", got)
	}
}

func TestUnackedEventDefersIdleSuspend(t *testing.T) {
	s, conn, _, suspender := newTestSession(t, 50*time.Millisecond)
	sink := &fakeSink{}
	s.Attach(sink, 0)

	conn.frames <- eventFrame("e1")
	waitFor(t, "delivery", func() bool { return len(sink.delivered()) == 1 })

	// A delivered-but-unacked event must hold suspend off.
	time.Sleep(120 * time.Millisecond)
	if got := suspender.suspends(); got != 0 {
		t.Fatalf("suspended with an unacked event: %d", got)
	}

	// Acking it lets the actor go idle and suspend.
	s.Ack(1)
	waitFor(t, "idle suspend", func() bool { return suspender.suspends() == 1 })
}

func TestHandlingHoldDefersSuspendUntilReply(t *testing.T) {
	// Long handling grace, short idle grace: acking receipt must NOT let the
	// actor suspend while the agent is still producing its reply.
	s, conn, suspender := newHandlingSession(t, 20*time.Millisecond, time.Second)
	sink := &fakeSink{}
	s.Attach(sink, 0)

	conn.frames <- eventFrame("e1")
	waitFor(t, "delivery", func() bool { return len(sink.delivered()) == 1 })
	s.Ack(1) // Socket Mode acks on receipt, before the reply

	// Idle grace elapses, but the handling hold keeps the actor alive.
	time.Sleep(80 * time.Millisecond)
	if got := suspender.suspends(); got != 0 {
		t.Fatalf("suspended mid-handle, before the reply: %d", got)
	}

	// The agent replies (an egress relay); once it completes, idle suspend fires.
	s.beginForward()
	s.endForward()
	waitFor(t, "idle suspend after reply", func() bool { return suspender.suspends() == 1 })
}

func TestHandlingHoldExpiresWhenAgentNeverReplies(t *testing.T) {
	// If the agent acks but never replies (e.g. ignores the message), the hold
	// must cap so the actor still suspends.
	s, conn, suspender := newHandlingSession(t, 20*time.Millisecond, 60*time.Millisecond)
	sink := &fakeSink{}
	s.Attach(sink, 0)

	conn.frames <- eventFrame("e1")
	waitFor(t, "delivery", func() bool { return len(sink.delivered()) == 1 })
	s.Ack(1)

	waitFor(t, "suspend after handling grace", func() bool { return suspender.suspends() == 1 })
}

func TestIdleSuspendFiresAndForwardDefersIt(t *testing.T) {
	s, _, _, suspender := newTestSession(t, 50*time.Millisecond)
	sink := &fakeSink{}

	s.Attach(sink, 0)
	s.beginForward()
	time.Sleep(120 * time.Millisecond)
	if got := suspender.suspends(); got != 0 {
		t.Fatalf("suspended mid-forward: %d", got)
	}
	s.endForward()
	waitFor(t, "idle suspend", func() bool { return suspender.suspends() == 1 })

	// The checkpoint tears the stream down -> Detach; no further suspends.
	s.Detach(sink)
	time.Sleep(120 * time.Millisecond)
	if got := suspender.suspends(); got != 1 {
		t.Fatalf("suspend fired while detached: %d", got)
	}
}
