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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ronlv10/substrate-ws-poc/internal/socketmode"
)

// fakeSlackConn feeds scripted frames to the session's Slack read loop.
type fakeSlackConn struct {
	frames chan []byte
	acked  chan string
}

func newFakeSlackConn() *fakeSlackConn {
	return &fakeSlackConn{frames: make(chan []byte, 16), acked: make(chan string, 16)}
}

func (f *fakeSlackConn) Read() (socketmode.Envelope, []byte, error) {
	raw, ok := <-f.frames
	if !ok {
		return socketmode.Envelope{}, nil, io.EOF
	}
	env, err := socketmode.DecodeEnvelope(raw)
	return env, raw, err
}
func (f *fakeSlackConn) Ack(id string) error { f.acked <- id; return nil }
func (f *fakeSlackConn) Close() error        { return nil }

type fakeDialer struct{ conn *fakeSlackConn }

func (d *fakeDialer) Dial(ctx context.Context, token string) (SlackConn, error) {
	return d.conn, nil
}

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

// fakeSink records delivered (seq, payload) pairs.
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

func newTestSession(t *testing.T, idleGrace time.Duration) (*session, *fakeSlackConn, *fakeResumer, *fakeSuspender) {
	t.Helper()
	conn := newFakeSlackConn()
	resumer := &fakeResumer{}
	suspender := &fakeSuspender{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry(resumer, suspender, &fakeDialer{conn: conn}, idleGrace, log)
	s := reg.GetOrCreate(ActorRef{Atespace: "demo", Name: "echo-1"})
	s.EnsureStarted("xapp-test")
	t.Cleanup(func() { s.Close(); close(conn.frames) })
	return s, conn, resumer, suspender
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

func TestEventWhileDetachedAcksSlackBuffersAndResumesOnce(t *testing.T) {
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

	// A second attach re-sends nothing.
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

func TestIdleSuspendFiresAndForwardDefersIt(t *testing.T) {
	s, _, _, suspender := newTestSession(t, 50*time.Millisecond)
	sink := &fakeSink{}

	// An in-flight relay holds off the idle suspend.
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
