package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func newTestFace(t *testing.T) (*Core, *httptest.Server) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	core := NewCore(log)
	face := NewSlackFace(core, "slack.com", log)
	srv := httptest.NewServer(face.Handler())
	t.Cleanup(srv.Close)
	return core, srv
}

// agentClient is a fake stock agent: a websocket client whose reader
// goroutine streams received envelope ids. A dedicated goroutine (instead of
// deadline-bounded reads) mirrors how a real client reads and avoids
// gorilla's connection-poisoning on read timeouts.
type agentClient struct {
	conn *websocket.Conn
	ids  chan string
}

func dialAgent(t *testing.T, srv *httptest.Server) *agentClient {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + WSSPath
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial agent ws: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	var hello struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&hello); err != nil || hello.Type != "hello" {
		t.Fatalf("expected hello, got %+v err=%v", hello, err)
	}

	a := &agentClient{conn: conn, ids: make(chan string, 16)}
	go func() {
		defer close(a.ids)
		for {
			var env struct {
				EnvelopeID string `json:"envelope_id"`
			}
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			a.ids <- env.EnvelopeID
		}
	}()
	return a
}

func (a *agentClient) heartbeat(t *testing.T) {
	t.Helper()
	if err := a.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("heartbeat ping: %v", err)
	}
}

func (a *agentClient) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case id, ok := <-a.ids:
		if !ok || id != want {
			t.Fatalf("expected %s, got %q (open=%v)", want, id, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}

func (a *agentClient) expectNothing(t *testing.T) {
	t.Helper()
	select {
	case id := <-a.ids:
		t.Fatalf("unexpected delivery of %q", id)
	case <-time.After(300 * time.Millisecond):
	}
}

func eventRaw(seq uint64) []byte {
	raw, _ := json.Marshal(map[string]any{
		"type":        "events_api",
		"envelope_id": fmt.Sprintf("env-%d", seq),
		"payload":     map[string]any{"type": "event_callback"},
	})
	return raw
}

func TestConnectionsOpenCapturesTokenAndPointsLocal(t *testing.T) {
	core, srv := newTestFace(t)
	req, _ := http.NewRequest("POST", srv.URL+"/api/apps.connections.open", nil)
	req.Header.Set("Authorization", "Bearer xapp-test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		OK  bool   `json:"ok"`
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.OK || out.URL != "wss://slack.com"+WSSPath {
		t.Fatalf("unexpected response: %+v", out)
	}
	if core.AppToken() != "xapp-test-token" {
		t.Fatalf("token not captured: %q", core.AppToken())
	}
}

func TestDeliveryHeldUntilHeartbeat(t *testing.T) {
	core, srv := newTestFace(t)
	agent := dialAgent(t, srv)

	if err := core.Deliver(1, eventRaw(1)); err != nil {
		t.Fatal(err)
	}
	agent.expectNothing(t) // held: no heartbeat yet

	agent.heartbeat(t)
	agent.expect(t, "env-1")

	// Once ready, delivery is immediate and ordered.
	core.Deliver(2, eventRaw(2))
	core.Deliver(3, eventRaw(3))
	agent.expect(t, "env-2")
	agent.expect(t, "env-3")
}

func TestAckDropsPendingAndReportsUpstream(t *testing.T) {
	core, srv := newTestFace(t)
	agent := dialAgent(t, srv)
	agent.heartbeat(t)

	var mu sync.Mutex
	var acked []uint64
	core.SetOnAck(func(seq uint64) { mu.Lock(); acked = append(acked, seq); mu.Unlock() })

	core.Deliver(1, eventRaw(1))
	agent.expect(t, "env-1")
	if err := agent.conn.WriteJSON(map[string]string{"envelope_id": "env-1"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for core.LastContiguousAcked() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("ack never processed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acked) != 1 || acked[0] != 1 {
		t.Fatalf("upstream ack hook: %v", acked)
	}
}

func TestReattachRedeliversUnackedAfterHeartbeat(t *testing.T) {
	core, srv := newTestFace(t)
	agent1 := dialAgent(t, srv)
	agent1.heartbeat(t)
	core.Deliver(1, eventRaw(1))
	agent1.expect(t, "env-1")

	// Agent churns without acking: a new connection replaces the old, and the
	// unacked event is redelivered only after the NEW connection heartbeats.
	agent2 := dialAgent(t, srv)
	agent2.expectNothing(t)
	agent2.heartbeat(t)
	agent2.expect(t, "env-1")
}

func TestPassthroughUsesEgressHook(t *testing.T) {
	core, srv := newTestFace(t)
	core.SetEgress(func(method, path string, header http.Header, body []byte) (*EgressResult, error) {
		if method != "POST" || path != "/api/chat.postMessage" {
			t.Errorf("unexpected egress %s %s", method, path)
		}
		return &EgressResult{Status: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: []byte(`{"ok":true,"ts":"1.2"}`)}, nil
	})
	resp, err := http.Post(srv.URL+"/api/chat.postMessage", "application/json",
		strings.NewReader(`{"channel":"C1","text":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"ok":true`) {
		t.Fatalf("passthrough response: %d %s", resp.StatusCode, b)
	}
}

func TestStandaloneStubsEcho(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	core := NewCore(log)
	s := NewStandalone(core, log)
	res, err := s.egress("POST", "/api/auth.test", nil, nil)
	if err != nil || res.Status != 200 || !strings.Contains(string(res.Body), `"ok":true`) {
		t.Fatalf("auth.test stub: %v %+v", err, res)
	}
	res, err = s.egress("POST", "/api/chat.postMessage", nil, []byte(`{"text":"echo: hi"}`))
	if err != nil || res.Status != 200 {
		t.Fatalf("chat.postMessage stub: %v %+v", err, res)
	}
}

func TestReadyzRequiresQuiescence(t *testing.T) {
	core, srv := newTestFace(t)
	if core.AgentQuiescent() {
		t.Fatal("quiescent with no agent")
	}
	agent := dialAgent(t, srv)
	if core.AgentQuiescent() {
		t.Fatal("quiescent before heartbeat")
	}
	agent.heartbeat(t)
	waitForQ := func(want bool) {
		deadline := time.Now().Add(2 * time.Second)
		for core.AgentQuiescent() != want {
			if time.Now().After(deadline) {
				t.Fatalf("quiescence never became %v", want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitForQ(true)

	// An in-flight egress must NOT block readiness: a client retry loop would
	// hold readyz hostage and deadlock golden-snapshot creation.
	release := make(chan struct{})
	core.SetEgress(func(string, string, http.Header, []byte) (*EgressResult, error) {
		<-release
		return &EgressResult{Status: 200}, nil
	})
	done := make(chan struct{})
	go func() { core.Egress("POST", "/api/auth.test", nil, nil); close(done) }()
	time.Sleep(50 * time.Millisecond)
	waitForQ(true)
	close(release)
	<-done
}
