package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

const (
	// WSSPath is where the proxy serves the Socket Mode WebSocket; the
	// synthesized apps.connections.open response points here.
	WSSPath = "/local/socketmode"

	// agentPingInterval paces proxy->agent pings. The stock client monitors
	// time-since-last-server-ping (real Slack pings continuously), so silence
	// here would itself trigger a reconnect.
	agentPingInterval = 5 * time.Second
)

// SlackFace is the agent-facing HTTPS surface: the Socket Mode bootstrap, the
// WebSocket itself, and pass-through of every other Web API call.
type SlackFace struct {
	core     *Core
	log      *slog.Logger
	wssHost  string
	upgrader websocket.Upgrader
}

func NewSlackFace(core *Core, wssHost string, log *slog.Logger) *SlackFace {
	return &SlackFace{core: core, wssHost: wssHost, log: log}
}

func (s *SlackFace) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+slack.PathConnectionsOpen, s.handleConnectionsOpen)
	mux.HandleFunc("GET "+WSSPath, s.handleSocket)
	mux.HandleFunc("/", s.handlePassthrough)
	return mux
}

// ReadyzHandler serves /readyz: 200 only while the agent is quiescent (see
// Core.AgentQuiescent).
func (s *SlackFace) ReadyzHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.core.AgentQuiescent() {
			http.Error(w, "agent not quiescent", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
}

func (s *SlackFace) handleConnectionsOpen(w http.ResponseWriter, r *http.Request) {
	s.core.SetAppToken(bearerToken(r.Header.Get("Authorization")))
	s.log.Info("local-proxy: apps.connections.open, pointing agent at loopback")
	writeJSON(w, slack.ConnectionsOpenResponse{OK: true, URL: "wss://" + s.wssHost + WSSPath})
}

func (s *SlackFace) handleSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("local-proxy: websocket upgrade failed", slog.Any("error", err))
		return
	}
	n := s.core.Attach(conn)
	s.log.Info("local-proxy: agent socket attached", slog.Int("attach", n))

	// First heartbeat ping on this connection = the client is dispatching:
	// release buffered events, and pong so its staleness clock resets.
	conn.SetPingHandler(func(appData string) error {
		s.core.MarkReady(conn)
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error { return nil })

	hello, _ := json.Marshal(slack.Envelope{Type: slack.TypeHello, NumConnections: 1})
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		s.log.Warn("local-proxy: sending hello failed", slog.Any("error", err))
		s.core.Detach(conn)
		return
	}

	stop := make(chan struct{})
	go s.pingAgent(conn, stop)
	defer close(stop)
	defer s.core.Detach(conn)
	s.readAgentAcks(conn, n)
}

// readAgentAcks reads the agent's Socket Mode acks until it disconnects.
func (s *SlackFace) readAgentAcks(conn *websocket.Conn, attach int) {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			s.log.Info("local-proxy: agent socket read loop ended",
				slog.Int("attach", attach), slog.Any("error", err))
			return
		}
		if id := decodeAck(raw); id != "" {
			s.core.OnAgentAck(id)
		}
	}
}

func (s *SlackFace) pingAgent(conn *websocket.Conn, stop <-chan struct{}) {
	t := time.NewTicker(agentPingInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			deadline := time.Now().Add(5 * time.Second)
			if err := conn.WriteControl(websocket.PingMessage, []byte("local-proxy"), deadline); err != nil {
				return
			}
		}
	}
}

// handlePassthrough relays any other Web API call (chat.postMessage,
// auth.test, ...) upstream and mirrors the response.
func (s *SlackFace) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path += "?" + r.URL.RawQuery
	}
	res, err := s.core.Egress(r.Method, path, r.Header, body)
	if err != nil {
		s.log.Warn("local-proxy: egress failed", slog.String("path", r.URL.Path), slog.Any("error", err))
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	copyHeader(w.Header(), res.Header)
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
}

func bearerToken(h string) string {
	const p = "Bearer "
	if strings.HasPrefix(h, p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
