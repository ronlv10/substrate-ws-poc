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

// Spike: does a STOCK @slack/socket-mode client survive gVisor
// checkpoint/restore on a surviving loopback socket, or does its wall-clock
// pong/ping staleness check fire ServerPongsNotReceived and churn?
//
// This binary impersonates Slack for a single co-resident Bolt agent: PID 1,
// TLS on 127.0.0.1:443 with a slack.com certificate (the image bakes the CA
// into NODE_EXTRA_CA_CERTS and /etc/hosts points slack.com at loopback).
// It serves apps.connections.open, a Socket Mode WebSocket (hello, server
// pings every 5s like real Slack, auto-pong of client pings), stubs of
// auth.test / chat.postMessage, and injects one synthetic events_api
// envelope shortly after boot and after every detected restore.
//
// PASS/FAIL is read from the logs: more than one "WS: upgrade" or any Bolt
// reconnect line means the stock client churned.
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wssPath      = "/spike/socketmode"
	injectDelay  = 5 * time.Second
	serverPingIv = 5 * time.Second
)

type server struct {
	mu       sync.Mutex // guards conn writes and replacement
	conn     *websocket.Conn
	upgrades int
	injected int
	upgrader websocket.Upgrader
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(os.Stderr)
	log.Printf("SERVER: starting pid=%d", os.Getpid())

	s := &server{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/apps.connections.open", s.handleConnectionsOpen)
	mux.HandleFunc(wssPath, s.handleSocket)
	mux.HandleFunc("/api/auth.test", stubJSON(map[string]any{
		"ok": true, "url": "https://slack.com/", "team": "spike",
		"user": "spikebot", "team_id": "T00000001", "user_id": "U00000001",
		"bot_id": "B00000001", "is_enterprise_install": false,
	}))
	mux.HandleFunc("/api/chat.postMessage", s.handlePostMessage)
	mux.HandleFunc("/api/", stubJSON(map[string]any{"ok": true}))

	go func() {
		rz := http.NewServeMux()
		rz.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// Not ready until the agent's Socket Mode WS is attached: the
			// golden checkpoint fires as soon as readyz is green, and it must
			// capture a settled Bolt with the loopback WS established — not
			// V8 mid-startup (observed to SIGILL on restore when frozen ~5s
			// into `node` boot).
			s.mu.Lock()
			ready := s.conn != nil
			s.mu.Unlock()
			if !ready {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintln(w, "agent not connected")
				return
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ok")
		})
		if err := http.ListenAndServe(":80", rz); err != nil {
			log.Fatalf("SERVER: readyz: %v", err)
		}
	}()

	go func() {
		srv := &http.Server{
			Addr:      "127.0.0.1:443",
			Handler:   mux,
			TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		}
		log.Printf("SERVER: TLS listening on 127.0.0.1:443 as slack.com")
		if err := srv.ListenAndServeTLS("/certs/tls.crt", "/certs/tls.key"); err != nil {
			log.Fatalf("SERVER: tls listen: %v", err)
		}
	}()

	go s.restoreDetector()
	go s.serverPinger()
	go func() {
		time.Sleep(injectDelay)
		s.injectEvent("boot")
	}()

	cmd := exec.Command("node", "/app/app.js")
	cmd.Stdout = os.Stderr // agent logs must not be lost to gVisor stdout buffering
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		log.Fatalf("SERVER: spawn agent: %v", err)
	}
	log.Printf("SERVER: spawned agent pid=%d", cmd.Process.Pid)
	err := cmd.Wait()
	log.Printf("SERVER: FATAL: agent exited: %v", err)
	for {
		time.Sleep(5 * time.Second)
		log.Printf("SERVER: FATAL-MARKER: agent is gone")
	}
}

func stubJSON(body map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("SERVER: API stub %s", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(body)
	}
}

func (s *server) handleConnectionsOpen(w http.ResponseWriter, r *http.Request) {
	log.Printf("SERVER: apps.connections.open called (auth=%.16q...)", r.Header.Get("Authorization"))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":  true,
		"url": "wss://slack.com" + wssPath,
	})
}

func (s *server) handlePostMessage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Channel string `json:"channel"`
		Text    string `json:"text"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	log.Printf("SERVER: REPLY-CAPTURED chat.postMessage channel=%s text=%q", req.Channel, req.Text)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok": true, "channel": req.Channel,
		"ts": fmt.Sprintf("%d.000200", time.Now().Unix()),
	})
}

func (s *server) handleSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("SERVER: upgrade failed: %v", err)
		return
	}
	s.mu.Lock()
	s.upgrades++
	n := s.upgrades
	if s.conn != nil {
		// A second upgrade while one connection is live IS the churn signal.
		log.Printf("SERVER: WS: upgrade #%d REPLACES a live connection (churn!)", n)
	} else {
		log.Printf("SERVER: WS: upgrade #%d", n)
	}
	s.conn = conn
	s.mu.Unlock()

	conn.SetPingHandler(func(appData string) error {
		log.Printf("SERVER: client ping received (conn #%d), ponging", n)
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error {
		log.Printf("SERVER: client pong received (conn #%d)", n)
		return nil
	})

	if err := s.writeFrame(map[string]any{"type": "hello", "num_connections": 1}); err != nil {
		log.Printf("SERVER: send hello on conn #%d: %v", n, err)
		return
	}
	log.Printf("SERVER: hello sent on conn #%d", n)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("SERVER: WS: conn #%d read loop ended: %v", n, err)
			s.mu.Lock()
			if s.conn == conn {
				s.conn = nil
			}
			s.mu.Unlock()
			return
		}
		var ack struct {
			EnvelopeID string `json:"envelope_id"`
		}
		if json.Unmarshal(msg, &ack) == nil && ack.EnvelopeID != "" {
			log.Printf("SERVER: ACK-RECEIVED envelope_id=%s (conn #%d)", ack.EnvelopeID, n)
		} else {
			log.Printf("SERVER: WS: conn #%d message: %.200s", n, msg)
		}
	}
}

func (s *server) writeFrame(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return fmt.Errorf("no agent connection")
	}
	return s.conn.WriteMessage(websocket.TextMessage, b)
}

func (s *server) injectEvent(reason string) {
	s.mu.Lock()
	s.injected++
	seq := s.injected
	s.mu.Unlock()
	now := time.Now()
	env := map[string]any{
		"type":                     "events_api",
		"envelope_id":              fmt.Sprintf("spike-env-%d", seq),
		"accepts_response_payload": false,
		"retry_attempt":            0,
		"retry_reason":             "",
		"payload": map[string]any{
			"token":      "spike-token",
			"team_id":    "T00000001",
			"api_app_id": "A00000001",
			"type":       "event_callback",
			"event_id":   fmt.Sprintf("Ev%08d", seq),
			"event_time": now.Unix(),
			"event": map[string]any{
				"type":         "message",
				"channel":      "C00000001",
				"channel_type": "channel",
				"user":         "U00000002",
				"text":         fmt.Sprintf("ping %d (%s)", seq, reason),
				"ts":           fmt.Sprintf("%d.%06d", now.Unix(), seq),
			},
		},
	}
	if err := s.writeFrame(env); err != nil {
		log.Printf("SERVER: INJECT-FAILED #%d (%s): %v", seq, reason, err)
		return
	}
	log.Printf("SERVER: INJECTED event #%d envelope_id=spike-env-%d (%s)", seq, seq, reason)
}

// serverPinger mimics real Slack: periodic server->client pings. The stock
// client monitors time-since-last-server-ping, so silence here would itself
// trigger a reconnect and contaminate the experiment.
func (s *server) serverPinger() {
	for range time.Tick(serverPingIv) {
		s.mu.Lock()
		conn := s.conn
		s.mu.Unlock()
		if conn == nil {
			continue
		}
		if err := conn.WriteControl(websocket.PingMessage,
			[]byte(fmt.Sprintf("%d", time.Now().Unix())), time.Now().Add(5*time.Second)); err != nil {
			log.Printf("SERVER: server ping failed: %v", err)
		}
	}
}

func (s *server) restoreDetector() {
	prev := time.Now()
	for {
		time.Sleep(250 * time.Millisecond)
		now := time.Now()
		monoGap := now.Sub(prev)
		wallGap := now.Round(0).Sub(prev.Round(0))
		if wallGap > 2*time.Second || monoGap > 2*time.Second {
			log.Printf("SERVER: RESTORE-DETECTED wallGap=%s monoGap=%s",
				wallGap.Truncate(time.Millisecond), monoGap.Truncate(time.Millisecond))
			// Give the thawed client a beat, then deliver — the v2 wake path is
			// "resume, then the buffered event arrives".
			go func() {
				time.Sleep(1 * time.Second)
				s.injectEvent("post-restore")
			}()
		}
		prev = now
	}
}
