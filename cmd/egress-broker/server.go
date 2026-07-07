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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ronlv10/substrate-ws-poc/internal/slackapi"
	"github.com/ronlv10/substrate-ws-poc/internal/socketmode"
)

// actorPingInterval is how often the broker pings the actor's Socket Mode
// WebSocket to keep it alive and to let the actor detect a dead connection
// after a checkpoint/restore. Must be comfortably shorter than the actor's read
// deadline (see echo-actor).
const actorPingInterval = 5 * time.Second

// Server terminates the actor's TLS to Slack. It synthesizes
// apps.connections.open (pointing the actor's Socket Mode WebSocket back at the
// broker), captures the app-level token, serves the actor-facing Socket Mode
// WebSocket, and forwards all other Slack API calls to real Slack.
type Server struct {
	reg      *Registry
	locator  Locator
	forward  *realSlackDialer // reused for its Slack-reaching HTTP client
	upgrader websocket.Upgrader
	log      *slog.Logger

	// wssHost is embedded in the synthesized wss URL. It must be a hostname the
	// actor's DNS maps to the broker (e.g. "wss-primary.slack.com").
	wssHost string
	// wssPath is the path the actor's Socket Mode WebSocket connects to.
	wssPath string

	ticketsMu sync.Mutex
	tickets   map[string]ticketEntry
}

type ticketEntry struct {
	ref     ActorRef
	expires time.Time
}

// NewServer builds the actor-facing broker HTTP handler.
func NewServer(reg *Registry, locator Locator, forward *realSlackDialer, wssHost, wssPath string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		reg:     reg,
		locator: locator,
		forward: forward,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		log:     log,
		wssHost: wssHost,
		wssPath: wssPath,
		tickets: make(map[string]ticketEntry),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == s.wssPath:
		s.handleActorSocket(w, r)
	case r.Method == http.MethodPost && r.URL.Path == slackapi.PathConnectionsOpen:
		s.handleConnectionsOpen(w, r)
	default:
		s.handlePassthrough(w, r)
	}
}

// handleConnectionsOpen intercepts the actor's Socket Mode bootstrap. It
// captures the app-level token, resolves which actor is calling, ensures that
// actor's persistent Slack connection is running, and returns a wss URL that
// points back at the broker.
func (s *Server) handleConnectionsOpen(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r.Header.Get("Authorization"))
	if token == "" {
		writeJSON(w, http.StatusOK, slackapi.ConnectionsOpenResponse{OK: false, Error: "not_authed"})
		return
	}

	ip := remoteIP(r.RemoteAddr)
	// Resolve which actor is calling, retrying briefly: right after a resume the
	// actor is RUNNING but ListActors (Redis) can lag by a moment. Returning an
	// error here makes sophisticated clients (Bolt) tear down and reconnect with
	// backoff, churning for many seconds and redelivering events they then
	// dedupe-drop. Waiting a beat lets the actor connect cleanly on the first try.
	ref, err := s.locateWithRetry(r.Context(), ip)
	if err != nil {
		s.log.Warn("egress-broker: could not identify actor for apps.connections.open",
			slog.String("src_ip", ip), slog.Any("err", err))
		writeJSON(w, http.StatusOK, slackapi.ConnectionsOpenResponse{OK: false, Error: "ws-poc_actor_not_identified"})
		return
	}

	s.reg.GetOrCreate(ref).EnsureStarted(token)

	ticket := s.issueTicket(ref)
	wssURL := (&url.URL{Scheme: "wss", Host: s.wssHost, Path: s.wssPath, RawQuery: "ticket=" + ticket}).String()
	s.log.Info("egress-broker: actor opened Socket Mode connection",
		slog.String("actor", ref.String()), slog.String("src_ip", ip))
	writeJSON(w, http.StatusOK, slackapi.ConnectionsOpenResponse{OK: true, URL: wssURL})
}

// locateWithRetry resolves the actor for a source IP, retrying for a few
// seconds to ride out ListActors/Redis lag right after a resume.
func (s *Server) locateWithRetry(ctx context.Context, ip string) (ActorRef, error) {
	const attempts = 20
	var lastErr error
	for i := 0; i < attempts; i++ {
		ref, err := s.locator.LocateByPodIP(ctx, ip)
		if err == nil {
			return ref, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ActorRef{}, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return ActorRef{}, lastErr
}

// handleActorSocket upgrades the actor's Socket Mode WebSocket and attaches it
// to its session, which sends hello and drains any buffered events.
func (s *Server) handleActorSocket(w http.ResponseWriter, r *http.Request) {
	ref, ok := s.redeemTicket(r.URL.Query().Get("ticket"))
	if !ok {
		http.Error(w, "invalid or expired ticket", http.StatusUnauthorized)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("egress-broker: actor WebSocket upgrade failed", slog.Any("err", err))
		return
	}
	defer conn.Close()

	sink := &gorillaActorSink{conn: conn}
	sess := s.reg.GetOrCreate(ref)

	// The client's first Socket Mode heartbeat ping means it has completed the
	// handshake and reached connected:ready — deliver buffered events on that,
	// not into its reconnect churn. We must still send a pong ourselves since a
	// custom ping handler replaces gorilla's default.
	conn.SetPingHandler(func(appData string) error {
		sess.MarkReady(sink) // delivers buffered events on the first heartbeat; idempotent after
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		if err == websocket.ErrCloseSent {
			return nil
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil
		}
		return err
	})

	sess.Attach(sink)
	defer sess.Detach(sink)

	s.log.Info("egress-broker: actor Socket Mode WebSocket attached", slog.String("actor", ref.String()))

	// Keepalive: ping the actor periodically (as real Slack does). This is what
	// lets a checkpointed-then-restored actor notice its old connection is dead —
	// the actor holds a read deadline that these pings refresh, so once the actor
	// is suspended and the pings stop, its next read times out and it reconnects.
	// WriteControl is safe to call concurrently with the session's data writes.
	stopPing := make(chan struct{})
	defer close(stopPing)
	go func() {
		t := time.NewTicker(actorPingInterval)
		defer t.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-t.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()

	// Read the actor's acks until it disconnects. Each ack (an envelope_id the
	// actor echoes back) removes that event from the buffer; unacked events are
	// redelivered on the next attach. We already acked Slack separately.
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			s.log.Info("egress-broker: actor WebSocket closed", slog.String("actor", ref.String()), slog.Any("err", err))
			return
		}
		if ack, derr := socketmode.DecodeEnvelope(data); derr == nil && ack.EnvelopeID != "" {
			sess.Ack(ack.EnvelopeID)
		}
	}
}

// handlePassthrough forwards any other Slack API call (chat.postMessage, etc.)
// to real Slack and relays the response. This keeps non-WS-PoC traffic (and
// the echo actor's own reply) working under the cluster-wide DNS redirect.
func (s *Server) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	target := s.forward.apiBaseURL + r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	copyHeader(outReq.Header, r.Header)
	outReq.Header.Del("Accept-Encoding") // let Go negotiate; avoids double-encoding

	resp, err := s.forward.httpClient.Do(outReq)
	if err != nil {
		s.log.Warn("egress-broker: forwarding to Slack failed", slog.String("path", r.URL.Path), slog.Any("err", err))
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	s.log.Info("egress-broker: forwarded Slack API call",
		slog.String("method", r.Method), slog.String("path", r.URL.Path),
		slog.Int("status", resp.StatusCode), slog.String("src_ip", remoteIP(r.RemoteAddr)))

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) issueTicket(ref ActorRef) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	t := hex.EncodeToString(b[:])
	s.ticketsMu.Lock()
	s.tickets[t] = ticketEntry{ref: ref, expires: time.Now().Add(2 * time.Minute)}
	s.ticketsMu.Unlock()
	return t
}

func (s *Server) redeemTicket(t string) (ActorRef, bool) {
	s.ticketsMu.Lock()
	defer s.ticketsMu.Unlock()
	e, ok := s.tickets[t]
	if !ok || time.Now().After(e.expires) {
		delete(s.tickets, t)
		return ActorRef{}, false
	}
	delete(s.tickets, t)
	return e.ref, true
}

// gorillaActorSink is the write side of the actor-facing WebSocket. Writes are
// serialized by the session (sinkWrite), so no additional locking is needed.
type gorillaActorSink struct {
	conn *websocket.Conn
}

func (s *gorillaActorSink) WriteFrame(b []byte) error {
	return s.conn.WriteMessage(websocket.TextMessage, b)
}

func bearerToken(authHeader string) string {
	const prefix = "Bearer "
	if len(authHeader) > len(prefix) && strings.EqualFold(authHeader[:len(prefix)], prefix) {
		return strings.TrimSpace(authHeader[len(prefix):])
	}
	return ""
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
