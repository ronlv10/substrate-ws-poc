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
	"github.com/ronlv10/substrate-ws-poc/internal/slack"
)

// actorPingInterval is how often the broker pings the actor's Socket Mode
// WebSocket to keep it alive and to let the actor detect a dead connection
// after a checkpoint/restore. Must be comfortably shorter than the actor's read
// deadline (see echo-actor).
const actorPingInterval = 5 * time.Second

// goldenAtespace is the atespace substrate places golden-snapshot template actors
// in. Connections identified as belonging to it are served the Socket Mode
// handshake but never get a persistent Slack session (see handleConnectionsOpen).
const goldenAtespace = "ate-golden"

// wssHost / wssPath describe the Socket Mode WebSocket the broker synthesizes for
// actors. wssHost is a Slack hostname the actor's /etc/hosts maps to the broker.
const (
	wssHost = "wss-primary.slack.com"
	wssPath = "/ws-poc/socketmode"
)

// Server terminates the actor's TLS to Slack. It synthesizes
// apps.connections.open (pointing the actor's Socket Mode WebSocket back at the
// broker), captures the app-level token, serves the actor-facing Socket Mode
// WebSocket, and forwards all other Slack API calls to real Slack.
type Server struct {
	reg      *Registry
	locator  Locator
	forward  *slack.Dialer // reused for its Slack-reaching HTTP client (passthrough)
	upgrader websocket.Upgrader
	log      *slog.Logger

	ticketsMu sync.Mutex
	tickets   map[string]ticketEntry
}

type ticketEntry struct {
	ref     ActorRef
	expires time.Time
}

// NewServer builds the actor-facing broker HTTP handler.
func NewServer(reg *Registry, locator Locator, forward *slack.Dialer, log *slog.Logger) *Server {
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
		tickets: make(map[string]ticketEntry),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == wssPath:
		s.handleActorSocket(w, r)
	case r.Method == http.MethodPost && r.URL.Path == slack.PathConnectionsOpen:
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
		writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: false, Error: "not_authed"})
		return
	}

	ip := remoteIP(r.RemoteAddr)

	// Identify which actor is calling. Preferred: the actor announces its own
	// substrate identity in the X-Ate-Actor header (deterministic, no races). We
	// fall back to source-IP correlation only if the header is absent — but that
	// path is inherently flaky, because it matches the connection's source IP
	// against the control plane's eventually-consistent AteomPodIp, which lags
	// right after a boot/resume and makes identification fail.
	var (
		ref    ActorRef
		via    string
		hadRef bool
	)
	if h := strings.TrimSpace(r.Header.Get("X-Ate-Actor")); h != "" {
		if parsed, ok := parseActorRef(h); ok {
			ref, hadRef, via = parsed, true, "header"
		} else {
			s.log.Warn("egress-broker: ignoring malformed X-Ate-Actor header", slog.String("value", h))
		}
	}
	if !hadRef {
		located, err := s.locateWithRetry(r.Context(), ip)
		if err != nil {
			s.log.Warn("egress-broker: could not identify actor for apps.connections.open",
				slog.String("src_ip", ip), slog.Any("err", err))
			writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: false, Error: "ws-poc_actor_not_identified"})
			return
		}
		ref, via = located, "source-ip"
	}

	// The golden-snapshot template actor (atespace "ate-golden") is ephemeral: it
	// runs only to warm a snapshot the real actor is cloned from. Serve its Socket
	// Mode handshake so it reaches connected:ready and can be checkpointed, but do
	// NOT open a persistent Slack connection under the golden identity — that
	// connection would sit on the app token and intercept events meant for the
	// real actor. Only real actors get a Slack session.
	if ref.Atespace == goldenAtespace {
		ticket := s.issueTicket(ref)
		s.log.Info("egress-broker: golden template actor opened Socket Mode connection (no Slack session)",
			slog.String("actor", ref.String()), slog.String("identified_via", via))
		writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: true, URL: wssURL(ticket)})
		return
	}

	s.reg.GetOrCreate(ref).EnsureStarted(token)

	ticket := s.issueTicket(ref)
	s.log.Info("egress-broker: actor opened Socket Mode connection",
		slog.String("actor", ref.String()), slog.String("src_ip", ip), slog.String("identified_via", via))
	writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: true, URL: wssURL(ticket)})
}

// wssURL is the Socket Mode wss URL the broker hands the actor: it points back at
// the broker (via wssHost, which the actor's /etc/hosts maps here) and carries the
// one-time ticket the actor redeems on the WebSocket.
func wssURL(ticket string) string {
	return (&url.URL{Scheme: "wss", Host: wssHost, Path: wssPath, RawQuery: "ticket=" + ticket}).String()
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
		if ack, derr := slack.DecodeEnvelope(data); derr == nil && ack.EnvelopeID != "" {
			sess.Ack(ack.EnvelopeID)
		}
	}
}

// handlePassthrough forwards any other Slack API call (chat.postMessage, etc.)
// to real Slack and relays the response. This keeps non-WS-PoC traffic (and
// the echo actor's own reply) working under the cluster-wide DNS redirect.
func (s *Server) handlePassthrough(w http.ResponseWriter, r *http.Request) {
	target := s.forward.APIBaseURL() + r.URL.Path
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

	// If this call carries an actor identity (our actor sends X-Ate-Actor on every
	// slack.com request, chat.postMessage included), hold off that actor's idle
	// suspend until the forward finishes — chat.postMessage can be slow/retried,
	// and suspending mid-send would drop the reply.
	if h := strings.TrimSpace(r.Header.Get("X-Ate-Actor")); h != "" {
		if ref, ok := parseActorRef(h); ok {
			if sess := s.reg.Lookup(ref); sess != nil {
				sess.beginForward()
				defer sess.endForward()
			}
		}
	}

	resp, err := s.forward.HTTPClient().Do(outReq)
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

// parseActorRef parses an "<atespace>/<name>" identity string (as sent in the
// X-Ate-Actor header) into an ActorRef. Both parts must be non-empty and name
// must not itself contain a slash.
func parseActorRef(v string) (ActorRef, bool) {
	i := strings.IndexByte(v, '/')
	if i <= 0 || i >= len(v)-1 {
		return ActorRef{}, false
	}
	atespace, name := v[:i], v[i+1:]
	if strings.ContainsRune(name, '/') {
		return ActorRef{}, false
	}
	return ActorRef{Atespace: atespace, Name: name}, true
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

func bearerToken(authHeader string) string {
	const prefix = "Bearer "
	if len(authHeader) > len(prefix) && strings.EqualFold(authHeader[:len(prefix)], prefix) {
		return strings.TrimSpace(authHeader[len(prefix):])
	}
	return ""
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
