package broker

import (
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

// actorPingInterval: stopping these pings on suspend is how a restored actor
// notices its old socket is dead. Must be shorter than the actor's read deadline.
const actorPingInterval = 5 * time.Second

// goldenAtespace holds substrate's golden-snapshot template actors.
const goldenAtespace = "ate-golden"

const (
	wssHost = "wss-primary.slack.com"
	wssPath = "/ws-poc/socketmode"
)

// headerActor is the header the actor announces its "<atespace>/<name>" identity in.
const headerActor = "X-Ate-Actor"

// Server terminates the actor's TLS to Slack: it synthesizes
// apps.connections.open, captures the app token, serves the actor-facing Socket
// Mode WebSocket, and forwards all other Slack API calls to real Slack.
type Server struct {
	reg      *Registry
	forward  *slack.Dialer // reused for its Slack-reaching HTTP client (passthrough)
	upgrader websocket.Upgrader
	log      *slog.Logger

	ticketsMu sync.Mutex
	tickets   map[string]ticketEntry
}

func NewServer(reg *Registry, forward *slack.Dialer, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		reg:     reg,
		forward: forward,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		log:     log,
		tickets: make(map[string]ticketEntry),
	}
}

// ServeHTTP routes the three things a Socket Mode client does: bootstrap
// (apps.connections.open), the WebSocket itself, and every other Slack API call.
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

// --- Bootstrap: apps.connections.open ---

// handleConnectionsOpen intercepts the actor's Socket Mode bootstrap: capture
// the app token, identify the actor, ensure its Slack connection is running, and
// return a wss URL pointing back at the broker.
func (s *Server) handleConnectionsOpen(w http.ResponseWriter, r *http.Request) {
	token := bearerToken(r.Header.Get("Authorization"))
	if token == "" {
		writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: false, Error: "not_authed"})
		return
	}

	// The actor announces its substrate identity in the header (read fresh from its
	// /run/ate mount), so identification is deterministic with no source-IP races.
	ref, ok := actorFromRequest(r)
	if !ok {
		s.log.Warn("egress-broker: missing or malformed X-Ate-Actor on apps.connections.open",
			slog.String("src_ip", remoteIP(r.RemoteAddr)))
		writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: false, Error: "ws-poc_actor_not_identified"})
		return
	}

	// Golden template actor: serve the handshake so it can checkpoint, but open no
	// Slack session — that would sit on the app token and steal the real actor's events.
	if ref.Atespace == goldenAtespace {
		ticket := s.issueTicket(ref)
		s.log.Info("egress-broker: golden template actor opened Socket Mode connection (no Slack session)",
			slog.String("actor", ref.String()))
		writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: true, URL: wssURL(ticket)})
		return
	}

	s.reg.GetOrCreate(ref).EnsureStarted(token)

	ticket := s.issueTicket(ref)
	s.log.Info("egress-broker: actor opened Socket Mode connection",
		slog.String("actor", ref.String()), slog.String("src_ip", remoteIP(r.RemoteAddr)))
	writeJSON(w, http.StatusOK, slack.ConnectionsOpenResponse{OK: true, URL: wssURL(ticket)})
}

// wssURL is the wss URL handed to the actor: wssHost (mapped here by /etc/hosts)
// plus a one-time ticket it redeems on the WebSocket.
func wssURL(ticket string) string {
	return (&url.URL{Scheme: "wss", Host: wssHost, Path: wssPath, RawQuery: "ticket=" + ticket}).String()
}

// --- Actor Socket Mode WebSocket ---

// handleActorSocket redeems the ticket, upgrades the WebSocket, and attaches it to
// the actor's session; while attached it keeps the connection alive and reads acks.
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

	// The first heartbeat ping means the client reached connected:ready; deliver
	// on that, not into its reconnect churn. A custom ping handler replaces
	// gorilla's default, so we send the pong ourselves.
	conn.SetPingHandler(func(appData string) error {
		sess.MarkReady(sink)
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

	stop := make(chan struct{})
	defer close(stop)
	go s.keepAlivePings(conn, stop)

	s.readActorAcks(conn, sess, ref)
}

// keepAlivePings pings the actor every actorPingInterval (as real Slack does) so
// its read deadline stays fresh. When stop closes on suspend/disconnect the pings
// cease, which is how a restored actor learns its old socket is dead.
func (s *Server) keepAlivePings(conn *websocket.Conn, stop <-chan struct{}) {
	t := time.NewTicker(actorPingInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			// WriteControl is safe alongside the session's data writes.
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}

// readActorAcks reads the actor's Socket Mode acks until it disconnects; each ack
// drops its event from the session buffer. Slack was already acked separately.
func (s *Server) readActorAcks(conn *websocket.Conn, sess *session, ref ActorRef) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			s.log.Info("egress-broker: actor WebSocket closed", slog.String("actor", ref.String()), slog.Any("err", err))
			return
		}
		if id, ok := isAckMessage(data); ok {
			sess.Ack(id)
		}
	}
}

// isAckMessage reports whether data is a Socket Mode ack (the only thing the actor
// sends over this WebSocket) and returns its envelope_id.
func isAckMessage(data []byte) (string, bool) {
	env, err := slack.DecodeEnvelope(data)
	if err != nil || env.EnvelopeID == "" {
		return "", false
	}
	return env.EnvelopeID, true
}

// gorillaActorSink is the write side of the actor-facing WebSocket. Writes are
// serialized by the session (sinkWrite), so no additional locking is needed.
type gorillaActorSink struct {
	conn *websocket.Conn
}

func (s *gorillaActorSink) WriteFrame(b []byte) error {
	return s.conn.WriteMessage(websocket.TextMessage, b)
}

// --- Passthrough to real Slack ---

// handlePassthrough forwards any other Slack API call (chat.postMessage, etc.) to
// real Slack, since the actor dials slack.com and is redirected here.
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

	// Hold off idle suspend until the forward finishes, so a slow chat.postMessage
	// is not cut off mid-send. The actor sends its identity header on every call.
	if ref, ok := actorFromRequest(r); ok {
		if sess := s.reg.Lookup(ref); sess != nil {
			sess.beginForward()
			defer sess.endForward()
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
	if _, err := io.Copy(w, resp.Body); err != nil {
		s.log.Warn("egress-broker: copying Slack response to actor failed",
			slog.String("path", r.URL.Path), slog.Any("err", err))
	}
}

// --- Actor identity ---

func actorFromRequest(r *http.Request) (ActorRef, bool) {
	return parseActorRef(strings.TrimSpace(r.Header.Get(headerActor)))
}

// parseActorRef parses an "<atespace>/<name>" identity (the X-Ate-Actor header).
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

// --- Tickets ---

type ticketEntry struct {
	ref     ActorRef
	expires time.Time
}

// issueTicket mints a short-lived one-time token binding ref to the wss URL, so
// the WebSocket upgrade can recover the actor identity — the WS carries no header.
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

// --- HTTP helpers ---

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
