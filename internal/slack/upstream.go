package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	slackgo "github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

// Conn is a live Socket Mode connection to real Slack, kept open across the
// actor's suspend/resume cycles.
type Conn interface {
	// Read returns the next frame; events_api frames also carry raw bytes to
	// forward to the actor.
	Read() (Envelope, []byte, error)
	Ack(envelopeID string) error
	Close() error
}

// Dialer reaches real Slack. Only actors are redirected (via their /etc/hosts),
// so the broker resolves Slack hostnames with the default resolver.
type Dialer struct {
	httpClient *http.Client
	wsDialer   *websocket.Dialer
}

const apiBaseURL = "https://" + APIHost

func NewDialer() *Dialer {
	return &Dialer{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		wsDialer:   &websocket.Dialer{HandshakeTimeout: 15 * time.Second},
	}
}

// HTTPClient and APIBaseURL back the broker's passthrough of non-Socket-Mode
// calls (e.g. chat.postMessage) to real Slack.
func (d *Dialer) HTTPClient() *http.Client { return d.httpClient }
func (d *Dialer) APIBaseURL() string       { return apiBaseURL }

// Dial opens a Socket Mode connection to real Slack; slack-go handles the
// handshake, heartbeats, and reconnection on its own background context.
func (d *Dialer) Dial(appToken string) (Conn, error) {
	api := slackgo.New("",
		slackgo.OptionAppLevelToken(appToken),
		slackgo.OptionHTTPClient(d.httpClient),
		slackgo.OptionAPIURL(apiBaseURL+"/api/"),
	)
	client := socketmode.New(api, socketmode.OptionDialer(d.wsDialer))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = client.RunContext(ctx) // blocks and reconnects until ctx is cancelled
		cancel()                   // unblock Read if Run returns on its own
	}()
	return &conn{client: client, ctx: ctx, cancel: cancel, pending: map[string]socketmode.Request{}}, nil
}

// conn adapts a slack-go socketmode client to Conn. Non-event frames surface only
// their type name; the session logs and ignores them.
type conn struct {
	client *socketmode.Client
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	pending map[string]socketmode.Request
}

func (c *conn) Read() (Envelope, []byte, error) {
	select {
	case <-c.ctx.Done():
		return Envelope{}, nil, c.ctx.Err()
	case evt := <-c.client.Events:
		// Only events_api frames carry a payload to forward to the actor; other
		// types (hello, disconnect, connection errors) surface as type-only
		// envelopes for the session to log and ignore.
		if evt.Type != socketmode.EventTypeEventsAPI || evt.Request == nil {
			return Envelope{Type: string(evt.Type)}, nil, nil
		}
		req := *evt.Request
		raw, err := json.Marshal(struct {
			Type       string          `json:"type"`
			EnvelopeID string          `json:"envelope_id"`
			Payload    json.RawMessage `json:"payload"`
		}{Type: TypeEventsAPI, EnvelopeID: req.EnvelopeID, Payload: req.Payload})
		if err != nil {
			return Envelope{}, nil, err
		}
		c.mu.Lock()
		c.pending[req.EnvelopeID] = req
		c.mu.Unlock()
		return Envelope{Type: TypeEventsAPI, EnvelopeID: req.EnvelopeID}, raw, nil
	}
}

func (c *conn) Ack(envelopeID string) error {
	c.mu.Lock()
	req, ok := c.pending[envelopeID]
	delete(c.pending, envelopeID)
	c.mu.Unlock()
	if !ok {
		return nil
	}
	return c.client.Ack(req)
}

func (c *conn) Close() error {
	c.cancel()
	return nil
}
