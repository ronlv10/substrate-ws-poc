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
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/ronlv10/substrate-ws-poc/internal/slackapi"
	sm "github.com/ronlv10/substrate-ws-poc/internal/socketmode"
)

// SlackConn is a live Socket Mode connection to real Slack, owned by the broker
// and kept open across actor suspend/resume cycles.
type SlackConn interface {
	// Read returns the next frame as an envelope; events_api frames also carry
	// the raw bytes to forward to the actor.
	Read() (sm.Envelope, []byte, error)
	// Ack acknowledges an event envelope back to Slack so it is not redelivered.
	Ack(envelopeID string) error
	Close() error
}

// SlackDialer opens a persistent Socket Mode connection to real Slack using a
// captured app-level token.
type SlackDialer interface {
	Dial(ctx context.Context, appToken string) (SlackConn, error)
}

// realSlackDialer reaches the real Slack API. It resolves Slack hostnames via an
// explicit upstream DNS server rather than cluster DNS, because the cluster-wide
// CoreDNS rewrite that redirects actors' slack.com to the broker would otherwise
// make the broker connect to itself.
type realSlackDialer struct {
	httpClient *http.Client
	wsDialer   *websocket.Dialer
	apiBaseURL string // defaults to https://slack.com
}

// newRealSlackDialer builds a dialer whose DNS resolution uses dnsUpstream
// (e.g. "8.8.8.8:53"). TLS uses the system root store (real Slack certificates).
func newRealSlackDialer(dnsUpstream, apiBaseURL string) *realSlackDialer {
	netDialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				d := net.Dialer{Timeout: 5 * time.Second}
				return d.DialContext(ctx, "udp", dnsUpstream)
			},
		},
	}
	if apiBaseURL == "" {
		apiBaseURL = "https://" + slackapi.APIHost
	}
	return &realSlackDialer{
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{DialContext: netDialer.DialContext},
		},
		wsDialer: &websocket.Dialer{
			NetDialContext:   netDialer.DialContext,
			HandshakeTimeout: 15 * time.Second,
		},
		apiBaseURL: strings.TrimRight(apiBaseURL, "/"),
	}
}

// Dial opens a Socket Mode connection to real Slack. slack-go handles
// apps.connections.open, the WebSocket, heartbeats, and reconnection; both the
// API call and the WebSocket use the upstream-DNS dialer.
func (d *realSlackDialer) Dial(_ context.Context, appToken string) (SlackConn, error) {
	api := slack.New("",
		slack.OptionAppLevelToken(appToken),
		slack.OptionHTTPClient(d.httpClient),
		slack.OptionAPIURL(d.apiBaseURL+"/api/"),
	)
	client := socketmode.New(api, socketmode.OptionDialer(d.wsDialer))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = client.RunContext(ctx) // blocks and reconnects until ctx is cancelled
		cancel()                   // unblock Read if Run returns on its own
	}()
	return &slackGoConn{client: client, ctx: ctx, cancel: cancel, pending: map[string]socketmode.Request{}}, nil
}

// slackGoConn adapts a slack-go socketmode client to SlackConn. Only events_api
// frames carry raw bytes (the envelope forwarded to the actor); other frame
// types surface their name so the session can log and ignore them.
type slackGoConn struct {
	client *socketmode.Client
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	pending map[string]socketmode.Request
}

func (c *slackGoConn) Read() (sm.Envelope, []byte, error) {
	select {
	case <-c.ctx.Done():
		return sm.Envelope{}, nil, c.ctx.Err()
	case evt := <-c.client.Events:
		if evt.Type != socketmode.EventTypeEventsAPI || evt.Request == nil {
			return sm.Envelope{Type: string(evt.Type)}, nil, nil
		}
		req := *evt.Request
		raw, err := json.Marshal(struct {
			Type       string          `json:"type"`
			EnvelopeID string          `json:"envelope_id"`
			Payload    json.RawMessage `json:"payload"`
		}{Type: sm.TypeEventsAPI, EnvelopeID: req.EnvelopeID, Payload: req.Payload})
		if err != nil {
			return sm.Envelope{}, nil, err
		}
		c.mu.Lock()
		c.pending[req.EnvelopeID] = req
		c.mu.Unlock()
		return sm.Envelope{Type: sm.TypeEventsAPI, EnvelopeID: req.EnvelopeID}, raw, nil
	}
}

func (c *slackGoConn) Ack(envelopeID string) error {
	c.mu.Lock()
	req, ok := c.pending[envelopeID]
	delete(c.pending, envelopeID)
	c.mu.Unlock()
	if !ok {
		return nil
	}
	return c.client.Ack(req)
}

func (c *slackGoConn) Close() error {
	c.cancel()
	return nil
}
