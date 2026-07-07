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
	"fmt"
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

// SlackConn is a live Socket Mode connection to real Slack, owned by the broker
// and kept open across actor suspend/resume cycles.
type SlackConn interface {
	// Read returns the next envelope and its raw bytes.
	Read() (socketmode.Envelope, []byte, error)
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

// Dial bootstraps a Socket Mode connection: apps.connections.open to obtain a
// wss URL, then connect it.
func (d *realSlackDialer) Dial(ctx context.Context, appToken string) (SlackConn, error) {
	wssURL, err := d.openConnection(ctx, appToken)
	if err != nil {
		return nil, err
	}
	conn, _, err := d.wsDialer.DialContext(ctx, wssURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dialing Slack wss: %w", err)
	}
	return &gorillaSlackConn{conn: conn}, nil
}

func (d *realSlackDialer) openConnection(ctx context.Context, appToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.apiBaseURL+slackapi.PathConnectionsOpen, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+appToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling apps.connections.open: %w", err)
	}
	defer resp.Body.Close()

	var out slackapi.ConnectionsOpenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decoding apps.connections.open response: %w", err)
	}
	if !out.OK {
		return "", fmt.Errorf("apps.connections.open failed: %s", out.Error)
	}
	if _, err := url.Parse(out.URL); err != nil {
		return "", fmt.Errorf("apps.connections.open returned invalid url %q: %w", out.URL, err)
	}
	return out.URL, nil
}

// gorillaSlackConn adapts a gorilla WebSocket to SlackConn. Reads happen from a
// single goroutine (the session loop); writes (acks) are serialized with a mutex
// since the gorilla connection permits only one concurrent writer.
type gorillaSlackConn struct {
	conn   *websocket.Conn
	writeM sync.Mutex
}

func (c *gorillaSlackConn) Read() (socketmode.Envelope, []byte, error) {
	_, raw, err := c.conn.ReadMessage()
	if err != nil {
		return socketmode.Envelope{}, nil, err
	}
	env, err := socketmode.DecodeEnvelope(raw)
	if err != nil {
		return socketmode.Envelope{}, raw, err
	}
	return env, raw, nil
}

func (c *gorillaSlackConn) Ack(envelopeID string) error {
	c.writeM.Lock()
	defer c.writeM.Unlock()
	return c.conn.WriteJSON(socketmode.Ack{EnvelopeID: envelopeID})
}

func (c *gorillaSlackConn) Close() error { return c.conn.Close() }
