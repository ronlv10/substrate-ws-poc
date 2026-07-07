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

// Package socketmode implements the small subset of the Slack Socket Mode wire
// protocol the WS-PoC PoC needs, on both sides:
//
//   - the broker speaks it as a client to real Slack (holding the persistent
//     connection), and
//   - the broker speaks it as a server to the actor (impersonating Slack), while
//     the echo actor speaks it as a client (believing it is talking to Slack).
//
// The protocol is a JSON-envelope exchange over a WebSocket. On connect the
// server sends a "hello"; thereafter it sends event envelopes carrying an
// envelope_id that the client must acknowledge by echoing the id back. The
// server may send a "disconnect" asking the client to reconnect. Only event
// envelopes represent real work; hello/disconnect and WebSocket ping/pong are
// connection-management traffic that must not wake a suspended actor.
//
// Reference: https://docs.slack.dev/apis/events-api/using-socket-mode
package socketmode

import (
	"encoding/json"
	"fmt"
)

// Envelope types sent by the Slack Socket Mode server.
const (
	// TypeHello is sent once when a connection is established.
	TypeHello = "hello"
	// TypeDisconnect asks the client to close and reconnect.
	TypeDisconnect = "disconnect"
	// TypeEventsAPI carries an Events API payload (e.g. a message).
	TypeEventsAPI = "events_api"
	// TypeSlashCommands carries a slash-command invocation.
	TypeSlashCommands = "slash_commands"
	// TypeInteractive carries an interactive component payload.
	TypeInteractive = "interactive"
)

// Envelope is a Socket Mode frame. Only the fields the PoC needs are modeled;
// unknown fields are ignored on decode and omitted on encode.
type Envelope struct {
	Type string `json:"type,omitempty"`

	// EnvelopeID is present on event envelopes and must be echoed back to ack.
	EnvelopeID string `json:"envelope_id,omitempty"`

	// AcceptsResponsePayload indicates the ack may carry a response payload.
	AcceptsResponsePayload bool `json:"accepts_response_payload,omitempty"`

	// Payload is the type-specific body (e.g. an Events API callback).
	Payload json.RawMessage `json:"payload,omitempty"`

	// NumConnections is set on hello frames.
	NumConnections int `json:"num_connections,omitempty"`

	// Reason is set on disconnect frames (e.g. "warning", "refresh_requested").
	Reason string `json:"reason,omitempty"`
}

// IsConnectionManagement reports whether the envelope is Slack keepalive /
// lifecycle traffic (hello or disconnect) that must NOT resume a suspended
// actor. WebSocket ping/pong frames are handled at the transport layer and
// never surface as envelopes, so they are inherently excluded.
func (e Envelope) IsConnectionManagement() bool {
	return e.Type == TypeHello || e.Type == TypeDisconnect
}

// IsEvent reports whether the envelope carries a real event that must be
// delivered to the actor and acknowledged.
func (e Envelope) IsEvent() bool {
	switch e.Type {
	case TypeEventsAPI, TypeSlashCommands, TypeInteractive:
		return true
	default:
		return false
	}
}

// Ack is the acknowledgement a client sends back for an event envelope. Slack
// expects the ack within ~3 seconds or it redelivers the event, so the broker
// acks on the actor's behalf immediately and buffers the event.
type Ack struct {
	EnvelopeID string          `json:"envelope_id"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// DecodeEnvelope parses a raw Socket Mode frame.
func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("socketmode: decoding envelope: %w", err)
	}
	return e, nil
}

// EventCallback is the Events API payload wrapper carried in an events_api
// envelope's Payload.
type EventCallback struct {
	Type      string       `json:"type"` // typically "event_callback"
	TeamID    string       `json:"team_id"`
	APIAppID  string       `json:"api_app_id"`
	Event     MessageEvent `json:"event"`
	EventID   string       `json:"event_id"`
	EventTime int64        `json:"event_time"`
}

// MessageEvent is the subset of a Slack message event the echo actor reads.
type MessageEvent struct {
	Type        string `json:"type"` // e.g. "message", "app_mention"
	Subtype     string `json:"subtype,omitempty"`
	Channel     string `json:"channel"`
	ChannelType string `json:"channel_type,omitempty"`
	User        string `json:"user,omitempty"`
	BotID       string `json:"bot_id,omitempty"`
	Text        string `json:"text"`
	TS          string `json:"ts"`
	ThreadTS    string `json:"thread_ts,omitempty"`
}

// ParseEventCallback extracts the Events API callback from an events_api
// envelope. It returns an error for non-events_api envelopes.
func (e Envelope) ParseEventCallback() (EventCallback, error) {
	if e.Type != TypeEventsAPI {
		return EventCallback{}, fmt.Errorf("socketmode: envelope type %q is not %q", e.Type, TypeEventsAPI)
	}
	var cb EventCallback
	if err := json.Unmarshal(e.Payload, &cb); err != nil {
		return EventCallback{}, fmt.Errorf("socketmode: decoding event callback: %w", err)
	}
	return cb, nil
}

// IsHumanMessage reports whether the event is a plain user-authored message
// (not a bot echo, edit, join notice, or other subtype). The echo actor uses
// this to avoid replying to its own messages, which would loop forever.
func (m MessageEvent) IsHumanMessage() bool {
	if m.Type != "message" && m.Type != "app_mention" {
		return false
	}
	if m.Subtype != "" { // message_changed, bot_message, channel_join, ...
		return false
	}
	if m.BotID != "" || m.User == "" {
		return false
	}
	return true
}
