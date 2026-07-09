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

// Package socketmode holds the small set of Slack Socket Mode wire types the
// broker uses: it classifies frames arriving from Slack (real events vs.
// lifecycle traffic), encodes the hello it sends the actor on connect, and
// decodes the ack the actor echoes back for a delivered event.
//
// A frame is a JSON envelope. Event envelopes carry an envelope_id that must be
// echoed to acknowledge; hello and disconnect are lifecycle traffic that must not
// wake a suspended actor.
//
// Reference: https://docs.slack.dev/apis/events-api/using-socket-mode
package socketmode

import (
	"encoding/json"
	"fmt"
)

const (
	// TypeHello is sent once when a connection is established.
	TypeHello = "hello"
	// TypeDisconnect asks the client to close and reconnect.
	TypeDisconnect = "disconnect"
	// TypeEventsAPI carries an Events API payload (e.g. a message).
	TypeEventsAPI = "events_api"
)

// Envelope is the subset of a Socket Mode frame the broker reads or writes.
type Envelope struct {
	Type string `json:"type,omitempty"`

	// EnvelopeID is present on event envelopes and must be echoed back to ack.
	EnvelopeID string `json:"envelope_id,omitempty"`

	// NumConnections is set on hello frames.
	NumConnections int `json:"num_connections,omitempty"`
}

// IsConnectionManagement reports whether the envelope is lifecycle traffic (hello
// or disconnect) that must NOT resume a suspended actor. WebSocket ping/pong is
// handled at the transport layer and never surfaces as an envelope.
func (e Envelope) IsConnectionManagement() bool {
	return e.Type == TypeHello || e.Type == TypeDisconnect
}

// IsEvent reports whether the envelope carries a real event that must be
// delivered to the actor and acknowledged.
func (e Envelope) IsEvent() bool {
	return e.Type == TypeEventsAPI
}

// DecodeEnvelope parses a raw Socket Mode frame.
func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("socketmode: decoding envelope: %w", err)
	}
	return e, nil
}
