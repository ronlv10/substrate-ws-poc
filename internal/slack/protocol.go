// Package slack is the Slack egress provider: the Socket Mode wire types, the
// Web API shapes the broker synthesizes, the upstream connection to real Slack,
// and the Provider that ties them together. It is the one place the broker's
// Slack specifics live; the broker core is written against a provider interface.
//
// Reference: https://docs.slack.dev/apis/events-api/using-socket-mode
package slack

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

// DecodeEnvelope parses a raw Socket Mode frame.
func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("slack: decoding envelope: %w", err)
	}
	return e, nil
}
