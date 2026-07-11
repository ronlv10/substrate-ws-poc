// Package slack holds the Slack wire types the egress broker speaks: Socket Mode
// envelopes, the Web API shapes it synthesizes, and the connection to real Slack.
//
// https://docs.slack.dev/apis/events-api/using-socket-mode
package slack

import (
	"encoding/json"
	"fmt"
)

const (
	TypeHello      = "hello"
	TypeDisconnect = "disconnect" // Slack asks the client to reconnect
	TypeEventsAPI  = "events_api"
)

// Envelope is the subset of a Socket Mode frame the broker reads or writes.
type Envelope struct {
	Type           string `json:"type,omitempty"`
	EnvelopeID     string `json:"envelope_id,omitempty"` // echoed back to ack an event
	NumConnections int    `json:"num_connections,omitempty"`
}

func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("slack: decoding envelope: %w", err)
	}
	return e, nil
}
