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

package socketmode

import "testing"

func TestDecodeEnvelopeClassification(t *testing.T) {
	tests := []struct {
		name           string
		raw            string
		wantType       string
		wantConnMgmt   bool
		wantEvent      bool
		wantEnvelopeID string
	}{
		{
			name:         "hello is connection management, not an event",
			raw:          `{"type":"hello","num_connections":1}`,
			wantType:     TypeHello,
			wantConnMgmt: true,
			wantEvent:    false,
		},
		{
			name:         "disconnect is connection management, not an event",
			raw:          `{"type":"disconnect","reason":"refresh_requested"}`,
			wantType:     TypeDisconnect,
			wantConnMgmt: true,
			wantEvent:    false,
		},
		{
			name:           "events_api is a real event that must be acked",
			raw:            `{"type":"events_api","envelope_id":"env-1","payload":{"type":"event_callback"}}`,
			wantType:       TypeEventsAPI,
			wantConnMgmt:   false,
			wantEvent:      true,
			wantEnvelopeID: "env-1",
		},
		{
			name:           "slash_commands is not forwarded (broker echoes messages only)",
			raw:            `{"type":"slash_commands","envelope_id":"env-2"}`,
			wantType:       "slash_commands",
			wantConnMgmt:   false,
			wantEvent:      false,
			wantEnvelopeID: "env-2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := DecodeEnvelope([]byte(tc.raw))
			if err != nil {
				t.Fatalf("DecodeEnvelope() error = %v", err)
			}
			if env.Type != tc.wantType {
				t.Errorf("Type = %q, want %q", env.Type, tc.wantType)
			}
			if got := env.IsConnectionManagement(); got != tc.wantConnMgmt {
				t.Errorf("IsConnectionManagement() = %v, want %v", got, tc.wantConnMgmt)
			}
			if got := env.IsEvent(); got != tc.wantEvent {
				t.Errorf("IsEvent() = %v, want %v", got, tc.wantEvent)
			}
			if env.EnvelopeID != tc.wantEnvelopeID {
				t.Errorf("EnvelopeID = %q, want %q", env.EnvelopeID, tc.wantEnvelopeID)
			}
		})
	}
}
