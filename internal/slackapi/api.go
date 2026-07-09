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

// Package slackapi holds the Slack Web API shapes the broker synthesizes for the
// Socket Mode bootstrap (apps.connections.open) it intercepts, plus the well-known
// host and paths it routes on.
package slackapi

// Well-known Slack hosts and paths. Actors reach these transparently: cluster
// DNS points them at the broker, which terminates TLS and either synthesizes
// the response (apps.connections.open) or forwards to real Slack.
const (
	// APIHost is the Slack Web API host actors dial for HTTPS calls.
	APIHost = "slack.com"

	// PathConnectionsOpen bootstraps a Socket Mode connection.
	PathConnectionsOpen = "/api/apps.connections.open"
)

// ConnectionsOpenResponse is the apps.connections.open response. The broker
// synthesizes this, returning a wss URL that points back at itself so the
// actor's Socket Mode WebSocket lands on the broker.
type ConnectionsOpenResponse struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error,omitempty"`
}
