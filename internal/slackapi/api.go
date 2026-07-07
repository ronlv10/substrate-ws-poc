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

// Package slackapi models the small slice of the Slack Web API the WS-PoC PoC
// touches: the Socket Mode bootstrap (apps.connections.open) and posting the
// echo reply (chat.postMessage). These shapes are used both by the broker (to
// synthesize / forward API calls it intercepts) and by the echo actor (to make
// them).
package slackapi

// Well-known Slack hosts and paths. Actors reach these transparently: cluster
// DNS points them at the broker, which terminates TLS and either synthesizes
// the response (apps.connections.open) or forwards to real Slack.
const (
	// APIHost is the Slack Web API host actors dial for HTTPS calls.
	APIHost = "slack.com"

	// PathConnectionsOpen bootstraps a Socket Mode connection.
	PathConnectionsOpen = "/api/apps.connections.open"

	// PathChatPostMessage posts a message to a channel.
	PathChatPostMessage = "/api/chat.postMessage"
)

// ConnectionsOpenResponse is the apps.connections.open response. The broker
// synthesizes this, returning a wss URL that points back at itself so the
// actor's Socket Mode WebSocket lands on the broker.
type ConnectionsOpenResponse struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error,omitempty"`
}

// ChatPostMessageRequest is the subset of chat.postMessage arguments the echo
// actor sends. Slack accepts this as JSON or as form-encoded; the actor uses
// JSON with a bot-token bearer credential.
type ChatPostMessageRequest struct {
	Channel  string `json:"channel"`
	Text     string `json:"text"`
	ThreadTS string `json:"thread_ts,omitempty"`
}

// ChatPostMessageResponse is the subset of the chat.postMessage response the
// echo actor checks.
type ChatPostMessageResponse struct {
	OK      bool   `json:"ok"`
	Channel string `json:"channel,omitempty"`
	TS      string `json:"ts,omitempty"`
	Error   string `json:"error,omitempty"`
}
