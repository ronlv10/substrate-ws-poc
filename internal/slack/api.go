package slack

// Well-known Slack Web API host and paths. Actors reach these transparently:
// their traffic is redirected to the broker, which synthesizes the bootstrap
// response and forwards everything else to real Slack.
const (
	// APIHost is the Slack Web API host actors dial for HTTPS calls.
	APIHost = "slack.com"

	// PathConnectionsOpen bootstraps a Socket Mode connection.
	PathConnectionsOpen = "/api/apps.connections.open"
)

// ConnectionsOpenResponse is the apps.connections.open response the broker
// synthesizes, returning a wss URL that points back at itself.
type ConnectionsOpenResponse struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error,omitempty"`
}
