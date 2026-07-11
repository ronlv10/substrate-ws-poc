package slack

const (
	APIHost             = "slack.com"
	PathConnectionsOpen = "/api/apps.connections.open" // bootstraps a Socket Mode connection
)

// ConnectionsOpenResponse is the apps.connections.open reply the broker
// synthesizes, pointing the wss URL back at itself.
type ConnectionsOpenResponse struct {
	OK    bool   `json:"ok"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error,omitempty"`
}
