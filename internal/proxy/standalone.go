// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Standalone drives the proxy with no broker: it stubs the Web API and
// injects synthetic events, one shortly after start and one after every
// detected restore. It exists to prove the whole agent-facing half —
// loopback TLS, Socket Mode serving, hold-until-heartbeat delivery, acks —
// in isolation on a cluster, before the broker protocol enters the picture.
type Standalone struct {
	core *Core
	log  *slog.Logger
	seq  atomic.Uint64
}

func NewStandalone(core *Core, log *slog.Logger) *Standalone {
	s := &Standalone{core: core, log: log}
	core.SetEgress(s.egress)
	return s
}

// Start arms the boot injection and the restore-triggered injections.
func (s *Standalone) Start(injectDelay time.Duration) {
	go func() {
		time.Sleep(injectDelay)
		s.inject("boot")
	}()
	go RestoreDetector(s.log, func(time.Duration) {
		// Give the thawed client a beat; delivery is held until its
		// heartbeat anyway, this just keeps the logs readable.
		time.Sleep(time.Second)
		s.inject("post-restore")
	})
}

func (s *Standalone) inject(reason string) {
	seq := s.seq.Add(1)
	now := time.Now()
	env := map[string]any{
		"type":                     "events_api",
		"envelope_id":              fmt.Sprintf("standalone-env-%d", seq),
		"accepts_response_payload": false,
		"payload": map[string]any{
			"token":      "standalone-token",
			"team_id":    "T00000001",
			"api_app_id": "A00000001",
			"type":       "event_callback",
			"event_id":   fmt.Sprintf("Ev%08d", seq),
			"event_time": now.Unix(),
			"event": map[string]any{
				"type":         "message",
				"channel":      "C00000001",
				"channel_type": "channel",
				"user":         "U00000002",
				"text":         fmt.Sprintf("ping %d (%s)", seq, reason),
				"ts":           fmt.Sprintf("%d.%06d", now.Unix(), seq),
			},
		},
	}
	raw, _ := json.Marshal(env)
	s.log.Info("local-proxy: standalone inject", slog.Uint64("seq", seq), slog.String("reason", reason))
	if err := s.core.Deliver(seq, raw); err != nil {
		s.log.Warn("local-proxy: standalone inject failed", slog.Any("error", err))
	}
}

// egress stubs the two Web API calls a stock Bolt echo bot makes.
func (s *Standalone) egress(method, path string, header http.Header, body []byte) (*EgressResult, error) {
	ok := func(v any) (*EgressResult, error) {
		b, _ := json.Marshal(v)
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		return &EgressResult{Status: http.StatusOK, Header: h, Body: b}, nil
	}
	switch {
	case strings.HasPrefix(path, "/api/auth.test"):
		return ok(map[string]any{
			"ok": true, "url": "https://slack.com/", "team": "standalone",
			"user": "standalonebot", "team_id": "T00000001",
			"user_id": "U00000001", "bot_id": "B00000001",
		})
	case strings.HasPrefix(path, "/api/chat.postMessage"):
		s.log.Info("local-proxy: standalone reply captured", slog.String("body", string(body)))
		return ok(map[string]any{
			"ok": true, "channel": "C00000001",
			"ts": fmt.Sprintf("%d.000200", time.Now().Unix()),
		})
	default:
		return ok(map[string]any{"ok": true})
	}
}
