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
	"log/slog"
	"time"
)

// RestoreDetector notices checkpoint/restore from the inside: a ticker that
// observes a multi-second gap between 250ms ticks can only have been frozen.
// gVisor advances both the wall and monotonic clocks by the suspend duration
// on restore (verified in the phase-0 spike), so a plain time comparison works
// and no substrate hook is needed.
func RestoreDetector(log *slog.Logger, onRestore func(gap time.Duration)) {
	const tick = 250 * time.Millisecond
	const threshold = 2 * time.Second
	prev := time.Now()
	for {
		time.Sleep(tick)
		now := time.Now()
		if gap := now.Sub(prev); gap > threshold {
			log.Info("local-proxy: restore detected", slog.Duration("gap", gap))
			onRestore(gap)
		}
		prev = now
	}
}
