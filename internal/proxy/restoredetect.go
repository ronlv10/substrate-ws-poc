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
