package proxy

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func newTestBrokerClient() *BrokerClient {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewBrokerClient(NewCore(log), "unused:0", "/run/ate", log)
}

func TestWaitBeforeReconnectBacksOffOnFailure(t *testing.T) {
	bc := newTestBrokerClient()
	start := time.Now()
	next := bc.waitBeforeReconnect(40 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("did not wait out the backoff: %s", elapsed)
	}
	if next != 80*time.Millisecond {
		t.Fatalf("backoff should double: got %s", next)
	}
}

func TestRedialWakesReconnectImmediatelyAndResetsBackoff(t *testing.T) {
	bc := newTestBrokerClient()
	bc.Redial() // stands in for a restore: buffers a wake

	start := time.Now()
	next := bc.waitBeforeReconnect(30 * time.Second) // would sleep 30s without the wake
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("wake did not cut the sleep short: waited %s", elapsed)
	}
	if next != time.Second {
		t.Fatalf("wake should reset backoff to 1s: got %s", next)
	}
}
