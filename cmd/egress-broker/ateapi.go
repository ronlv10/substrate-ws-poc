package main

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ActorRef identifies an actor. An actor name is only unique within its
// atespace, so both fields are always carried together.
type ActorRef struct {
	Atespace string
	Name     string
}

func (r ActorRef) String() string { return r.Atespace + "/" + r.Name }

// Resumer ensures a (possibly suspended) actor is running. ResumeActor on the
// substrate control plane blocks until the actor's readyz probe returns 200, so
// a nil error means the actor is live and about to reconnect to the broker.
type Resumer interface {
	Resume(ctx context.Context, ref ActorRef) error
}

// Suspender checkpoints a running actor from the OUTSIDE. Suspend must be driven
// by the broker, not the actor itself: a self-suspend is a checkpoint taken
// mid-call, so the actor's process freezes before SuspendActor returns and the
// checkpoint jams. An external caller is not part of the checkpoint, so it
// completes cleanly.
type Suspender interface {
	Suspend(ctx context.Context, ref ActorRef) error
}

// Locator resolves the actor that owns a given worker-pod source IP. Actor
// egress is SNAT'd behind the worker pod IP, so the broker sees that IP as the
// connection's remote address and maps it back to an actor identity.
type Locator interface {
	LocateByPodIP(ctx context.Context, ip string) (ActorRef, error)
}

// controlClient adapts the substrate Control gRPC service to the Resumer and
// Locator interfaces the broker depends on.
type controlClient struct {
	api    ateapipb.ControlClient
	flight singleflight.Group

	// bootOnResume makes Resume boot the actor fresh from its image instead of
	// restoring its checkpoint. Off by default; only needed for workloads gVisor
	// cannot restore.
	bootOnResume bool
}

func newControlClient(api ateapipb.ControlClient, bootOnResume bool) *controlClient {
	return &controlClient{api: api, bootOnResume: bootOnResume}
}

// Resume deduplicates concurrent resumes of the same actor, detaches from the
// caller's context so one caller giving up does not abort the resume, and retries
// only on Aborted (a concurrent-resume conflict).
func (c *controlClient) Resume(ctx context.Context, ref ActorRef) error {
	ch := c.flight.DoChan(ref.String(), func() (any, error) {
		bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		backoff := wait.Backoff{Steps: 7, Duration: 200 * time.Millisecond, Factor: 1.5, Jitter: 0.2}
		return nil, wait.ExponentialBackoffWithContext(bgCtx, backoff, func(ctx context.Context) (bool, error) {
			_, err := c.api.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: ref.Atespace, Name: ref.Name},
				Boot:     c.bootOnResume,
			})
			if err == nil {
				return true, nil
			}
			if status.Code(err) == codes.Aborted {
				return false, nil // concurrent resume, retry
			}
			return false, err
		})
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		return res.Err
	}
}

// Suspend checkpoints the actor from outside. A nil error means the checkpoint
// was accepted; the actor's broker-facing WebSocket then dies as it freezes,
// which the session observes as a Detach.
func (c *controlClient) Suspend(ctx context.Context, ref ActorRef) error {
	_, err := c.api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: ref.Atespace, Name: ref.Name},
	})
	return err
}

// LocateByPodIP finds the RUNNING actor whose assigned worker pod IP matches ip.
// It is a linear scan over all actors — used only as a fallback when the actor
// does not announce its own identity, so the O(actors) cost is acceptable.
func (c *controlClient) LocateByPodIP(ctx context.Context, ip string) (ActorRef, error) {
	var pageToken string
	for {
		resp, err := c.api.ListActors(ctx, &ateapipb.ListActorsRequest{PageSize: 1000, PageToken: pageToken})
		if err != nil {
			return ActorRef{}, fmt.Errorf("listing actors: %w", err)
		}
		for _, a := range resp.GetActors() {
			if a.GetAteomPodIp() == ip && a.GetStatus() == ateapipb.Actor_STATUS_RUNNING {
				return ActorRef{Atespace: a.GetAtespace(), Name: a.GetActorId()}, nil
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return ActorRef{}, fmt.Errorf("no running actor found at pod IP %q", ip)
		}
	}
}
