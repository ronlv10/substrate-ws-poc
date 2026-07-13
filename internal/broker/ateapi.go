package broker

import (
	"context"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ActorRef identifies an actor; a name is unique only within its atespace.
type ActorRef struct {
	Atespace string
	Name     string
}

func (r ActorRef) String() string { return r.Atespace + "/" + r.Name }

// Resumer runs a possibly-suspended actor. Resume blocks until the actor's readyz
// returns 200, so a nil error means it is live and about to reconnect.
type Resumer interface {
	Resume(ctx context.Context, ref ActorRef) error
}

// Suspender checkpoints a running actor. It must be driven from outside, not by
// the actor: a self-suspend freezes the process mid-call and the checkpoint jams.
type Suspender interface {
	Suspend(ctx context.Context, ref ActorRef) error
}

type controlClient struct {
	api    ateapipb.ControlClient
	flight singleflight.Group

	// bootOnResume boots the actor fresh instead of restoring its checkpoint;
	// only for workloads gVisor cannot restore.
	bootOnResume bool
}

func NewControlClient(api ateapipb.ControlClient, bootOnResume bool) *controlClient {
	return &controlClient{api: api, bootOnResume: bootOnResume}
}

// Resume dedupes concurrent resumes of the same actor, runs on a detached context
// so one caller giving up does not abort it, and retries only on Aborted.
func (c *controlClient) Resume(ctx context.Context, ref ActorRef) error {
	ch := c.flight.DoChan(ref.String(), func() (any, error) {
		// Must exceed substrate's readyz gate (the ResumeActor RPC blocks until
		// the restored agent answers /readyz) plus a cold restore's GCS snapshot
		// pull, which alone can run past 30s on a worker that hasn't cached it.
		bgCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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

// Suspend checkpoints the actor; its WebSocket then dies as it freezes, which the
// session observes as a Detach.
func (c *controlClient) Suspend(ctx context.Context, ref ActorRef) error {
	_, err := c.api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: ref.Atespace, Name: ref.Name},
	})
	return err
}
