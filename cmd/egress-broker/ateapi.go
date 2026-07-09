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

package main

import (
	"context"
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

// controlClient adapts the substrate Control gRPC service to the Resumer and
// Suspender interfaces the broker depends on.
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

