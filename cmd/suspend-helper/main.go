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

// Command suspend-helper asks the substrate control plane to checkpoint the
// actor it runs inside. It is a tiny static binary bundled into the (Python)
// echo-actor image and invoked by the bot when it goes idle — Python's gRPC
// stack can't easily skip TLS verification the way the substrate demos do, so
// the actual SuspendActor call lives here in Go, reusing the same
// InsecureSkipVerify path as demos/agent-secret.
//
// It reads the actor's identity from the per-resume identity mount
// (/run/ate/actor-id and /run/ate/atespace, written by atelet) so it addresses
// the right actor even after migrations.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	id, err := os.ReadFile("/run/ate/actor-id")
	if err != nil {
		log.Fatalf("suspend-helper: reading actor id: %v", err)
	}
	atespace, err := os.ReadFile("/run/ate/atespace")
	if err != nil {
		log.Fatalf("suspend-helper: reading atespace: %v", err)
	}
	ref := &ateapipb.ActorRef{
		Atespace: strings.TrimSpace(string(atespace)),
		Name:     strings.TrimSpace(string(id)),
	}

	apiAddr := os.Getenv("ATE_API_ADDR")
	if apiAddr == "" {
		apiAddr = "api.ate-system.svc.cluster.local:443"
	}

	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // matches demos/agent-secret; in-cluster mTLS identity
	conn, err := grpc.NewClient(apiAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("suspend-helper: dialing %s: %v", apiAddr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("suspend-helper: requesting self-suspension for %s/%s\n", ref.GetAtespace(), ref.GetName())
	if _, err := ateapipb.NewControlClient(conn).SuspendActor(ctx, &ateapipb.SuspendActorRequest{ActorRef: ref}); err != nil {
		log.Fatalf("suspend-helper: SuspendActor failed: %v", err)
	}
}
