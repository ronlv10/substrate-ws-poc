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

// Command egress-broker is an always-on service that owns the persistent
// Slack Socket Mode connection on behalf of suspendable substrate actors.
//
// Each actor image ships a local proxy that impersonates Slack for its
// co-resident agent over loopback; the proxy speaks this broker's gRPC
// Session protocol (proto/brokerproxy). The broker holds the real Slack
// connection, filters keepalive traffic, buffers events, resumes suspended
// actors via the substrate Control API when a real message arrives, and
// suspends idle ones. Identity comes from the proxy's Announce — the broker
// has no actor-facing TLS, no DNS rewrite, and no source-IP correlation.
//
// See README.md for the full design.
package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	brokerproxypb "github.com/ronlv10/substrate-ws-poc/proto/brokerproxy/v1"
)

func main() {
	var (
		grpcListen   = pflag.String("grpc-listen", ":9090", "Proxy-facing gRPC listen address")
		dnsUpstream  = pflag.String("dns-upstream", "8.8.8.8:53", "Upstream DNS used to reach real Slack (independent of cluster DNS)")
		slackAPIBase = pflag.String("slack-api-base", "https://slack.com", "Base URL for real Slack API (override for testing)")
		ateapiAddr   = pflag.String("ateapi-address", "api.ate-system.svc:443", "Substrate Control API (ateapi) address")
		bootOnResume = pflag.Bool("boot-on-resume", false, "Boot actors fresh on resume instead of restoring the checkpoint (defeats warm restore; debugging only)")
		idleGrace    = pflag.Duration("idle-grace", 5*time.Second, "Suspend an actor after its proxy stream is quiet in both directions (no event, ack, or relayed API call; keepalives excluded) for this long. 0 disables broker-driven suspend")
	)
	pflag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	api, err := dialControl(*ateapiAddr)
	if err != nil {
		log.Error("egress-broker: dialing Control API", slog.Any("err", err))
		os.Exit(1)
	}
	control := newControlClient(api, *bootOnResume)

	realSlack := newRealSlackDialer(*dnsUpstream, *slackAPIBase)
	reg := NewRegistry(control, control, realSlack, *idleGrace, log)

	ln, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Error("egress-broker: listen", slog.String("addr", *grpcListen), slog.Any("err", err))
		os.Exit(1)
	}
	// Proxies ping every 10s so a broker restart is noticed quickly; permit
	// that even on a quiet stream.
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	brokerproxypb.RegisterBrokerProxyServer(srv, NewGRPCServer(reg, realSlack, log))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("egress-broker: shutting down")
		srv.GracefulStop()
	}()

	log.Info("egress-broker: serving proxy sessions", slog.String("addr", *grpcListen))
	if err := srv.Serve(ln); err != nil {
		log.Error("egress-broker: serve exited", slog.Any("err", err))
		os.Exit(1)
	}
}

// dialControl connects to the substrate Control API (ateapi). Client identity is
// carried by the pod-projected mTLS credentials; the server verifies them but
// (in mtls mode) performs no app-level authorization, so an in-cluster client
// dials with InsecureSkipVerify — matching the substrate demos.
func dialControl(addr string) (ateapipb.ControlClient, error) {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // in-cluster mTLS identity
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return ateapipb.NewControlClient(conn), nil
}
