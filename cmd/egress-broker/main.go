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

// Command egress-broker is the WebSocket egress broker: an always-on service
// that owns the persistent Slack Socket Mode connection on behalf of suspendable
// substrate actors.
//
// Actors believe they dial slack.com directly; cluster DNS points those
// hostnames at this broker, which terminates TLS with a per-SNI certificate
// signed by a CA the actors trust. The broker captures the app-level token from
// the actor's apps.connections.open, holds the real Slack connection itself,
// filters keepalive/hello frames, and — when a real message arrives while the
// actor is suspended — resumes the actor via the substrate Control API and
// delivers the buffered event when the actor's connection signals ready.
//
// See README.md for the full design.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var (
		listenAddr   = pflag.String("listen", ":443", "TLS listen address for actor-facing traffic")
		caCertFile   = pflag.String("ca-cert", "/etc/ws-poc/ca/tls.crt", "Broker CA certificate (PEM) used to mint per-SNI leaves")
		caKeyFile    = pflag.String("ca-key", "/etc/ws-poc/ca/tls.key", "Broker CA private key (PEM)")
		dnsUpstream  = pflag.String("dns-upstream", "8.8.8.8:53", "Upstream DNS used to reach REAL Slack, bypassing the cluster DNS redirect")
		slackAPIBase = pflag.String("slack-api-base", "https://slack.com", "Base URL for real Slack API (override for testing)")
		wssHost      = pflag.String("wss-host", "wss-primary.slack.com", "Host embedded in the synthesized wss URL; must be DNS-mapped to the broker")
		wssPath      = pflag.String("wss-path", "/ws-poc/socketmode", "Path the actor's Socket Mode WebSocket connects to")
		ateapiAddr   = pflag.String("ateapi-address", "api.ate-system.svc:443", "Substrate Control API (ateapi) address")
		bootOnResume = pflag.Bool("boot-on-resume", false, "Boot actors fresh on resume instead of restoring the checkpoint")
		deliverDelay = pflag.Duration("deliver-delay", 0, "Fallback: deliver buffered events this long after (re)connect even if no heartbeat is seen. Normally delivery is triggered by the client's first heartbeat (its connected:ready signal). 0 = heartbeat-only")
		idleGrace    = pflag.Duration("idle-grace", 5*time.Second, "Suspend an actor after its broker-facing connection is quiet in both directions (no event, ack, or forwarded API call; keepalive pings excluded) for this long. 0 disables broker-driven suspend")
	)
	pflag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	minter, err := newCertMinterFromFiles(*caCertFile, *caKeyFile)
	if err != nil {
		log.Error("egress-broker: loading broker CA", slog.Any("err", err))
		os.Exit(1)
	}

	api, err := dialControl(*ateapiAddr)
	if err != nil {
		log.Error("egress-broker: dialing Control API", slog.Any("err", err))
		os.Exit(1)
	}
	control := newControlClient(api, *bootOnResume)

	realSlack := newRealSlackDialer(*dnsUpstream, *slackAPIBase)
	reg := NewRegistry(control, control, realSlack, *deliverDelay, *idleGrace, log)
	srv := NewServer(reg, control, realSlack, *wssHost, *wssPath, log)

	httpServer := &http.Server{
		Addr:      *listenAddr,
		Handler:   srv,
		TLSConfig: minter.TLSConfig(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("egress-broker: shutting down")
		_ = httpServer.Close()
	}()

	log.Info("egress-broker: broker listening", slog.String("addr", *listenAddr), slog.String("wss_host", *wssHost))
	// Cert and key are provided by the minter's GetCertificate, so the file
	// arguments are empty.
	if err := httpServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("egress-broker: broker server exited", slog.Any("err", err))
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
