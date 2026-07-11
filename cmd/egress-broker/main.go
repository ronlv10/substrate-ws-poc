// Command egress-broker is an always-on service that holds a suspendable actor's
// persistent Slack connection. The actor's Slack traffic is redirected here (TLS
// terminated with a cert the actor trusts); the broker captures its app token,
// holds the real Slack connection, and on an incoming message resumes the actor
// via the substrate Control API and delivers the event once it reconnects.
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
	"github.com/ronlv10/substrate-ws-poc/internal/broker"
	"github.com/ronlv10/substrate-ws-poc/internal/slack"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var (
		listenAddr   = pflag.String("listen", ":443", "TLS listen address for actor-facing traffic")
		caCertFile   = pflag.String("ca-cert", "/etc/ws-poc/ca/tls.crt", "Broker TLS certificate (PEM) served to actors on the redirected Slack hostnames")
		caKeyFile    = pflag.String("ca-key", "/etc/ws-poc/ca/tls.key", "Broker TLS private key (PEM)")
		ateapiAddr   = pflag.String("ateapi-address", "api.ate-system.svc:443", "Substrate Control API (ateapi) address")
		bootOnResume = pflag.Bool("boot-on-resume", false, "Boot actors fresh on resume instead of restoring the checkpoint")
		idleGrace    = pflag.Duration("idle-grace", 5*time.Second, "Suspend an actor after its broker-facing connection is quiet in both directions (no event, ack, or forwarded API call; keepalive pings excluded) for this long. 0 disables broker-driven suspend")
	)
	pflag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cert, err := tls.LoadX509KeyPair(*caCertFile, *caKeyFile)
	if err != nil {
		log.Error("egress-broker: loading broker TLS certificate", slog.Any("err", err))
		os.Exit(1)
	}

	api, err := dialControl(*ateapiAddr)
	if err != nil {
		log.Error("egress-broker: dialing Control API", slog.Any("err", err))
		os.Exit(1)
	}
	control := broker.NewControlClient(api, *bootOnResume)

	dialer := slack.NewDialer()
	reg := broker.NewRegistry(control, control, dialer, *idleGrace, log)
	srv := broker.NewServer(reg, dialer, log)

	httpServer := &http.Server{
		Addr:      *listenAddr,
		Handler:   srv,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("egress-broker: shutting down")
		_ = httpServer.Close()
	}()

	log.Info("egress-broker: broker listening", slog.String("addr", *listenAddr))
	// The certificate is set on TLSConfig above, so the file arguments are empty.
	if err := httpServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("egress-broker: broker server exited", slog.Any("err", err))
		os.Exit(1)
	}
}

// dialControl connects to the substrate Control API (ateapi) over in-cluster mTLS.
func dialControl(addr string) (ateapipb.ControlClient, error) {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // in-cluster mTLS identity
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return ateapipb.NewControlClient(conn), nil
}
