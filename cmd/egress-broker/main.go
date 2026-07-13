// Command egress-broker wires and serves the broker runtime (internal/broker):
// an always-on service that owns each suspendable actor's persistent Slack
// Socket Mode connection and speaks the gRPC session protocol
// (proto/brokerproxy) to the local proxy inside every actor image.
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

	"github.com/ronlv10/substrate-ws-poc/internal/broker"
	"github.com/ronlv10/substrate-ws-poc/internal/slack"
	brokerproxypb "github.com/ronlv10/substrate-ws-poc/proto/brokerproxy/v1"
)

func main() {
	var (
		grpcListen    = pflag.String("grpc-listen", ":9090", "proxy-facing gRPC listen address")
		ateapiAddr    = pflag.String("ateapi-address", "api.ate-system.svc:443", "substrate Control API address")
		bootOnResume  = pflag.Bool("boot-on-resume", false, "boot actors fresh on resume instead of restoring the checkpoint (defeats warm restore; debugging only)")
		idleGrace     = pflag.Duration("idle-grace", 5*time.Second, "suspend an actor after its proxy stream is quiet for this long (keepalives excluded); 0 disables")
		handlingGrace = pflag.Duration("handling-grace", 90*time.Second, "after an actor acks an inbound event, hold idle-suspend up to this long so the agent can produce its reply; 0 disables")
	)
	pflag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	api, err := dialControl(*ateapiAddr)
	if err != nil {
		log.Error("egress-broker: dialing Control API", slog.Any("err", err))
		os.Exit(1)
	}
	control := broker.NewControlClient(api, *bootOnResume)

	dialer := slack.NewDialer()
	reg := broker.NewRegistry(control, control, dialer, *idleGrace, *handlingGrace, log)

	ln, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Error("egress-broker: listen", slog.String("addr", *grpcListen), slog.Any("err", err))
		os.Exit(1)
	}
	// Proxies ping every 10s to notice broker restarts quickly; permit that
	// even on a quiet stream.
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	brokerproxypb.RegisterBrokerProxyServer(srv, broker.NewServer(reg, dialer, log))

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

// dialControl connects to the substrate Control API. Client identity is carried
// by pod-projected mTLS credentials; an in-cluster client dials with
// InsecureSkipVerify, matching the substrate demos.
func dialControl(addr string) (ateapipb.ControlClient, error) {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return ateapipb.NewControlClient(conn), nil
}
