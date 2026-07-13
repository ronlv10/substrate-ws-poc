// local-proxy impersonates Slack on loopback for a co-resident stock agent and
// relays events and Web API calls to the egress broker. It runs one of two ways:
//
//   - PID 1 supervising the agent, given as argv after "--" (single-container
//     actor); the proxy binds its listeners before spawning the agent.
//   - Standalone in its own sidecar container (no argv): it just serves, and the
//     agent — in a sibling container sharing the sandbox loopback — connects over
//     127.0.0.1. The agent reaches it because the agent image's /etc/hosts points
//     slack.com at loopback; readiness is this container's /readyz.
//
// With no --broker-address it runs a broker-less standalone loop: Web API stubs
// plus synthetic event injection, for exercising the agent-facing half on its own.
package main

import (
	"crypto/tls"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/ronlv10/substrate-ws-poc/internal/proxy"
)

func main() {
	var (
		tlsListen    = pflag.String("tls-listen", "127.0.0.1:443", "agent-facing HTTPS listen address")
		readyzListen = pflag.String("readyz-listen", ":80", "readiness probe listen address")
		certFile     = pflag.String("cert", "/etc/local-proxy/tls/tls.crt", "TLS cert for the slack.com SNI")
		keyFile      = pflag.String("key", "/etc/local-proxy/tls/tls.key", "TLS key")
		wssHost      = pflag.String("wss-host", "slack.com", "host in the synthesized wss URL (must resolve to loopback in the image)")
		brokerAddr   = pflag.String("broker-address", "", "egress broker gRPC address; empty runs standalone")
		identityDir  = pflag.String("identity-dir", "/run/ate", "substrate per-resume identity mount")
		injectDelay  = pflag.Duration("standalone-inject-delay", 5*time.Second, "standalone: delay before the boot event injection")
	)
	pflag.Parse()
	agentArgv := pflag.Args()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	core := proxy.NewCore(log)
	face := proxy.NewSlackFace(core, *wssHost, log)

	go func() {
		srv := &http.Server{
			Addr:      *tlsListen,
			Handler:   face.Handler(),
			TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		}
		log.Info("local-proxy: agent-facing TLS listening", slog.String("addr", *tlsListen))
		if err := srv.ListenAndServeTLS(*certFile, *keyFile); err != nil {
			log.Error("local-proxy: TLS server failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()
	go func() {
		log.Info("local-proxy: readyz listening", slog.String("addr", *readyzListen))
		if err := http.ListenAndServe(*readyzListen, face.ReadyzHandler()); err != nil {
			log.Error("local-proxy: readyz server failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	if *brokerAddr == "" {
		log.Info("local-proxy: standalone mode (no broker)")
		proxy.NewStandalone(core, log).Start(*injectDelay)
	} else {
		bc := proxy.NewBrokerClient(core, *brokerAddr, *identityDir, log)
		go bc.Run()
		go proxy.RestoreDetector(log, func(time.Duration) { bc.Redial() })
	}

	if len(agentArgv) == 0 {
		// Sidecar mode: the agent runs in a sibling container, so there is nothing
		// to supervise here — serve until the sandbox is torn down.
		log.Info("local-proxy: sidecar mode; serving until terminated")
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		os.Exit(0)
	}
	os.Exit(proxy.RunAgent(log, agentArgv))
}
