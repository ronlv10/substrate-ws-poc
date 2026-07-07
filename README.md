# WS-PoC — a Slack egress broker for suspendable agents

WS-PoC is a proof of concept for running an **always-on Slack agent on
agent-substrate while it is suspended almost all of the time**. A normal Slack
bot holds a long-lived WebSocket (Socket Mode) and idles waiting for messages.
On substrate, suspending an actor is a process checkpoint that frees the worker
and **destroys every socket the actor holds** — so an actor can never keep a
Slack WebSocket alive across a suspend.

WS-PoC solves this by **inverting socket ownership**. An always-on *egress
broker* owns the persistent connection to Slack. The actor still believes it
dials `slack.com` directly; its traffic is transparently redirected to the
broker, which terminates TLS with a certificate the actor trusts and holds the
real Slack socket itself. While the actor is suspended the broker keeps the
Slack connection open and filters keepalive frames. When a real message
arrives, the broker resumes the actor, the actor reconnects, and the broker
delivers the buffered message. The actor replies `echo: <message>` and suspends
itself again.

This is a concrete first step toward the "AgentGateway" egress phase already
flagged in the codebase (`cmd/ateom-gvisor/main.go` — "the later AgentGateway
phase should replace the broad masquerade path with transparent TCP capture").

## Why this is the only shape that works

- Suspend is `runsc` checkpoint/restore (substrate runs it with
  `--allow-connected-on-save`). An established outbound TCP cannot be restored
  onto a different worker with a different pod IP, so the socket is dead after a
  restore — the process must **reconnect**. Socket Mode clients already
  reconnect on `disconnect`, so a stock bot needs no changes.
- Therefore the durable Slack connection must live **outside** the actor. The
  broker owns it; the actor's connection to the broker is ephemeral and
  re-established on each resume.
- `Control.ResumeActor` blocks until the actor's `/readyz` returns 200
  (`internal/readyz`), so "resume, then the actor reconnects and we deliver" is
  race-free by construction.
- All actor↔broker traffic is actor-initiated **egress**, so the broker never
  needs the inbound atenet router or the actor's pod IP to deliver a message.

## Architecture

```
                       persistent, survives actor suspend
  Slack  <===== wss (real TLS, broker holds it) =====>  BROKER  ── gRPC ResumeActor ──>  ateapi
                                                            │   (in-cluster Deployment;
   actor believes it dials slack.com;                       │    reaches real Slack via its
   cluster DNS points slack.com at the broker; broker        │    own upstream resolver)
   terminates TLS with a per-SNI cert the actor trusts        │
        ECHO ACTOR  <===== wss + HTTPS (MITM'd) =====>  BROKER
        (Socket Mode client + /readyz;                       forwards chat.postMessage → real Slack
         self-suspends when idle)                            captures xapp-/xoxb- tokens from traffic
```

Two live connections, bridged by the broker:

- **Broker ↔ Slack** — a real Socket Mode WebSocket over real TLS, kept open
  across the actor's suspend/resume cycles. Opened with the app-level token the
  broker captured from the actor's own `apps.connections.open` (the broker holds
  **no** pre-shared Slack secrets).
- **Actor ↔ Broker** — a stock Socket Mode client whose `slack.com` traffic is
  redirected to the broker. Ephemeral: it dies on suspend and the actor
  re-establishes it on resume.

State is keyed **per actor** (`atespace/name`), so each Slack connection maps to
exactly one actor and inbound events route unambiguously. The broker identifies
which actor a connection belongs to from its source IP (actor egress is SNAT'd
behind the worker pod IP; `Actor.AteomPodIp` from the Control API maps it back).

### Message lifecycle

1. First run: the actor connects, the broker captures the app token and opens
   the persistent Slack connection; the actor goes idle and self-suspends.
2. A user posts in Slack → the broker's Slack connection receives an
   `events_api` envelope.
3. The broker acks Slack immediately (within the ~3s window, so Slack does not
   redeliver), buffers the event, and calls `ResumeActor`.
4. `ResumeActor` returns once the actor is live; the actor's Socket Mode client
   reconnects (its old socket died on restore).
5. The broker attaches the reconnected actor, sends `hello`, and drains the
   buffered event(s).
6. The actor replies `echo: <text>` via `chat.postMessage` (MITM'd through the
   broker to real Slack) and, after an idle grace period, self-suspends again.

Slack `hello` / `disconnect` frames and WebSocket ping/pong are **never**
delivered and never wake the actor.

## Components

| Path | What it is |
|------|------------|
| `cmd/egress-broker/` | The egress broker: per-SNI TLS minting, Slack HTTPS handling (synthesize `apps.connections.open`, capture tokens, forward the rest), persistent Socket Mode client to Slack, Socket Mode server facing the actor, per-actor event buffer, and the `ResumeActor` client. |
| `cmd/suspend-helper/` | Tiny Go binary bundled into the actor image; asks the control plane to checkpoint the actor (self-suspend). |
| `echo-actor/` | A normal Slack Bolt (Node) bot that echoes messages, exposes `/readyz`, and self-suspends when idle. It has no knowledge of the broker. |
| `internal/socketmode/` | The small Socket Mode envelope protocol (shared by broker and actor). |
| `internal/slackapi/` | The slice of the Slack Web API used (`apps.connections.open`, `chat.postMessage`). |
| `deploy/` | Broker Deployment/Service, CA installer DaemonSet, echo-actor WorkerPool/ActorTemplate, CoreDNS rewrite. |
| `certs/` | Broker CA generation. |

This is a standalone Go module (`github.com/ronlv10/substrate-ws-poc`). It
depends on public substrate only for the generated `ateapipb` gRPC client; the
ateapi connection is dialed with `InsecureSkipVerify` (in-cluster mTLS
identity), so no substrate `internal/` packages are imported.

## Transparent redirect and CA trust (PoC mechanisms)

- **Redirect — CoreDNS.** A cluster-wide `rewrite` rule points `slack.com` and
  `wss-primary.slack.com` at the broker Service. Actors pick this up through the
  worker pod's `/etc/resolv.conf`. The broker reaches *real* Slack via its own
  upstream resolver (`--dns-upstream`, default `8.8.8.8:53`), so it is not caught
  by its own rewrite. See `deploy/coredns-rewrite.md`.
- **CA trust — node bundle mounted into actors.** `certs/gen-ca.sh` produces the
  broker CA. The `ca-installer` DaemonSet publishes `system CAs + broker CA` to
  the shared ateom hostPath on every node, and atelet is pointed at it via the
  `ATE_ACTOR_CA_BUNDLE` env var; atelet then bind-mounts it into every actor
  sandbox over `/etc/ssl/certs/ca-certificates.crt`.

### The substrate changes (in the fork)

The only substrate-side changes live on the **`ws-poc` branch of the substrate
fork**: <https://github.com/ronlv10/substrate/tree/ws-poc>. Both are additive and
opt-in (`cmd/atelet/oci.go`, `cmd/atelet/main.go`):

- when atelet is started with `ATE_ACTOR_CA_BUNDLE=<path>`, it bind-mounts that
  CA bundle read-only over the actor's system CA store (inert when unset), so
  actors trust the broker's CA;
- atelet also writes the actor's atespace into the per-resume identity mount
  (`/run/ate/atespace`) so an actor can address itself to the control plane
  (e.g. self-suspend).

Everything else uses the existing Control API. Build the cluster from that fork
branch so the atelet image includes these changes.

## Prerequisites

- A running substrate cluster built from the `ws-poc` fork branch (kind
  quickstart: `hack/create-kind-cluster.sh && hack/install-ate-kind.sh
  --deploy-ate-system`). Note the snapshot bucket (`BUCKET_NAME`) and build the
  `ateom-gvisor` image (`ko build github.com/agent-substrate/substrate/cmd/ateom-gvisor`)
  — pass its digest as `ATEOM_IMAGE`.
- A Slack app with **Socket Mode enabled**. Collect:
  - an **app-level token** (`xapp-…`) with `connections:write`,
  - a **bot token** (`xoxb-…`) with `chat:write`,
  - event subscriptions for `message.channels` (and/or `app_mention`),
  - the bot invited to a test channel.
  The tokens live only in the actor's `slack-tokens` secret; the broker learns
  them from traffic.
- `ko`, `kubectl`, `kubectl-ate`, `jq`, `openssl`.

## Deploy (kind)

```bash
# From the repo root.

# 1. Broker CA + secret, broker + CA installer, atelet wiring, DNS rewrite.
make gen-ca
make deploy                 # ca-secret + deploy-broker + atelet-ca + coredns-patch

# 2. Slack tokens (stored in the actor's namespace only).
make slack-secret APP_TOKEN=xapp-... BOT_TOKEN=xoxb-...

# 3. The echo actor template + worker pool. ATEOM_IMAGE is the substrate
#    ateom-gvisor image built from the fork.
make deploy-actor BUCKET_NAME=<your-bucket> ATEOM_IMAGE=<ateom-gvisor-digest>

# 4. Create the actor. Its first run bootstraps the broker's Slack connection.
make create-actor
```

## Demo

1. Confirm the actor bootstrapped and then suspended:
   ```bash
   kubectl logs -n ws-poc deploy/egress-broker | grep "persistent Slack connection established"
   kubectl ate get actors -a demo        # echo-1 → SUSPENDED
   ```
2. Post `hello world` in the test channel. Watch it wake, reply, and re-suspend:
   ```bash
   kubectl logs -n ws-poc deploy/egress-broker -f
   #   ... real Slack event received; delivering to actor
   #   ... resuming suspended actor to deliver event
   #   ... actor Socket Mode WebSocket attached
   kubectl ate get actors -a demo        # RESUMING → RUNNING → SUSPENDED
   ```
   Slack shows `echo: hello world`. The broker's Slack connection never dropped.
3. **Keepalive check:** leave it idle. Slack ping/`disconnect` frames appear in
   the broker log as ignored connection-management traffic, and `echo-1` stays
   `SUSPENDED` — keepalive does not wake the actor.

## Tests

```bash
make test        # go test ./poc/...
```

Unit tests cover per-SNI certificate minting (leaves verify against the CA),
Socket Mode envelope classification (keepalive vs. real event), the event
buffer + immediate Slack-ack, keepalive filtering, resume orchestration, and
per-actor session keying — using fakes, no cluster required.

## Blast radius (accepted for this PoC)

The CoreDNS rewrite and the CA mount are **cluster-wide**: every actor's
`slack.com` traffic routes through the broker and every actor trusts the broker
CA. To avoid breaking other Slack-using actors, the broker forwards all
`slack.com/api/*` calls it does not specifically handle straight to real Slack.
This is acceptable on a dedicated demo cluster; see below for the scoped
production design.

## Production hardening (documented, not built)

- **Per-workload transparent capture.** Replace the cluster-wide CoreDNS rewrite
  with in-pod `nftables` TPROXY/DNAT egress capture in
  `cmd/ateom-gvisor/main.go` (`installActorNftablesRules`) and
  `cmd/ateom-microvm/net.go`, scoped to opted-in actors. This also captures
  IP-literal egress (which DNS redirect cannot) and is the sanctioned
  "AgentGateway" direction.
- **Centrally managed CA**, gated per ActorTemplate rather than cluster-wide.
- **Multi-tenant Socket Mode.** The broker forwards HTTPS for any actor but only
  brokers Socket Mode for identified WS-PoC actors; full per-app Socket Mode
  multiplexing (so unrelated Slack apps coexist) is the next extension.
- **Durable buffer / HA.** The event buffer and captured tokens are in-memory; a
  broker restart loses them until an actor reconnects. Production would persist
  them and run the broker HA.
