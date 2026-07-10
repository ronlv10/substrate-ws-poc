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
delivers the buffered message. The actor replies `echo: <message>`, and once it
goes idle the broker suspends it again.

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
- `Control.ResumeActor` blocks until the actor's `/readyz` returns 200, so
  "resume, then the actor reconnects and we deliver" is race-free by construction.
- All actor↔broker traffic is actor-initiated **egress**, so the broker never
  needs the inbound atenet router or the actor's pod IP to deliver a message.

## Architecture

```
                       persistent, survives actor suspend
  Slack  <===== wss (real TLS, broker holds it) =====>  BROKER  ── gRPC ResumeActor ──>  ateapi
                                                            │   (in-cluster Deployment;
   actor believes it dials slack.com;                       │    reaches real Slack via the
   its /etc/hosts points slack.com at the broker; broker      │    default cluster resolver)
   terminates TLS with a per-SNI cert the actor trusts        │
        ECHO ACTOR  <===== wss + HTTPS (MITM'd) =====>  BROKER
        (Socket Mode client + /readyz;                       forwards chat.postMessage → real Slack
         broker suspends it when idle)                       captures the xapp- app token from traffic
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
exactly one actor and inbound events route unambiguously. The actor announces its
own identity in an `X-Ate-Actor` header (read fresh from its per-resume `/run/ate`
mount); the broker falls back to source-IP correlation (`Actor.AteomPodIp` from
the Control API) only when the header is absent.

### Message lifecycle

1. First run: the actor connects, the broker captures the app token and opens
   the persistent Slack connection; the actor goes idle and the broker suspends it.
2. A user posts in Slack → the broker's Slack connection receives an
   `events_api` envelope.
3. The broker acks Slack immediately (within the ~3s window, so Slack does not
   redeliver), buffers the event, and calls `ResumeActor`.
4. `ResumeActor` returns once the actor is live; the actor's Socket Mode client
   reconnects (its old socket died on restore).
5. The broker attaches the reconnected actor, sends `hello`, and drains the
   buffered event(s).
6. The actor replies `echo: <text>` via `chat.postMessage` (forwarded through the
   broker to real Slack). After an idle grace period, the broker suspends it again
   from the outside.

Slack `hello` / `disconnect` frames and WebSocket ping/pong are **never**
delivered and never wake the actor.

## Components

| Path | What it is |
|------|------------|
| `cmd/egress-broker/` | The egress broker: per-SNI TLS minting, Slack HTTPS handling (synthesize `apps.connections.open`, capture tokens, forward the rest), persistent Socket Mode client to Slack, Socket Mode server facing the actor, per-actor event buffer, and the `ResumeActor` client. |
| `echo-actor/` | A normal Slack Bolt (Node) bot that echoes messages and exposes `/readyz`. It has no knowledge of the broker or of its own suspend/resume. |
| `internal/socketmode/` | The small Socket Mode envelope types the broker uses. |
| `internal/slackapi/` | The Slack Web API shape the broker synthesizes (`apps.connections.open`). |
| `deploy/` | Broker Deployment/Service, CA installer DaemonSet, echo-actor WorkerPool/ActorTemplate. |
| `certs/` | Broker CA generation. |

This is a standalone Go module (`github.com/ronlv10/substrate-ws-poc`). It
depends on public substrate only for the generated `ateapipb` gRPC client; the
ateapi connection is dialed with `InsecureSkipVerify` (in-cluster mTLS
identity), so no substrate `internal/` packages are imported.

## Transparent redirect and CA trust (PoC mechanisms)

- **Redirect — per-actor `/etc/hosts`.** The actor's `entrypoint.sh` resolves the
  broker Service (`BROKER_SERVICE`, a stable ClusterIP) and appends
  `<ip> slack.com wss-primary.slack.com` to its own `/etc/hosts` before starting
  Bolt. `slack.com` stays the TLS SNI, so the broker's cert still matches. The
  redirect lives only in the actor, so the broker is never caught by it and cluster
  DNS is untouched — no upstream-resolver workaround, no blast radius.
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
  (`/run/ate/atespace`) so an actor can read its own identity — the echo actor
  sends it to the broker as the `X-Ate-Actor` header.

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

# 1. Broker CA + secret, broker + CA installer, atelet wiring.
make gen-ca
make deploy                 # ca-secret + deploy-broker + atelet-ca

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
make test        # go test ./...
```

Unit tests cover per-SNI certificate minting (leaves verify against the CA),
Socket Mode envelope classification (keepalive vs. real event), the event
buffer + immediate Slack-ack, keepalive filtering, resume orchestration, and
per-actor session keying — using fakes, no cluster required.

## Blast radius (accepted for this PoC)

The redirect is per-actor (`/etc/hosts`), so it has no blast radius. The one
cluster-wide mechanism left is the **CA mount**: atelet mounts the broker CA into
every actor sandbox, so every actor trusts it. That is acceptable on a dedicated
demo cluster; production would gate the CA per ActorTemplate (see below).

## Production hardening (documented, not built)

- **Per-workload transparent capture.** The `/etc/hosts` redirect only catches
  hostname egress; production would use in-pod `nftables` TPROXY/DNAT egress
  capture in `cmd/ateom-gvisor/main.go` (`installActorNftablesRules`) and
  `cmd/ateom-microvm/net.go`, scoped to opted-in actors. That also catches
  IP-literal egress and is the sanctioned "AgentGateway" direction.
- **Centrally managed CA**, gated per ActorTemplate rather than cluster-wide.
- **Multi-tenant Socket Mode.** The broker forwards HTTPS for any actor but only
  brokers Socket Mode for identified WS-PoC actors; full per-app Socket Mode
  multiplexing (so unrelated Slack apps coexist) is the next extension.
- **Durable buffer / HA.** The event buffer and captured tokens are in-memory; a
  broker restart loses them until an actor reconnects. Production would persist
  them and run the broker HA.
