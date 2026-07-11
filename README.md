# WS-PoC — a Slack agent that stays online while suspended

WS-PoC runs an **always-on Slack agent on agent-substrate while it is suspended
almost all of the time**. A Slack bot holds a long-lived Socket Mode WebSocket
and idles waiting for messages; on substrate, suspending an actor checkpoints
the process and **destroys every socket it holds**, so a stock bot cannot keep
that WebSocket alive across a suspend.

v2 solves this with a **local egress proxy baked into the actor image**. The
agent (stock Bolt) talks Socket Mode to the proxy over **loopback** — a
connection whose endpoints are both inside the sandbox, so `runsc`
checkpoint/restore preserves it intact and the agent never sees a dead socket.
The proxy speaks a resumable gRPC protocol to an always-on **egress broker**,
which owns the real, persistent Slack connection. On suspend only the
proxy↔broker leg drops; on resume the proxy redials and the agent's own socket
is already there.

## Architecture

```mermaid
flowchart LR
    Slack(["Slack"])

    subgraph K8s["Kubernetes cluster · agent-substrate"]
        direction TB
        CP["Substrate control plane"]
        Broker["Egress Broker<br/>persistent · always-on"]
        subgraph Actor["Echo Actor · suspended between messages"]
            direction TB
            Agent["@slack/bolt — stock echo bot"]
            Proxy["Local proxy<br/>impersonates slack.com"]
            Agent <-->|"Socket Mode WS · loopback<br/>survives checkpoint"| Proxy
        end
        Broker -- "Resume / Suspend Actor" --> CP
        Proxy <-.->|"gRPC session · only while resumed"| Broker
        CP -.->|restore / checkpoint| Actor
    end

    Slack <==>|Socket Mode WSS · persistent| Broker

    linkStyle 0 stroke:#3ec7d4,stroke-width:3px
    linkStyle 3 stroke:#2ea043,stroke-width:2px
    linkStyle 5 stroke:#3ec7d4,stroke-width:2px

    classDef slack fill:#0b1e3a,stroke:#3b82f6,color:#e5edff;
    classDef cp fill:#241833,stroke:#a855f7,color:#f3e8ff;
    classDef broker fill:#2a1e07,stroke:#d99a1c,color:#fde9b8;
    classDef actor fill:#0c2417,stroke:#3f8f5f,color:#cfe8d6;
    class Slack slack
    class CP cp
    class Broker broker
    class Agent,Proxy actor
```

Three connections, two of them durable across suspend:

- **Broker ↔ Slack** — a real Socket Mode WebSocket over real TLS, held open
  across the actor's suspend/resume cycles. Opened with the app-level token the
  broker captured from the agent's own `apps.connections.open` (the broker holds
  **no** pre-shared Slack secrets).
- **Agent ↔ Proxy** — a stock Socket Mode client over loopback TLS. Both ends
  are in the same sandbox, so the checkpoint preserves it; the agent's process
  memory survives too.
- **Proxy ↔ Broker** — a gRPC session (`proto/brokerproxy`). This is the only
  leg that dies on suspend; the proxy redials and re-announces on resume,
  resuming the event stream from its last ack.

The agent still churns its Socket Mode connection once *locally* after each
restore (its pong-staleness check reads the wall clock, which jumps on
restore), but that reconnect is a ~40ms loopback round-trip to the co-resident
proxy — and the proxy holds each event until the reconnected agent heartbeats,
so nothing lands in the churn window.

### Message lifecycle

1. First run: the agent connects to the proxy, the proxy announces to the broker
   with the captured app token, the broker opens the persistent Slack connection,
   and the actor is suspended once idle.
2. A user posts in Slack → the broker's Slack connection receives an
   `events_api` envelope.
3. The broker acks Slack immediately (within the ~3s window), buffers the event,
   and calls `ResumeActor`.
4. On resume the proxy redials the broker and re-announces; the broker re-sends
   the buffered event over the gRPC session.
5. The proxy delivers it to the agent over the surviving loopback socket, once
   the agent has heartbeated on its (reconnected) connection.
6. The agent replies `echo: <text>` via `chat.postMessage`, which the proxy
   relays to the broker and on to real Slack. After an idle grace period, the
   broker suspends the actor from the outside.

Slack `hello` / `disconnect` frames and WebSocket ping/pong never wake the actor.

## Layout

| Path | What it is |
|------|------------|
| `cmd/egress-broker/` | Thin wiring: flags, gRPC server, control-plane dial. |
| `internal/broker/` | Broker runtime: per-actor session, event buffer, resume/suspend, gRPC session handler. |
| `internal/slack/` | Slack wire types and the persistent connection to real Slack (slack-go). |
| `cmd/local-proxy/` + `internal/proxy/` | The in-actor proxy: Slack impersonation on loopback, hold-until-heartbeat delivery, broker session client, PID-1 supervisor. |
| `proto/brokerproxy/` | The broker↔proxy gRPC protocol. |
| `echo-actor/` | The stock Bolt bot and the image that bundles it with the proxy. |
| `deploy/`, `certs/` | Broker Deployment/Service, actor template, proxy cert generation. |
| `spikes/` | The phase-0 experiments that validated loopback survival and the Bolt churn. |

Standalone Go module (`github.com/ronlv10/substrate-ws-poc`); it depends on
public substrate only for the generated `ateapipb` gRPC client.

## Redirect and trust — per image, no cluster-wide blast radius

- **Redirect.** The actor image's `/etc/hosts` points `slack.com` and
  `wss-primary.slack.com` at `127.0.0.1`, where the proxy listens. atelet mounts
  nothing over `/etc/hosts`, so this is entirely self-contained — no CoreDNS
  rewrite.
- **Trust.** The image bakes a CA and a `slack.com` leaf (`certs/gen-proxy-cert.sh`);
  the agent trusts it via `NODE_EXTRA_CA_CERTS`. No node-level CA install.

### Identity

The proxy reads its own identity (`atespace/name`) fresh from the per-resume
`/run/ate` mount on every broker (re)connect and sends it in its `Announce` —
never cached, because a golden-restored actor reads the template's identity
until the mount is regenerated on resume. This needs the `/run/ate/atespace`
write from the substrate fork's **`ws-poc` branch**
(<https://github.com/ronlv10/substrate/tree/ws-poc>), which is the only
substrate-side change v2 requires.

## Deploy (kind)

```bash
# A substrate cluster built from the ws-poc fork branch, e.g.
#   hack/create-kind-cluster.sh && hack/install-ate-kind.sh --deploy-ate-system
# Note BUCKET_NAME and build ateom-gvisor (ko build .../cmd/ateom-gvisor) → ATEOM_IMAGE.

make deploy-broker
make slack-secret APP_TOKEN=xapp-... BOT_TOKEN=xoxb-...
make deploy-actor BUCKET_NAME=<bucket> ATEOM_IMAGE=<digest> \
     BROKER_ADDRESS=egress-broker.ws-poc.svc.cluster.local:9090
make create-actor
```

A Slack app with Socket Mode enabled is required: an app-level token (`xapp-…`,
`connections:write`), a bot token (`xoxb-…`, `chat:write`), event subscriptions
for `message.channels` / `app_mention`, and the bot invited to a channel. The
tokens live only in the actor's `slack-tokens` secret.

Omit `BROKER_ADDRESS` to deploy the proxy **standalone** — it stubs the Web API
and injects synthetic events, exercising the whole agent-facing path (including
suspend/resume) with no broker or real Slack.

## Demo

Post `@bot hello` in the test channel and watch the actor wake, echo, and
re-suspend:

```bash
kubectl -n ws-poc logs deploy/egress-broker -f
#   ... real Slack event received; delivering to actor
#   ... resuming suspended actor to deliver event
#   ... proxy announced / proxy acked event / actor idle; suspending
kubectl ate get actors -a demo        # RUNNING → SUSPENDED
```

## Tests

```bash
make test        # go test ./...
```

Covers the broker session (keepalive filtering, buffer + immediate Slack-ack,
resume-once, attach/re-send by `last_acked_seq`, idle suspend), the gRPC session
end to end over bufconn (announce/event/ack/egress, golden-skip), and the proxy
(token capture, hold-until-heartbeat delivery, ack propagation, readiness) — all
with fakes, no cluster.

## Not built (backlog)

- **Multi-tenant Socket Mode.** One broker brokers many actors, but two actors
  sharing a Slack app token would split its events; per-app session keying is the
  next step.
- **Durable buffer / HA.** The event buffer and captured tokens are in-memory; a
  broker restart loses them until a proxy reconnects.
- **Transport auth.** The broker's gRPC port is plaintext; identity of record is
  the `Announce`. Production would add mTLS or a per-actor token.
