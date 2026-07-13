# WS-PoC — a Slack agent that stays online while suspended

A Slack bot holds a long-lived Socket Mode WebSocket. Suspending an actor on
agent-substrate checkpoints the process and **destroys every socket it holds**,
so a stock bot can't stay connected across a suspend.

The fix: a **local egress proxy baked into the actor image**. The agent talks
Socket Mode to the proxy over **loopback** — both ends inside the sandbox, so
`runsc` checkpoint/restore preserves it and the agent never sees a dead socket.
The proxy speaks resumable gRPC to an always-on **egress broker** that owns the
real Slack connection. On suspend only the proxy↔broker leg drops.

The proxy is agent-agnostic. Two actors ship on it:

- **`echo-actor`** — a stock `@slack/bolt` bot. Isolates the transport.
- **`openclaw-actor`** — a real Claude agent (OpenClaw, ~1.5 GB), run unmodified.

## Architecture

```mermaid
flowchart LR
    Slack(["Slack"])

    subgraph K8s["Kubernetes cluster · agent-substrate"]
        direction TB
        CP["Substrate control plane"]
        Broker["Egress Broker<br/>persistent · always-on"]
        subgraph Actor["Actor · suspended between messages"]
            direction TB
            Agent["Stock Slack agent<br/>(echo / OpenClaw)"]
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

- **Broker ↔ Slack** — real Socket Mode WSS, held across suspend. Opened with the
  app token the broker captured from the agent's `apps.connections.open`; the
  broker holds no pre-shared Slack secrets.
- **Agent ↔ Proxy** — stock Socket Mode over loopback TLS. Survives the checkpoint.
- **Proxy ↔ Broker** — gRPC (`proto/brokerproxy`). The only leg that drops on
  suspend; the proxy re-announces on resume and replays from its last ack.

After each restore the agent re-dials its Socket Mode connection once (pong
staleness reads the jumped wall clock), but it's a ~40 ms loopback hop and the
proxy holds every event until the agent heartbeats, so nothing is lost.

## Running a real agent: four constraints a stateless bot hides

- **Keep the rootfs clean.** OpenClaw writes state/cache/logs constantly; a dirty
  overlay trips `runsc restore`'s filestore check and wedges the actor. The
  template points `HOME` and every writable path at the `/data` durableDir.
- **Don't checkpoint mid-startup.** A V8 runtime frozen during init `SIGILL`s on
  restore. The proxy defers its broker announce (which arms idle-suspend) until
  the agent is quiescent — `Core.WaitQuiescent`.
- **Don't suspend mid-turn.** Socket Mode acks on receipt, but the agent thinks
  for ~10–20 s before replying. The broker holds suspend from ack until the reply
  or a bounded grace (`--handling-grace`, 90 s).
- **The agent ignores ambient creds.** OpenClaw reads no `ANTHROPIC_*` env; the
  model provider is set in `openclaw-actor/openclaw.json`, referencing the key via
  `{source: env, id: ANTHROPIC_API_KEY}` so the secret stays out of the image.

Cycle latency ≈ 18 s: ~12 s resume, ~3 s agent setup, ~3 s model call. The
config disables per-turn features a responder doesn't need (memory search,
startup context, commitment inference, browser), which cut setup from ~20 s.

## Layout

| Path | What it is |
|------|------------|
| `cmd/egress-broker/`, `internal/broker/` | Broker: gRPC server, per-actor session, event buffer, resume/suspend, handling hold. |
| `internal/slack/` | Slack wire types + the persistent real-Slack connection (slack-go). |
| `cmd/local-proxy/`, `internal/proxy/` | In-actor proxy: loopback Slack impersonation, hold-until-heartbeat, announce-when-quiescent, PID-1 supervisor. |
| `proto/brokerproxy/` | The broker↔proxy gRPC protocol. |
| `echo-actor/`, `openclaw-actor/` | The two actor images. |
| `deploy/`, `certs/` | Broker manifests, actor templates, proxy cert generation. |
| `spikes/` | Phase-0 experiments (loopback survival, Bolt churn). |

Standalone Go module; depends on public substrate only for the generated
`ateapipb` gRPC client.

## Redirect and trust — per image, no cluster-wide blast radius

- **Redirect.** The image's `/etc/hosts` points `slack.com` at `127.0.0.1`.
  atelet mounts nothing over it — no CoreDNS rewrite.
- **Trust.** The image bakes a CA + `slack.com` leaf (`certs/gen-proxy-cert.sh`);
  the agent trusts it via `NODE_EXTRA_CA_CERTS`. No node-level CA install.
- **Identity.** The proxy reads `atespace/name` fresh from the per-resume
  `/run/ate` mount on every announce (never cached). Needs the substrate fork's
  [`ws-poc` branch](https://github.com/ronlv10/substrate/tree/ws-poc) — the only
  substrate-side change v2 requires.

## Deploy (kind)

```bash
make deploy-broker
make slack-secret APP_TOKEN=xapp-... BOT_TOKEN=xoxb-...

# echo actor
make deploy-actor  BUCKET_NAME=<bucket> ATEOM_IMAGE=<digest> \
     BROKER_ADDRESS=egress-broker.ws-poc.svc.cluster.local:9090
make create-actor

# openclaw actor (also needs a model-endpoint key)
make anthropic-secret API_KEY=<inference key>
make deploy-openclaw BUCKET_NAME=<bucket> ATEOM_IMAGE=<digest> \
     BROKER_ADDRESS=egress-broker.ws-poc.svc.cluster.local:9090
make create-openclaw
```

Needs a Socket Mode Slack app: app token (`xapp-…`, `connections:write`), bot
token (`xoxb-…`, `chat:write`), `message.im` + `app_mention` subscriptions. One
app serves one actor (shared tokens split events — see backlog). Omit
`BROKER_ADDRESS` to run the proxy standalone (stubs + synthetic events, no broker).

## Tests

```bash
make test
```

Broker session (buffer, resume-once, attach-by-`last_acked_seq`, idle suspend,
handling hold), the gRPC session over bufconn, and the proxy (token capture,
hold-until-heartbeat, readiness) — all with fakes, no cluster.

## Backlog

- **Multi-tenant Socket Mode** — per-app session keying so actors can share an app.
- **Durable buffer / HA** — buffer and captured tokens are in-memory; a broker
  restart loses them (and resets the event sequence).
- **Transport auth** — gRPC is plaintext; identity of record is the `Announce`.
  Production would add mTLS or a per-actor token.
