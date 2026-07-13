# WS-PoC — a Slack agent that stays online while suspended

A Slack bot holds a long-lived Socket Mode WebSocket. Suspending an actor on
agent-substrate checkpoints the process and **destroys every socket it holds**,
so a stock bot can't stay connected across a suspend.

The fix: a **suspend-aware local egress proxy baked into the actor image**. The
agent talks Socket Mode to the proxy over **loopback** — both ends inside the
sandbox, so `runsc` checkpoint/restore preserves it and the agent never sees a
dead socket. The proxy absorbs the suspend: it speaks resumable gRPC to an
always-on **egress broker** (which owns the real Slack connection) and
re-announces from its last ack on resume. Only the proxy↔broker leg drops, and
the agent itself stays stock.

The proxy is agent-agnostic. Two actors ship on it:

- **`echo-actor`** — a stock `@slack/bolt` bot. Isolates the transport.
- **`openclaw-actor`** — OpenClaw, a stateful agent, run unmodified.

## Architecture

![Architecture](docs/architecture.png)

- **Broker ↔ Slack** — real Socket Mode WSS, held across suspend. Opened with the
  app token the broker captured from the agent's `apps.connections.open`; the
  broker holds no pre-shared Slack secrets.
- **Agent ↔ Proxy** — stock Socket Mode over loopback TLS. Survives the checkpoint.
- **Proxy ↔ Broker** — gRPC (`proto/brokerproxy`). The only leg that drops on
  suspend; the proxy re-announces on resume and replays from its last ack.

After each restore the agent re-dials its Socket Mode connection once (pong
staleness reads the jumped wall clock), but it's a loopback hop and the proxy
holds every event until the agent heartbeats, so nothing is lost.

TLS terminates twice: the proxy presents a baked `slack.com` leaf to the agent
over loopback (trusted via `NODE_EXTRA_CA_CERTS`), and the broker holds the real
TLS session to Slack.

## Redirect and trust — per image, no cluster-wide blast radius

- **Redirect.** The image's `/etc/hosts` points `slack.com` at `127.0.0.1`.
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

## Backlog

- **Multi-tenant Socket Mode** — per-app session keying so actors can share an app.
- **Durable buffer / HA** — buffer and captured tokens are in-memory; a broker
  restart loses them (and resets the event sequence).
- **Transport auth** — gRPC is plaintext; identity of record is the `Announce`.
  Production would add mTLS or a per-actor token.
