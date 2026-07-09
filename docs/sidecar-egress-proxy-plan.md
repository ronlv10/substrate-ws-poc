# Plan — Sidecar egress-proxy evolution for suspendable Slack agents (ws-poc v2)

> Status: proposal / to be implemented on a new **`ws-poc-v2` branch** of the
> existing PoC repos (§12) on a **fresh kind cluster**.
> Supersedes the v1 "agent connects directly to the egress broker" design.

---

## 0. TL;DR

Today the agent (Bolt) opens its Socket Mode WebSocket **directly to the external
egress broker**. That socket crosses the sandbox boundary, so on suspend/resume it
is dead and the agent must reconnect — and Bolt's reconnect (primary/secondary
"smooth reconnect") intermittently **drops the delivered event**, plus the
broker's source-IP → actor identity lookup races. Result: flaky echoes.

**v2:** bake a small **local egress proxy** into the actor image. The agent talks
to it over **localhost**; that loopback connection is checkpointed/restored
*together* with the agent, so it **survives resume intact** — the agent never
reconnects, never churns, never drops the event. The local proxy (our code, fully
broker-aware) owns the external connection to the always-on broker and reconnects
transparently on resume.

Three tiers:

```
  Agent (stock Bolt)          Local egress proxy            Egress broker (always-on)
  ────────────────────        (in the actor image, ours)    (external, unchanged role)
      Socket Mode      ── loopback TLS, survives resume ──>  Socket Mode SERVER to agent
      client to                                              +  broker-protocol CLIENT ── cluster net,
      "slack.com"                                             reconnects on resume ──>  broker-protocol SERVER
                                                                                        +  REAL Socket Mode ──> Slack
                                                                                        +  event buffer + ResumeActor
```

The durable, always-on Slack connection still lives in the broker (the proxy is
suspended with the actor, so it cannot hold it). The proxy's job is to make the
**agent-facing** side of the connection local, so resume is invisible to the agent.

---

## 1. Why v1 is flaky (what we're fixing)

Established over a long debugging session on v1:

1. **Event drop on resume (primary).** On resume the agent's socket to the broker
   is dead. Bolt reconnects via its dual-connection dance (`open secondary →
   switch → terminate primary`), reaches `connected:ready`, **receives the
   delivered event, and silently drops it** (no ack, no dispatch) when the event
   lands during the churn. Intermittent; ruined the echo reliability.
2. **Heartbeat-timeout churn.** Bolt's heartbeat timer was mid-wait at checkpoint;
   on restore it immediately concludes "no pong in 5s → dead" and reconnects —
   guaranteeing churn on *every* warm resume. Broker pong timing is irrelevant
   (the peer is external and gone).
3. **Identity race.** Broker maps the connection's **source IP** → control-plane
   `AteomPodIp` via `ListActors`, which is eventually-consistent and lags right
   after boot/resume → `could not identify actor` → the persistent Slack
   connection never establishes → the event never even reaches the broker.
4. **Golden-snapshot identity contamination.** The actor's `/run/ate` identity
   mount reports the *golden snapshot* actor's id for golden-derived actors, so
   both source-IP and header identity announce the wrong actor. (Substrate-fork
   `atelet` issue; see §9.)

Common root of 1–3: **the durable socket the agent holds crosses the sandbox
boundary.** Fix the boundary, and 1–3 dissolve.

---

## 2. Core insight (validate this first)

- A **loopback** connection (127.0.0.1) between two processes in the **same**
  gVisor sandbox has *both* endpoints inside the sandbox's netstack. `runsc
  checkpoint` serialises the whole sandbox (all processes + netstack), so the
  loopback connection — sockets, buffers, seq numbers — is restored **as a
  consistent pair**. Neither side observes a reset. No reconnect.
- An **external** connection (to the broker's cluster IP) has its peer outside the
  checkpoint; on restore (often onto a different worker/IP) it is dead → reconnect
  required.

⇒ Put the durable, agent-facing socket on **loopback** by interposing a local
proxy. The agent↔proxy socket survives resume. The proxy↔broker socket dies on
resume but the proxy is **our** code, purpose-built to reconnect cleanly and
invisibly.

### Bonus: heartbeat problem also disappears
The agent's Bolt heartbeat ping is answered by the **co-resident** proxy, which
shares the same frozen monotonic clock and restores together — so on resume the
pong is "immediately" there in the agent's timeline. No timeout, no churn.

---

## 3. Architecture (v2)

### 3.1 Agent (echo actor) — stays a stock Bolt Socket Mode bot
- No lifecycle code, no substrate awareness.
- Only difference from a vanilla bot: its Slack traffic resolves to the **local
  proxy** instead of the internet. Two options (pick in a spike, §8):
  - **(preferred, keeps it truly unmodified)** `/etc/hosts` (or pod `HostAliases`)
    maps `slack.com` and `wss-primary.slack.com` → `127.0.0.1`. Agent still thinks
    it talks to `slack.com`.
  - **(fallback)** set the Slack SDK base URL (`slackApiUrl` / socket-mode
    endpoint) to `https://127.0.0.1`. Slightly less "unmodified".
- Trusts the proxy's TLS via a CA baked into the image (`NODE_EXTRA_CA_CERTS`).
  This is **per-image**, not node-wide.

### 3.2 Local egress proxy — NEW, in the actor image, our code (Go)
Two faces:

**A. Socket Mode SERVER + TLS MITM facing the agent (localhost:443):**
> The MITM does **not** disappear in v2 — it *relocates* from the broker into the
> proxy. The agent is still a stock Bolt client that believes it dials `slack.com`
> over TLS, so the proxy must terminate that TLS and impersonate Slack. The only
> change is it's now **local and cheap**: one client, one SNI, loopback only.
- Terminate TLS for `slack.com` SNI with a cert the agent trusts. Because it only
  ever serves `slack.com` + `wss-primary.slack.com` to its **single co-resident**
  agent over loopback, a **static baked-in leaf** suffices — no dynamic per-SNI
  `GetCertificate` minting like the broker needed cluster-wide (reuse v1 `tlsmint`
  only if convenient).
- Synthesize `apps.connections.open` → return `wss://.../socketmode` pointing at
  itself (localhost).
- Serve the Socket Mode WS to the agent: send `hello`, deliver event envelopes,
  receive acks, filter keepalives.
- Forward `chat.postMessage` (and any other Slack Web API) up to the broker.

**B. Broker-protocol CLIENT facing the broker (cluster network):**
- Maintain a connection to the always-on broker. Clean, resumable protocol (§5).
- **Announce identity explicitly** (from `/run/ate`, or alternative §9) — no
  source-IP guessing, re-announced on every reconnect.
- Hand the broker the captured app token so the broker can open/keep the real
  Slack connection.
- Receive events (relay to the agent over the surviving loopback WS), send acks,
  relay the agent's outbound API calls.
- On resume: detect the dropped broker connection, reconnect with backoff,
  re-announce, resync unacked events. **Invisible to the agent.**

Survives resume *with* the agent; is **not** always-on (suspended with the actor).

### 3.3 Egress broker — evolve v1
- **Keep:** persistent real Slack Socket Mode connection (always-on), token
  capture, event buffer, `ResumeActor`, keepalive filtering.
- **Replace:** the actor-facing Socket Mode MITM + per-SNI cert minting + source-IP
  identity → a single clean **broker↔proxy protocol** endpoint (§5). All the
  agent-facing MITM/TLS complexity moves *into the proxy* (localhost, simpler).
- **Identity:** from the proxy's explicit `Announce`, cached per actor. No
  `ListActors`/source-IP correlation.

---

## 4. Suspend/resume lifecycle (detailed)

Steady state (actor suspended):
- Broker holds the real Slack connection, filtering keepalives; buffer empty.
- Actor (agent + proxy) checkpointed. The agent↔proxy loopback WS is frozen intact.
- The proxy↔broker connection is closed/half-open (broker treats "proxy gone" as
  "actor suspended" — expected).

Wake:
1. User posts → broker's Slack connection receives an `events_api` envelope; broker
   acks Slack immediately, buffers it, calls `ResumeActor`.
2. `ResumeActor` blocks until the actor's `/readyz` is 200; sandbox restores. The
   agent↔proxy loopback WS is intact → **Bolt is still `connected:ready`, no
   reconnect, no churn.** Bolt's heartbeat pings the co-resident proxy, which pongs
   immediately → no timeout.
3. The proxy notices its broker connection is gone → reconnects → re-announces
   identity → broker delivers the buffered event over the broker protocol.
4. Proxy relays the event to the agent over the **surviving** loopback WS. Bolt
   dispatches it (it never reconnected, so nothing to drop) → handler runs.
5. Agent replies `chat.postMessage` → proxy (localhost) → broker → real Slack.
6. Actor goes idle → suspends (broker-driven, or self-suspend — see §7). Loop.

`hello`/`disconnect`/ping-pong between broker and Slack never wake the actor
(unchanged from v1).

---

## 5. Broker ↔ proxy protocol (our design — clean & resumable)

Transport: **gRPC bidi stream** (preferred; typed, backpressure, easy reconnect)
or a WebSocket. Not a MITM of Slack Socket Mode.

Messages:
- `proxy → broker`
  - `Announce { atespace, name, app_token, last_acked_seq }` — first frame on every
    (re)connect; identifies the actor and lets the broker resume the stream.
  - `Ack { seq }` — event fully handled (agent acked it).
  - `Egress { method, path, headers, body }` — relay agent's outbound Slack Web API
    (chat.postMessage, etc.); broker forwards to real Slack, returns `EgressResp`.
- `broker → proxy`
  - `Event { seq, payload }` — a real Slack event to deliver to the agent.
  - `EgressResp { corr_id, status, headers, body }`.
- Semantics: monotonically increasing `seq`; broker re-sends unacked events after a
  reconnect (`last_acked_seq`); at-least-once + agent-side de-dup by Slack `ts`
  (as in v1). No fresh-envelope-id hacks needed — the proxy owns the Socket Mode
  envelope it hands to the agent.

Auth: in-cluster mTLS / the same `InsecureSkipVerify` demo path v1 used, or a
per-actor token. Identity of record is the `Announce`, not the transport.

---

## 6. Redirect & trust (much smaller blast radius than v1)

- **Redirect:** per-image `/etc/hosts` (or pod `HostAliases`) `slack.com`,
  `wss-primary.slack.com` → `127.0.0.1`. **No cluster-wide CoreDNS rewrite.** Only
  actors that ship the proxy + hosts entry are affected.
- **Trust:** a CA baked into the actor image + `NODE_EXTRA_CA_CERTS`. **No
  node-level CA install, no atelet CA bind-mount.** Per-image, opt-in.
- This directly resolves v1's "cluster-wide blast radius" caveat and matches the
  per-workload "AgentGateway" egress direction.

---

## 7. Process model in the actor image

The actor container must run **two** processes (agent + proxy) inside one gVisor
sandbox. Options (validate in §8):
- **Proxy as PID 1 / supervisor:** proxy starts, then spawns the agent (`node`);
  proxy owns `/readyz` on :80 (returns 200 once it is listening on :443 AND has a
  broker connection — or just once listening, to pass the resume gate fast).
- Or a tiny init (e.g. `tini`/a shell) starts both; proxy and agent are peers.
- Ordering: the proxy must be listening on :443 **before** the agent dials
  `slack.com`, else the agent's first `apps.connections.open` fails. Supervisor
  model makes this deterministic.
- Image: multi-stage — build the Go proxy + `ncc`-bundle the Node agent → runtime
  image with both. Keep it lean (distroless-ish) for fast `oci_unpack` (we learned
  `oci_unpack` dominates restore time and scales with **file count**, not bytes).

---

## 8. Risks & spikes — DO THESE FIRST, before building the full thing

Ordered; each is a tiny standalone experiment.

1. **[CRITICAL] Loopback survives gVisor checkpoint/restore.**
   Spike: a minimal image with two processes — a server on `127.0.0.1:9000` and a
   client holding an open connection exchanging a heartbeat every 1s. Run as a
   substrate actor, suspend, resume, and verify the *same* connection is alive and
   still exchanging heartbeats (no reconnect, no reset) after restore.
   - PASS → the whole design is unlocked.
   - FAIL → fall back to "agent reconnects to localhost proxy": still far better
     (local, instant, cooperative proxy that holds the event and feeds it on a
     clean single connection). Re-scope, don't abandon.
2. **Redirect works in-sandbox:** `slack.com` → `127.0.0.1` via `/etc/hosts` or
   `HostAliases` inside the gVisor actor (confirm substrate doesn't clobber
   `/etc/hosts`).
3. **Two-process actor container** boots under substrate, `/readyz` passes, both
   processes run, and both are checkpointed/restored together.
4. **TLS on localhost:** Bolt (Node) trusts the baked CA for `slack.com` presented
   by the local proxy over loopback.
5. **Identity source** (`/run/ate`) is correct for the proxy on a real (non-golden)
   actor; confirm the golden-contamination fix path (§9).

Only after 1–3 pass do we build the broker↔proxy protocol and full E2E.

---

## 9. Dependency: golden-snapshot identity contamination (`/run/ate`)

Observed in v1: for actors instantiated from a golden snapshot, `/run/ate/actor-id`
and `/run/ate/atespace` report the **golden** actor's identity (`ate-golden/<uuid>`)
rather than the real actor (`demo/echo-1`) — even on `--boot`. This poisons *any*
identity mechanism that reads `/run/ate`.

Options for v2:
- (a) **Fix `atelet`** (substrate fork) so the per-actor identity dir always
  reflects the real `atespace/actor-id` for golden-derived / booted actors — the
  proper fix. Investigate `cmd/atelet/main.go` `prepareOCIBundles` / identityDir
  bind-mount reuse.
- (b) **Alternative identity source** for the proxy: e.g. downward-API/env unique
  per actor, or the proxy queries the control API for its own identity by a
  substrate-provided token. Less clean.
- (c) **Disable golden snapshot** for this template (always boot from image), if
  that yields a correct `/run/ate`. Costs the golden fast-start.

Track as a hard dependency; the proxy's `Announce` is only as correct as `/run/ate`.

---

## 10. Phased implementation (on the `ws-poc-v2` branch)

- **Phase 0 — Spikes (§8.1–8.3).** Prove loopback-survives + two-process actor +
  redirect. Gate everything on §8.1.
- **Phase 1 — Local proxy, standalone loop.** Proxy = Socket Mode server on
  localhost that *itself* echoes (no broker yet). Agent (stock Bolt) connects,
  gets a synthetic event, replies. Suspend/resume: prove the agent never churns and
  the echo still works with the proxy feeding it. This validates the whole
  agent-facing half in isolation.
- **Phase 2 — Broker↔proxy protocol.** Define the gRPC service (§5). Proxy dials
  broker, announces, relays. Broker evolves: drop the actor-facing Socket Mode
  MITM, add the protocol server; keep Slack side + buffer + ResumeActor.
- **Phase 3 — Real Slack E2E.** Broker holds the real Slack connection; proxy
  relays events to the agent and egress out. Suspend/resume across real messages.
- **Phase 4 — Suspend policy.** Decide broker-driven vs self-suspend (broker-driven
  preferred — external, no in-actor timers to freeze; see v1 findings). Idle on the
  broker↔proxy stream.
- **Phase 5 — Hardening.** Reconnect/resync edge cases, exactly-once-ish acks,
  multi-actor keying, blast-radius review, docs.

Each phase ends with a suspend→post→echo→re-suspend E2E on a fresh cluster.

---

## 11. What to reuse from v1

- Broker internals worth keeping: persistent Slack Socket Mode client
  (`realslack.go`), event buffer + immediate Slack-ack, keepalive filtering,
  `ResumeActor` client (`ateapi.go`), token capture concept.
- `internal/socketmode`, `internal/slackapi` envelope/type helpers → reused by the
  **proxy** now (it's the Socket Mode server) and the broker (Slack client).
- `tlsmint` → reused by the proxy for the localhost `slack.com` cert (or replace
  with a static leaf).
- Substrate fork changes: keep the `/run/ate/atespace` write; add the identity-mount
  fix (§9). The node-CA bind-mount (`ATE_ACTOR_CA_BUNDLE`) is **no longer needed**
  (trust is per-image now) — can be dropped.
- From v1's final working state (now on `substrate-ws-poc@main`): **broker-driven
  suspend** (external `SuspendActor` + idle tracking), **`onPause: Data` cold-boot
  on resume**, the **golden-atespace skip**, and the **fresh-per-request
  `X-Ate-Actor` identity** all carry forward. In v2 the identity header moves onto
  the proxy↔broker `Announce` (§5); the rest is reused as-is.

What's deleted: actor-facing Socket Mode MITM in the broker, per-SNI minting in the
broker, source-IP `LocateByPodIP`, cluster-wide CoreDNS rewrite, node CA installer.

---

## 12. Repositories & branching

v2 builds on the **existing** PoC repos — don't start a throwaway repo/module.
Create a new `ws-poc-v2` branch in each so v1 (the working demo) stays intact on
its current branch as the reference.

- **PoC repo** — `github.com/ronlv10/substrate-ws-poc` (v1 lives on `main`: the
  `egress-broker`, `echo-actor`, `deploy/`, and `demo/`). All v2 application code
  goes here: evolve `cmd/egress-broker/`, add the new `cmd/local-proxy/`, keep
  `echo-actor/`, add `proto/` for the proxy↔broker protocol (§5), update `deploy/`.
  ```bash
  git clone https://github.com/ronlv10/substrate-ws-poc && cd substrate-ws-poc
  git checkout -b ws-poc-v2 main
  ```
- **Substrate fork** — `github.com/ronlv10/substrate`, branch `ws-poc` (holds the
  atelet CA-mount + atespace-identity change, commit `5fdfb813`). Only branch this
  if v2 needs *new* substrate-side changes (e.g. the `/run/ate` identity-mount fix
  from §9). Branch off `ws-poc`, **never** touch `agent-substrate/substrate`
  upstream and open **no** PR there:
  ```bash
  # in the fork checkout:
  git checkout -b ws-poc-v2 ws-poc
  ```

Keep v1 as the known-good baseline: `substrate-ws-poc@main` (with `demo/`) and the
fork's `ws-poc` branch. Do all v2 work on the `ws-poc-v2` branches so the two
don't entangle.

### Fresh cluster (isolate from any degraded state)
```bash
# from the substrate fork checkout (on ws-poc-v2):
KIND_CLUSTER_NAME=kind ./hack/delete-kind-cluster.sh || true
KIND_CLUSTER_NAME=kind ./hack/create-kind-cluster.sh
KIND_CLUSTER_NAME=kind ./hack/install-ate-kind.sh --deploy-ate-system
ko build github.com/agent-substrate/substrate/cmd/ateom-gvisor   # digest → ATEOM_IMAGE
```

---

## 13. Open questions to resolve during spikes

- Does gVisor restore a loopback **TCP** connection functionally (not just allow the
  save)? (§8.1 — the whole thing depends on it.)
- WebSocket-over-loopback specifically (Bolt's `ws` lib): any read/write deadline
  that fires on restore even without a reconnect? (Monotonic-clock behaviour under
  gVisor S/R.)
- Broker-side detection of "proxy connection dropped == actor suspended" vs a
  transient network blip — debounce.
- Multi-actor: one broker, many proxies; keying by `Announce` identity; one real
  Slack connection per app-token/actor (the per-actor vs per-app concern from v1).
- `/readyz` timing: fast enough that `ResumeActor` returns quickly, but only green
  once the proxy can actually serve the agent.
```
