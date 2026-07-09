# Plan — Sidecar egress-proxy evolution for suspendable Slack agents (ws-poc v2)

> Status: **implemented on the `ws-poc-v2` branch** (fresh kind cluster;
> phase-0 spikes under `spikes/`, proxy under `cmd/local-proxy` +
> `internal/proxy`, protocol under `proto/brokerproxy`). Findings from the
> spikes and implementation are folded in below, marked **[VERIFIED]** where
> the original design's assumptions were tested.
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

### Heartbeats: the churn relocates, it does not disappear **[VERIFIED]**
The spike (`spikes/boltloop`) settled this: the loopback socket itself
survives restore (ping/pong exchanged on the original connection afterwards),
but the stock `@slack/socket-mode` client **still reconnects on every warm
restore** — its pong/ping staleness checks use the wall clock, and gVisor
advances both the wall and monotonic clocks by the suspend duration on
restore. The difference from v1 is that the churn is now a ~40 ms loopback
ritual against a cooperative server: the proxy holds event delivery until the
(re)attached connection proves itself with a heartbeat ping, so nothing can
land in the churn window (an event written during it is silently dropped —
reproduced 4/4 in the spike). Delivered-after-heartbeat events were 100%
reliable, with original envelope ids and no duplicates.

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

Messages (implemented in `proto/brokerproxy/v1/brokerproxy.proto`):
- `proxy → broker`
  - `Announce { atespace, name, app_token, last_acked_seq }` — first frame on every
    (re)connect; identifies the actor and lets the broker resume the stream.
  - `Ack { seq }` — event fully handled (agent acked it).
  - `Egress { corr_id, method, path, headers, body }` — relay agent's outbound
    Slack Web API (chat.postMessage, etc.); broker forwards to real Slack,
    returns the matching `EgressResp`.
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

- **Redirect:** per-image `/etc/hosts` mapping `slack.com`,
  `wss-primary.slack.com` → `127.0.0.1`. **No cluster-wide CoreDNS rewrite.** Only
  actors that ship the proxy + hosts entry are affected. **[VERIFIED]** atelet
  mounts nothing over `/etc/hosts`, so the image's copy is what the sandbox
  resolves with (`HostAliases` is not a mechanism substrate actors have — the
  image file is the whole story).
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

## 8. Risks & spikes — RESULTS (run 2026-07-09, `spikes/`)

1. **[CRITICAL] Loopback survives gVisor checkpoint/restore — PASS.**
   `spikes/loopback`: one dial, one accept, zero errors across the
   golden-snapshot instantiation plus 3 explicit suspend/resume cycles; the
   heartbeat seq continued on the same connection and ephemeral port every
   time. Bonus finding: the connection even survives golden→derived
   instantiation (each derived actor resumes its own copy of the golden's
   connection). Wall and monotonic clocks both jump by the suspend duration.
2. **Redirect works in-sandbox — PASS.** Image-baked `/etc/hosts` resolves
   `slack.com → 127.0.0.1` under netgo; substrate does not clobber it.
3. **Two-process actor container — PASS.** PID-1 supervisor + child, both
   checkpointed/restored together, `/readyz` gating works.
4. **TLS on localhost — PASS.** Bolt trusts the image-baked CA via
   `NODE_EXTRA_CA_CERTS` for the proxy's static `slack.com` leaf.
5. **Identity source — PASS** (see §9: contamination does not reproduce).
6. **NEW FINDING — checkpoint quiescence.** `spikes/boltloop` + the echo
   actor: a Node process checkpointed while busy (mid-V8-startup, or holding
   an in-flight HTTP request with retry timers) **SIGILLs on restore**; a
   settled Node restores reliably (4/4, repeatedly). Consequence: `/readyz`
   must be green only once the agent is attached and has heartbeated, so both
   the golden checkpoint and broker-driven suspends capture a quiet process.
   Readiness must NOT wait for in-flight egress to drain — a retrying client
   would hold readyz hostage and deadlock golden creation.

---

## 9. Dependency: golden-snapshot identity contamination (`/run/ate`)

Observed in v1: for actors instantiated from a golden snapshot, `/run/ate/actor-id`
and `/run/ate/atespace` report the **golden** actor's identity (`ate-golden/<uuid>`)
rather than the real actor (`demo/echo-1`) — even on `--boot`. This poisons *any*
identity mechanism that reads `/run/ate`.

**[VERIFIED — does not reproduce; no fork changes needed.]** The spike showed
the identity mount is regenerated on every resume and reads correctly
(`demo/spike-2`, not `ate-golden/…`) for golden-derived actors — **as long as
it is read after the restore**. Reads taken at process start execute in the
golden era and see the golden identity; that is what poisoned v1. The rule the
proxy implements: read `/run/ate` fresh on every broker (re)connect, never
cache. The fork's `ws-poc` branch is still required for the
`/run/ate/atespace` write itself (commit 5fdfb813).

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

---

## 14. Warm restore: why v1's Socket Mode churned, and the fix

v1 resumes **cold** (`onPause: Data` → the actor process restarts from scratch on
every resume). That is reliable but throws away all process memory. The reason we
fell back to cold boot is that **warm** restore (`onPause: Full`, resume with the
heap intact) reliably *churned* and dropped the just-delivered event. This section
records exactly why — grounded in `@slack/socket-mode@1.3.6` (what Bolt 3.22 bundles
into the actor) and our broker — and the concrete fix, so v2 doesn't rediscover it.

### 14.1 Root cause

Warm restore reanimates the actor's Socket Mode client holding
**{a dead socket + a stale wall-clock pong timestamp + live heartbeat timers}**, and
the SDK's only recovery path is the *graceful smooth-reconnect* designed for a
still-alive socket. Applied to an already-dead socket it is the wrong path.

State trace (SDK line refs are `dist/SocketModeClient.js`):

1. **Checkpoint** freezes the client in `Connected/Ready`: `this.websocket` = a live
   WS to the broker, two timers armed, `lastPongReceivedTimestamp` = a wall-clock
   value. Broker sees the TCP tear down → `Detach(sink)` → `s.sink = nil`
   (`cmd/egress-broker/registry.go` `Detach`), event buffer retained.
2. A Slack message arrives → broker acks Slack immediately (`registry.go` `readSlackUntilClose`),
   buffers, `sink==nil && !resuming` → sets `resuming=true`, calls `ResumeActor`.
3. **Warm restore.** The process is thawed with the state machine *still Connected/Ready*.
   The client **believes it is connected** and does **not** dial
   `apps.connections.open` → no new WS, no broker `Attach`, no heartbeat, `resuming`
   stuck `true`, event stuck in the buffer. Meanwhile:
   - **`/readyz` on :80 answers 200 instantly** (the HTTP server is in the restored
     heap) → substrate's `ResumeActor` readiness gate reports the actor **ready**
     while its Socket Mode layer is dead. The "resume-then-deliver is safe" guarantee
     silently breaks. (Cold boot has no such gap: the :80 server doesn't exist until
     the process boots, so readyz ≈ app-startup.)
   - **~1.7 s later** the client-ping interval (`clientPingTimeoutMillis/3`, ~line 511)
     fires and hits two clock landmines at once: `this.websocket.ping()` on a **dead
     FD** throws → `handlePingPongErrorReconnection()`; and the pong-age check uses
     **wall clock** (`new Date().getTime()`, ~line 512/532) so `now - lastPong` = the
     whole suspend duration `> clientPingTimeoutMillis` → both fire
     `ServerPongsNotReceived`. The dead primary's `ws` also emits `close`/`error` →
     `WebSocketClose`. Multiple concurrent reconnect triggers.
4. **The churn.** `Reconnecting.do()` sets `isSwitchingConnection = true` (~line 206).
   The reconnect transition ran `markCurrentWebSocketAsInactive()` (~line 184), which
   flips flags but **does not clear `this.websocket`** — so on re-auth
   `setupWebSocket()` sees `this.websocket !== undefined` and builds a **secondary**
   (~line 415), i.e. the smooth-reconnect path. Spurious `close`/`error` from the dead
   primary interleave with the switch and can knock the machine back into
   `Reconnecting` *after* it promoted the good socket, tearing it down again. Net: the
   client either **wedges** with the dead socket as `this.websocket` (its heartbeat
   never reaches the broker → `MarkReady` never fires → event never delivered → the
   observed *"sent multiple times, nothing"*), or it churns and a delivery lands on a
   sink torn down mid-cycle. Slack won't redeliver — the broker already acked it.

Our broker is already heavily armored against ordinary churn — buffer-until-ack,
redeliver-on-every-`Attach`, deliver only on the client's first heartbeat
(`server.go` `SetPingHandler` → `session.MarkReady`), fresh per-delivery
`envelope_id`. Those defenses are necessary but **not sufficient** here, because warm
restore can prevent the client from ever presenting a single clean Ready connection
that heartbeats once.

### 14.2 Why cold boot dodges all of it

The process restarts → one `app.start()` → one `apps.connections.open` → one
**primary** socket → clean `hello → Ready → first heartbeat`, and `/readyz` tracks
that. No stale timers, no dead FD, no secondary, no spurious events. Reliable — at the
cost of discarding the heap.

### 14.3 The v1-side fix (designed, never implemented — superseded by v2's proxy)

Two halves: make the actor recover *cleanly*, and make readiness tell the *truth*.

**Actor side — turn the churn into one deterministic restart, keep the heap
(`echo-actor/app.js`):**
- **Freeze-gap restore detector.** A 250 ms `setInterval` records `Date.now()`; a tick
  observing a multi-second gap could only have been frozen → we were checkpointed.
  No substrate hook needed; robust to any suspend duration.
- **On detect, hard-reset the WS layer only:** `await app.stop(); await app.start();`.
  `stop()` drives the SDK's `Disconnecting` → `terminateAllConnections()` +
  `removeAllListeners()` (kills the dead primary **and its spurious events**) +
  `terminateActiveHeartBeatJobs()`; `start()` builds a single fresh **primary** with
  fresh timers and a fresh pong timestamp — the reliable cold-boot code path, but
  preserving process memory (the `echoed` de-dupe `Set`, V8 JIT warmth, and — in a
  real agent — in-memory context). Sidesteps §14.1's landmines by construction.

**Broker/substrate side — stop lying about readiness:**
- **`/readyz` reflects Socket Mode connectivity, not process liveness.** Drive it from
  the client's real state events (`app.receiver.client` emits
  `connecting|connected|reconnecting|disconnecting|disconnected`): 200 only after
  `connected`, 503 the instant a (re)connect starts. Then `ResumeActor`'s gate waits
  for the *real* WS and the resume-then-deliver guarantee holds under warm restore.
- **Keep** the existing buffer-until-ack + fresh-`envelope_id` + `MarkReady`-on-first-
  heartbeat machinery — now *sufficient*, because the two changes above guarantee a
  single clean Ready connection that heartbeats exactly once.

### 14.4 Why v2 makes this structural (not a patch)

v1's actor↔broker socket is **external**, so it is dead on restore no matter how
cleanly you reconnect — §14.3's actor-side reset is the best v1 could have done.
v2's sidecar fixes it at the root: the agent↔proxy socket is **in-sandbox
loopback** and survives the checkpoint (**[VERIFIED]**, §8.1); only the
proxy→broker leg reconnects, and the proxy is a purpose-built reconnector with
**none** of the SDK's smooth-reconnect baggage. One honest amendment from the
spikes: the agent's Socket Mode client still *chooses* to reconnect locally
after each restore (wall-clock staleness, §2) — but it never sees a dead
socket mid-delivery, because the proxy holds events until the settled
connection heartbeats. "Resume with memory" holds: the heap (dedup set, app
state) survives; the reconnect costs one loopback `apps.connections.open`.

**When warm restore is worth its cost:** for the echo PoC the heap is trivial, so cold
boot wins on simplicity. Warm restore earns its cost for a real agent — large heap,
in-memory LLM context and tool caches, JIT warmth — where resuming in ~one
`apps.connections.open` RTT instead of a full `app.start()` + module load is a real
latency and correctness win.
