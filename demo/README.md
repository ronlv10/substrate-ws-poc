# WS-PoC demos

Two ways to showcase the suspendable Slack agent live: a **visual dashboard** and a
**terminal narration**. Both watch the running cluster — post a message that
@-mentions the bot and watch the actor wake, echo, and get suspended again.

## Prerequisites

- The PoC is deployed and running (see the repo `README.md`): the `egress-broker`
  in namespace `ws-poc`, and the `echo-1` actor in atespace `demo`.
- `kubectl` **and** the `kubectl-ate` plugin on `PATH`. If `kubectl ate …` isn't
  found, prefix commands with your Go bin dir, e.g.
  `PATH="$HOME/go/1.25.4/bin:$PATH"`.
- `python3` (dashboard only; standard library, no pip installs).

Override the targets with env vars if yours differ:
`BROKER_NS` (default `ws-poc`), `ACTOR_NS` (`ate-demo-ws-poc`),
`ATESPACE` (`demo`), `ACTOR` (`echo-1`).

---

## `dashboard.py` — live component diagram (recommended)

A tiny local web server that renders an architecture diagram + status board and
streams the broker's lifecycle events. When a message flows, the diagram animates
it hop-by-hop.

```bash
PATH="$HOME/go/1.25.4/bin:$PATH" python3 demo/dashboard.py
# → open http://localhost:8080   (PORT=8090 to change the port)
```

What you see:

- **Component diagram** — `Slack ⇄ Egress Broker ⇄ Substrate control plane ⇄ Echo
  Actor`, inside the `Kubernetes cluster · agent-substrate` boundary.
  - **Slack ⇄ Broker** — a **green, bidirectional, always-on** link: the persistent
    Socket Mode connection the broker holds on the actor's behalf. This never drops.
  - **Actor ⇄ Broker** — a **teal ephemeral link** that **appears when the actor
    resumes and fades out on suspend** (the proxy's gRPC session to the broker).
  - The **gray** arrows (Resume/Suspend, restore/checkpoint) light up and animate a
    packet **one hop at a time (~1.5 s each)** only while that request is happening.
- **⏱ Time awake** — cumulative seconds the actor was actually on a worker (ticks
  while running, **freezes while suspended**) + the awake/suspended ratio. The
  scale-to-zero story in one number.
- **Component board** — broker, substrate control plane, and the actor's status +
  which worker (or "none — checkpointed off all workers" when suspended).
- **Live lifecycle event feed** and two **`tail -f` panels** (egress broker + echo
  actor) streaming the real logs.

## `demo.sh` — terminal narration / evidence

A no-dependencies shell script. First prints **source-level proof** that the actor
is a stock Slack bot (0 lifecycle/gRPC calls) while the broker owns the lifecycle
(`SuspendActor`/`ResumeActor`), then follows the broker + actor logs live, tagging
every line `[BROKER]` (orchestration) or `[ACTOR]` (plain Bolt/echo), plus a
`[STATUS]` line on each `SUSPENDED ⇄ RUNNING` transition.

```bash
PATH="$HOME/go/1.25.4/bin:$PATH" ./demo/demo.sh
```

Ctrl-C to stop either tool. See the repo `README.md` for the architecture.
