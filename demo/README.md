# WS-PoC demos

Two ways to watch the suspendable Slack agent live: a **visual dashboard** and a
**terminal narration**. Both follow the running cluster — message the bot and
watch the actor wake, reply, and get suspended again.

They work with any deployed actor. Point them at one with env vars:

| Actor | `ACTOR` | `DEPLOY` (worker deployment substring) |
|-------|---------|----------------------------------------|
| echo (baked-in proxy) | `echo-1` | `ws-poc-echo-deployment` |
| openclaw (baked-in proxy) | `openclaw-1` | `ws-poc-openclaw-deployment` |
| openclaw (sidecar proxy) | `openclaw-sc-1` | `ws-poc-openclaw-sidecar-deployment` |

Defaults target the echo actor. Other overrides: `BROKER_NS` (`ws-poc`),
`ACTOR_NS` (`ate-demo-ws-poc`), `ATESPACE` (`demo`).

## Prerequisites

- The PoC deployed and running (see the repo `README.md`): the `egress-broker`
  and the actor you're targeting. The openclaw actors also need the
  `anthropic-api-key` secret.
- `kubectl` **and** the `kubectl-ate` plugin on `PATH` (else prefix with your Go
  bin dir, e.g. `PATH="$HOME/go/1.25.4/bin:$PATH"`).
- `python3` (dashboard only; standard library, no installs).

---

## `dashboard.py` — live component diagram

A local web server rendering the architecture + a status board and streaming the
broker's lifecycle events; a message animates hop-by-hop.

```bash
# echo actor (default)
python3 demo/dashboard.py            # → http://localhost:8080  (PORT=8090 to change)

# openclaw sidecar actor
ACTOR=openclaw-sc-1 DEPLOY=ws-poc-openclaw-sidecar-deployment python3 demo/dashboard.py
```

Shows: the **Slack ⇄ Broker** always-on link, the **Actor ⇄ Broker** link that
appears on resume and fades on suspend, **⏱ time awake** (ticks while running,
freezes while suspended — the scale-to-zero story in one number), a component
board, and live `tail -f` panels for the broker and actor.

## `demo.sh` — terminal narration / evidence

First prints **source-level proof** that the agent is unmodified — for `echo`, the
Bolt app has zero lifecycle/gRPC calls; a third-party agent like OpenClaw runs
from a stock upstream image — while the broker owns the lifecycle
(`SuspendActor`/`ResumeActor`). Then follows the broker + actor logs live, tagging
each line `[BROKER]` or `[ACTOR]`, with a `[STATUS]` line on every
`SUSPENDED ⇄ RUNNING` transition.

```bash
# echo actor (default)
./demo/demo.sh

# openclaw sidecar actor
ACTOR=openclaw-sc-1 DEPLOY=ws-poc-openclaw-sidecar-deployment ./demo/demo.sh
```

Ctrl-C to stop either. See the repo `README.md` for the architecture.
