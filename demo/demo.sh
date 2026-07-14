#!/usr/bin/env bash
# Copyright 2026 Google LLC
# SPDX-License-Identifier: Apache-2.0
#
# demo.sh — EVIDENCE that the actor is an unmodified Slack bot and that the
# suspend/resume lifecycle is driven entirely from OUTSIDE it.
#
# It first shows source-level proof (the actor has no lifecycle code; the broker
# does), then follows both components live and labels every line, so as you post
# a message you SEE the actor only doing Bolt/echo work while the BROKER calls
# ResumeActor / SuspendActor.
#
#   ./demo.sh          # run from the repo root
#
# Ctrl-C to stop.

set -uo pipefail

BROKER_NS="${BROKER_NS:-ws-poc}"
ACTOR_NS="${ACTOR_NS:-ate-demo-ws-poc}"
ATESPACE="${ATESPACE:-demo}"
ACTOR="${ACTOR:-echo-1}"
# Substring matching the actor's worker deployment (its pods carry the agent).
DEPLOY="${DEPLOY:-ws-poc-echo-deployment}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"

if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
  GRN=$'\033[32m'; CYN=$'\033[36m'; YEL=$'\033[33m'; RED=$'\033[31m'
else
  BOLD=; DIM=; RESET=; GRN=; CYN=; YEL=; RED=
fi
hr() { printf '%s\n' "${DIM}──────────────────────────────────────────────────────────────${RESET}"; }

kubectl get deploy egress-broker -n "$BROKER_NS" >/dev/null 2>&1 || {
  echo "egress-broker not found in namespace '$BROKER_NS'. Is the PoC deployed?" >&2; exit 1; }

clear 2>/dev/null || true
hr
echo "${BOLD}  WS-PoC — evidence: the actor doesn't manage its own lifecycle${RESET}"
hr
echo
if [ -f "$REPO/echo-actor/app.js" ] && [ -z "${ACTOR##echo*}" ]; then
  echo "${BOLD}  1) The actor is a stock Slack bot — proof from its source${RESET}  ${DIM}(echo-actor/app.js)${RESET}"
  echo
  echo "     modules it imports:"
  grep -nE 'require\(' "$REPO/echo-actor/app.js" | sed -E 's/^/       /'
  lc=$(grep -cE 'child_process|execFile|spawn|SuspendActor|ResumeActor|CheckpointWorkload|ateapi|@grpc|grpc' "$REPO/echo-actor/app.js" 2>/dev/null || echo 0)
  echo
  echo "     lifecycle/exec/gRPC calls in the actor:  ${BOLD}${GRN}${lc} matches${RESET}"
  echo "     ${DIM}→ no child_process, no gRPC, no SuspendActor/ResumeActor. It CANNOT${RESET}"
  echo "     ${DIM}  suspend or resume itself, and has no substrate awareness — the${RESET}"
  echo "     ${DIM}  proxy owns identity and lifecycle.${RESET}"
else
  echo "${BOLD}  1) The agent runs unmodified — a stock upstream image${RESET}"
  echo
  echo "     ${DIM}This actor (e.g. OpenClaw) is a third-party agent run from its stock${RESET}"
  echo "     ${DIM}image; it has no substrate awareness and cannot suspend or resume${RESET}"
  echo "     ${DIM}itself. The proxy (baked in, or a sidecar container) owns identity${RESET}"
  echo "     ${DIM}and lifecycle; the always-on broker checkpoints and wakes the actor.${RESET}"
fi
echo
echo "${BOLD}  2) The lifecycle lives in the broker — proof from its source${RESET}  ${DIM}(internal/broker)${RESET}"
sus=$(grep -rhoE 'SuspendActor' "$REPO/internal/broker/" 2>/dev/null | wc -l | tr -d ' ')
res=$(grep -rhoE 'ResumeActor'  "$REPO/internal/broker/" 2>/dev/null | wc -l | tr -d ' ')
echo
echo "     broker references to ${BOLD}SuspendActor${RESET}: ${CYN}${sus}${RESET}   ${BOLD}ResumeActor${RESET}: ${CYN}${res}${RESET}"
echo "     ${DIM}→ the always-on broker is what checkpoints and wakes the actor.${RESET}"
echo
hr
echo "  ${BOLD}Actor now:${RESET} $(kubectl ate get actors -a "$ATESPACE" 2>/dev/null | awk -v a="$ACTOR" '$0~a{print $5" (v"$NF"), ATEOM pod="$6}')"
echo
echo "  ${BOLD}${GRN}▶ Post a message that @-mentions the bot in Slack.${RESET}"
echo "  ${DIM}  Below, every line is tagged ${RESET}${CYN}[BROKER]${DIM} (orchestration) or ${RESET}${GRN}[ACTOR]${DIM} (plain bot).${RESET}"
echo "  ${DIM}  Note: the [ACTOR] lines never say suspend/resume — only Bolt + echo.${RESET}   ${DIM}(Ctrl-C to stop)${RESET}"
echo
hr

# --- role-based line formatter (heredoc so python can quote freely) --------
FMT="$(mktemp -t ws-poc-fmt.XXXXXX.py)"
cat > "$FMT" <<'PYEOF'
import sys, os, json
role = sys.argv[1]
tty = sys.stdout.isatty()
def c(x): return x if tty else ""
RESET=c("\033[0m"); BOLD=c("\033[1m"); DIM=c("\033[2m")
CYN=c("\033[36m"); GRN=c("\033[32m"); YEL=c("\033[33m")

def emit(tag, tagcolor, t, text, color=""):
    print(f"  {tagcolor}{tag}{RESET} {DIM}{t}{RESET}  {color}{text}{RESET}", flush=True)

for raw in sys.stdin:
    try:
        d = json.loads(raw)
    except Exception:
        continue
    m = d.get("msg") or d.get("message") or ""
    t = d.get("time", "")[11:19]

    if role == "broker":
        p = d.get("path", ""); st = d.get("status", "")
        if "real Slack event received" in m:
            print()
            emit("[BROKER]", CYN, t, "Slack sent a message on the persistent connection", CYN)
        elif "resuming suspended actor" in m:
            emit("[BROKER]", CYN, t, "→ calls ResumeActor on the substrate control plane", BOLD+CYN)
        elif "proxy announced" in m:
            emit("[BROKER]", CYN, t, "the resumed actor's proxy reconnects and re-announces")
        elif "proxy acked event" in m:
            emit("[BROKER]", CYN, t, "the actor handled the event (proxy acked)")
        elif "idle; suspending from broker" in m:
            emit("[BROKER]", CYN, t, "→ calls SuspendActor — checkpoints it from OUTSIDE", BOLD+CYN)
    else:  # actor: the proxy, plus the agent's own stdout (Bolt, or OpenClaw)
        if "event delivered to agent" in m:
            emit("[ACTOR]", GRN, t, "proxy delivers the buffered event to the agent over loopback")
        elif "Bolt app is running" in m:
            emit("[ACTOR]", GRN, t, "Bolt app running")
        elif "agent socket attached" in m:
            emit("[ACTOR]", GRN, t, "agent (re)connects to the proxy over loopback")
        elif "handling app_mention" in m or "handling message" in m:
            emit("[ACTOR]", GRN, t, "handles the @-mention and composes a reply", BOLD+GRN)
        elif "model-fetch] start" in m:
            emit("[ACTOR]", GRN, t, "calls the model to compose a reply", BOLD+GRN)
        elif "reply posted" in m or "delivered reply to channel" in m:
            emit("[ACTOR]", GRN, t, "posts the reply via chat.postMessage", BOLD+GRN)
PYEOF

# --- watch the actor's status so the SUSPENDED state itself is visible -----
# Prints a [STATUS] line on every transition. When SUSPENDED the ATEOM pod is
# <none> — the actor is checkpointed off ALL workers, using zero compute; that is
# the evidence it is genuinely suspended, not just idle.
status_watch() {
  local prev="" line cur pod ver t
  while true; do
    line=$(kubectl ate get actors -a "$ATESPACE" 2>/dev/null | awk -v a="$ACTOR" '$0~a{print $5"|"$6"|"$NF}')
    cur="${line%%|*}"
    if [ -n "$cur" ] && [ "$cur" != "$prev" ]; then
      t=$(date -u +%H:%M:%S); pod=$(printf '%s' "$line" | cut -d'|' -f2); ver=$(printf '%s' "$line" | cut -d'|' -f3)
      if [ "$cur" = "STATUS_SUSPENDED" ]; then
        printf '  %s[STATUS]%s %s%s%s  %sSUSPENDED — checkpointed OFF all workers (ATEOM pod=%s), 0 compute%s\n' \
          "$YEL$BOLD" "$RESET" "$DIM" "$t" "$RESET" "$YEL" "$pod" "$RESET"
      else
        printf '  %s[STATUS]%s %s%s%s  %s%s on worker %s (v%s)%s\n' \
          "$YEL$BOLD" "$RESET" "$DIM" "$t" "$RESET" "$YEL" "${cur#STATUS_}" "$pod" "$ver" "$RESET"
      fi
      prev="$cur"
    fi
    sleep 1
  done
}

# --- follow the broker and BOTH worker pods (the actor runs inside them) ----
pids=()
status_watch &
pids+=($!)
kubectl logs -n "$BROKER_NS" deploy/egress-broker -f --tail=0 2>/dev/null | python3 -u "$FMT" broker &
pids+=($!)
for pod in $(kubectl get pods -n "$ACTOR_NS" -o name 2>/dev/null | grep "$DEPLOY"); do
  kubectl logs -n "$ACTOR_NS" "$pod" -f --tail=0 --all-containers 2>/dev/null | python3 -u "$FMT" actor &
  pids+=($!)
done
trap 'rm -f "$FMT"; kill "${pids[@]}" 2>/dev/null' EXIT INT TERM
wait
