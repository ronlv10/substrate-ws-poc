#!/usr/bin/env python3
# Copyright 2026 Google LLC
# SPDX-License-Identifier: Apache-2.0
#
# dashboard.py — a live component diagram for the WS-PoC.
#
# Serves an architecture diagram + status board at http://localhost:8080. It polls
# the cluster for component health and streams the broker's lifecycle events
# (Server-Sent Events); when you post a Slack message the diagram animates the
# request/message flow along labeled arrows:
#   Slack -> Broker -> ateapi (ResumeActor) -> Actor (warm restore) -> deliver ->
#   echo -> Slack -> Broker (SuspendActor) -> Actor SUSPENDED (checkpoint).
#
#   python3 dashboard.py            # then open http://localhost:8080
#   (needs kubectl + the kubectl-ate plugin on PATH)
#
# Env: PORT (default 8080), BROKER_NS, ACTOR_NS, ATESPACE, ACTOR.

import json, os, subprocess, threading, time, queue
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT       = int(os.environ.get("PORT", "8080"))
BROKER_NS  = os.environ.get("BROKER_NS", "ws-poc")
ACTOR_NS   = os.environ.get("ACTOR_NS", "ate-demo-ws-poc")
ATESPACE   = os.environ.get("ATESPACE", "demo")
ACTOR      = os.environ.get("ACTOR", "echo-1")
DEPLOY     = os.environ.get("DEPLOY", "ws-poc-echo-deployment")

STATE = {"broker": {}, "substrate": {}, "actor": {}, "slack": {}, "metrics": {}, "ts": 0}
# SSE channels: "events" (classified lifecycle events), "broker"/"actor" (raw-ish
# tail -f log lines). Each holds a list of per-client Queues.
CHANNELS = {"events": [], "broker": [], "actor": []}
CH_LOCK = threading.Lock()
AWAKE = {"total": 0.0, "since": 0.0, "start": time.time()}
BROKER_PROC = None


def publish(channel, obj):
    data = "data: " + json.dumps(obj) + "\n\n"
    with CH_LOCK:
        dead = []
        for q in CHANNELS[channel]:
            try:
                q.put_nowait(data)
            except Exception:
                dead.append(q)
        for q in dead:
            CHANNELS[channel].remove(q)


def run(cmd, timeout=10):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


def poll_state():
    while True:
        st = {"ts": int(time.time())}
        out = run(["kubectl", "get", "pods", "-n", BROKER_NS, "-l", "app=egress-broker",
                   "-o", "jsonpath={.items[0].status.phase}"])
        st["broker"] = {"status": out.strip() or "NotFound", "ok": out.strip() == "Running"}

        out = run(["kubectl", "get", "pods", "-n", "ate-system", "--no-headers"])
        total = ready = 0
        for ln in out.splitlines():
            f = ln.split()
            if len(f) < 3:
                continue
            total += 1
            r = f[1]
            if ("/" in r and r.split("/")[0] == r.split("/")[1]) or f[2] in ("Running", "Completed"):
                ready += 1
        st["substrate"] = {"status": f"{ready}/{total} pods ready", "ok": total > 0 and ready >= total - 1}

        out = run(["kubectl", "ate", "get", "actors", "-a", ATESPACE])
        actor = {"status": "Unknown", "pod": "", "version": "", "suspended": False}
        for ln in out.splitlines():
            if ACTOR in ln:
                f = ln.split()
                actor["status"] = f[4].replace("STATUS_", "")
                actor["pod"] = f[5] if len(f) > 5 else ""
                actor["version"] = f[-1]
                actor["suspended"] = "SUSPEND" in f[4]
                break
        st["actor"] = actor
        st["slack"] = {"status": "connected via broker", "ok": st["broker"]["ok"]}

        now = time.time()
        has_worker = actor["pod"] not in ("", "<none>")
        if has_worker and AWAKE["since"] == 0:
            AWAKE["since"] = now
        elif not has_worker and AWAKE["since"] > 0:
            AWAKE["total"] += now - AWAKE["since"]
            AWAKE["since"] = 0
        awake_current = (now - AWAKE["since"]) if AWAKE["since"] > 0 else 0.0
        st["metrics"] = {
            "up": has_worker,
            "awake_total": round(AWAKE["total"] + awake_current, 1),
            "awake_current": round(awake_current, 1),
            "elapsed": round(now - AWAKE["start"], 1),
        }
        STATE.update(st)
        time.sleep(2)


def classify(d):
    m = d.get("msg", "")
    p = d.get("path", "")
    st = d.get("status", "")
    t = d.get("time", "")[11:19]
    if "real Slack event received" in m:
        return {"type": "message", "t": t, "label": "Slack delivered a message on the persistent connection"}
    if "resuming suspended actor" in m:
        return {"type": "resume", "t": t, "label": "Broker calls ResumeActor on the substrate control plane"}
    if "proxy announced" in m and "golden" not in m:
        return {"type": "boot", "t": t, "label": "Resumed actor's proxy reconnects and re-announces"}
    if "proxy acked event" in m:
        return {"type": "deliver", "t": t, "label": "Actor handled the event (proxy acked)"}
    if "idle; suspending from broker" in m:
        return {"type": "suspend", "t": t, "label": "Broker calls SuspendActor — checkpoints the actor from OUTSIDE"}
    return None


def fmt_broker(d):
    m = d.get("msg", "").replace("egress-broker: ", "")
    extra = " ".join(f"{k}={d[k]}" for k in ("actor", "path", "status", "trigger", "err")
                     if d.get(k) not in (None, "", 0))
    return (d.get("time", "")[11:19] + "  " + m + ("  " + extra if extra else "")).strip()


def fmt_actor(m):
    m = m.strip()
    if "Received a message on the WebSocket" in m:
        return "socket-mode: received a frame from the broker"
    for pfx in ("[DEBUG]  ", "[INFO]  ", "[WARN]  ", "[ERROR]  "):
        m = m.replace(pfx, "")
    return m[:160]


def tail_broker():
    global BROKER_PROC
    while True:
        p = subprocess.Popen(
            ["kubectl", "logs", "-n", BROKER_NS, "deploy/egress-broker", "-f", "--tail=0"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        BROKER_PROC = p
        try:
            for line in p.stdout:
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                publish("broker", {"line": fmt_broker(d)})   # live tail
                ev = classify(d)
                if ev:
                    publish("events", ev)                    # animate the diagram
        except Exception:
            pass
        time.sleep(2)


# Lines worth showing in the actor tail (the rest is gVisor/loader noise).
_ACTOR_KEEP = ("echo-actor:", "starting Socket Mode", "Bolt app is running",
               "Now connected to Slack", "connected:ready", "Reconnecting to Slack",
               "apps.connections.open", "Received a message on the WebSocket",
               "Switched to the secondary", "Actor restored",
               # OpenClaw / proxy markers
               "agent socket attached", "event delivered to agent",
               "model-fetch] start", "model-fetch] response", "delivered reply to channel")


def tail_actor():
    started = set()

    def tail_pod(pod):
        while True:
            p = subprocess.Popen(
                ["kubectl", "logs", "-n", ACTOR_NS, pod, "-f", "--tail=0", "--all-containers"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            try:
                for line in p.stdout:
                    try:
                        d = json.loads(line)
                        m = d.get("message") or d.get("msg") or ""
                        t = d.get("time", "")[11:19]
                    except Exception:
                        m, t = line, ""
                    if any(k in m for k in _ACTOR_KEEP):
                        publish("actor", {"line": (t + "  " + fmt_actor(m)).strip()})
            except Exception:
                pass
            time.sleep(2)

    while True:
        for ln in run(["kubectl", "get", "pods", "-n", ACTOR_NS, "-o", "name"]).splitlines():
            if DEPLOY in ln:
                pod = ln.split("/")[-1]
                if pod not in started:
                    started.add(pod)
                    threading.Thread(target=tail_pod, args=(pod,), daemon=True).start()
        time.sleep(10)


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>WS-PoC — live</title>
<style>
  :root{--bg:#0b0f17;--card:#141b28;--line:#263247;--dim:#8595ad;--txt:#e6edf5;
        --blue:#4c8dff;--orange:#e0a133;--purple:#a371f7;--green:#2ea043;--accent:#39a7ff;--sleep:#6b7688;}
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--txt);
    font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
  header{padding:16px 24px;border-bottom:1px solid var(--line)}
  h1{margin:0;font-size:18px}.sub{color:var(--dim);font-size:12px;margin-top:3px}
  .wrap{max-width:1120px;margin:0 auto;padding:20px 24px}
  .board{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:8px 8px 4px}
  svg{width:100%;height:auto;display:block}
  .legend{display:flex;gap:18px;flex-wrap:wrap;color:var(--dim);font-size:12px;padding:6px 12px 10px}
  .legend b{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px;vertical-align:middle}
  .cols{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}
  .panel{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
  .panel h3{margin:0 0 10px;font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:.06em}
  .kv{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px dashed var(--line);font-size:13px}
  .kv:last-child{border:none}.kv .v{color:var(--dim)}
  .metrics{display:flex;gap:34px;align-items:baseline;flex-wrap:wrap;margin-top:16px}
  .metrics .lbl{text-transform:uppercase;letter-spacing:.06em;font-size:11px;color:var(--dim)}
  #m-total{font-size:30px;font-weight:700;line-height:1.1}
  #feed{max-height:260px;overflow:auto;font:12px/1.6 ui-monospace,Menlo,monospace}
  #feed .ev{padding:4px 0;border-bottom:1px solid #0d1420}
  #feed .tag{display:inline-block;width:78px;color:var(--accent);font-weight:600}
  #feed .tag.echo{color:var(--green)}#feed .tag.suspend{color:var(--sleep)}
  /* svg element states */
  .box{transition:filter .25s} .box.pulse{filter:drop-shadow(0 0 10px var(--accent))}
  .box.pulse.echo{filter:drop-shadow(0 0 10px var(--green))}
  .edge{transition:stroke .2s,stroke-width .2s}
  .edge.flash{stroke:var(--accent)!important;stroke-width:3!important}
  .edge.flash.echo{stroke:var(--green)!important}
  .wsgroup{opacity:0;transition:opacity .55s} /* ephemeral WS: fully hidden until the actor connects */
  @keyframes beat{0%,100%{opacity:.35}50%{opacity:1}}
  .live{animation:beat 1.6s ease-in-out infinite}
  .logs{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}
  .logbox{background:var(--card);border:1px solid var(--line);border-radius:12px;overflow:hidden}
  .logbox h3{margin:0;padding:10px 14px;font-size:12px;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--line)}
  .logbox.broker h3{color:var(--orange)} .logbox.actor h3{color:var(--green)}
  .logbox pre{margin:0;height:236px;overflow:auto;padding:10px 14px;
    font:11.5px/1.55 ui-monospace,Menlo,monospace;color:#c7d3e3;white-space:pre-wrap;word-break:break-word}
</style></head>
<body>
<header>
  <h1>WS-PoC — suspendable Slack agent · live component diagram</h1>
  <div class="sub">Message the bot; watch the request flow animate and the actor wake, reply, and get suspended.</div>
</header>
<div class="wrap">
  <div class="board">
    <svg viewBox="0 0 1000 400" role="img">
      <defs>
        <marker id="arw" markerWidth="9" markerHeight="9" refX="7" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 Z" fill="#6b7a92"/></marker>
        <marker id="arwLive" markerWidth="10" markerHeight="10" refX="7" refY="4" orient="auto-start-reverse">
          <path d="M0,0 L8,4 L0,8 Z" fill="#2ea043"/></marker>
        <marker id="arwWs" markerWidth="10" markerHeight="10" refX="7" refY="4" orient="auto-start-reverse">
          <path d="M0,0 L8,4 L0,8 Z" fill="#3ec7d4"/></marker>
        <filter id="glow"><feGaussianBlur stdDeviation="2.4" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
      </defs>

      <!-- cluster boundary -->
      <rect x="206" y="40" width="770" height="336" rx="14" fill="none"
            stroke="#334156" stroke-dasharray="6 6"/>
      <text x="226" y="64" fill="#8595ad" font-size="13">Kubernetes cluster · agent-substrate</text>

      <!-- ===== edges (drawn first, under boxes) ===== -->
      <!-- Slack <-> Broker : PERSISTENT Socket Mode (always active, bidirectional) -->
      <path id="e-slack-broker" class="edge liveedge" d="M182,272 L298,272" fill="none" stroke="#2ea043" stroke-width="2.5" marker-start="url(#arwLive)" marker-end="url(#arwLive)"/>
      <text x="240" y="256" fill="#cfe9d6" font-size="12" text-anchor="middle">Socket Mode WSS</text>
      <circle cx="212" cy="290" r="4" fill="#2ea043" class="live"/>
      <text x="224" y="294" fill="#2ea043" font-size="11">persistent · always connected</text>

      <!-- Broker -> ateapi : Resume/Suspend -->
      <path id="e-broker-ateapi" class="edge" d="M360,224 C360,196 360,186 360,156" fill="none" stroke="#3a4a63" stroke-width="2" marker-end="url(#arw)"/>
      <text x="372" y="196" fill="#8595ad" font-size="12">Resume / Suspend Actor</text>

      <!-- ateapi -> Actor : restore/checkpoint (dashed) -->
      <path id="e-ateapi-actor" class="edge" d="M518,112 C610,112 600,175 636,214" fill="none" stroke="#3a4a63" stroke-width="2" stroke-dasharray="5 5" marker-end="url(#arw)"/>
      <text x="560" y="150" fill="#8595ad" font-size="12">restore / checkpoint</text>

      <!-- Proxy <-> Broker : EPHEMERAL gRPC session — exists only while the actor
           runs. The whole group fades IN when it connects and OUT (disappears) on
           suspend. deliver (broker->proxy) and echo (proxy->broker) flow along it. -->
      <g id="ws-group" class="wsgroup">
        <path id="e-actor-ws" class="edge" d="M518,280 L634,280" fill="none" stroke="#3ec7d4" stroke-width="2.5" marker-start="url(#arwWs)" marker-end="url(#arwWs)"/>
        <text x="576" y="266" fill="#bfeaf0" font-size="12" text-anchor="middle">gRPC session</text>
        <text id="ws-state" x="576" y="300" fill="#3ec7d4" font-size="11" text-anchor="middle">● connected</text>
      </g>

      <!-- ===== boxes ===== -->
      <!-- Slack (blue, external) -->
      <g id="n-slack" class="box">
        <rect x="26" y="216" width="156" height="100" rx="12" fill="#17233a" stroke="#4c8dff" stroke-width="1.5"/>
        <circle id="st-slack" cx="168" cy="228" r="5" fill="#6b7688"/>
        <text x="104" y="272" fill="#e6edf5" font-size="16" font-weight="700" text-anchor="middle">Slack</text>
      </g>
      <!-- ateapi (purple, control plane) -->
      <g id="n-ateapi" class="box">
        <rect x="300" y="76" width="216" height="80" rx="12" fill="#221833" stroke="#a371f7" stroke-width="1.5"/>
        <circle id="st-ateapi" cx="502" cy="88" r="5" fill="#6b7688"/>
        <text x="408" y="122" fill="#e6edf5" font-size="15" font-weight="700" text-anchor="middle">Substrate control plane</text>
      </g>
      <!-- Egress Broker (orange) -->
      <g id="n-broker" class="box">
        <rect x="300" y="224" width="216" height="108" rx="12" fill="#241d10" stroke="#e0a133" stroke-width="1.5"/>
        <circle id="st-broker" cx="502" cy="236" r="5" fill="#6b7688"/>
        <text x="408" y="284" fill="#e6edf5" font-size="16" font-weight="700" text-anchor="middle">Egress Broker</text>
      </g>
      <!-- Actor (green, gVisor) -->
      <g id="n-actor" class="box">
        <rect x="636" y="202" width="272" height="150" rx="12" fill="#10231a" stroke="#2ea043" stroke-width="1.5"/>
        <circle id="st-actor" cx="894" cy="214" r="5" fill="#6b7688"/>
        <text x="772" y="228" fill="#e6edf5" font-size="14" font-weight="700" text-anchor="middle">Actor</text>
        <rect x="656" y="248" width="232" height="86" rx="9" fill="#0c1a13" stroke="#1f3b2a"/>
        <text x="772" y="272" fill="#e6edf5" font-size="13" font-weight="600" text-anchor="middle">@slack/bolt ⇄ local proxy</text>
        <text x="772" y="290" fill="#8595ad" font-size="11" text-anchor="middle">stock bot + proxy · loopback WS survives</text>
        <text x="772" y="320" fill="#8595ad" font-size="11" text-anchor="middle" id="actor-sub">suspended / running…</text>
      </g>
    </svg>
    <div class="legend">
      <span><b style="background:#4c8dff"></b>external (Slack)</span>
      <span><b style="background:#e0a133"></b>egress broker (bridges the suspend gap)</span>
      <span><b style="background:#a371f7"></b>substrate control plane</span>
      <span><b style="background:#2ea043;border-radius:50%"></b>persistent live connection</span>
      <span><b style="background:#2ea043"></b>sandboxed actor (suspendable)</span>
    </div>
  </div>

  <div class="metrics panel">
    <div><div class="lbl">Time actor was awake (on a worker)</div><div id="m-total">0s</div></div>
    <div><div class="lbl">Right now</div><div id="m-now" style="font-size:18px;font-weight:600">…</div></div>
    <div><div class="lbl">Awake ratio (since dashboard start)</div><div id="m-ratio" style="font-size:18px;font-weight:600">…</div></div>
  </div>

  <div class="cols">
    <div class="panel">
      <h3>Components</h3>
      <div class="kv"><span>Slack connection</span><span class="v" id="p-slack">…</span></div>
      <div class="kv"><span>Egress broker</span><span class="v" id="p-broker">…</span></div>
      <div class="kv"><span>Substrate control plane</span><span class="v" id="p-sub">…</span></div>
      <div class="kv"><span style="font-weight:600">Actor</span><span class="v" id="p-actor" style="font-weight:600">…</span></div>
      <div class="kv"><span>Actor worker (ATEOM pod)</span><span class="v" id="p-pod">…</span></div>
    </div>
    <div class="panel">
      <h3>Live lifecycle events</h3>
      <div id="feed"><div class="ev" style="color:var(--dim)">waiting for a message…</div></div>
    </div>
  </div>

  <div class="logs">
    <div class="logbox broker"><h3>tail -f · egress broker  (ns: ws-poc)</h3><pre id="log-broker">…</pre></div>
    <div class="logbox actor"><h3>tail -f · echo actor  (the Bolt bot)</h3><pre id="log-actor">…</pre></div>
  </div>
</div>
<script>
const $ = s => document.querySelector(s);
const NS = "http://www.w3.org/2000/svg";
function dotFill(id, color){ const e=$(id); if(e) e.setAttribute("fill", color); }
const OK="#2ea043", WARN="#d29922", SLEEP="#6b7688", RUN="#2ea043";

let M=null, Mat=0;
let liveUp=null;   // event-driven "actor is up" for the counter (null = use poll)
// Set the actor node's visual state (node dim/glow, status dot, WS link).
function setActor(suspended, dotColor, sub){
  $('#n-actor').style.opacity = suspended ? .55 : 1;
  dotFill('#st-actor', dotColor);
  if(sub) $('#actor-sub').textContent = sub;
  // fade the whole ephemeral WS group fully in/out (disappears on suspend)
  const wsg=$('#ws-group'); if(wsg) wsg.style.opacity = suspended ? 0 : 1;
}
// When a live event just told us the actor's state, trust it over the (laggy)
// poll for a short window so the node reacts instantly.
let actorHint = 0;
async function refresh(){
  try{
    const st = await (await fetch('/api/state')).json();
    $('#p-slack').textContent = st.slack.status;  dotFill('#st-slack', st.slack.ok?OK:WARN);
    $('#p-broker').textContent = st.broker.status; dotFill('#st-broker', st.broker.ok?OK:WARN);
    $('#p-sub').textContent = st.substrate.status; dotFill('#st-ateapi', st.substrate.ok?OK:WARN);
    const a = st.actor;
    $('#p-actor').textContent = a.status + ' (v'+a.version+')';
    $('#p-pod').textContent = (a.pod && a.pod!=='<none>') ? a.pod.split('/').pop() : '— none (checkpointed off all workers)';
    // Only let the (laggy) poll drive the node when no fresh live hint is active.
    if(performance.now() > actorHint){
      setActor(a.suspended, a.suspended?SLEEP:(a.status==='RUNNING'?RUN:WARN),
               a.suspended?'SUSPENDED — off all workers':(a.status+' · v'+a.version));
    }
    M = st.metrics || null; Mat = performance.now();
  }catch(e){}
}
setInterval(refresh, 1500); refresh();

function fmtDur(s){ s=Math.max(0,Math.round(s)); if(s<60) return s+'s';
  const m=Math.floor(s/60), r=s%60; if(m<60) return m+'m '+r+'s';
  return Math.floor(m/60)+'h '+(m%60)+'m'; }
setInterval(()=>{
  if(!M) return;
  const dt=(performance.now()-Mat)/1000;
  const up = (liveUp!==null) ? liveUp : M.up;   // trust live events over the laggy poll
  const total=M.awake_total+(up?dt:0), cur=up?(M.awake_current+dt):0, el=M.elapsed+dt;
  $('#m-total').textContent = fmtDur(total);
  $('#m-now').textContent = up ? ('🟢 awake '+fmtDur(cur)) : '💤 suspended · 0 compute';
  const pct = el>0 ? 100*total/el : 0;
  $('#m-ratio').textContent = pct.toFixed(1)+'% awake · '+(100-pct).toFixed(1)+'% suspended';
},1000);

// animate a packet dot travelling along an edge path; colour the arrow only while
// active, then revert to its idle stroke (gray for per-request edges; green for
// the always-live Slack connection). Slow (1.5s) so each hop is easy to follow.
const PDUR = 1500;
function packet(pathId, color, reverse){
  const path=document.getElementById(pathId); if(!path||!path.getTotalLength) return;
  const len=path.getTotalLength();
  const dot=document.createElementNS(NS,'circle');
  dot.setAttribute('r','5.5'); dot.setAttribute('fill',color); dot.setAttribute('filter','url(#glow)');
  path.parentNode.appendChild(dot);
  const prevS=path.style.stroke, prevW=path.style.strokeWidth;
  path.style.stroke=color; path.style.strokeWidth='3';
  const t0=performance.now();
  (function step(t){
    let k=Math.min(1,(t-t0)/PDUR); const at=reverse?(1-k)*len:k*len;
    const p=path.getPointAtLength(at); dot.setAttribute('cx',p.x); dot.setAttribute('cy',p.y);
    if(k<1) requestAnimationFrame(step);
    else { dot.remove(); path.style.stroke=prevS; path.style.strokeWidth=prevW; }
  })(t0);
}
function pulse(boxId, echo){ const g=$(boxId); if(!g) return; g.classList.add('pulse'); if(echo) g.classList.add('echo');
  setTimeout(()=>g.classList.remove('pulse','echo'), PDUR); }

const CTL="#a371f7", MSG="#39a7ff", ECHO="#2ea043";
// each event → { hops:[[pathId,color,reverse,delayMs]...], box:[...] }
const SEQ = {
  message: {hops:[['e-slack-broker',MSG,false,0]], box:['#n-slack','#n-broker']},
  resume:  {hops:[['e-broker-ateapi',CTL,false,0]], box:['#n-broker','#n-ateapi']},
  boot:    {hops:[['e-ateapi-actor',CTL,false,0]], box:['#n-ateapi','#n-actor']},
  deliver: {hops:[['e-actor-ws',MSG,false,0]], box:['#n-broker','#n-actor']},
  echo:    {hops:[['e-actor-ws',ECHO,true,0],['e-slack-broker',ECHO,true,1600]], box:['#n-actor','#n-broker','#n-slack'], echo:true},
  suspend: {hops:[['e-broker-ateapi',CTL,false,0],['e-ateapi-actor',CTL,false,1600]], box:['#n-broker','#n-ateapi','#n-actor']},
};
function play(ev){
  // Drive the node/WS/counter from live events (the poll is slow and misses the
  // short RUNNING window). Hold off the poll for ~25s while a wake is in flight,
  // and update the node AFTER its arrow has travelled so it doesn't jump ahead.
  const now=performance.now(), after=(fn,d)=>setTimeout(fn,d);
  if(ev.type==='resume'){ actorHint=now+25000; liveUp=true; after(()=>setActor(false, WARN, 'RESUMING — warm restore…'), 1300); }
  else if(ev.type==='boot'){ actorHint=now+25000; liveUp=true; after(()=>setActor(false, RUN, 'RUNNING — booted & connected'), 1300); }
  else if(ev.type==='deliver' || ev.type==='echo'){ actorHint=now+25000; liveUp=true; setActor(false, RUN, 'RUNNING'); }
  else if(ev.type==='suspend'){ actorHint=now+5000;
    after(()=>{ liveUp=false; actorHint=0; setActor(true, SLEEP, 'SUSPENDED — off all workers'); }, 1600+PDUR); }

  const s=SEQ[ev.type]; if(!s) return;
  s.hops.forEach(([id,col,rev,delay])=>setTimeout(()=>packet(id,col,rev), delay));
  s.box.forEach(b=>pulse(b, s.echo));
  const feed=$('#feed');
  if(feed.firstChild && feed.firstChild.textContent.startsWith('waiting')) feed.innerHTML='';
  const div=document.createElement('div'); div.className='ev';
  const tc = ev.type==='echo'?'echo':(ev.type==='suspend'?'suspend':'');
  div.innerHTML='<span class="tag '+tc+'">'+ev.type.toUpperCase()+'</span><span style="color:var(--dim)">'+ev.t+'</span>  '+ev.label;
  feed.insertBefore(div, feed.firstChild);
  setTimeout(refresh,300); setTimeout(refresh,1500);
}
const es=new EventSource('/api/events');
es.onmessage=e=>{ try{ play(JSON.parse(e.data)); }catch(_){} };

// live tail -f boxes
function logStream(url, sel){
  const el=$(sel); let primed=false;
  const src=new EventSource(url);
  src.onmessage=ev=>{ let line; try{ line=JSON.parse(ev.data).line; }catch(_){ return; }
    if(!line) return;
    if(!primed){ el.textContent=''; primed=true; }
    const atBottom = el.scrollTop+el.clientHeight >= el.scrollHeight-10;
    el.textContent += line+'\n';
    if(el.textContent.length>24000) el.textContent = el.textContent.slice(-18000);
    if(atBottom) el.scrollTop = el.scrollHeight;
  };
}
logStream('/api/logs/broker','#log-broker');
logStream('/api/logs/actor','#log-actor');
</script>
</body></html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/state":
            body = json.dumps(STATE).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path in ("/api/events", "/api/logs/broker", "/api/logs/actor"):
            channel = {"/api/events": "events",
                       "/api/logs/broker": "broker",
                       "/api/logs/actor": "actor"}[self.path]
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q = queue.Queue(maxsize=256)
            with CH_LOCK:
                CHANNELS[channel].append(q)
            try:
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                while True:
                    try:
                        data = q.get(timeout=15)
                    except queue.Empty:
                        data = ": ping\n\n"
                    self.wfile.write(data.encode())
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                with CH_LOCK:
                    if q in CHANNELS[channel]:
                        CHANNELS[channel].remove(q)
        else:
            self.send_response(404)
            self.end_headers()


def main():
    threading.Thread(target=poll_state, daemon=True).start()
    threading.Thread(target=tail_broker, daemon=True).start()
    threading.Thread(target=tail_actor, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"WS-PoC dashboard → http://localhost:{PORT}   (Ctrl-C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if BROKER_PROC:
                BROKER_PROC.terminate()
        except Exception:
            pass


if __name__ == "__main__":
    main()
