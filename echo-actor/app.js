// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// echo-actor: a stock Slack Bolt (Node) bot that replies "echo: <message>"
// in-thread. It is an ordinary Bolt Socket Mode app with NO knowledge of its own
// lifecycle: it does not suspend, resume, or checkpoint itself, and it makes no
// substrate control-plane calls. It just connects to slack.com and echoes; the
// broker is interposed transparently (cluster DNS points slack.com at the broker;
// the broker's CA is trusted via NODE_EXTRA_CA_CERTS), holds the real Slack
// connection while this actor is suspended, resumes the actor when a message
// arrives, and SUSPENDS it again from the outside once it goes idle.
//
// The only substrate-aware line is reading /run/ate to learn its own actor id,
// which it forwards to the broker as a header (so the broker can key the session
// to it). Everything else is plain Bolt.
//
// The image ships this bundled into a single file (via ncc) so cold start is a
// handful of file reads rather than a walk over a large node_modules tree —
// which matters because filesystem access is the slow path under gVisor.

const fs = require("fs");
const http = require("http");
const https = require("https");

// actorRef reads this actor's substrate identity FRESH from the per-resume
// identity mount atelet writes (/run/ate/atespace, /run/ate/actor-id) and returns
// "<atespace>/<name>". We send it to the broker as an X-Ate-Actor header so the
// broker identifies the actor DETERMINISTICALLY (no source-IP → ListActors race).
//
// It MUST be read fresh per request, not cached: an actor restored from a golden
// snapshot keeps in memory whatever it read at startup (the *golden* template
// actor's id), but the mounted file is regenerated per-resume and is correct
// (verified: demo:echo-1/identity holds id=echo-1). So we read it inside the
// https hook below, which runs on every apps.connections.open — including Bolt's
// reconnect after a restore — guaranteeing the current id.
function actorRef() {
  try {
    const atespace = fs.readFileSync("/run/ate/atespace", "utf8").trim();
    const name = fs.readFileSync("/run/ate/actor-id", "utf8").trim();
    if (atespace && name) return `${atespace}/${name}`;
  } catch (e) {
    // Non-substrate/local run: no identity mount. Fall through to "" (the broker
    // then falls back to source-IP resolution).
  }
  return "";
}

// Inject X-Ate-Actor (read fresh) on every HTTPS request to slack.com. The Slack
// SDK (web-api 6) has no per-request header hook, and the actor process can't be
// relied on to cold-boot and re-read a cached value, so we patch at the transport
// layer. This is the request the broker reads to identify us on apps.connections.open.
function hostOf(options) {
  if (typeof options === "string") { try { return new URL(options).hostname; } catch { return ""; } }
  return (options && (options.hostname || options.host)) || "";
}
const _httpsRequest = https.request.bind(https);
https.request = function (options, ...rest) {
  try {
    if (typeof options === "object" && /(^|\.)slack\.com$/.test(hostOf(options))) {
      const ref = actorRef();
      if (ref) {
        options.headers = Object.assign({}, options.headers, { "X-Ate-Actor": ref });
      }
    }
  } catch (e) { /* never break the request */ }
  return _httpsRequest(options, ...rest);
};

const { App, LogLevel } = require("@slack/bolt");

// Readiness endpoint first, so substrate's readyz gate passes immediately.
http
  .createServer((_req, res) => {
    res.writeHead(200);
    res.end("ok\n");
  })
  .listen(80);

// Log to stderr: Node block-buffers stdout to a pipe (invisible under gVisor
// until flush), while stderr is unbuffered.
const log = (...a) => console.error("echo-actor:", ...a);

// No self-suspend: the actor does NOT manage its own lifecycle. The broker
// suspends it from the outside when it goes idle. Self-suspending is a checkpoint
// taken mid-call, which — with onPause=Data — kills the actor's process before the
// call returns and jams it in SUSPENDING. External (broker) suspend is clean.

log("actor identity (at startup):", actorRef() || "(none)");

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logLevel: LogLevel.DEBUG,
  // Identity is NOT set here (that would cache a value that goes stale after a
  // golden-snapshot restore). The X-Ate-Actor header is injected fresh per
  // request by the https.request patch above.
});

// Diagnostic: log every event Bolt dispatches through middleware, so we can see
// whether events reach the dispatcher and what type they are.
app.use(async ({ payload, body, next }) => {
  log("middleware:", "payload.type=", payload && payload.type, "event.type=", body && body.event && body.event.type);
  await next();
});

// De-dupe: a channel message that mentions the bot can arrive as BOTH a
// `message` and an `app_mention` event, so echo each Slack ts at most once.
const echoed = new Set();
function firstTime(ts) {
  if (!ts || echoed.has(ts)) return false;
  echoed.add(ts);
  setTimeout(() => echoed.delete(ts), 60000);
  return true;
}

async function echoReply(event, say) {
  if (event.bot_id || event.subtype) return; // skip our own echoes / edits / joins
  if (!firstTime(event.ts)) return;
  const threadTs = event.thread_ts || event.ts;
  log(`handling ${event.type} in ${event.channel}; replying in thread ${threadTs}`);
  try {
    await say({ text: `echo: ${event.text}`, thread_ts: threadTs });
    log("reply posted");
  } catch (e) {
    log("chat.postMessage failed:", e && e.message ? e.message : e);
  }
}

// Handle both plain channel messages and @-mentions (mentions arrive as
// app_mention; the previous Go actor accepted both, which is why it echoed).
app.message(async ({ message, say }) => echoReply(message, say));
app.event("app_mention", async ({ event, say }) => echoReply(event, say));

(async () => {
  log("starting Socket Mode");
  await app.start();
  log("Bolt app is running");
})();
