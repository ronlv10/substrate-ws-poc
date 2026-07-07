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
// in-thread. It is an ordinary Bolt Socket Mode app with no knowledge of the
// WS-PoC egress broker: it connects to slack.com, and the broker is interposed
// transparently (cluster DNS points slack.com at the broker; the broker's CA is
// trusted via NODE_EXTRA_CA_CERTS). The broker holds the real Slack connection
// while this actor is suspended and resumes it when a message arrives.
//
// The only substrate-aware behavior is idle self-suspension: after a quiet
// period it runs the bundled /suspend-self helper (a tiny Go binary) which asks
// the control plane to checkpoint it. Everything Slack-facing is plain Bolt.
//
// The image ships this bundled into a single file (via ncc) so cold start is a
// handful of file reads rather than a walk over a large node_modules tree —
// which matters because filesystem access is the slow path under gVisor.

const http = require("http");
const { execFile } = require("child_process");
const { App, LogLevel } = require("@slack/bolt");

const IDLE_GRACE_MS = (parseFloat(process.env.IDLE_GRACE_SECONDS || "15")) * 1000;

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

let idleTimer = null;
function suspendSelf() {
  log("idle; requesting self-suspension");
  execFile("/suspend-self", (err) => {
    if (err) log("suspend-self failed:", err.message);
  });
}
function armIdle() {
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(suspendSelf, IDLE_GRACE_MS);
}

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logLevel: LogLevel.DEBUG,
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
  armIdle();
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
  armIdle();
  log("starting Socket Mode");
  await app.start();
  log("Bolt app is running");
})();
