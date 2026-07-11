const fs = require("fs");
const http = require("http");
const https = require("https");

// actorRef reads this actor's substrate identity, sent to the broker as the
// X-Ate-Actor header so it can key the session. Read fresh per request: a
// golden-restored actor keeps the template's id until /run/ate is rewritten on resume.
function actorRef() {
  try {
    const atespace = fs.readFileSync("/run/ate/atespace", "utf8").trim();
    const name = fs.readFileSync("/run/ate/actor-id", "utf8").trim();
    if (atespace && name) return `${atespace}/${name}`;
  } catch {
    // No identity mount; only happens outside substrate (local runs).
  }
  return "";
}

// The Slack SDK has no per-request header hook, so inject X-Ate-Actor at the
// transport layer on every slack.com request.
function hostOf(options) {
  if (typeof options === "string") { try { return new URL(options).hostname; } catch { return ""; } }
  return (options && (options.hostname || options.host)) || "";
}
const _httpsRequest = https.request.bind(https);
https.request = function (options, ...rest) {
  try {
    if (typeof options === "object" && /(^|\.)slack\.com$/.test(hostOf(options))) {
      const ref = actorRef();
      if (ref) options.headers = Object.assign({}, options.headers, { "X-Ate-Actor": ref });
    }
  } catch { /* never break the request */ }
  return _httpsRequest(options, ...rest);
};

const { App, LogLevel } = require("@slack/bolt");

// Serves substrate's readyz gate.
http
  .createServer((_req, res) => {
    res.writeHead(200);
    res.end("ok\n");
  })
  .listen(80);

// stderr, not stdout: Node block-buffers stdout to a pipe, invisible under gVisor.
const log = (...a) => console.error("echo-actor:", ...a);

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logLevel: LogLevel.INFO,
});

// A mention arrives as both `message` and `app_mention`; echo each ts once.
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

app.message(async ({ message, say }) => echoReply(message, say));
app.event("app_mention", async ({ event, say }) => echoReply(event, say));

(async () => {
  log("starting Socket Mode");
  await app.start();
  log("Bolt app is running");
})();
