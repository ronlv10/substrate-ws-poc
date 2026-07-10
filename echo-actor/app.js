const fs = require("fs");
const http = require("http");
const https = require("https");

// actorRef reads this actor's substrate identity (/run/ate/atespace,
// /run/ate/actor-id) and returns "<atespace>/<name>", sent to the broker as the
// X-Ate-Actor header so it can key the session to this actor.
//
// It MUST be read fresh per request, never cached: an actor restored from a
// golden snapshot holds whatever id it read at startup (the template's), but the
// mounted file is regenerated on every resume and is correct.
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

// Inject the X-Ate-Actor header (read fresh) on every HTTPS request to slack.com.
// The Slack web-api SDK has no per-request header hook, so patch at the transport
// layer; the broker reads this header on apps.connections.open to identify us.
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

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logLevel: LogLevel.INFO,
  // Identity is injected per-request by the https patch above, not set here: a
  // value cached here goes stale after a golden-snapshot restore.
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
// app_mention).
app.message(async ({ message, say }) => echoReply(message, say));
app.event("app_mention", async ({ event, say }) => echoReply(event, say));

(async () => {
  log("starting Socket Mode");
  await app.start();
  log("Bolt app is running");
})();
