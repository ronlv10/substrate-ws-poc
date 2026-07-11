// echo-actor is a Slack Bolt (Node) bot that replies "echo: <message>" in-thread.
//
// It is a completely stock Bolt Socket Mode app: no substrate identity, no
// lifecycle hooks, no readiness endpoint. The co-resident local proxy owns all
// of that — in this image "slack.com" resolves to loopback, where the proxy
// terminates TLS and impersonates Slack.
//
// Bundled into a single file with ncc so cold start touches a handful of files
// instead of a large node_modules tree — filesystem access is the slow path
// under gVisor.

const { App, LogLevel } = require("@slack/bolt");

// Log to stderr: Node block-buffers stdout to a pipe (invisible under gVisor
// until flush), while stderr is unbuffered.
const log = (...a) => console.error("echo-actor:", ...a);

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logLevel: LogLevel.INFO,
});

// De-dupe: a channel message that mentions the bot can arrive as BOTH a
// `message` and an `app_mention` event, and broker delivery is at-least-once
// across suspend/resume — so echo each Slack ts at most once. The Set lives in
// process memory, which warm restore preserves.
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
