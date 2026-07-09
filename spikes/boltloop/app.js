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

// A deliberately STOCK Bolt Socket Mode app — the experiment's subject.
// No lifecycle hooks, no restore detection, no reconnect handling of our own:
// whatever the bundled @slack/socket-mode does across a gVisor
// checkpoint/restore must be visible untouched in the DEBUG logs.
//
// All output goes to stderr: Node block-buffers stdout under gVisor and the
// logs are the experiment's only instrument.

const { App, LogLevel } = require("@slack/bolt");

const log =
  (lvl) =>
  (...args) =>
    console.error(new Date().toISOString(), `[bolt:${lvl}]`, ...args);
const logger = {
  debug: log("debug"),
  info: log("info"),
  warn: log("warn"),
  error: log("error"),
  setLevel() {},
  getLevel() {
    return LogLevel.DEBUG;
  },
  setName() {},
};

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true,
  logger,
});

const seen = new Set();
app.message(async ({ message, say }) => {
  if (seen.has(message.ts)) {
    console.error(`SPIKE-APP: DUPLICATE ts=${message.ts}`);
    return;
  }
  seen.add(message.ts);
  console.error(`SPIKE-APP: GOT message ts=${message.ts} text=${JSON.stringify(message.text)}`);
  await say(`echo: ${message.text}`);
  console.error(`SPIKE-APP: REPLIED ts=${message.ts}`);
});

(async () => {
  await app.start();
  console.error("SPIKE-APP: STARTED (socket mode)");
})();
