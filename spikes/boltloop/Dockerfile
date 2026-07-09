# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Bolt-over-loopback spike image: the Go fake-Slack server (PID 1, built by
# build.sh into this directory) plus a stock ncc-bundled Bolt app. /etc/hosts
# points slack.com at loopback; certs/ carries the CA the app trusts via
# NODE_EXTRA_CA_CERTS and the slack.com leaf the server presents.

FROM node:22 AS build
WORKDIR /src
COPY package.json app.js ./
RUN npm install && npm ls @slack/socket-mode && npx ncc build app.js -o /dist

FROM node:22-slim
COPY --from=build /dist/index.js /app/app.js
COPY server /server
COPY certs/ca.crt certs/tls.crt certs/tls.key /certs/
COPY hosts /etc/hosts
ENTRYPOINT ["/server"]
