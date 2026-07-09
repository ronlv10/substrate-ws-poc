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
# The v2 actor image: the local egress proxy (PID 1) plus the stock Bolt echo
# app it supervises. Built by the Makefile (build-actor-image), which first
# cross-compiles cmd/local-proxy into this directory and generates the proxy
# TLS material into proxy-certs/.
#
# The app is bundled into a single file with ncc so cold start touches a handful
# of files instead of a large node_modules tree — filesystem access is the slow
# path under gVisor, so this keeps wake latency low.
#
# /etc/hosts points slack.com (and the wss host) at loopback. atelet mounts
# nothing over /etc/hosts, so the image's copy is what the sandbox resolves
# with — this is the entire redirect mechanism; no CoreDNS rewrite exists.
#
# NOTE: substrate ignores image ENTRYPOINT/CMD/ENV — the actor's command and env
# come from the ActorTemplate. The CMD below is only for local `docker run`.

FROM node:22 AS build
WORKDIR /build
COPY package.json ./
RUN npm install --no-audit --no-fund
COPY app.js ./
RUN npx ncc build app.js -o /dist

FROM node:22-slim
COPY --from=build /dist/index.js /app/app.js
COPY local-proxy /local-proxy
COPY proxy-certs/proxy-ca.crt /etc/local-proxy/ca/proxy-ca.crt
COPY proxy-certs/tls.crt /etc/local-proxy/tls/tls.crt
COPY proxy-certs/tls.key /etc/local-proxy/tls/tls.key
COPY hosts /etc/hosts
CMD ["/local-proxy", "--", "node", "/app/app.js"]
