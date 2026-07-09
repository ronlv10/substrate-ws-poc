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
# The Bolt (Node) echo actor image. Built by the Makefile (build-actor-image).
#
# The app is bundled into a single file with ncc so cold start touches a handful
# of files instead of a large node_modules tree — filesystem access is the slow
# path under gVisor, so this keeps wake latency low.
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
CMD ["node", "/app/app.js"]
