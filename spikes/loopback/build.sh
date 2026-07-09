#!/usr/bin/env bash
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
# Build + push the loopback spike image and render actor.yaml.
# Env: ATEOM_IMAGE (required), BUCKET_NAME (default ate-snapshots),
#      SPIKE_IMAGE (default localhost:5001/ws-poc-spike-loopback),
#      ACTOR_ARCH (default arm64).
set -euo pipefail
cd "$(dirname "$0")"

: "${ATEOM_IMAGE:?set ATEOM_IMAGE to the ko-built ateom-gvisor image ref}"
BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
SPIKE_IMAGE="${SPIKE_IMAGE:-localhost:5001/ws-poc-spike-loopback}"
ACTOR_ARCH="${ACTOR_ARCH:-arm64}"

# netgo: resolve hostnames from /etc/hosts inside the static binary.
GOOS=linux GOARCH="${ACTOR_ARCH}" CGO_ENABLED=0 \
  go build -tags netgo -o spike .

docker build --platform "linux/${ACTOR_ARCH}" -t "${SPIKE_IMAGE}:latest" .
docker push "${SPIKE_IMAGE}:latest"
DIGEST="$(docker inspect --format='{{index .RepoDigests 0}}' "${SPIKE_IMAGE}:latest")"
echo "pushed ${DIGEST}"

sed -e "s|\${SPIKE_IMAGE}|${DIGEST}|g" \
    -e "s|\${ATEOM_IMAGE}|${ATEOM_IMAGE}|g" \
    -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
    actor.yaml.tmpl > actor.yaml
echo "rendered $(pwd)/actor.yaml"
