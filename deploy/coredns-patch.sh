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
# Adds the slack.com -> egress-broker CoreDNS rewrite rules (see
# coredns-rewrite.md). Idempotent. Requires kubectl and jq.

set -euo pipefail

NS=kube-system
CM=coredns
BROKER_FQDN=egress-broker.ws-poc.svc.cluster.local

cur=$(kubectl -n "$NS" get configmap "$CM" -o jsonpath='{.data.Corefile}')

if grep -q "egress-broker" <<<"$cur"; then
  echo "CoreDNS already has the WS-PoC rewrite; nothing to do."
  exit 0
fi

patched=$(awk -v fqdn="$BROKER_FQDN" '
  /\.:53[[:space:]]*\{/ && !done {
    print
    print "    rewrite name exact slack.com " fqdn
    print "    rewrite name exact wss-primary.slack.com " fqdn
    done=1
    next
  }
  { print }
' <<<"$cur")

if ! grep -q "egress-broker" <<<"$patched"; then
  echo "ERROR: could not find a '.:53 {' server block in the CoreDNS Corefile." >&2
  echo "Add the rewrite rules manually (see deploy/coredns-rewrite.md)." >&2
  exit 1
fi

cf_json=$(jq -Rs . <<<"$patched")
kubectl -n "$NS" patch configmap "$CM" --type merge -p "{\"data\":{\"Corefile\":$cf_json}}"
kubectl -n "$NS" rollout restart deployment coredns
echo "CoreDNS rewrite added and CoreDNS restarted."
