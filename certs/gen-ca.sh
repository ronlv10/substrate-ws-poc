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
# Generates the egress broker CA (an EC P-256 self-signed CA). The broker mints
# per-SNI leaf certificates signed by this CA; actors trust this CA (installed
# into the node trust store and mounted into the sandbox by atelet), so they
# accept the broker's certificates for slack.com transparently.
#
# Outputs ca.crt and ca.key in the current directory (or $OUT_DIR).

set -euo pipefail

OUT_DIR="${OUT_DIR:-.}"
mkdir -p "${OUT_DIR}"

echo "Generating egress broker CA in ${OUT_DIR}/ ..."

openssl ecparam -name prime256v1 -genkey -noout -out "${OUT_DIR}/ca.key"

openssl req -x509 -new -key "${OUT_DIR}/ca.key" -sha256 -days 3650 \
  -out "${OUT_DIR}/ca.crt" \
  -subj "/CN=Egress Broker CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

echo "Wrote ${OUT_DIR}/ca.crt and ${OUT_DIR}/ca.key"
echo
echo "Next: create the broker CA secret and (re)deploy:"
echo "  kubectl create namespace ws-poc --dry-run=client -o yaml | kubectl apply -f -"
echo "  kubectl -n ws-poc create secret tls egress-broker-ca \\"
echo "      --cert=${OUT_DIR}/ca.crt --key=${OUT_DIR}/ca.key"
