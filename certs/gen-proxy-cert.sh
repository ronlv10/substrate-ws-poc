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
# Generate the local proxy's CA and slack.com leaf. Both are baked into the
# actor image only: the CA lands in NODE_EXTRA_CA_CERTS for the agent, the
# leaf is what the proxy presents on 127.0.0.1:443. Nothing is installed on
# nodes or in the cluster — the trust blast radius is one image.
#
# Outputs (into $OUT_DIR, default echo-actor/proxy-certs/):
#   proxy-ca.crt  — the CA the agent trusts
#   tls.crt/tls.key — the slack.com leaf + key the proxy serves
set -euo pipefail

OUT_DIR="${OUT_DIR:-$(dirname "$0")/../echo-actor/proxy-certs}"
mkdir -p "${OUT_DIR}"
cd "${OUT_DIR}"

if [[ -f proxy-ca.crt && -f tls.crt && -f tls.key ]]; then
  echo "proxy certs already present in ${OUT_DIR}; delete them to regenerate"
  exit 0
fi

openssl ecparam -name prime256v1 -genkey -noout -out proxy-ca.key
openssl req -x509 -new -key proxy-ca.key -sha256 -days 3650 \
  -subj "/CN=ws-poc local proxy CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out proxy-ca.crt

openssl ecparam -name prime256v1 -genkey -noout -out tls.key
openssl req -new -key tls.key -subj "/CN=slack.com" -out leaf.csr
openssl x509 -req -in leaf.csr -CA proxy-ca.crt -CAkey proxy-ca.key \
  -CAcreateserial -days 3650 -sha256 \
  -extfile <(printf "subjectAltName=DNS:slack.com,DNS:wss-primary.slack.com\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n") \
  -out tls.crt
rm -f leaf.csr proxy-ca.srl

echo "wrote proxy-ca.crt, tls.crt, tls.key to ${OUT_DIR}"
