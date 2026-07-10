#!/usr/bin/env bash
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
