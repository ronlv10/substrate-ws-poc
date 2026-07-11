#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-.}"
mkdir -p "${OUT_DIR}"

echo "Generating egress broker certificate in ${OUT_DIR}/ ..."

# A single self-signed certificate that actors both trust (it is bind-mounted
# into their CA store) and are served by the broker on the redirected Slack
# hostnames. CA:TRUE makes it an unambiguous trust anchor; the SANs make it a
# valid server certificate for the two hostnames actors dial.
openssl ecparam -name prime256v1 -genkey -noout -out "${OUT_DIR}/ca.key"

openssl req -x509 -new -key "${OUT_DIR}/ca.key" -sha256 -days 3650 \
  -out "${OUT_DIR}/ca.crt" \
  -subj "/CN=Egress Broker" \
  -addext "subjectAltName=DNS:slack.com,DNS:wss-primary.slack.com" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,digitalSignature,keyCertSign" \
  -addext "extendedKeyUsage=serverAuth"

echo "Wrote ${OUT_DIR}/ca.crt and ${OUT_DIR}/ca.key"
echo
echo "Next: create the broker CA secret and (re)deploy:"
echo "  kubectl create namespace ws-poc --dry-run=client -o yaml | kubectl apply -f -"
echo "  kubectl -n ws-poc create secret tls egress-broker-ca \\"
echo "      --cert=${OUT_DIR}/ca.crt --key=${OUT_DIR}/ca.key"
