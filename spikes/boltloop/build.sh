#!/usr/bin/env bash
# Build + push the boltloop spike image and render actor.yaml.
# Env: BUCKET_NAME (default ate-snapshots),
#      SPIKE_IMAGE (default localhost:5001/ws-poc-spike-boltloop),
#      ACTOR_ARCH (default arm64).
set -euo pipefail
cd "$(dirname "$0")"

BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
SPIKE_IMAGE="${SPIKE_IMAGE:-localhost:5001/ws-poc-spike-boltloop}"
ACTOR_ARCH="${ACTOR_ARCH:-arm64}"

# Throwaway CA + slack.com leaf; the CA lands in NODE_EXTRA_CA_CERTS, the
# leaf is what the in-image server presents on 127.0.0.1:443.
if [[ ! -f certs/ca.crt ]]; then
  mkdir -p certs
  openssl ecparam -name prime256v1 -genkey -noout -out certs/ca.key
  openssl req -x509 -new -key certs/ca.key -sha256 -days 365 \
    -subj "/CN=boltloop spike CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out certs/ca.crt
  openssl ecparam -name prime256v1 -genkey -noout -out certs/tls.key
  openssl req -new -key certs/tls.key -subj "/CN=slack.com" -out certs/leaf.csr
  openssl x509 -req -in certs/leaf.csr -CA certs/ca.crt -CAkey certs/ca.key \
    -CAcreateserial -days 365 -sha256 \
    -extfile <(printf "subjectAltName=DNS:slack.com,DNS:wss-primary.slack.com\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n") \
    -out certs/tls.crt
  rm -f certs/leaf.csr certs/ca.srl
fi

GOOS=linux GOARCH="${ACTOR_ARCH}" CGO_ENABLED=0 go build -o server .

docker build --platform "linux/${ACTOR_ARCH}" -t "${SPIKE_IMAGE}:latest" .
docker push "${SPIKE_IMAGE}:latest"
DIGEST="$(docker inspect --format='{{index .RepoDigests 0}}' "${SPIKE_IMAGE}:latest")"
echo "pushed ${DIGEST}"

sed -e "s|\${SPIKE_IMAGE}|${DIGEST}|g" \
    -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
    actor.yaml.tmpl > actor.yaml
echo "rendered $(pwd)/actor.yaml"
