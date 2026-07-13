# The agent half of the sidecar layout: the upstream OpenClaw image plus only
# what it needs to talk to a proxy running in a sibling container — an /etc/hosts
# redirect and the proxy's CA. No proxy binary; the proxy is proxy-sidecar/.
#
# This is as close to a stock agent image as the ActorTemplate allows today: the
# template has no hostAliases, so the slack.com->loopback redirect is a baked
# /etc/hosts, and the CA is a baked file the agent trusts via NODE_EXTRA_CA_CERTS
# (set in the template). Everything else — command, secrets, writable-path
# redirects to /data — comes from the ActorTemplate.

FROM ghcr.io/openclaw/openclaw@sha256:3814fb1f62f9cfc5944de088c5817c68c88b5d721feebe36420b666a90a61ce7

ENV OPENCLAW_HOME=/home/node \
    OPENCLAW_STATE_DIR=/home/node/.openclaw \
    OPENCLAW_CONFIG_PATH=/home/node/.openclaw/openclaw.json
USER node
RUN node /app/openclaw.mjs plugins install @openclaw/slack \
 && node /app/openclaw.mjs plugins enable slack
COPY --chown=node:node openclaw.json /home/node/.openclaw/openclaw.json

USER root
# slack.com -> the proxy on the shared sandbox loopback, and the proxy CA the
# agent trusts (NODE_EXTRA_CA_CERTS points here, set by the ActorTemplate).
COPY proxy-certs/proxy-ca.crt /etc/local-proxy/ca/proxy-ca.crt
COPY hosts /etc/hosts
CMD ["node", "/app/openclaw.mjs", "gateway"]
