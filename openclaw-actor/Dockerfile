# The openclaw actor image: a real Claude agent (OpenClaw, a multi-channel AI
# gateway) supervised by the same local egress proxy the echo actor uses. Built
# by the Makefile (build-openclaw-image), which first cross-compiles
# cmd/local-proxy into this directory and generates the proxy TLS material into
# proxy-certs/.
#
# Unlike the echo actor, OpenClaw is a large prebuilt image (~1.5 GB) we layer
# onto rather than a single bundled file. The proxy, certs, and /etc/hosts
# redirect are added identically — the agent talks Socket Mode to slack.com over
# loopback and never knows it is impersonated.
#
# NOTE: substrate ignores image ENTRYPOINT/CMD/ENV — the actor's command and env
# come from the ActorTemplate (deploy/openclaw-actor.yaml.tmpl). In particular
# the template points HOME and every OpenClaw/Node writable path at the /data
# durableDir, NOT the overlay rootfs: a dirty rootfs overlay is what makes
# `runsc restore` fail the filestore-collision check, so keeping the rootfs clean
# (like the stateless echo actor) is what makes warm restore reliable.

FROM ghcr.io/openclaw/openclaw@sha256:3814fb1f62f9cfc5944de088c5817c68c88b5d721feebe36420b666a90a61ce7

# The Slack channel is an external OpenClaw plugin; install it into the image so
# cold boot has nothing to fetch. OpenClaw resolves plugins under OPENCLAW_HOME.
ENV OPENCLAW_HOME=/home/node \
    OPENCLAW_STATE_DIR=/home/node/.openclaw \
    OPENCLAW_CONFIG_PATH=/home/node/.openclaw/openclaw.json
USER node
RUN node /app/openclaw.mjs plugins install @openclaw/slack \
 && node /app/openclaw.mjs plugins enable slack

# Bake the agent config: Slack over Socket Mode, the model provider pointed at
# the inference endpoint, and the heavyweight per-turn features (memory search,
# startup-context, commitment inference, browser) disabled — a stateless Slack
# responder does not need them, and they dominate per-turn latency. The template
# seeds this onto the durableDir on first boot.
COPY --chown=node:node openclaw.json /home/node/.openclaw/openclaw.json

# Local egress proxy (PID 1) + its baked TLS material + the loopback redirect,
# identical to the echo actor.
USER root
COPY local-proxy /local-proxy
COPY proxy-certs/proxy-ca.crt /etc/local-proxy/ca/proxy-ca.crt
COPY proxy-certs/tls.crt /etc/local-proxy/tls/tls.crt
COPY proxy-certs/tls.key /etc/local-proxy/tls/tls.key
COPY hosts /etc/hosts
CMD ["/local-proxy", "--", "node", "/app/openclaw.mjs", "gateway"]
