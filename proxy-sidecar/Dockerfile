# The egress proxy as a standalone sidecar container: the same local-proxy
# binary and TLS material, but run without an agent to supervise. It shares the
# sandbox network namespace with the agent container, which reaches it on
# loopback (the agent image's /etc/hosts points slack.com at 127.0.0.1).
#
# The proxy is a static binary that writes nothing to its rootfs, so its overlay
# stays clean across checkpoint/restore. It needs the per-resume /run/ate
# identity mount (substrate provides it) and the broker address (from the
# ActorTemplate command).
#
# NOTE: substrate ignores image ENTRYPOINT/CMD/ENV — the command comes from the
# ActorTemplate. The CMD below is only for local `docker run`.

FROM gcr.io/distroless/static-debian12
COPY local-proxy /local-proxy
COPY proxy-certs/proxy-ca.crt /etc/local-proxy/ca/proxy-ca.crt
COPY proxy-certs/tls.crt /etc/local-proxy/tls/tls.crt
COPY proxy-certs/tls.key /etc/local-proxy/tls/tls.key
CMD ["/local-proxy"]
