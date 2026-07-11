# Loopback checkpoint/restore spike image. The `spike` binary is cross-compiled
# into this directory before the build (see spikes/loopback/build.sh). The
# /etc/hosts override is the v2 redirect mechanism under test: atelet mounts
# nothing over /etc/hosts, so the image's copy must be what the sandbox sees.

FROM gcr.io/distroless/static-debian13
COPY spike /spike
COPY hosts /etc/hosts
ENTRYPOINT ["/spike"]
