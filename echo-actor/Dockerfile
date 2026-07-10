# The Bolt (Node) echo actor image. Built by the Makefile (build-actor-image).
# app.js is bundled with ncc so cold start touches a handful of files, not a large
# node_modules tree (filesystem access is the slow path under gVisor).
#
# The actor's command runs entrypoint.sh, which resolves the egress broker Service
# and points slack.com at it via /etc/hosts before starting Bolt. substrate ignores
# image CMD/ENV — the ActorTemplate sets the authoritative command (also entrypoint.sh).

FROM node:22 AS build
WORKDIR /build
COPY package.json ./
RUN npm install --no-audit --no-fund
COPY app.js ./
RUN npx ncc build app.js -o /dist

FROM node:22-slim
COPY --from=build /dist/index.js /app/app.js
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
CMD ["/app/entrypoint.sh"]
