# Bolt-over-loopback spike image: the Go fake-Slack server (PID 1, built by
# build.sh into this directory) plus a stock ncc-bundled Bolt app. /etc/hosts
# points slack.com at loopback; certs/ carries the CA the app trusts via
# NODE_EXTRA_CA_CERTS and the slack.com leaf the server presents.

FROM node:22 AS build
WORKDIR /src
COPY package.json app.js ./
RUN npm install && npm ls @slack/socket-mode && npx ncc build app.js -o /dist

FROM node:22-slim
COPY --from=build /dist/index.js /app/app.js
COPY server /server
COPY certs/ca.crt certs/tls.crt certs/tls.key /certs/
COPY hosts /etc/hosts
ENTRYPOINT ["/server"]
