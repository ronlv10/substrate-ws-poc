#!/bin/sh
# Point the provider's hostnames at the egress broker for THIS actor only, by
# resolving the broker Service (a stable ClusterIP, unaffected by broker pod
# churn) and appending it to /etc/hosts. slack.com stays the TLS SNI, so the
# broker's slack.com certificate still matches. Nothing is written to cluster
# DNS, so the broker itself is never redirected.
set -e

broker="${BROKER_SERVICE:-egress-broker.ws-poc.svc.cluster.local}"
ip=""
for _ in 1 2 3 4 5; do
	set -- $(getent hosts "$broker" 2>/dev/null | head -n1)
	ip="$1"
	[ -n "$ip" ] && break
	sleep 1
done

if [ -n "$ip" ]; then
	echo "$ip slack.com wss-primary.slack.com" >>/etc/hosts
	echo "entrypoint: slack.com -> $ip (egress broker $broker)" >&2
else
	echo "entrypoint: WARNING could not resolve $broker; slack.com not redirected" >&2
fi

exec node /app/app.js
