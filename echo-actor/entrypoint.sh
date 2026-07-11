#!/bin/sh
# Redirect Slack to the broker for THIS actor only, via /etc/hosts (the broker
# Service ClusterIP is stable across broker pod churn). slack.com stays the TLS
# SNI, and cluster DNS is untouched so the broker is never redirected onto itself.
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
