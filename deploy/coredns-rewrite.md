# CoreDNS redirect for the WS-PoC PoC

Actors inherit the worker pod's `/etc/resolv.conf`, which points at cluster
CoreDNS. Adding a `rewrite` rule to the CoreDNS `Corefile` makes actors resolve
the Slack hostnames to the broker Service — this is the PoC's transparent
redirect (see the README's "Production hardening" for the per-workload
nftables/TPROXY replacement).

The broker itself is unaffected: it reaches real Slack via its own upstream
resolver (`--dns-upstream`, default `8.8.8.8:53`), bypassing this rewrite.

## Rules to add

Inside the main server block (`.:53 { ... }`) of the `coredns` ConfigMap in
`kube-system`, add:

```
    rewrite name exact slack.com egress-broker.ws-poc.svc.cluster.local
    rewrite name exact wss-primary.slack.com egress-broker.ws-poc.svc.cluster.local
```

`rewrite name exact` rewrites the query to the broker Service's FQDN (which
CoreDNS resolves to its ClusterIP) and restores the original name in the answer,
so the actor sees `slack.com` → broker ClusterIP.

## Apply

```
kubectl -n kube-system edit configmap coredns
# add the two lines, save, then roll CoreDNS:
kubectl -n kube-system rollout restart deployment coredns
```

`make -C poc/ws-poc coredns-patch` performs this edit automatically for the
default kind/CoreDNS layout; verify the result if your CoreDNS config is
customized.

## Verify

From any actor-capable pod:

```
# should resolve to the egress-broker ClusterIP, NOT a real Slack address:
kubectl run dnstest --rm -it --image=busybox --restart=Never -- nslookup slack.com
```
