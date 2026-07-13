# Build and deploy helpers for the WebSocket egress-broker PoC. Run from the
# repo root. See README.md for the full runbook.

SHELL := /bin/bash
KO_DOCKER_REPO ?= localhost:5001
export KO_DOCKER_REPO

CADIR ?= $(CURDIR)/certs
BROKER_PKG := github.com/ronlv10/substrate-ws-poc/cmd/egress-broker

# The actor images are Docker images (not ko-built). ACTOR_ARCH must match the
# cluster nodes. The echo actor is a tiny stateless Bolt app; the openclaw actor
# is a real Claude agent layered onto the upstream OpenClaw image.
ECHO_IMAGE ?= localhost:5001/ws-poc-echo-actor
OPENCLAW_IMAGE ?= localhost:5001/ws-poc-openclaw-actor
ACTOR_ARCH ?= arm64

# ateom-gvisor lives in substrate (the fork), not this repo, so its image is an
# input: ko build github.com/agent-substrate/substrate/cmd/ateom-gvisor from a
# substrate checkout and pass the pinned digest here.
ATEOM_IMAGE ?=

.PHONY: help
help:
	@echo "substrate-ws-poc targets:"
	@echo "  test           - run the unit tests"
	@echo "  gen-proto      - regenerate the broker<->proxy gRPC stubs"
	@echo "  slack-secret   - create the slack-tokens secret (needs APP_TOKEN, BOT_TOKEN)"
	@echo "  anthropic-secret - create the anthropic-api-key secret (needs API_KEY)"
	@echo "  build          - ko build the broker + build the echo actor image"
	@echo "  deploy-broker  - apply the broker (ko apply)"
	@echo "  deploy-actor   - apply the echo-actor WorkerPool + ActorTemplate (needs BUCKET_NAME, ATEOM_IMAGE; BROKER_ADDRESS optional)"
	@echo "  create-actor   - create the demo atespace + actor echo-1"
	@echo "  deploy-openclaw  - build + apply the openclaw-actor (needs BUCKET_NAME, ATEOM_IMAGE; BROKER_ADDRESS optional)"
	@echo "  create-openclaw  - create the demo atespace + actor openclaw-1"
	@echo "  deploy         - deploy-broker"
	@echo "  clean          - delete PoC resources"

.PHONY: test
test:
	go test ./...

# Regenerate the broker↔proxy gRPC stubs (generated code is committed).
.PHONY: gen-proto
gen-proto:
	go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate

.PHONY: slack-secret
slack-secret:
	@test -n "$(APP_TOKEN)" || { echo "set APP_TOKEN=xapp-..."; exit 1; }
	@test -n "$(BOT_TOKEN)" || { echo "set BOT_TOKEN=xoxb-..."; exit 1; }
	kubectl create namespace ate-demo-ws-poc --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ate-demo-ws-poc create secret generic slack-tokens \
		--from-literal=app-token=$(APP_TOKEN) \
		--from-literal=bot-token=$(BOT_TOKEN) \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: anthropic-secret
anthropic-secret:
	@test -n "$(API_KEY)" || { echo "set API_KEY=<inference endpoint key>"; exit 1; }
	kubectl create namespace ate-demo-ws-poc --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ate-demo-ws-poc create secret generic anthropic-api-key \
		--from-literal=api-key=$(API_KEY) \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: build
build:
	ko build $(BROKER_PKG)
	$(MAKE) build-actor-image

# Build and push the actor image: the local proxy (cross-compiled here) as
# PID 1 plus the ncc-bundled Bolt app, with the proxy TLS material baked in.
.PHONY: build-actor-image
build-actor-image:
	bash $(CADIR)/gen-proxy-cert.sh
	GOOS=linux GOARCH=$(ACTOR_ARCH) CGO_ENABLED=0 \
		go build -o echo-actor/local-proxy ./cmd/local-proxy
	docker build --platform linux/$(ACTOR_ARCH) -t $(ECHO_IMAGE):latest $(CURDIR)/echo-actor
	docker push $(ECHO_IMAGE):latest

.PHONY: deploy-broker
deploy-broker:
	ko apply -f deploy/egress-broker.yaml

# BROKER_ADDRESS empty deploys the proxy standalone (phase-1 loop); set it to
# egress-broker.ws-poc.svc.cluster.local:9090 for the full path.
BROKER_ADDRESS ?=

.PHONY: deploy-actor
deploy-actor: build-actor-image
	@test -n "$(BUCKET_NAME)" || { echo "set BUCKET_NAME=<snapshot bucket>"; exit 1; }
	@test -n "$(ATEOM_IMAGE)" || { echo "set ATEOM_IMAGE=<substrate ateom-gvisor image digest>"; exit 1; }
	@ECHO_REF=$$(docker inspect --format='{{index .RepoDigests 0}}' $(ECHO_IMAGE):latest); \
		echo "echo actor image: $$ECHO_REF"; \
		sed -e "s|\$${BUCKET_NAME}|$(BUCKET_NAME)|g" -e "s|\$${ECHO_IMAGE}|$$ECHO_REF|g" \
			-e "s|\$${ATEOM_IMAGE}|$(ATEOM_IMAGE)|g" -e "s|\$${BROKER_ADDRESS}|$(BROKER_ADDRESS)|g" \
			deploy/echo-actor.yaml.tmpl | kubectl apply -f -
	kubectl -n ate-demo-ws-poc rollout status deployment/ws-poc-echo-deployment --timeout=300s || true
	kubectl wait --for=condition=Ready actortemplate/echo -n ate-demo-ws-poc --timeout=300s

.PHONY: create-actor
create-actor:
	kubectl ate create atespace demo || true
	kubectl ate create actor echo-1 -a demo --template ate-demo-ws-poc/echo

# Build and push the openclaw actor image: the same local proxy (cross-compiled
# here) as PID 1, layered onto the upstream OpenClaw image with the Slack plugin,
# agent config, and proxy TLS material baked in.
.PHONY: build-openclaw-image
build-openclaw-image:
	OUT_DIR=$(CURDIR)/openclaw-actor/proxy-certs bash $(CADIR)/gen-proxy-cert.sh
	GOOS=linux GOARCH=$(ACTOR_ARCH) CGO_ENABLED=0 \
		go build -o openclaw-actor/local-proxy ./cmd/local-proxy
	docker build --platform linux/$(ACTOR_ARCH) -t $(OPENCLAW_IMAGE):latest $(CURDIR)/openclaw-actor
	docker push $(OPENCLAW_IMAGE):latest

.PHONY: deploy-openclaw
deploy-openclaw: build-openclaw-image
	@test -n "$(BUCKET_NAME)" || { echo "set BUCKET_NAME=<snapshot bucket>"; exit 1; }
	@test -n "$(ATEOM_IMAGE)" || { echo "set ATEOM_IMAGE=<substrate ateom-gvisor image digest>"; exit 1; }
	@OPENCLAW_REF=$$(docker inspect --format='{{index .RepoDigests 0}}' $(OPENCLAW_IMAGE):latest); \
		echo "openclaw actor image: $$OPENCLAW_REF"; \
		sed -e "s|\$${BUCKET_NAME}|$(BUCKET_NAME)|g" -e "s|\$${OPENCLAW_IMAGE}|$$OPENCLAW_REF|g" \
			-e "s|\$${ATEOM_IMAGE}|$(ATEOM_IMAGE)|g" -e "s|\$${BROKER_ADDRESS}|$(BROKER_ADDRESS)|g" \
			deploy/openclaw-actor.yaml.tmpl | kubectl apply -f -
	kubectl -n ate-demo-ws-poc rollout status deployment/ws-poc-openclaw-deployment --timeout=300s || true
	kubectl wait --for=condition=Ready actortemplate/openclaw -n ate-demo-ws-poc --timeout=300s

.PHONY: create-openclaw
create-openclaw:
	kubectl ate create atespace demo || true
	kubectl ate create actor openclaw-1 -a demo --template ate-demo-ws-poc/openclaw

.PHONY: deploy
deploy: deploy-broker
	@echo "Broker deployed. Now: make slack-secret APP_TOKEN=.. BOT_TOKEN=.. && make deploy-actor BUCKET_NAME=.. ATEOM_IMAGE=.. BROKER_ADDRESS=egress-broker.ws-poc.svc.cluster.local:9090 && make create-actor"

.PHONY: clean
clean:
	-kubectl ate delete actor echo-1 -a demo
	-kubectl ate delete actor openclaw-1 -a demo
	-sed -e "s|\$${BUCKET_NAME}|placeholder|g" -e "s|\$${ECHO_IMAGE}|placeholder@sha256:0|g" -e "s|\$${ATEOM_IMAGE}|placeholder@sha256:0|g" -e "s|\$${BROKER_ADDRESS}||g" deploy/echo-actor.yaml.tmpl | kubectl delete --ignore-not-found -f -
	-sed -e "s|\$${BUCKET_NAME}|placeholder|g" -e "s|\$${OPENCLAW_IMAGE}|placeholder@sha256:0|g" -e "s|\$${ATEOM_IMAGE}|placeholder@sha256:0|g" -e "s|\$${BROKER_ADDRESS}||g" deploy/openclaw-actor.yaml.tmpl | kubectl delete --ignore-not-found -f -
	-kubectl delete --ignore-not-found -f deploy/egress-broker.yaml
