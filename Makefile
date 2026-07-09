# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build and deploy helpers for the WebSocket egress-broker PoC. Run from the
# repo root. See README.md for the full runbook.

SHELL := /bin/bash
KO_DOCKER_REPO ?= localhost:5001
export KO_DOCKER_REPO

CADIR ?= $(CURDIR)/certs
BROKER_PKG := github.com/ronlv10/substrate-ws-poc/cmd/egress-broker

# The Bolt (Node) echo actor is a Docker image (not ko-built). ACTOR_ARCH must
# match the cluster nodes.
ECHO_IMAGE ?= localhost:5001/ws-poc-echo-actor
ACTOR_ARCH ?= arm64

# ateom-gvisor lives in substrate (the fork), not this repo, so its image is an
# input: ko build github.com/agent-substrate/substrate/cmd/ateom-gvisor from a
# substrate checkout and pass the pinned digest here.
ATEOM_IMAGE ?=

ATELET_CA_PATH := /var/lib/ateom-gvisor/ws-poc-ca-certificates.crt

.PHONY: help
help:
	@echo "substrate-ws-poc targets:"
	@echo "  test           - run the unit tests"
	@echo "  gen-ca         - generate the broker CA (certs/ca.crt, certs/ca.key)"
	@echo "  ca-secret      - create the egress-broker-ca TLS secret from certs/"
	@echo "  slack-secret   - create the slack-tokens secret (needs APP_TOKEN, BOT_TOKEN)"
	@echo "  build          - ko build the broker + build the actor image"
	@echo "  deploy-broker  - apply broker + ca-installer (ko apply)"
	@echo "  atelet-ca      - point atelet at the actor CA bundle (ATE_ACTOR_CA_BUNDLE)"
	@echo "  coredns-patch  - add the slack.com -> broker CoreDNS rewrite"
	@echo "  deploy-actor   - apply the echo-actor WorkerPool + ActorTemplate (needs BUCKET_NAME, ATEOM_IMAGE)"
	@echo "  create-actor   - create the demo atespace + actor echo-1"
	@echo "  deploy         - ca-secret + deploy-broker + atelet-ca + coredns-patch"
	@echo "  clean          - delete PoC resources"

.PHONY: test
test:
	go test ./...

.PHONY: gen-ca
gen-ca:
	cd $(CADIR) && OUT_DIR=. bash gen-ca.sh

.PHONY: ca-secret
ca-secret:
	kubectl create namespace ws-poc --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ws-poc create secret tls egress-broker-ca \
		--cert=$(CADIR)/ca.crt --key=$(CADIR)/ca.key \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: slack-secret
slack-secret:
	@test -n "$(APP_TOKEN)" || { echo "set APP_TOKEN=xapp-..."; exit 1; }
	@test -n "$(BOT_TOKEN)" || { echo "set BOT_TOKEN=xoxb-..."; exit 1; }
	kubectl create namespace ate-demo-ws-poc --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ate-demo-ws-poc create secret generic slack-tokens \
		--from-literal=app-token=$(APP_TOKEN) \
		--from-literal=bot-token=$(BOT_TOKEN) \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: build
build:
	ko build $(BROKER_PKG)
	$(MAKE) build-actor-image

# Build and push the Bolt (Node) actor image.
.PHONY: build-actor-image
build-actor-image:
	docker build --platform linux/$(ACTOR_ARCH) -t $(ECHO_IMAGE):latest $(CURDIR)/echo-actor
	docker push $(ECHO_IMAGE):latest

.PHONY: deploy-broker
deploy-broker:
	ko apply -f deploy/egress-broker.yaml
	kubectl apply -f deploy/ca-installer.yaml

.PHONY: atelet-ca
atelet-ca:
	kubectl -n ate-system set env daemonset/atelet ATE_ACTOR_CA_BUNDLE=$(ATELET_CA_PATH)
	kubectl -n ate-system rollout status daemonset/atelet --timeout=120s

.PHONY: coredns-patch
coredns-patch:
	@bash $(CURDIR)/deploy/coredns-patch.sh

.PHONY: deploy-actor
deploy-actor: build-actor-image
	@test -n "$(BUCKET_NAME)" || { echo "set BUCKET_NAME=<snapshot bucket>"; exit 1; }
	@test -n "$(ATEOM_IMAGE)" || { echo "set ATEOM_IMAGE=<substrate ateom-gvisor image digest>"; exit 1; }
	@ECHO_REF=$$(docker inspect --format='{{index .RepoDigests 0}}' $(ECHO_IMAGE):latest); \
		echo "echo actor image: $$ECHO_REF"; \
		sed -e "s|\$${BUCKET_NAME}|$(BUCKET_NAME)|g" -e "s|\$${ECHO_IMAGE}|$$ECHO_REF|g" \
			-e "s|\$${ATEOM_IMAGE}|$(ATEOM_IMAGE)|g" deploy/echo-actor.yaml.tmpl | kubectl apply -f -
	kubectl -n ate-demo-ws-poc rollout status deployment/ws-poc-echo-deployment --timeout=300s || true
	kubectl wait --for=condition=Ready actortemplate/echo -n ate-demo-ws-poc --timeout=300s

.PHONY: create-actor
create-actor:
	kubectl ate create atespace demo || true
	kubectl ate create actor echo-1 -a demo --template ate-demo-ws-poc/echo

.PHONY: deploy
deploy: ca-secret deploy-broker atelet-ca coredns-patch
	@echo "Broker deployed. Now: make slack-secret APP_TOKEN=.. BOT_TOKEN=.. && make deploy-actor BUCKET_NAME=.. ATEOM_IMAGE=.. && make create-actor"

.PHONY: clean
clean:
	-kubectl ate delete actor echo-1 -a demo
	-sed -e "s|\$${BUCKET_NAME}|placeholder|g" -e "s|\$${ECHO_IMAGE}|placeholder@sha256:0|g" -e "s|\$${ATEOM_IMAGE}|placeholder@sha256:0|g" deploy/echo-actor.yaml.tmpl | kubectl delete --ignore-not-found -f -
	-kubectl delete --ignore-not-found -f deploy/ca-installer.yaml
	-kubectl delete --ignore-not-found -f deploy/egress-broker.yaml
	-kubectl -n ate-system set env daemonset/atelet ATE_ACTOR_CA_BUNDLE-
