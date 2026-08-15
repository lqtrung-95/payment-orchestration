.DEFAULT_GOAL := help
SHELL := /bin/bash

# Local development reads configuration from .env. It is optional so that CI and
# deployed environments, which inject real environment variables, are unaffected.
ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE := docker compose -f deploy/docker-compose.yml
BIN_DIR := bin

.PHONY: help
help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# --- Toolchain -------------------------------------------------------------

.PHONY: tidy
tidy: ## Sync go.mod and go.sum
	go mod tidy

.PHONY: fmt
fmt: ## Format all Go source
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

# -p 1 runs one package at a time. Integration tests share a single database and
# truncate it between cases, so packages running concurrently would wipe each
# other's fixtures mid-test.
.PHONY: test
test: ## Run tests with race detection
	go test -race -count=1 -p 1 ./...

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	go test -race -count=1 -p 1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage report: coverage.html"

.PHONY: check
check: fmt vet lint test ## Run the full pre-commit gate

# --- Build and run ---------------------------------------------------------

.PHONY: build
build: ## Build binaries into bin/
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/orchestrator ./cmd/orchestrator
	go build -o $(BIN_DIR)/migrate ./cmd/migrate
	go build -o $(BIN_DIR)/worker ./cmd/worker
	go build -o $(BIN_DIR)/pspsim ./cmd/pspsim
	go build -o $(BIN_DIR)/dlqctl ./cmd/dlqctl
	go build -o $(BIN_DIR)/webhookctl ./cmd/webhookctl
	go build -o $(BIN_DIR)/reconctl ./cmd/reconctl

.PHONY: run
run: ## Run the orchestrator against the local stack
	go run ./cmd/orchestrator

# --- Provider simulator ----------------------------------------------------

PSPSIM_URL ?= http://localhost:9091

# Where the simulator delivers its callbacks. It is the orchestrator's public
# webhook route, so the local stack exercises the same path a real provider uses.
PSPSIM_WEBHOOK_URL ?= http://localhost:8080/webhooks/psp-sim

.PHONY: worker
worker: ## Run the payment worker (consumes Kafka, calls providers)
	go run ./cmd/worker

.PHONY: dlq
dlq: ## List dead-lettered payment messages
	go run ./cmd/dlqctl list

.PHONY: pspsim
pspsim: ## Run the fault-injecting provider simulator (separate process)
	PSPSIM_WEBHOOK_URL=$(PSPSIM_WEBHOOK_URL) go run ./cmd/pspsim

.PHONY: replay
replay: ## Re-evaluate the stored webhook log against current state (writes nothing)
	go run ./cmd/webhookctl replay -v

.PHONY: breaks
breaks: ## List open reconciliation breaks
	go run ./cmd/reconctl breaks -status open

.PHONY: charges
charges: ## Show what the simulated provider believes it charged
	@curl -s $(PSPSIM_URL)/admin/charges | jq '{count, charges: [.charges[] | {reference, status, amount_minor}]}'

# --- Load testing ----------------------------------------------------------

RUN_ID ?= $(shell date +%s)

.PHONY: load-smoke
load-smoke: ## Correctness-only load run (10 rps, 30s)
	k6 run -e PROFILE=smoke -e RUN_ID=$(RUN_ID) loadtest/payments.js

.PHONY: load-baseline
load-baseline: ## Ramping load run against a healthy provider
	k6 run -e PROFILE=baseline -e RUN_ID=$(RUN_ID) loadtest/payments.js

.PHONY: load-chaos
load-chaos: ## Ramping load run with the full fault catalogue live
	@curl -s -X PUT "$(PSPSIM_URL)/admin/faults/preset?name=chaos" >/dev/null
	k6 run -e PROFILE=chaos -e RUN_ID=$(RUN_ID) loadtest/payments.js

.PHONY: invariants
invariants: ## Show the must-be-zero counters and the queue depths
	@curl -s http://localhost:8080/metrics \
		| grep -E '^payment_(double_charges|lost_payments|ledger_imbalance|outbox_pending|dlq_depth) '

# --- Demo ------------------------------------------------------------------

.PHONY: demo
demo: ## Run the narrated end-to-end demo (starts and stops everything itself)
	./scripts/demo.sh

.PHONY: demo-verify
demo-verify: ## Run the demo with no pauses and assert every claim
	./scripts/demo.sh --fast

.PHONY: faults
faults: ## Show the simulator's current fault configuration
	@curl -s $(PSPSIM_URL)/admin/faults

.PHONY: healthy
healthy: ## Switch the simulator to the healthy preset
	@curl -s -X PUT "$(PSPSIM_URL)/admin/faults/preset?name=healthy" >/dev/null && echo "preset: healthy"

.PHONY: degraded
degraded: ## Switch the simulator to the degraded preset
	@curl -s -X PUT "$(PSPSIM_URL)/admin/faults/preset?name=degraded" >/dev/null && echo "preset: degraded"

.PHONY: chaos
chaos: ## Switch the simulator to the chaos preset
	@curl -s -X PUT "$(PSPSIM_URL)/admin/faults/preset?name=chaos" >/dev/null && echo "preset: chaos"

.PHONY: outage
outage: ## Take the simulated provider down (SECONDS=30)
	@curl -s -X POST "$(PSPSIM_URL)/admin/outage?seconds=$(or $(SECONDS),30)" >/dev/null \
		&& echo "provider down for $(or $(SECONDS),30)s"

# --- Database --------------------------------------------------------------

.PHONY: migrate-up
migrate-up: ## Apply all pending migrations
	go run ./cmd/migrate up

.PHONY: migrate-down
migrate-down: ## Revert the most recent migration
	go run ./cmd/migrate down

.PHONY: migrate-version
migrate-version: ## Print the current schema version
	go run ./cmd/migrate version

# --- Local stack -----------------------------------------------------------

.PHONY: up
up: ## Start the local stack and wait for it to become healthy
	$(COMPOSE) up -d --wait
	@echo "stack ready"

.PHONY: up-observability
up-observability: ## Start the stack including Jaeger, Prometheus, and Grafana
	$(COMPOSE) --profile observability up -d --wait
	@echo "stack ready — jaeger :16686  prometheus :9090  grafana :3000"

.PHONY: down
down: ## Stop the local stack, preserving volumes
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the local stack and delete all data volumes
	$(COMPOSE) --profile observability down -v
	rm -rf $(BIN_DIR) coverage.out coverage.html

.PHONY: logs
logs: ## Tail logs from the local stack
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## Show local stack status
	$(COMPOSE) ps

# --- Composite -------------------------------------------------------------

.PHONY: dev
dev: up migrate-up run ## Start the stack, migrate, and run the service
