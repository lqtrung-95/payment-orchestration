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

.PHONY: test
test: ## Run tests with race detection
	go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	go test -race -count=1 -coverprofile=coverage.out ./...
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

.PHONY: run
run: ## Run the orchestrator against the local stack
	go run ./cmd/orchestrator

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
