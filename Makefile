SHELL := /bin/bash
COMPOSE ?= docker compose
GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X github.com/yugrinovD/ledger/internal/version.Version=$(VERSION) \
	-X github.com/yugrinovD/ledger/internal/version.Commit=$(COMMIT) \
	-X github.com/yugrinovD/ledger/internal/version.Date=$(DATE)

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development
.PHONY: fmt
fmt: ## Format Go code
	gofmt -w cmd internal tests api

.PHONY: tidy
tidy: ## Tidy modules
	$(GO) mod tidy

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (if installed)
	@command -v golangci-lint >/dev/null || (echo "golangci-lint is not installed" && exit 1)
	golangci-lint run ./...

.PHONY: test
test: ## Run unit tests
	$(GO) test -race ./internal/... ./api/...

TEST_DATABASE_URL ?= postgres://ledger:ledger@localhost:55432/ledger_test?sslmode=disable
TEST_REDIS_ADDR ?= localhost:55433

.PHONY: test-integration
test-integration: ## Run PostgreSQL-backed integration tests
	@docker start ledger-test-postgres >/dev/null 2>&1 || \
		docker run -d --name ledger-test-postgres \
			-e POSTGRES_USER=ledger -e POSTGRES_PASSWORD=ledger -e POSTGRES_DB=ledger_test \
			-p 55432:5432 postgres:17-alpine >/dev/null
	@until docker exec ledger-test-postgres pg_isready -U ledger -d ledger_test >/dev/null 2>&1; do sleep 0.5; done
	@docker start ledger-test-redis >/dev/null 2>&1 || \
		docker run -d --name ledger-test-redis -p 55433:6379 redis:7-alpine >/dev/null
	@until docker exec ledger-test-redis redis-cli ping >/dev/null 2>&1; do sleep 0.5; done
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_REDIS_ADDR='$(TEST_REDIS_ADDR)' $(GO) test -race -count=1 ./tests/integration

.PHONY: cover
cover: ## Coverage for unit tests
	$(GO) test -race -coverprofile=coverage.out ./internal/...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: build
build: ## Build binaries into bin/
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ledger-api ./cmd/api
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ledger-worker ./cmd/worker
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ledgerctl ./cmd/ledgerctl

##@ Docker
.PHONY: up
up: ## Start the stack with observability
	$(COMPOSE) --profile observability up -d --build

.PHONY: core
core: ## Start only core services
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the stack and remove volumes
	$(COMPOSE) --profile observability --profile load --profile tools down -v

.PHONY: ps
ps: ## Show running services
	$(COMPOSE) ps

.PHONY: logs
logs: ## Follow API and worker logs
	$(COMPOSE) logs -f api worker

.PHONY: migrate
migrate: ## Apply migrations through the CLI container
	$(COMPOSE) --profile tools run --rm ledgerctl migrate up

.PHONY: seed
seed: ## Seed demo accounts and write load/accounts.json
	$(COMPOSE) --profile tools run --rm ledgerctl seed --customers $(or $(CUSTOMERS),200) --output /out/accounts.json

.PHONY: apikey
apikey: ## Create a demo API key and write load/api-key.json
	$(COMPOSE) --profile tools run --rm ledgerctl apikey-create --owner demo --name demo --scopes '*' | tee load/api-key.json

.PHONY: demo
demo: ## Run the end-to-end demo script
	./scripts/demo.sh

.PHONY: drift
drift: ## Corrupt a balance and prove reconciliation detects it
	./scripts/demo-drift.sh

.PHONY: demo-outbox
demo-outbox: ## Stop the broker, post a transfer, restart, and show the outbox drain
	./scripts/demo-outbox.sh

.PHONY: reconcile
reconcile: ## Run reconciliation
	$(COMPOSE) --profile tools run --rm ledgerctl reconcile

.PHONY: load
load: ## Run k6 steady load (make seed first)
	@test -f load/accounts.json || (echo "run make seed first" && exit 1)
	$(COMPOSE) --profile tools run --rm ledgerctl apikey-create --owner demo --name k6 --scopes '*' > load/api-key.json
	@API_KEY=$$(jq -r .key load/api-key.json) $(COMPOSE) --profile load run --rm k6

.PHONY: load-duplicates
load-duplicates: ## Run k6 duplicate-heavy load
	@test -f load/accounts.json || (echo "run make seed first" && exit 1)
	$(COMPOSE) --profile tools run --rm ledgerctl apikey-create --owner demo --name k6 --scopes '*' > load/api-key.json
	@API_KEY=$$(jq -r .key load/api-key.json) $(COMPOSE) --profile load run --rm k6 run /scripts/duplicates.js

.PHONY: load-read
load-read: ## Run k6 read-heavy load
	@test -f load/accounts.json || (echo "run make seed first" && exit 1)
	$(COMPOSE) --profile tools run --rm ledgerctl apikey-create --owner demo --name k6 --scopes '*' > load/api-key.json
	@API_KEY=$$(jq -r .key load/api-key.json) $(COMPOSE) --profile load run --rm k6 run /scripts/read.js

.PHONY: psql
psql: ## Open psql
	$(COMPOSE) exec postgres psql -U ledger -d ledger

##@ Tooling
.PHONY: openapi
openapi: ## Validate the OpenAPI document
	$(GO) test ./api/...

.PHONY: vuln
vuln: ## Run govulncheck (if installed)
	@command -v govulncheck >/dev/null || (echo "govulncheck is not installed" && exit 1)
	govulncheck ./...

.PHONY: clean
clean: ## Remove build and load artifacts
	rm -rf bin coverage.out load/results load/accounts.json load/api-key.json
