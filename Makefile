GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT_BASE := CGO_ENABLED=0 go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
CUSTOM_GCL := $(CURDIR)/custom-gcl

.PHONY: build test lint custom-gcl gen migrate dev clean

build:
	go build ./...

test:
	go test ./...

$(CUSTOM_GCL): .custom-gcl.yml
	$(GOLANGCI_LINT_BASE) custom
	chmod +x $(CUSTOM_GCL)

custom-gcl: $(CUSTOM_GCL)

lint: custom-gcl
	./custom-gcl run ./...

# Regenerate sqlc query code (internal/store/dbgen).
gen:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

migrate:
	go run ./cmd/ghsyncd migrate

# Local development: Postgres + fake GitHub + the daemon.
dev:
	docker compose up -d --wait postgres fake-github
	DATABASE_URL=postgres://ghsync:ghsync@localhost:5433/ghsync?sslmode=disable \
		go run ./cmd/ghsyncd migrate
	DATABASE_URL=postgres://ghsync:ghsync@localhost:5433/ghsync?sslmode=disable \
	GITHUB_WEBHOOK_SECRET=dev-secret \
	GITHUB_BASE_URL=http://localhost:9797 \
	GITHUB_TOKEN=dev-token \
	GITHUB_INSTALLATION_ID=1 \
	GITHUB_ORG_ID=1 \
		go run ./cmd/ghsyncd serve

clean:
	docker compose down -v
