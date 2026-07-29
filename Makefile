.PHONY: build test lint gen migrate dev clean

build:
	go build ./...

test:
	go test ./...

lint:
	go vet ./...
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint is required" >&2; exit 1; }
	golangci-lint run ./...

# Regenerate sqlc query code (internal/store/dbgen).
gen:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

migrate:
	go run ./cmd/frontier-syncd migrate

# Local development: Postgres + fake GitHub + the daemon.
dev:
	docker compose up -d --wait postgres fake-github
	DATABASE_URL=postgres://frontier:frontier@localhost:5433/frontier?sslmode=disable \
		go run ./cmd/frontier-syncd migrate
	DATABASE_URL=postgres://frontier:frontier@localhost:5433/frontier?sslmode=disable \
	GITHUB_WEBHOOK_SECRET=dev-secret \
	GITHUB_BASE_URL=http://localhost:9797 \
	GITHUB_TOKEN=dev-token \
	GITHUB_INSTALLATION_ID=1 \
	GITHUB_ORG_ID=1 \
		go run ./cmd/frontier-syncd serve

clean:
	docker compose down -v
