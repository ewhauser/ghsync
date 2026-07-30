# ghsync

`ghsync` is a Go and PostgreSQL service that maintains a queryable local
mirror of GitHub repository, pull request, stacked pull request, review, and
check state.

GitHub webhooks are treated as hints about what changed, while authoritative
REST and GraphQL responses are used as the source of truth. This makes the
mirror resilient to duplicate, delayed, missing, and out-of-order deliveries.
Consumers can read the mirror directly from PostgreSQL and follow a durable,
transactionally consistent change stream.

## Features

- HMAC-verified webhook ingestion with durable-before-acknowledge storage
- Authoritative REST and GraphQL refreshes with burst coalescing
- Installation-wide GitHub API rate-budget management
- Resumable installation backfills and reconciliation sweeps
- Delivery-gap healing and semantic drift detection
- PostgreSQL-backed queues, coordination, cache, and change stream
- A reference Go consumer in [`pkg/streamclient`](pkg/streamclient)
- Health checks, Prometheus metrics, alert rules, and operational runbooks

The core design rule is simple:

> Webhooks are hints. Fetches are truth.

## Scope

`ghsync` is currently designed for one GitHub App installation and one
organization on GitHub Enterprise Cloud. PostgreSQL is its only stateful
dependency.

The version 1 delivery interface is PostgreSQL. `ghsync` does not currently
ship a user interface, gRPC API, or SSE API. Its stacked pull request support
also depends on GitHub stack surfaces that may not be available to every
installation.

See [`docs/SYNC_ENGINE.md`](docs/SYNC_ENGINE.md) for the architecture and
invariants, and [`db/CONTRACT.md`](db/CONTRACT.md) for the public database and
change-stream contract.

## Quick start

### Prerequisites

- The Go version declared in [`go.mod`](go.mod)
- Docker with Docker Compose
- `make`

Start PostgreSQL, the included fake GitHub server, and all `ghsync` roles:

```bash
make dev
```

In another terminal, seed the fake installation:

```bash
DATABASE_URL='postgres://ghsync:ghsync@localhost:5433/ghsync?sslmode=disable' \
GITHUB_INSTALLATION_ID=1 \
go run ./cmd/ghsyncd backfill
```

You can then inspect the service or follow its change stream:

```bash
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8080/metrics

DATABASE_URL='postgres://ghsync:ghsync@localhost:5433/ghsync?sslmode=disable' \
go run ./cmd/stream-tail --bootstrap
```

Stop the local services with `Ctrl-C`. To also remove the containers and local
PostgreSQL volume, run `make clean`.

## Running with GitHub

Build the daemon:

```bash
go build -o ./bin/ghsyncd ./cmd/ghsyncd
```

A real deployment needs a PostgreSQL database and a GitHub App installed on
the organization to mirror. Configure the App's webhook URL as:

```text
https://your-ghsync-host.example/webhooks/github
```

Subscribe the App to the events used by the default dispatcher:

- Pull requests
- Pull request reviews
- Pull request review comments
- Pull request review threads
- Check runs
- Check suites
- Pushes

The principal runtime settings are:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection string |
| `GITHUB_APP_ID` | GitHub App ID |
| `GITHUB_INSTALLATION_ID` | App installation to mirror |
| `GITHUB_ORG_ID` | Stable GitHub organization ID stored in mirror rows |
| `GITHUB_PRIVATE_KEY_PATH` | Absolute path to the App's PEM private key |
| `GITHUB_WEBHOOK_SECRET` | Secret used to verify webhook signatures |
| `HTTP_ADDR` | Health, metrics, and webhook listen address; defaults to `:8080` |

Apply migrations before starting a new version:

```bash
ghsyncd migrate
```

After the service roles are running, start or resume the initial installation
backfill:

```bash
ghsyncd backfill
```

Production runs the daemon as several role-specific process groups. In
particular, `fetch`, `sweep`, and `drift` must run together as exactly one
GitHub-facing singleton. Do not use `serve --roles=all` as a production
rolling-deployment topology. The complete role layout, configuration
reference, migration procedure, and backup guidance are in
[`ops/DEPLOYMENT.md`](ops/DEPLOYMENT.md).

Every process exposes:

- `GET /healthz`
- `GET /metrics`

Only a process with the `ingress` role exposes
`POST /webhooks/github`.

OpenTelemetry trace export is opt-in and uses OTLP/HTTP. See
[`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) for configuration, River trace
propagation, sampling, and local Jaeger inspection.

## Commands

| Command | Description |
| --- | --- |
| `ghsyncd serve --roles=...` | Run one or more service roles |
| `ghsyncd migrate` | Apply River and `ghsync` database migrations |
| `ghsyncd backfill` | Start or resume the configured installation backfill |
| `ghsyncd requeue --guid=...` | Replay a parked webhook delivery |
| `ghsyncd version` | Print build version information |
| `go run ./cmd/stream-tail` | Run the reference change-stream consumer |

Available service roles are `ingress`, `dispatch`, `fetch`, `sweep`, `drift`,
`pruner`, `watermarker`, `deriver`, and `metrics`. `all` enables every role
for local development and CI.

## Consuming the mirror

Applications should use [`pkg/streamclient`](pkg/streamclient) to bootstrap a
snapshot and consume changes. The package handles safe-watermark paging,
durable cursors, retention horizons, retries, and resynchronization after a
consumer falls behind.

Run exactly one tailer for each `(consumer, stream)` pair. Apply projection
updates and cursor advancement in the transaction supplied to the event
handler. See [`db/CONTRACT.md`](db/CONTRACT.md) for database grants, the
versioned public schema, and the full consumer protocol.

## Development

Common development commands:

```bash
make build
make test     # DB tests skip without TEST_DATABASE_URL
              # DB tests run in parallel: pgtestdb clones a migrated template
              # database per test, so the TEST_DATABASE_URL user must be able
              # to create roles and databases (the docker/CI user is)
make lint
make gen
```

Database-backed tests skip when `TEST_DATABASE_URL` is not set. To run the
complete test suite locally:

```bash
docker compose up -d --wait postgres

TEST_DATABASE_URL='postgres://ghsync:ghsync@localhost:5433/ghsync?sslmode=disable&pool_max_conns=20' \
make test
```

Run `make gen` after changing files under `db/queries` or `db/migrations`, and
include the regenerated `internal/store/dbgen` files in the same change.

Issues and pull requests are welcome. Please keep changes focused, add tests
for behavior changes, and run the relevant build, test, lint, and generation
checks before submitting a pull request.

## Operations and design documentation

- [`docs/SYNC_ENGINE.md`](docs/SYNC_ENGINE.md) — architecture and correctness
  constraints
- [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) — OpenTelemetry tracing,
  River propagation, sampling, and data-handling policy
- [`db/CONTRACT.md`](db/CONTRACT.md) — public PostgreSQL and change-stream
  contract
- [`ops/DEPLOYMENT.md`](ops/DEPLOYMENT.md) — deployment topology and
  configuration
- [`ops/DASHBOARD.md`](ops/DASHBOARD.md) — cache-trust dashboard specification
- [`ops/alerts.yaml`](ops/alerts.yaml) — Prometheus alert rules
- [`ops/runbooks`](ops/runbooks) — incident response procedures
- [`ops/SOAK.md`](ops/SOAK.md) — load and convergence testing

## License

This repository does not currently include an open-source license. Until one
is added, copyright law reserves reuse and redistribution rights to the
copyright holder.
