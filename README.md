# Frontier Sync Engine

The `sync-engine` branch holds the Go + Postgres sync engine — webhook
ingestion, authoritative refetch, the mirror cache, coalescing, budget
management, reconciliation, and the change stream. The design and its
constraints live in [docs/SYNC_ENGINE.md](docs/SYNC_ENGINE.md); milestones in
[docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md). The UI prototype
lives on `main` and is untouched here. The gRPC API
([docs/API_SPEC.md](docs/API_SPEC.md)) is a future project — this system's
delivery interface is the documented Postgres contract.

Module path `github.com/acme/frontier` is a placeholder until a remote exists.

## Development

```bash
make build     # compile everything
make test      # unit tests (DB tests skip without TEST_DATABASE_URL)
make dev       # docker compose postgres + migrate + run the daemon
make gen       # regenerate sqlc code after editing db/queries or db/migrations
```

`frontier-syncd` commands: `serve --roles=...`, `migrate`,
`requeue --guid=…|--all-parked`, `version`.
`fake-github` (cmd/fake-github) serves a canned enrolled repo and emits
HMAC-signed webhooks; docker-compose runs it beside Postgres.

The `webhook_deliveries.headers` JSONB value is a semantic request envelope:
the canonical header map plus host, parsed content length, and transfer
encoding. Wire-exact header casing and ordering are intentionally not retained.

## Status

- [x] M0 — foundations: module, migrations (River + own), three River
      queues, config, fake GitHub skeleton, CI
- [x] M1 — GitHub plumbing & budgeter
- [x] M2 — ingestion, dispatch, coalescing
- [ ] M3 — cache & fetchers
- [ ] M4 — reconciliation & webhook validation
- [ ] M5 — change stream, derivation seam, contract
- [ ] M6 — hardening & operations
