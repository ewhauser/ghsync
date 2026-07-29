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
`backfill` (the configured installation), `requeue --guid=…|--all-parked`,
`version`.
`fake-github` (cmd/fake-github) serves a canned enrolled repo and emits
HMAC-signed webhooks; docker-compose runs it beside Postgres.

The `webhook_deliveries.headers` JSONB value is a semantic request envelope:
the canonical header map plus host, parsed content length, and transfer
encoding. Wire-exact header casing and ordering are intentionally not retained.

### M3 fetch coordination

River continues to claim and acknowledge one `refresh_pr` pointer at a time.
The workers call a shared 5 ms fetch coordinator that gangs concurrently due,
already-known PR node IDs into `nodes(ids:)` calls of at most 25. Results are
sorted by immutable installation/repository/entity key. A dedicated Postgres
connection holds a session advisory lock from before the GitHub observation
through the cache transaction; repository observations take their own
immutable lock. This keeps M2's per-job refresh-generation recheck intact while
meeting C-P4 without custom multi-job River acknowledgement. Follow-up
generations/jobs commit in the entity transaction. New PRs and explicit
stack-membership resolution use conditional REST GETs until a node ID is
cached; review connections are fully paginated.

Installation backfill enumerates repositories, refreshes repository rules,
stacks, and PR detail as ordinary jobs, moves long tails to the `sweep` queue,
and marks cursors complete only after durable child generations complete.

## Status

- [x] M0 — foundations: module, migrations (River + own), three River
      queues, config, fake GitHub skeleton, CI
- [x] M1 — GitHub plumbing & budgeter
- [x] M2 — ingestion, dispatch, coalescing
- [x] M3 — cache & fetchers
- [ ] M4 — reconciliation & webhook validation
- [ ] M5 — change stream, derivation seam, contract
- [ ] M6 — hardening & operations
