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
Serve roles are `ingress`, `dispatch`, `fetch`, `sweep`, `drift`, and
`pruner`; `all` enables the complete pipeline.
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

### M4 reconciliation and validation

Every leader-eligible River process registers the same configured C-R1
schedule table. Component jobs use isolated `reconcile`, `drift`, and `pruner`
queues, while only fetch processes poll the three fetch priority queues.
Authoritative repository, stack, and open-PR listings persist cursors, per-page
ETags, and list-membership freshness; a restarted worker resumes the recorded
page. A list 304 never advances member-entity freshness. PR discovery uses a
stable creation order plus repeated overlap passes, and members receive
ordinary per-entity reconciliation. Listing disappearance always takes the
verify-then-404 path before a retained tombstone is written.

Startup and scheduled gap healing compare a fixed-time App-deliveries window
against retained `webhook_deliveries` GUID skeletons and request redelivery for
missing GUIDs. Capped scans emit a signal and durably continue from GitHub's
opaque cursor. The drift job rotates a quota through every semantic entity
class, deduplicates attached diffs, and permits one self-healing generation
before persistent divergence is escalated. The pruner removes webhook
bodies/headers and `check_history` older than the locked minimum 90-day
boundary in bounded transactions; it does not prune `change_events`.

The C-R1 durations, gap window, drift sample, and retention settings are
environment configuration. Dispatcher rules can be loaded from
[`config/dispatcher-rules.yaml`](config/dispatcher-rules.yaml) with
`DISPATCH_RULES_FILE`; the real-repository validation protocol is
[`ops/PHASE0-WEBHOOK-VALIDATION.md`](ops/PHASE0-WEBHOOK-VALIDATION.md).

## Status

- [x] M0 — foundations: module, migrations (River + own), three River
      queues, config, fake GitHub skeleton, CI
- [x] M1 — GitHub plumbing & budgeter
- [x] M2 — ingestion, dispatch, coalescing
- [x] M3 — cache & fetchers
- [x] M4 — reconciliation & webhook validation
- [ ] M5 — change stream, derivation seam, contract
- [ ] M6 — hardening & operations
