# Deployment

## Artifact and roles

Production deploys one `frontier-syncd` binary. `serve --roles=all` is the
default; roles may split across `ingress`, `dispatch`, `fetch`, `sweep`,
`drift`, `pruner`, `watermarker`, and `deriver`. Every process exposes
`GET /healthz` and `GET /metrics`; only ingress exposes the webhook POST.
`fake-github`, `stream-tail`, and `soak` are operator/development utilities.

## Configuration reference

Durations use Go syntax (`250ms`, `5m`, `24h`).

| Variable | Default | Required / meaning |
|---|---:|---|
| `DATABASE_URL` | none | Required; the one stateful Postgres dependency. |
| `HTTP_ADDR` | `:8080` | Health, metrics, and optional ingress address. |
| `GITHUB_APP_ID` | `0` | Production fetch credential. |
| `GITHUB_INSTALLATION_ID` | `0` | Required for fetch/periodic work. |
| `GITHUB_ORG_ID` | `0` | Required stable mirror org ID. |
| `GITHUB_PRIVATE_KEY_PATH` | none | App PEM path. |
| `GITHUB_WEBHOOK_SECRET` | none | Required by ingress only. |
| `GITHUB_TOKEN` | none | Development/fake escape hatch only. |
| `GITHUB_BASE_URL` | `https://api.github.com` | API base. |
| `WEBHOOK_MAX_BODY_BYTES` | `2097152` | Positive ingress limit. |
| `DISPATCH_BATCH_SIZE` | `100` | C-P2 claim size. |
| `DISPATCH_MAX_ATTEMPTS` | `5` | C-I5 attempts before parking. |
| `DISPATCH_DEBOUNCE` | `5s` | C-Q2 delay; hard max `15s`. |
| `DISPATCH_POLL_INTERVAL` | `250ms` | Idle claim interval. |
| `DISPATCH_RULES_FILE` | built in | Optional classifier YAML/JSON. |
| `SWEEP_OPEN_STACK_MAX_STALENESS` | `5m` | C-R1 stack bound. |
| `SWEEP_OPEN_PR_MAX_STALENESS` | `10m` | C-R1 PR bound. |
| `SWEEP_REPO_RULES_MAX_STALENESS` | `1h` | C-R1 rules bound. |
| `SWEEP_CLOSED_MAX_STALENESS` | `24h` | C-R1 closed bound. |
| `SWEEP_REPOSITORY_LIST_PERIOD` | `1h` | Repository list period. |
| `SWEEP_PAGE_SIZE` | `100` | GitHub page size; max `100`. |
| `GAP_HEAL_PERIOD` | `5m` | C-R4 cadence. |
| `GAP_COMPARISON_WINDOW` | `6h` | Fixed delivery window. |
| `GAP_PAGE_SIZE` | `100` | Delivery page size; max `100`. |
| `GAP_MAX_PAGES` | `10` | Pages per resumable job. |
| `DRIFT_PERIOD` | `1h` | C-O3 cadence. |
| `DRIFT_SAMPLE_SIZE` | `10` | Must cover six classes. |
| `DRIFT_RESOLVED_RETENTION` | `720h` | Resolved evidence retention. |
| `RETENTION_PERIOD` | `24h` | Payload/history prune cadence. |
| `RETENTION_AGE` | `2160h` | Locked minimum 90 days. |
| `RETENTION_BATCH_SIZE` | `1000` | Rows per prune transaction. |
| `STREAM_WATERMARK_REFRESH` | `100ms` | C-S2 step interval. |
| `STREAM_WATERMARK_LEASE_TTL` | `3s` | Singleton lease TTL. |
| `STREAM_RETENTION_PERIOD` | `1h` | Change-event prune cadence. |
| `STREAM_RETENTION_AGE` | `168h` | Locked minimum seven days. |
| `STREAM_RETENTION_BATCH_SIZE` | `1000` | Events per transaction. |
| `DERIVER_POLL_INTERVAL` | `500ms` | Dirty poll fallback. |
| `DERIVER_DIRTY_CAP` | `500` | C-P5 scopes per pass. |

`serve --roles=...` is a flag, not an environment variable. Invalid unsafe
configuration fails before startup.

## Migration order

Before replicas start:

```sh
frontier-syncd migrate
```

The command takes an advisory lock, applies River migrations first, then
embedded Frontier files lexically. Each Frontier migration is one transaction
recorded with a SHA-256 checksum. Changed applied bytes fail closed. Never edit
an applied file; add a forward migration. M6 adds only
`0020_m6_operational_state.sql`.

## Rolling restart

1. Verify backup; run `frontier-syncd migrate` once.
2. Deploy one replica with its existing roles; wait for `/healthz`.
3. Verify `frontier_c_o4_role_enabled`, the trust row, queues, and leases.
4. `SIGTERM` one old replica; allow its 10-second HTTP/River grace.
5. Repeat, always retaining ingress, dispatch, fetch, sweep, and watermarker.

C-O2 makes this safe: River jobs/leadership are durable, budget and watermark
leases fail over, cursors resume, ingress commits before ACK, and refresh
generations preserve signals arriving during a fetch. Restarts may retry
idempotent work but cannot lose it.

## Backup and restore

Postgres is the only stateful dependency. Use encrypted PITR-capable physical
backups and restore drills. Back up the entire database: River state, checksum
ledger, budget leases, cache, cursors, outbox, and horizons are one boundary.

After restore, run the same binary's `migrate`, then start watermarker, fetch,
and sweep. Verify:

```sql
SELECT name, encode(checksum, 'hex'), applied_at
FROM schema_migrations ORDER BY name;
SELECT safe_seq, updated_at FROM stream_watermark WHERE singleton;
SELECT entity_kind, count(*) FROM drift_findings
WHERE resolved_at IS NULL GROUP BY entity_kind;
```

Gap healing and C-R1 sweeps reconcile downtime. Consumers below the restored
horizon must bootstrap after `RESYNC_REQUIRED`; never fabricate cursor or
watermark values.
