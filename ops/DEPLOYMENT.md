# Deployment

## Artifact and supported topology

Production deploys one `frontier-syncd` artifact in the following process
groups. Every process receives the same positive
`GITHUB_INSTALLATION_ID`, exposes `GET /healthz` and `GET /metrics`, and only
the ingress group exposes the webhook POST.

| Process group | Command | Replicas | Replacement rule |
|---|---|---:|---|
| ingress | `serve --roles=ingress` | 2+ | rolling, new before old |
| dispatch | `serve --roles=dispatch` | 2+ | rolling, new before old |
| GitHub singleton | `serve --roles=fetch,sweep,drift` | exactly 1 | **stop old, then start new** |
| pruner | `serve --roles=pruner` | 1 | stop/start |
| watermarker | `serve --roles=watermarker` | 2 | rolling; lease elects one |
| deriver | `serve --roles=deriver` | 2+ | rolling |
| trust metrics | `serve --roles=metrics` | exactly 1 | start new only after old stops |

`fetch`, `sweep`, and `drift` are deliberately inseparable in the executable.
They share one installation-wide GitHub budget lease and one complete River
periodic table. Any partial combination fails startup. This is the topology
contract, not a scheduling suggestion.

The GitHub singleton cannot use replica-first rolling replacement: a second
instance correctly fails the exclusive budget lease. Its replacement is a
bounded stop/start handoff. Durable deliveries, refresh generations, River
jobs, sweep cursors, and drift cursors preserve work while the role is down.
Set the platform strategy for this group to `Recreate`/`maxSurge: 0`;
`maxUnavailable: 1`.

The `metrics` role is the sole database aggregate collector. Other roles still
publish their process-local counters and `frontier_c_o4_role_enabled`, but do
not repeat expensive cache/outbox aggregation on every scrape.

`serve --roles=all` remains a local/CI convenience, not the production rolling
topology. `fake-github`, `stream-tail`, and `soak` are operator/development
utilities.

## Configuration reference

Durations use Go syntax (`250ms`, `5m`, `24h`).

| Variable | Default | Required / meaning |
|---|---:|---|
| `DATABASE_URL` | none | Required; the one stateful Postgres dependency. |
| `HTTP_ADDR` | `:8080` | Health, metrics, and optional ingress address. |
| `GITHUB_APP_ID` | `0` | Production fetch credential. |
| `GITHUB_INSTALLATION_ID` | `0` | Required and positive for **every** serve process. |
| `GITHUB_ORG_ID` | `0` | Required stable mirror org ID for the GitHub singleton. |
| `GITHUB_PRIVATE_KEY_PATH` | none | App PEM path for the GitHub singleton. |
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
| `SWEEP_CLOSED_MAX_STALENESS` | `24h` | C-R1 display-retained closed bound. |
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

## Migration order

Before any new-version process starts:

```sh
frontier-syncd migrate
```

The command takes an advisory lock, applies River migrations first, then
embedded Frontier files lexically. Each Frontier migration is one transaction
recorded with a SHA-256 checksum. Changed applied bytes fail closed. Never edit
an applied file; add a forward migration. This pass adds
`0021_m6_review_fixes.sql`, `0022_m6_review_metrics.sql`, and
`0023_m6_operation_sample_age.sql`; migrations `0001` through `0020` remain
immutable.

Verify the exact ledger before continuing:

```sql
SELECT name, encode(checksum, 'hex'), applied_at
FROM schema_migrations
ORDER BY name;
```

## First installation bootstrap

On an empty cache, start the process groups and then seed the configured
installation exactly once:

```sh
frontier-syncd backfill
```

The combined GitHub singleton is safe to start before this command: the drift
detector records no successful trust pass until the installation backfill is
durably `done` and all cache-producing queues and refresh generations are
quiescent. Treat the intentionally absent/stale drift-success metric as firing
during bootstrap. Before declaring bootstrap complete, require:

```sql
SELECT installation_id, phase, completed_at
FROM installation_backfill_cursors
WHERE installation_id = :installation_id;

SELECT repo_full_name, phase, completed_at
FROM backfill_cursors
WHERE installation_id = :installation_id
  AND phase <> 'done';
```

The first query must return `done` with a non-null completion timestamp, the
second must return no rows, and the trust-metrics singleton must subsequently
report a new drift pass with a nonzero sample delta. Do not suppress the
absence alert or manufacture a bootstrap heartbeat.

## Replacement procedure

1. Verify backup/PITR health and run `frontier-syncd migrate` once.
2. Replace ingress, dispatch, watermarker, and deriver groups normally: start a
   new replica, require `/healthz` and its role metric, then `SIGTERM` one old
   replica.
3. Replace pruner with a stop/start handoff.
4. Replace the trust-metrics singleton with a stop/start handoff. Alerting is
   expected to fire while its metrics are absent; do not interpret the gap as
   success.
5. Replace the GitHub singleton:
   - verify event queue, outstanding generations, and open drift before stop;
   - `SIGTERM` the old `fetch,sweep,drift` process and allow its 10-second
     River grace;
   - verify its process exited and the budget lease is released/expired;
   - start exactly one new `fetch,sweep,drift` process;
   - require `/healthz`, all three role metrics, new GitHub request counters,
     and new durable sweep/drift heartbeats.
6. Verify no required-role alert, no absent trust metric, no parked delivery,
   no outstanding generation age breach, and a current watermark heartbeat.

The bounded GitHub-role outage is safe because ingress commits before ACK,
dispatch/refresh state is durable, refresh generations preserve coalesced
signals, River retries jobs, and reconciliation cursors resume. It is not
zero-downtime fetching and is never documented as such.

## Backup and restore

Postgres is the only stateful dependency. Use encrypted PITR-capable physical
backups and restore drills. Back up the entire database: River state, checksum
ledger, budget leases, cache, cursors, outbox, horizons, refresh generations,
and operation heartbeats are one boundary.

After restore, run the same binary's `migrate`, then start the supported
topology. Verify:

```sql
SELECT name, encode(checksum, 'hex'), applied_at
FROM schema_migrations ORDER BY name;
SELECT safe_seq, updated_at FROM stream_watermark WHERE singleton;
SELECT component, operation, success_count, sample_count,
       last_success_at, last_sample_at
FROM operation_heartbeats ORDER BY component, operation;
SELECT entity_kind, count(*) FROM drift_findings
WHERE resolved_at IS NULL GROUP BY entity_kind;
```

Gap healing and C-R1 sweeps reconcile downtime. Consumers below the restored
horizon must bootstrap after `RESYNC_REQUIRED`; never fabricate cursor,
heartbeat, or watermark values.
