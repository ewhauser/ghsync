# Deployment

## Artifact and supported topology

Production deploys one `ghsyncd` artifact in the following process
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

The budget lease defaults to a 30-second TTL and renews every 10 seconds. A
graceful shutdown drains admitted requests, saves the last budget snapshot, and
releases the lease, so the replacement can normally start immediately. If a
process or host disappears, the last confirmed TTL is the safety bound: wait
for the lease to expire rather than bypassing it. The 10-second renew cadence
leaves two renewal opportunities inside each TTL and makes a rolling restart a
bounded stop/start operation, not a replica-first overlap. Load verification may
use a shorter TTL to exercise failover without weakening its latency oracle, but
the renewal interval must remain shorter than the TTL.

The `metrics` role is the sole database aggregate collector. Other roles still
publish their process-local counters and `ghsync_c_o4_role_enabled`, but do
not repeat expensive cache/outbox aggregation on every scrape.

`serve --roles=all` remains a local/CI convenience, not the production rolling
topology. `fake-github` and `stream-tail` are operator/development
utilities.

## River queues and workers

The first three queues are request-priority classes. The remaining three are
component-owned maintenance queues. Worker counts are per started process and
are fixed defaults; they are intentionally not environment knobs.

| Queue | Family | Workers | Polling role | Work |
|---|---|---:|---|---|
| `interactive` | priority class | 4 | `fetch` | Operator/user backfill and cold-start work. |
| `event` | priority class | 8 | `fetch` | Webhook-originated refreshes. |
| `sweep` | priority class | 4 | `fetch` | Reconciliation-originated entity refreshes. |
| `reconcile` | component | 2 | `sweep` | Periodic sweep, gap-heal, and retention coordination. |
| `drift` | component | 1 | `drift` | Semantic drift detection and healing. |
| `pruner` | component | 1 | `pruner` | Payload/history and change-event pruning. |

The production `fetch,sweep,drift` process therefore polls `interactive`,
`event`, `sweep`, `reconcile`, and `drift`; the pruner polls only `pruner`.
Dispatch has a producer-only River client and polls no queue. Ingress,
watermarker, deriver, and metrics own no River queues.

## Configuration reference

Durations use Go syntax (`250ms`, `5m`, `24h`).

| Variable | Default | Required / meaning |
|---|---:|---|
| `DATABASE_URL` | none | Required; the one stateful Postgres dependency. In `rds-iam` mode, include one host and port, user, database, TLS, `search_path`, and other runtime parameters but configure no password source. |
| `DATABASE_AUTH` | `password` | Database authentication mode: `password` or `rds-iam`. IAM mode resolves the region and credentials through the default AWS SDK chain and generates a token for each new physical connection. |
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
| `FETCH_BATCH_WINDOW` | `5ms` | Collection window for a pull-request GraphQL gang. |
| `BACKFILL_PAGE_SIZE` | `100` | GitHub backfill page size; max `100`. |
| `BUDGET_SWEEP_FLOOR` | `0.20` | Fraction reserved from sweep traffic; strictly between zero and one. |
| `BUDGET_EVENT_FLOOR` | `0.10` | Fraction reserved from event traffic; must be lower than the sweep floor. |
| `BUDGET_MAX_CONCURRENT` | `40` | Installation-wide admitted GitHub request ceiling. |
| `BUDGET_REST_LIMIT` | `5000` | Initial REST denominator until GitHub headers are observed (public-GitHub baseline; GHEC's 15,000 is adopted from headers). |
| `BUDGET_GRAPHQL_LIMIT` | `5000` | Initial GraphQL denominator until GitHub rate data is observed. |
| `BUDGET_SECONDARY_FALLBACK` | `60s` | Backoff for a secondary limit with no valid `Retry-After`. |
| `BUDGET_LEASE_TTL` | `30s` | Budget singleton lease TTL and ungraceful failover bound. |
| `BUDGET_LEASE_RENEW_INTERVAL` | `10s` | Budget lease renewal cadence; must be shorter than the TTL. |
| `SWEEP_OPEN_STACK_MAX_STALENESS` | `5m` | C-R1 stack bound. |
| `SWEEP_OPEN_PR_MAX_STALENESS` | `10m` | C-R1 PR bound. |
| `SWEEP_REPO_RULES_MAX_STALENESS` | `1h` | C-R1 rules bound. |
| `SWEEP_CLOSED_MAX_STALENESS` | `24h` | C-R1 display-retained closed bound. |
| `SWEEP_REPOSITORY_LIST_PERIOD` | `1h` | Repository list period. |
| `SWEEP_PAGE_SIZE` | `100` | GitHub page size; max `100`. |
| `GAP_HEAL_PERIOD` | `5m` | C-R4 cadence. |
| `GAP_COMPARISON_WINDOW` | `6h` | Fixed delivery window. |
| `GAP_HEAL_LEASE_TTL` | `5m` | Stale-owner failover bound for gap healing. |
| `GAP_PAGE_SIZE` | `100` | Delivery page size; max `100`. |
| `GAP_MAX_PAGES` | `10` | Pages per resumable job. |
| `DRIFT_PERIOD` | `1h` | C-O3 cadence. |
| `DRIFT_SAMPLE_SIZE` | `10` | Must cover six classes. |
| `DRIFT_PAGE_SIZE` | `100` | Drift GitHub page size; max `100`, independent of sweeps. |
| `DRIFT_RESOLVED_RETENTION` | `720h` | Resolved evidence retention. |
| `RETENTION_PERIOD` | `24h` | Payload/history prune cadence. |
| `RETENTION_AGE` | `2160h` | Locked minimum 90 days. |
| `RETENTION_BATCH_SIZE` | `1000` | Rows per prune transaction. |
| `STREAM_WATERMARK_REFRESH` | `100ms` | C-S2 step interval. |
| `STREAM_WATERMARK_LEASE_TTL` | `3s` | Singleton lease TTL. |
| `STREAM_WATERMARK_FENCE_LOCK_TIMEOUT` | `1s` | Bounded wait for the exclusive writer fence. |
| `STREAM_RETENTION_PERIOD` | `1h` | Change-event prune cadence. |
| `STREAM_RETENTION_AGE` | `168h` | Locked minimum seven days. |
| `STREAM_RETENTION_BATCH_SIZE` | `1000` | Events per transaction. |
| `DERIVER_POLL_INTERVAL` | `500ms` | Dirty poll fallback. |
| `DERIVER_DIRTY_CAP` | `500` | C-P5 scopes per pass. |
| `OTEL_TRACES_EXPORTER` | disabled | Set to `otlp` to enable OTLP/HTTP trace export; `none` disables it. |
| `OTEL_SERVICE_NAME` | `ghsyncd` | Stable OpenTelemetry service name. |
| `OTEL_RESOURCE_ATTRIBUTES` | empty | Resource attributes such as the deployment environment. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | exporter default | Base OTLP/HTTP collector endpoint. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | exporter default | Optional trace-specific collector endpoint. |
| `OTEL_EXPORTER_OTLP_HEADERS` | empty | Collector authentication headers; manage as a secret. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | Head sampler; use `parentbased_traceidratio` in production. |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | Ratio for trace-ID ratio samplers. |

Pull-request GraphQL gangs collect for `FETCH_BATCH_WINDOW` and use a fixed
maximum batch size **K = 25**. K is constrained by the shipped GraphQL query and
is deliberately not configurable. `BACKFILL_PAGE_SIZE` independently controls
resumable REST pagination.

## Migration order

Before any new-version process starts:

```sh
ghsyncd migrate
```

The command takes an advisory lock, applies River migrations first, then
embedded ghsync files lexically. Each ghsync migration is one transaction
recorded with a SHA-256 checksum. Changed applied bytes fail closed. Never edit
an applied file; add a forward migration. The squashed baseline is `0001`
through `0003`; tracing adds the forward-only
`0004_webhook_trace_context.sql` migration. Every migration already present in
a target ledger is immutable.

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
ghsyncd backfill
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

1. Verify backup/PITR health and run `ghsyncd migrate` once.
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

## Postgres tuning and capacity

For Amazon RDS IAM database authentication, set `DATABASE_AUTH=rds-iam`, keep
all password sources unset (including `DATABASE_URL`, `PGPASSWORD`, and
password files), and configure the AWS SDK's region and credential sources
(for example `AWS_REGION` plus an instance, task, or pod role). A missing
region or unavailable startup credentials fails before the first database
connection. Keep TLS verification settings such as `sslmode=verify-full` and
all PostgreSQL runtime parameters in `DATABASE_URL`; ghsync changes only the
per-connection password. IAM mode supports one database endpoint because RDS
tokens are endpoint-specific. It applies to all ghsyncd database commands and
to `stream-tail`; loadgen accepts the equivalent `--database-auth=rds-iam`
setting for its assertion connection. `pkg/streamclient` accepts a pool and
does not create connections.

Wave A bounds the watermarker writer-fence wait with
`STREAM_WATERMARK_FENCE_LOCK_TIMEOUT` (default `1s`). A fence timeout is a
metered, retryable step rather than an unbounded global write barrier. Pooled
connections also default `idle_in_transaction_session_timeout` to `30s` so an
abandoned transaction cannot hold the shared writer fence forever. Override
that server runtime parameter only through `DATABASE_URL`, for example by
adding `idle_in_transaction_session_timeout=30s` as a connection-string query
parameter. Keep it positive and comfortably below application shutdown and
on-call detection bounds; disabling it restores the unbounded failure mode.

`stream_watermark` and `operation_heartbeats` are tiny tables with repeated
updates to a small hot row set. Their percentage-based autovacuum thresholds
are ineffective at that size. Apply small absolute thresholds and confirm the
values with the database team for the observed write rate:

```sql
ALTER TABLE stream_watermark SET (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 50
);
ALTER TABLE operation_heartbeats SET (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 50,
  autovacuum_analyze_scale_factor = 0,
  autovacuum_analyze_threshold = 50
);

SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
FROM pg_stat_user_tables
WHERE relname IN ('stream_watermark', 'operation_heartbeats');
```

Size `change_events` from production measurements, not payload intuition. The
minimum live window is seven days, so a first-order reservation is:

```text
seven-day rows × observed total bytes per row (table, TOAST, and indexes)
+ vacuum/compaction headroom
+ at least 100,000 retention-eligible backlog rows
```

Measure the total table-plus-index footprint and the current window directly:

```sql
SELECT count(*) AS rows,
       pg_total_relation_size('change_events') AS total_bytes,
       pg_total_relation_size('change_events')
         / GREATEST(count(*), 1) AS total_bytes_per_row,
       min(occurred_at) AS oldest,
       max(occurred_at) AS newest
FROM change_events;
```

The `GhsyncOutboxBacklog` warning fires when more than 100,000 rows are
already retention-eligible for 15 minutes. That threshold is a pruning-health
signal, not spare capacity: disk planning must accommodate the full seven-day
window plus that backlog and operational headroom. Alert separately on database
free space early enough to act before the outbox warning and autovacuum need
the same remaining disk.

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
