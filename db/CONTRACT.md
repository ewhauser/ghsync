# Frontier Postgres delivery contract

Contract version: **v1**, introduced by migration `0013`, extended
additively by migrations `0014`–`0019` and `0021`, and hardened by the
database-enforced writer fence in migration `0024`.

Postgres is the Frontier sync engine’s public delivery interface. Consumers
read snapshot-consistent cache rows and follow reference events through
`pkg/streamclient`. The Go package is the reference implementation; consumers
must not reproduce the watermark, cursor, retention, or resync algorithms.
Direct Postgres access is the **only v1 transport**. The gRPC and SSE surfaces
in `docs/API_SPEC.md` belong to a future API project and are not implemented
by the sync engine.

## Consumer facts

The suggested database role is `frontier_consumer`. Run this block once as a
role that may create roles and grant access, then grant `frontier_consumer` to
each application login that runs `pkg/streamclient`:

```sql
CREATE ROLE frontier_consumer NOLOGIN;

GRANT USAGE ON SCHEMA public TO frontier_consumer;

GRANT SELECT ON TABLE
    repos,
    repo_rules,
    stacks,
    pull_requests,
    review_threads,
    check_runs,
    check_history,
    work_items,
    change_events,
    stream_watermark,
    stream_horizons
TO frontier_consumer;

GRANT SELECT, INSERT, UPDATE ON TABLE consumer_cursors
TO frontier_consumer;
```

Run exactly **one tailer per `(consumer, stream)`**. Two processes must not
share that pair; elect one tailer and fan out locally, or use distinct consumer
names. `pkg/streamclient` locks the cursor row in a `REPEATABLE READ`
transaction so handler effects and cursor advancement remain atomic.
Concurrent tailers for the same pair therefore surface PostgreSQL
serialization errors by design.

Event order is `seq` alone. `occurred_at` is transaction-start time and is
**not** monotonic with `seq`: a long writer can commit a high sequence with an
older `occurred_at` than a faster writer. Never order, deduplicate, or resume
by `occurred_at`. Retention intentionally prunes by `occurred_at`; when that
time order differs from sequence order, `stream_horizons.pruned_through_seq`
may force a resync earlier than the remaining rows alone would require. That
is the conservative direction: it can cause an extra snapshot, never a silent
gap.

## Public v1 schema

The manifest below is normative and is checked against a freshly migrated
Postgres schema. `Nullable` describes SQL nullability, not whether a text or
JSON value may be empty.

<!-- v1-schema:start -->
| Table | Column | PostgreSQL type | Nullable | Key or join role |
| --- | --- | --- | --- | --- |
| `repos` | `id` | `bigint` | no | primary key; local join key |
| `repos` | `installation_id` | `bigint` | no | external scope key |
| `repos` | `org_id` | `bigint` | no | organization filter |
| `repos` | `gh_id` | `bigint` | no | unique GitHub repository identity |
| `repos` | `node_id` | `text` | no | GitHub GraphQL identity |
| `repos` | `owner` | `text` | no | current owner |
| `repos` | `name` | `text` | no | current repository name |
| `repos` | `full_name` | `text` | no | current owner/name |
| `repos` | `default_branch` | `text` | no | — |
| `repos` | `archived` | `boolean` | no | — |
| `repos` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `repos` | `head_sha` | `text` | no | default-branch head |
| `repos` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `repos` | `etag` | `text` | no | HTTP validator |
| `repos` | `sync_source` | `text` | no | provenance enum |
| `repos` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `repos` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `repo_rules` | `repo_id` | `bigint` | no | primary key part; references repos.id |
| `repo_rules` | `rule_key` | `text` | no | primary key part |
| `repo_rules` | `rule` | `jsonb` | no | current rule document |
| `repo_rules` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `repo_rules` | `head_sha` | `text` | no | repository head used for observation |
| `repo_rules` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `repo_rules` | `etag` | `text` | no | HTTP validator |
| `repo_rules` | `sync_source` | `text` | no | provenance enum |
| `repo_rules` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `repo_rules` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `stacks` | `id` | `bigint` | no | primary key; local join key |
| `stacks` | `repo_id` | `bigint` | no | references repos.id; unique with number |
| `stacks` | `gh_id` | `bigint` | yes | GitHub stack identity where supplied |
| `stacks` | `node_id` | `text` | no | GitHub GraphQL identity where supplied |
| `stacks` | `number` | `integer` | no | repository-local stack number |
| `stacks` | `base_ref` | `text` | no | — |
| `stacks` | `base_sha` | `text` | no | — |
| `stacks` | `open` | `boolean` | no | — |
| `stacks` | `entries` | `jsonb` | no | ordered stack-entry references |
| `stacks` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `stacks` | `head_sha` | `text` | no | stack head |
| `stacks` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `stacks` | `etag` | `text` | no | HTTP validator |
| `stacks` | `sync_source` | `text` | no | provenance enum |
| `stacks` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `stacks` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `stacks` | `display_until` | `timestamp with time zone` | yes | closed-row display-retention boundary |
| `pull_requests` | `id` | `bigint` | no | primary key; local join key |
| `pull_requests` | `repo_id` | `bigint` | no | references repos.id; unique with number |
| `pull_requests` | `gh_id` | `bigint` | yes | GitHub pull-request identity |
| `pull_requests` | `node_id` | `text` | no | GitHub GraphQL identity |
| `pull_requests` | `number` | `integer` | no | repository-local PR number |
| `pull_requests` | `title` | `text` | no | — |
| `pull_requests` | `state` | `text` | no | — |
| `pull_requests` | `draft` | `boolean` | no | — |
| `pull_requests` | `author_login` | `text` | no | — |
| `pull_requests` | `head_ref` | `text` | no | — |
| `pull_requests` | `head_sha` | `text` | no | check-run join input |
| `pull_requests` | `base_ref` | `text` | no | — |
| `pull_requests` | `base_sha` | `text` | no | — |
| `pull_requests` | `review_decision` | `text` | no | — |
| `pull_requests` | `mergeable_state` | `text` | no | — |
| `pull_requests` | `stack_number` | `integer` | yes | null means loose PR |
| `pull_requests` | `stack_position` | `integer` | yes | null exactly when stack_number is null |
| `pull_requests` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `pull_requests` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_requests` | `etag` | `text` | no | HTTP validator |
| `pull_requests` | `sync_source` | `text` | no | provenance enum |
| `pull_requests` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `pull_requests` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_requests` | `display_until` | `timestamp with time zone` | yes | closed-row display-retention boundary |
| `review_threads` | `id` | `text` | no | primary key; GitHub thread node ID |
| `review_threads` | `repo_id` | `bigint` | no | references repos.id |
| `review_threads` | `pr_number` | `integer` | no | references pull_requests(repo_id,number) |
| `review_threads` | `is_resolved` | `boolean` | no | — |
| `review_threads` | `is_outdated` | `boolean` | no | — |
| `review_threads` | `path` | `text` | no | — |
| `review_threads` | `line` | `integer` | yes | — |
| `review_threads` | `comments` | `jsonb` | no | ordered comment references |
| `review_threads` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `review_threads` | `head_sha` | `text` | no | observed PR head |
| `review_threads` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `review_threads` | `etag` | `text` | no | HTTP validator |
| `review_threads` | `sync_source` | `text` | no | provenance enum |
| `review_threads` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `review_threads` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `check_runs` | `gh_id` | `bigint` | no | primary key; GitHub check-run identity |
| `check_runs` | `repo_id` | `bigint` | no | references repos.id |
| `check_runs` | `node_id` | `text` | no | GitHub GraphQL identity |
| `check_runs` | `name` | `text` | no | — |
| `check_runs` | `status` | `text` | no | — |
| `check_runs` | `conclusion` | `text` | no | — |
| `check_runs` | `details_url` | `text` | no | — |
| `check_runs` | `app_slug` | `text` | no | — |
| `check_runs` | `started_at` | `timestamp with time zone` | yes | — |
| `check_runs` | `completed_at` | `timestamp with time zone` | yes | — |
| `check_runs` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `check_runs` | `head_sha` | `text` | no | PR lookup input |
| `check_runs` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `check_runs` | `etag` | `text` | no | HTTP validator |
| `check_runs` | `sync_source` | `text` | no | provenance enum |
| `check_runs` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `check_runs` | `semantic_version` | `text` | no | write-if-newer input |
| `check_runs` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `check_history` | `id` | `bigint` | no | primary key |
| `check_history` | `check_run_gh_id` | `bigint` | no | references check_runs.gh_id |
| `check_history` | `repo_id` | `bigint` | no | references repos.id |
| `check_history` | `name` | `text` | no | — |
| `check_history` | `status` | `text` | no | — |
| `check_history` | `conclusion` | `text` | no | — |
| `check_history` | `observed` | `jsonb` | no | accepted check observation |
| `check_history` | `gh_updated_at` | `timestamp with time zone` | yes | source version |
| `check_history` | `head_sha` | `text` | no | — |
| `check_history` | `synced_at` | `timestamp with time zone` | no | accepted transition time |
| `check_history` | `etag` | `text` | no | HTTP validator |
| `check_history` | `sync_source` | `text` | no | provenance enum |
| `check_history` | `tombstoned_at` | `timestamp with time zone` | yes | retained provenance |
| `check_history` | `semantic_version` | `text` | no | accepted semantic version |
| `work_items` | `identity_key` | `text` | no | primary key; stable domain identity |
| `work_items` | `org_id` | `bigint` | no | organization filter |
| `work_items` | `payload` | `jsonb` | no | current derived value |
| `work_items` | `updated_at` | `timestamp with time zone` | no | materialization change time |
| `work_items` | `scope_key` | `text` | no | owning C-D2 derivation scope |
| `change_events` | `seq` | `bigint` | no | primary key; global event order |
| `change_events` | `stream` | `text` | no | entities or work_items |
| `change_events` | `kind` | `text` | no | event variant below |
| `change_events` | `entity_key` | `text` | no | immutable lookup reference |
| `change_events` | `occurred_at` | `timestamp with time zone` | no | source transaction event time |
| `change_events` | `payload` | `jsonb` | no | versioned reference payload |
| `stream_watermark` | `singleton` | `boolean` | no | primary key; always true |
| `stream_watermark` | `safe_seq` | `bigint` | no | greatest safe consumer sequence |
| `stream_watermark` | `updated_at` | `timestamp with time zone` | no | last publication time |
| `consumer_cursors` | `consumer` | `text` | no | primary key part |
| `consumer_cursors` | `stream` | `text` | no | primary key part |
| `consumer_cursors` | `seq` | `bigint` | no | last transactionally applied event |
| `consumer_cursors` | `updated_at` | `timestamp with time zone` | no | last cursor change |
| `consumer_cursors` | `resync_count` | `bigint` | no | monotonic C-S4 resync count |
| `consumer_cursors` | `last_resync_at` | `timestamp with time zone` | yes | latest RESYNC_REQUIRED time |
| `stream_horizons` | `stream` | `text` | no | primary key |
| `stream_horizons` | `pruned_through_seq` | `bigint` | no | greatest removed sequence |
| `stream_horizons` | `updated_at` | `timestamp with time zone` | no | last horizon change |
<!-- v1-schema:end -->

Use a `REPEATABLE READ` transaction when rows from more than one public table
must agree. Foreign keys use local surrogate IDs; durable external references
use GitHub IDs plus repository-local numbers.

### Provenance, freshness, and tombstones

`sync_source` is one of `webhook`, `reconcile`, `backfill`, or `manual`.
`synced_at` records the last accepted domain change; `last_checked_at` records
the last authoritative validation, including 304 and identical responses. Do
not use `synced_at` as the freshness check after an unchanged response.

A non-null `tombstoned_at` means a mirror row is retained history and is not
live. Normal live reads include `tombstoned_at IS NULL`. A later authoritative
observation may resurrect the row through the monotonic writer.
For closed stacks and pull requests, `display_until > clock_timestamp()`
identifies the retained subset still eligible for display and therefore still
covered by the closed-entity C-R1 validation bound.
`check_history` is append-only transition history retained for at least 90
days. Other tombstoned mirror skeletons have no v1 expiry.

## Event-kind and entity-key grammar

This manifest is normative and schema-tested against the constants and key
constructors used by the entity writer and deriver.

<!-- v1-events:start -->
| Stream | Kind | Entity-key grammar | Public lookup target | Required payload |
| --- | --- | --- | --- | --- |
| `entities` | `repository.changed` | `repo:{installation_id}:{repo_gh_id}` | `repos(installation_id,gh_id)` | `{"version":1}` |
| `entities` | `repository.tombstoned` | `repo:{installation_id}:{repo_gh_id}` | `repos(installation_id,gh_id)` | `{"version":1}` |
| `entities` | `pull_request.changed` | `pr:{installation_id}:{repo_gh_id}:{pr_number}` | `pull_requests(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `pull_request.tombstoned` | `pr:{installation_id}:{repo_gh_id}:{pr_number}` | `pull_requests(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `stack.changed` | `stack:{installation_id}:{repo_gh_id}:{stack_number}` | `stacks(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `stack.tombstoned` | `stack:{installation_id}:{repo_gh_id}:{stack_number}` | `stacks(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `checks.changed` | `checks:{installation_id}:{repo_gh_id}:{head_sha}` | `check_runs(repos.installation_id,repos.gh_id,head_sha)` | `{"version":1}` |
| `entities` | `repo_rules.changed` | `repo_rules:{installation_id}:{repo_gh_id}` | `repo_rules(repos.installation_id,repos.gh_id)` | `{"version":1}` |
| `work_items` | `work_item.changed` | `repo:{repo_gh_id}:{work_item_kind}:{number}` | `work_items(identity_key)` | `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}` |
| `work_items` | `work_item.removed` | `repo:{repo_gh_id}:{work_item_kind}:{number}` | `work_items(identity_key), absent after removal` | `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}` |
<!-- v1-events:end -->

Every braced numeric field is a positive base-10 integer with no delimiter.
`head_sha` is the authoritative Git object ID. `work_item_kind` is exactly
`stack` or `pr`. The owning scopes are
`stack:{installation_id}:{repo_gh_id}:{stack_number}` and
`pr:{installation_id}:{repo_gh_id}:{pr_number}`. A deriver must return exactly
one complete result set for every claimed scope; an empty result removes that
scope’s prior work item and emits `work_item.removed`.

Payloads are references, never row images or patches. The JSON shown above is
the complete required v1 shape; additive fields are allowed. Consumers fetch
current state from the lookup target, ignore unknown kinds and fields, and use
`seq` as the idempotency key for external effects.

### Internal key grammars and the shared advisory-lock namespace

Three key namespaces coexist. They are not interchangeable even where two
forms happen to contain the same text:

1. **Public v1 event/entity keys.** `internal/outbox` constructors, called by
   the entity writer and deriver, produce immutable GitHub-identity keys. The
   numbered cache-entity form is
   `{kind}:{installation_id}:{repo_gh_id}:{number}`; repository and repository
   rules omit the final number, while checks use `head_sha` there. The
   `v1-events` manifest above is the exact public grammar, including the
   separate work-item identity forms. These values are stable across repository
   rename.
2. **Internal refresh/job keys.** Dispatcher classification and
   fetch/backfill/sweep producers create path-addressed pointers consumed by
   `internal/queue` and parsed by `internal/fetch`:
   `{kind}:{owner}/{name}:{number_or_value}`. Concrete forms are
   `repo:{owner}/{name}:metadata`, `repo_rules:{owner}/{name}:rules`,
   `pr:{owner}/{name}:{number}`, `stack:{owner}/{name}:{number}`,
   `checks:{owner}/{name}:{head_sha}`, and
   `branch:{owner}/{name}:{branch}`. They are private, rename-sensitive work
   pointers, not public event identities.
3. **Drift-detector fetch and lock keys.** The SQL `drift_entities` view
   produces a path-addressed `entity_key` for the authoritative refresh and a
   separate immutable `lock_key` for serialization. Its fetch forms are
   `repo:{owner}/{name}:metadata`, `pr:{owner}/{name}:{number}`,
   `stack:{owner}/{name}:{number}`, `repo_rules:{owner}/{name}:rules`,
   `review_threads:{owner}/{name}:{number}`, and
   `checks:{owner}/{name}:{head_sha}`. Its lock forms are
   `repo:{installation_id}:{repo_gh_id}`,
   `repo_rules:{installation_id}:{repo_gh_id}`,
   `pr:{installation_id}:{repo_gh_id}:{number}` (also used for review
   threads), `stack:{installation_id}:{repo_gh_id}:{number}`, and
   `checks:{installation_id}:{repo_gh_id}:{head_sha}`.

All entity observation and transaction locks share one PostgreSQL advisory-lock
keyspace, derived with `hashtextextended(key, 0)`. Drift `lock_key` values and
the entity-writer keys therefore coordinate with each other.
`store.RepositoryDiscoveryKey` also mints
`repo-discovery:{installation_id}:{owner}/{name}` into this same keyspace while
an as-yet-unknown repository is fetched. Do not introduce a new producer or
change a grammar without checking every producer that participates in this
shared lock space.

## Writer fence and visibility watermark

Every transaction that inserts `change_events` has a mandatory obligation:
before allocating its first sequence, it calls the shared internal helper that
takes:

```sql
SELECT pg_advisory_xact_lock_shared(5076242250190120306);
```

The value is the ASCII bytes for `Frontier` interpreted as a signed `BIGINT`;
clients pass it as a parameter. All internal entity-writer and deriver paths use
`internal/outbox.AcquireWriterFence`. Migration `0024` adds a
`BEFORE INSERT` trigger that rejects any `change_events` write whose backend
does not already hold this shared fence, so the obligation is enforced by
Postgres rather than convention.

The watermarker briefly takes the exclusive transaction lock on that same key.
Once acquired, every earlier participating writer has committed or rolled
back, and no new writer can allocate a sequence. In that transaction the
watermarker publishes `max(change_events.seq)` and immediately commits,
releasing the fence. Unrelated transactions and read-only or Bootstrap
snapshots never take the fence and cannot stall watermark progress.
The exclusive attempt is a bounded global write barrier: a local
`lock_timeout` makes a busy fence a retryable, metered watermarker outcome,
and pooled connections set `idle_in_transaction_session_timeout` so an
abandoned writer cannot hold the shared side forever.

`change_events.outbox_txid` and
`stream_watermark.{candidate_seq,candidate_xid,lease_token,lease_until}` are
private compatibility/coordination columns. The candidate/XID columns are
retained from applied migration `0013` but are no longer the safety proof.

## Cursor paging and snapshot then stream

For `(consumer, stream)`, `consumer_cursors.seq` is the last event whose
handler effects committed. `resync_count` and `last_resync_at` are operational
evidence updated when the client detects a cursor below the pruned horizon;
they do not change cursor semantics. A page transaction uses `REPEATABLE READ`, locks
the cursor row, checks the pruned horizon, reads `safe_seq`, and selects:

```sql
SELECT seq, stream, kind, entity_key, occurred_at, payload
FROM change_events
WHERE stream = $stream
  AND seq > $cursor
  AND seq <= $safe_seq
ORDER BY seq
LIMIT $batch_size;
```

Horizon validation and the event selection must share that one snapshot so
retention cannot delete unseen events between them. Handler database effects
and cursor advancement commit in the same transaction. A rollback applies
neither. External I/O requires its own `seq`-keyed idempotency.

Bootstrap begins `REPEATABLE READ`, locks the cursor, reads watermark **W**,
replaces the consumer projection from public cache rows, sets the cursor to
**W**, and commits. Tailing resumes at `seq > W`. The snapshot can already
contain state referenced by a later event, so consumers apply by stable key,
not as patches.

`LISTEN frontier_change_events` and its statement-level constant-payload
notification lower latency only. `pkg/streamclient` continues polling during a
listener failure and reconnects with bounded backoff.

## Retention and RESYNC_REQUIRED

Events are retained for at least seven days. Pruning is bounded, independent
of consumer cursors, and restricted to `seq <= stream_watermark.safe_seq`.
Each delete transaction advances the affected `stream_horizons` rows, so
`pruned_through_seq` can never exceed the safe watermark.

Before delivery:

- `cursor >= pruned_through_seq`: continue normally.
- `cursor < pruned_through_seq`: return `RESYNC_REQUIRED`.

There is no silent gap and no fallback to the oldest remaining row. On
`streamclient.ErrResyncRequired`, replace the projection through Bootstrap and
resume Tail.

## Evolution and private state

Public v1 evolution is additive: columns, streams, event kinds, and JSON fields
may be added; existing names, types, meanings, and identity grammars are not
changed or removed. A breaking change requires a versioned replacement and a
migration period.

All unlisted tables are private, including webhook, budget, queue, alias,
backfill, sweep, delivery-gap, drift, and `derivation_dirty` state, River
tables, and `schema_migrations`. Private state has no consumer compatibility
guarantee.
