# Frontier Postgres delivery contract

Contract version: **v1**, introduced by migration `0013`.

Postgres is the Frontier sync engine’s public delivery interface. Consumers
read snapshot-consistent cache rows and follow reference events through
`pkg/streamclient`. The Go package is the reference implementation; consumers
should not reproduce the watermark, cursor, or resync algorithms themselves.

## Public read model

The following tables are public, additive-only read surfaces:

| Table | Meaning |
| --- | --- |
| `repos` | Installed GitHub repositories. |
| `repo_rules` | Repository rules keyed by `(repo_id, rule_key)`. |
| `stacks` | GitHub stack snapshots keyed by repository and stack number. |
| `pull_requests` | Pull request snapshots, including stack membership. |
| `review_threads` | Review-thread snapshots keyed by GitHub node ID. |
| `check_runs` | Current check-run snapshots. |
| `check_history` | Accepted historical check transitions; retained for at least 90 days. |
| `work_items` | Minimal derived output keyed by stable `identity_key`. M5’s default no-op deriver leaves it empty. |

Use an explicit transaction when rows from more than one table must agree.
`REPEATABLE READ` gives one cache snapshot. Foreign keys use Frontier’s local
surrogate IDs for joins; durable external references use GitHub IDs plus
repository/PR/stack numbers.

### Provenance and freshness

Mirror tables carry:

- `synced_at`: when the stored domain snapshot last changed.
- `last_checked_at`: when GitHub last authoritatively validated the entity,
  including an unchanged response. `check_history` is append-only history and
  does not use this column.
- `etag`: the last authoritative HTTP validator where one exists.
- `sync_source`: `webhook`, `reconcile`, `backfill`, or `manual`.
- `gh_updated_at`, `head_sha`, and/or `semantic_version`: monotonic
  write-if-newer inputs appropriate to that entity.

Do not interpret `synced_at` as a freshness check after a 304. Use
`last_checked_at`.

### Tombstones

Synced entities are not hard-deleted on the write path. A non-null
`tombstoned_at` means the entity is retained history and is not live. Normal
live queries must include `tombstoned_at IS NULL`. A later authoritative
observation can resurrect a tombstone through the same monotonic writer.

`check_history` is separately retention-bound. Tombstoned mirror skeletons are
not part of stream retention and remain available until a future, explicitly
documented retention policy says otherwise.

## Change-event envelope

`change_events` is the append-only C-S1/C-S6 outbox:

```text
seq          BIGINT       global total order
stream       TEXT         entities | work_items (unknown future values allowed)
kind         TEXT         additive event variant
entity_key   TEXT         immutable entity reference
occurred_at  TIMESTAMPTZ  source transaction event time
payload      JSONB        versioned reference metadata
```

`payload` is a reference, not a row image. Version 1 payloads contain at least
`{"version": 1}` and may repeat the stable identity. On an `entities` event,
fetch current state from the public read model. Consumers must ignore unknown
kinds, streams, and payload fields.

`outbox_txid` is internal watermark evidence and is not part of the public
envelope. Events are inserted in the same transaction as the corresponding
cache or work-item mutation.

The database trigger `change_events_notify` calls
`pg_notify('frontier_change_events', stream)` after inserts. PostgreSQL
delivers that notification only after commit. It is a wake-up optimization:
polling by sequence is always the correctness path.

## Watermark, cursor, and delivery rules

`stream_watermark.safe_seq` is the greatest sequence below which no older
in-flight outbox transaction can later reveal an unseen row. Its candidate and
lease columns are private maintenance state.

For `(consumer, stream)`, `consumer_cursors.seq` is the last event whose
handler effects committed. A correct page is always:

```sql
SELECT seq, stream, kind, entity_key, occurred_at, payload
FROM change_events
WHERE stream = $stream
  AND seq > $cursor
  AND seq <= $safe_seq
ORDER BY seq
LIMIT $batch_size;
```

The consumer cursor row is locked while applying a page. Handler database
effects and the cursor advance commit in one transaction. If the transaction
fails or the process crashes, neither commits and the page is delivered again.
This is exactly-once **per durable cursor for transactional database effects**.
External I/O needs its own idempotency key, normally `seq`.

Consumers must never:

- read above `safe_seq`;
- derive safety from `max(seq)` or the sequence’s `last_value`;
- advance a cursor before handler effects commit;
- rely on `LISTEN/NOTIFY` without polling;
- delete change events based on cursor positions.

## Snapshot then stream

Bootstrap is:

1. Begin a repeatable-read transaction.
2. Lock `(consumer, stream)` in `consumer_cursors`.
3. Read `stream_watermark.safe_seq` as **W**.
4. Read the cache snapshot and replace the consumer’s projection.
5. Set the cursor to **W** and commit the snapshot and cursor together.
6. Tail events with `seq > W`, bounded by subsequent safe watermarks.

`streamclient.Client.Bootstrap` performs steps 1–3 and stages the cursor
update; the caller reads through the returned transaction and commits it.
`streamclient.Client.Tail` performs the remaining cursor-safe paging.

The watermark can conservatively lag cache visibility. Therefore a reference
event after W can point at state already present in the snapshot. Applying
events by stable entity reference and sequence is required; event payloads are
not patches.

## Retention and RESYNC_REQUIRED

Change events are retained for **at least seven days**, configurable upward.
Pruning uses bounded batches and never waits for consumers. In the same
transaction as each delete, `stream_horizons.pruned_through_seq` records the
greatest removed sequence for each affected stream.

Before delivering, compare the cursor with that stream’s pruned horizon:

- `cursor >= pruned_through_seq`: normal tailing may continue.
- `cursor < pruned_through_seq`: stop and return `RESYNC_REQUIRED`.

There is no silent gap and no “start from the oldest remaining row” fallback.
On `streamclient.ErrResyncRequired`, discard or replace the local projection,
run `Bootstrap`, commit the new snapshot, and resume `Tail`.

`stream_horizons` is public protocol state but consumers should normally reach
it only through `streamclient`.

## Evolution policy

Public v1 surfaces evolve additively:

- columns, event kinds, streams, and JSON fields may be added;
- existing meanings, identities, or types are not changed in place;
- public columns and tables are not renamed or removed;
- event payloads remain references rather than internal row images;
- consumers ignore fields and variants they do not understand.

A breaking change requires a new versioned surface and a migration period.

## Private engine tables

All other tables and views are implementation details. This includes
`webhook_deliveries`, `installation_budgets`,
`refresh_intent_generations`, `repo_aliases`, `repo_rule_sync_state`,
`derivation_dirty`, every backfill/sweep/gap/drift cursor or finding table,
`drift_entities`, River-owned tables, and `schema_migrations`.

Private tables may change without consumer compatibility guarantees.
`stream_watermark.candidate_*`, its lease fields, and
`change_events.outbox_txid` are likewise private columns on otherwise public
protocol tables.
