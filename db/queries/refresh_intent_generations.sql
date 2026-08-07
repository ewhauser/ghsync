-- name: TryAcquireRefreshIntentGenerationLocks :one
-- Producers acquire these transaction locks one key at a time in sorted order.
-- A failed try-lock is rolled back with its transaction or containing
-- savepoint before it takes any refresh-generation row locks. Include the
-- database in the hash because advisory locks are cluster-wide and tests use
-- parallel per-test databases.
WITH RECURSIVE generation_keys AS MATERIALIZED (
    SELECT
        row_number() OVER (ORDER BY kind, refresh_key) AS lock_index,
        kind,
        refresh_key
    FROM (
        SELECT DISTINCT
            element->>'kind' AS kind,
            element->>'refresh_key' AS refresh_key
        FROM jsonb_array_elements(sqlc.arg(keys)::jsonb) AS element
    ) AS parsed
), generation_locks AS (
    SELECT
        lock_index,
        pg_try_advisory_xact_lock(hashtextextended(
            current_database() || chr(31) || kind || chr(31) || refresh_key,
            0
        )) AS acquired
    FROM generation_keys
    WHERE lock_index = 1

    UNION ALL

    SELECT
        next_key.lock_index,
        pg_try_advisory_xact_lock(hashtextextended(
            current_database() || chr(31) || next_key.kind || chr(31) ||
                next_key.refresh_key,
            0
        )) AS acquired
    FROM generation_locks AS prior_lock
    JOIN generation_keys AS next_key
      ON next_key.lock_index = prior_lock.lock_index + 1
    WHERE prior_lock.acquired
), lock_summary AS (
    SELECT count(*) AS key_count
    FROM generation_keys
)
SELECT
    CASE
        WHEN COALESCE((
            SELECT bool_and(acquired)
            FROM generation_locks
        ), true)
          AND (SELECT count(*) FROM generation_locks) = lock_summary.key_count
        THEN true
        ELSE false
    END AS acquired,
    COALESCE((
        SELECT count(*) FILTER (WHERE acquired)
        FROM generation_locks
    ), 0::bigint)::bigint AS acquired_count,
    lock_summary.key_count
FROM lock_summary;

-- name: TryAcquireRefreshIntentGenerationRowLocks :one
-- Advisory locks serialize participating producers, but an already-open
-- transaction may hold only the generation row. Probe every existing row
-- without waiting so a hot row cannot make this transaction retain unrelated
-- generation locks.
WITH generation_keys AS MATERIALIZED (
    SELECT DISTINCT
        element->>'kind' AS kind,
        element->>'refresh_key' AS refresh_key
    FROM jsonb_array_elements(sqlc.arg(keys)::jsonb) AS element
), locked_generation_rows AS MATERIALIZED (
    SELECT generation.kind, generation.refresh_key
    FROM refresh_intent_generations AS generation
    JOIN generation_keys AS requested
      ON requested.kind = generation.kind
     AND requested.refresh_key = generation.refresh_key
    ORDER BY generation.kind, generation.refresh_key
    FOR UPDATE OF generation SKIP LOCKED
), lock_summary AS (
    SELECT
        (SELECT count(*) FROM generation_keys) AS key_count,
        (
            SELECT count(*)
            FROM refresh_intent_generations AS generation
            JOIN generation_keys AS requested
              ON requested.kind = generation.kind
             AND requested.refresh_key = generation.refresh_key
        ) AS existing_count,
        (SELECT count(*) FROM locked_generation_rows) AS locked_count
)
SELECT
    locked_count = existing_count AS acquired,
    (key_count - existing_count + locked_count)::bigint AS acquired_count,
    key_count
FROM lock_summary;

-- name: AcquireRefreshIntentGenerationLock :exec
-- Completion has only one generation key, so it may wait without convoying
-- unrelated keys. It takes this lock before the corresponding row lock.
SELECT pg_advisory_xact_lock(hashtextextended(
    current_database() || chr(31) || sqlc.arg(kind)::text || chr(31) ||
        sqlc.arg(refresh_key)::text,
    0
));

-- name: BumpRefreshIntentGenerations :many
-- Bump each coalesced key once in the same transaction that inserts its River
-- job. Production callers first acquire the transaction advisory lock for
-- every key; this ordered upsert remains a second deadlock-avoidance layer.
-- Workers snapshot it when they begin authoritative refresh work.
WITH intents AS (
    SELECT DISTINCT
        element->>'kind' AS kind,
        element->>'refresh_key' AS refresh_key,
        NULLIF(element->>'deadline_at', '')::timestamptz AS deadline_at,
        NULLIF(element->>'event_received_at', '')::timestamptz
            AS event_received_at
    FROM jsonb_array_elements(sqlc.arg(intents)::jsonb) AS element
    -- Upsert conflict-row locks must be acquired in the same order everywhere.
    ORDER BY kind, refresh_key
)
INSERT INTO refresh_intent_generations (
    kind, refresh_key, generation, deadline_at, event_received_at
)
SELECT kind, refresh_key, 1, deadline_at, event_received_at
FROM intents
ON CONFLICT (kind, refresh_key) DO UPDATE
SET generation = refresh_intent_generations.generation + 1,
    deadline_at = CASE
        WHEN EXCLUDED.deadline_at IS NULL
        THEN refresh_intent_generations.deadline_at
        WHEN refresh_intent_generations.deadline_at IS NULL
        THEN EXCLUDED.deadline_at
        ELSE LEAST(
            refresh_intent_generations.deadline_at,
            EXCLUDED.deadline_at
        )
    END,
    event_received_at = CASE
        WHEN EXCLUDED.event_received_at IS NULL
        THEN refresh_intent_generations.event_received_at
        WHEN refresh_intent_generations.event_received_at IS NULL
        THEN EXCLUDED.event_received_at
        ELSE LEAST(
            refresh_intent_generations.event_received_at,
            EXCLUDED.event_received_at
        )
    END,
    updated_at = now()
RETURNING kind, refresh_key, generation, deadline_at, event_received_at;

-- name: GetRefreshIntentState :one
SELECT generation, completed_generation, deadline_at, event_received_at
FROM refresh_intent_generations
WHERE kind = $1 AND refresh_key = $2;

-- name: GetRefreshIntentGenerationForShare :one
-- Post-fetch writers hold a shared row lock through their short entity
-- transaction. Producers either finish their generation bump first or wait
-- until this write commits; no advisory lock is retained across hooks.
SELECT generation
FROM refresh_intent_generations
WHERE kind = sqlc.arg(kind)
  AND refresh_key = sqlc.arg(refresh_key)
FOR SHARE;

-- name: GetRefreshIntentStateForShare :one
SELECT generation, completed_generation
FROM refresh_intent_generations
WHERE kind = sqlc.arg(kind)
  AND refresh_key = sqlc.arg(refresh_key)
FOR SHARE;

-- name: GetRefreshIntentGenerationForUpdate :one
-- Workers lock the generation before transactionally completing their River
-- job. A concurrent dispatcher therefore bumps either before the recheck or
-- after completion, when it can insert a fresh job.
SELECT generation
FROM refresh_intent_generations
WHERE kind = $1 AND refresh_key = $2
FOR UPDATE;

-- name: ListOutstandingRefreshIntentGenerations :many
-- Reconciliation input for orphaned pointer jobs (issue #61). An outstanding
-- generation whose unique River pointer reached a terminal state has no live
-- work left to complete it. Producers and completion both bump updated_at, so
-- a freshly signalled row is never scanned; a merely delayed row is harmless
-- because the reconciler's unique re-insert deduplicates against its live
-- pointer. Branch bulk targets self-claim their generations
-- (completed_generation = generation) and therefore never appear here.
SELECT kind, refresh_key, generation, completed_generation, updated_at
FROM refresh_intent_generations
WHERE generation > completed_generation
  AND updated_at <= sqlc.arg(stale_before)
ORDER BY updated_at, kind, refresh_key
LIMIT sqlc.arg(batch_limit);

-- name: GetLatestTerminalRefreshJobState :one
-- Observability for a replaced orphan pointer: the most recent terminal job
-- for the refresh identity explains how the previous pointer died. River may
-- have pruned it already, so callers treat no-rows as unknown.
SELECT state::text AS state
FROM river_job
WHERE kind = sqlc.arg(kind)::text
  AND args->>'key' = sqlc.arg(refresh_key)::text
  AND state IN ('cancelled', 'completed', 'discarded')
ORDER BY id DESC
LIMIT 1;

-- name: CompleteRefreshIntentGeneration :exec
UPDATE refresh_intent_generations
SET completed_generation = GREATEST(
        completed_generation,
        sqlc.arg(completed_generation)
    ),
    deadline_at = CASE
        WHEN generation = sqlc.arg(completed_generation)
        THEN NULL
        ELSE deadline_at
    END,
    event_received_at = CASE
        WHEN generation = sqlc.arg(completed_generation)
        THEN NULL
        ELSE event_received_at
    END,
    updated_at = now()
WHERE kind = sqlc.arg(kind)
  AND refresh_key = sqlc.arg(refresh_key)
  AND generation >= sqlc.arg(completed_generation);
