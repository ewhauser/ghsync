-- name: HasOutstandingRefresh :one
SELECT COALESCE((
    EXISTS (
        SELECT 1
        FROM refresh_intent_generations
        WHERE refresh_key = sqlc.arg(refresh_key)
          AND completed_generation < generation
    )
    OR EXISTS (
        SELECT 1
        FROM river_job
        WHERE args->>'key' = sqlc.arg(refresh_key)
          AND state IN (
              'available', 'pending', 'retryable', 'running', 'scheduled'
          )
    )
), false)::boolean;

-- name: GetDriftSampleCursor :one
SELECT source_id
FROM drift_sample_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind);

-- name: UpsertDriftSampleCursor :exec
INSERT INTO drift_sample_cursors (
    installation_id, entity_kind, source_id
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(entity_kind), sqlc.arg(source_id)
)
ON CONFLICT (installation_id, entity_kind) DO UPDATE
SET source_id = EXCLUDED.source_id,
    updated_at = now();

-- name: SampleCachedEntitiesAfter :many
-- Q11: the forward half of the rotating sample is a plain indexed source_id
-- range. Detect issues a second bounded range only when this reaches the end.
SELECT entity_kind, source_id, entity_key, lock_key, cache_snapshot,
       last_checked_at
FROM drift_entities
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind)
  AND source_id > sqlc.arg(after_source_id)
ORDER BY source_id
LIMIT sqlc.arg(sample_size);

-- name: SampleCachedEntitiesThrough :many
-- The wrap half cannot overlap the forward half and retains source_id order.
SELECT entity_kind, source_id, entity_key, lock_key, cache_snapshot,
       last_checked_at
FROM drift_entities
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind)
  AND source_id <= sqlc.arg(through_source_id)
ORDER BY source_id
LIMIT sqlc.arg(sample_size);

-- name: GetCachedEntitySnapshot :one
SELECT entity_kind, source_id, entity_key, lock_key, cache_snapshot,
       last_checked_at
FROM drift_entities
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind)
  AND entity_key = sqlc.arg(entity_key);

-- name: InsertDriftFinding :one
INSERT INTO drift_findings (
    installation_id, entity_kind, entity_key, detected_at, cache_snapshot,
    upstream_snapshot, diff, diff_hash, first_seen_at, last_seen_at,
    occurrence_count, refresh_enqueued_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(entity_kind),
    sqlc.arg(entity_key), sqlc.arg(detected_at),
    sqlc.arg(cache_snapshot), sqlc.arg(upstream_snapshot),
    sqlc.arg(diff), sqlc.arg(diff_hash), sqlc.arg(detected_at),
    sqlc.arg(detected_at), 1, sqlc.arg(refresh_enqueued_at)
)
RETURNING *;

-- name: GetOpenDriftFindingByHash :one
SELECT *
FROM drift_findings
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind)
  AND entity_key = sqlc.arg(entity_key)
  AND diff_hash = sqlc.arg(diff_hash)
  AND resolved_at IS NULL
FOR UPDATE;

-- name: SetDriftFindingHealGeneration :one
UPDATE drift_findings
SET heal_generation = sqlc.arg(heal_generation)
WHERE id = sqlc.arg(id)
RETURNING *;

-- name: TouchOpenDriftFinding :one
UPDATE drift_findings
SET last_seen_at = sqlc.arg(last_seen_at),
    cache_snapshot = sqlc.arg(cache_snapshot),
    upstream_snapshot = sqlc.arg(upstream_snapshot),
    diff = sqlc.arg(diff)
WHERE id = sqlc.arg(id)
  AND resolved_at IS NULL
RETURNING *;

-- name: EscalateOpenDriftFinding :one
UPDATE drift_findings
SET last_seen_at = sqlc.arg(last_seen_at),
    occurrence_count = occurrence_count + 1,
    cache_snapshot = sqlc.arg(cache_snapshot),
    upstream_snapshot = sqlc.arg(upstream_snapshot),
    diff = sqlc.arg(diff),
    escalated_at = COALESCE(escalated_at, sqlc.arg(escalated_at))
WHERE id = sqlc.arg(id)
  AND resolved_at IS NULL
RETURNING drift_findings.*;

-- name: ResolveOpenDriftFindings :execrows
UPDATE drift_findings
SET resolved_at = sqlc.arg(resolved_at),
    last_seen_at = sqlc.arg(resolved_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND entity_kind = sqlc.arg(entity_kind)
  AND entity_key = sqlc.arg(entity_key)
  AND resolved_at IS NULL;

-- name: PruneResolvedDriftFindingBatch :execrows
DELETE FROM drift_findings AS finding
WHERE finding.id IN (
    SELECT candidate.id
    FROM drift_findings AS candidate
    WHERE candidate.resolved_at < sqlc.arg(cutoff)
    ORDER BY candidate.resolved_at, candidate.id
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
);

-- name: GetDriftFinding :one
SELECT *
FROM drift_findings
WHERE id = sqlc.arg(id);

-- name: CountDriftFindings :one
SELECT count(*)
FROM drift_findings;
