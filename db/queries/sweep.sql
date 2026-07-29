-- name: EnsureSweepCursor :one
INSERT INTO sweep_cursors (
    installation_id, sweep_kind, scope_key
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(sweep_kind), sqlc.arg(scope_key)
)
ON CONFLICT (installation_id, sweep_kind, scope_key) DO UPDATE
SET sweep_kind = EXCLUDED.sweep_kind
RETURNING *;

-- name: GetSweepCursor :one
SELECT *
FROM sweep_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key);

-- name: GetSweepCursorForUpdate :one
SELECT *
FROM sweep_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
FOR UPDATE;

-- name: StartSweepCursor :one
WITH cleared AS (
    DELETE FROM sweep_seen_keys
    WHERE installation_id = sqlc.arg(installation_id)
      AND sweep_kind = sqlc.arg(sweep_kind)
      AND scope_key = sqlc.arg(scope_key)
    RETURNING entity_key
)
UPDATE sweep_cursors
SET cursor = sqlc.arg(first_cursor),
    pass_new_count = 0,
    started_at = sqlc.arg(started_at),
    updated_at = sqlc.arg(started_at),
    completed_at = NULL
WHERE sweep_cursors.installation_id = sqlc.arg(installation_id)
  AND sweep_cursors.sweep_kind = sqlc.arg(sweep_kind)
  AND sweep_cursors.scope_key = sqlc.arg(scope_key)
  AND (SELECT count(*) FROM cleared) >= 0
RETURNING *;

-- name: AdvanceSweepCursor :one
UPDATE sweep_cursors
SET cursor = sqlc.arg(next_cursor),
    pass_new_count = sqlc.arg(pass_new_count),
    updated_at = sqlc.arg(updated_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
  AND cursor = sqlc.arg(expected_cursor)
  AND completed_at IS NULL
RETURNING *;

-- name: RestartSweepCursorPass :one
UPDATE sweep_cursors
SET cursor = sqlc.arg(first_cursor),
    pass_new_count = 0,
    updated_at = sqlc.arg(updated_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
  AND cursor = sqlc.arg(expected_cursor)
  AND completed_at IS NULL
RETURNING *;

-- name: CompleteSweepCursor :one
UPDATE sweep_cursors
SET cursor = '',
    pass_new_count = 0,
    updated_at = sqlc.arg(completed_at),
    completed_at = sqlc.arg(completed_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
  AND cursor = sqlc.arg(expected_cursor)
  AND completed_at IS NULL
RETURNING *;

-- name: GetSweepPage :one
SELECT *
FROM sweep_pages
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
  AND cursor = sqlc.arg(cursor);

-- name: UpsertSweepPage :exec
INSERT INTO sweep_pages (
    installation_id, sweep_kind, scope_key, cursor, etag, next_cursor,
    entity_keys, list_seen_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(sweep_kind), sqlc.arg(scope_key),
    sqlc.arg(cursor), sqlc.arg(etag), sqlc.arg(next_cursor),
    sqlc.arg(entity_keys), sqlc.arg(list_seen_at)
)
ON CONFLICT (installation_id, sweep_kind, scope_key, cursor) DO UPDATE
SET etag = EXCLUDED.etag,
    next_cursor = EXCLUDED.next_cursor,
    entity_keys = EXCLUDED.entity_keys,
    list_seen_at = EXCLUDED.list_seen_at;

-- name: InsertSweepSeenKeys :one
WITH input AS (
    SELECT DISTINCT entity_key
    FROM unnest(sqlc.arg(entity_keys)::text[]) AS keys(entity_key)
),
inserted AS (
    INSERT INTO sweep_seen_keys (
        installation_id, sweep_kind, scope_key, entity_key, first_seen_at
    )
    SELECT sqlc.arg(installation_id), sqlc.arg(sweep_kind),
           sqlc.arg(scope_key), input.entity_key, sqlc.arg(first_seen_at)
    FROM input
    ON CONFLICT DO NOTHING
    RETURNING entity_key
)
SELECT count(*) FROM inserted;

-- name: ListMissingSweepEntityKeys :many
-- C-R3/Q13: disappearance candidates are cached live entities absent from the
-- row-oriented membership set accumulated by the completed listing.
WITH cached AS (
    SELECT ('repo:' || repos.full_name || ':metadata')::text AS entity_key
    FROM repos
    WHERE sqlc.arg(sweep_kind)::text = 'repositories'
      AND repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL

    UNION ALL

    SELECT ('stack:' || repos.full_name || ':' || stacks.number)::text
    FROM stacks
    JOIN repos ON repos.id = stacks.repo_id
    WHERE sqlc.arg(sweep_kind)::text = 'stacks'
      AND repos.installation_id = sqlc.arg(installation_id)
      AND repos.full_name = sqlc.arg(scope_key)
      AND repos.tombstoned_at IS NULL
      AND stacks.tombstoned_at IS NULL

    UNION ALL

    SELECT ('pr:' || repos.full_name || ':' ||
            pull_requests.number)::text
    FROM pull_requests
    JOIN repos ON repos.id = pull_requests.repo_id
    WHERE sqlc.arg(sweep_kind)::text = 'pull_requests'
      AND repos.installation_id = sqlc.arg(installation_id)
      AND repos.full_name = sqlc.arg(scope_key)
      AND repos.tombstoned_at IS NULL
      AND pull_requests.tombstoned_at IS NULL
      AND pull_requests.state = 'open'
)
SELECT cached.entity_key
FROM cached
WHERE NOT EXISTS (
    SELECT 1
    FROM sweep_seen_keys AS seen
    WHERE seen.installation_id = sqlc.arg(installation_id)
      AND seen.sweep_kind = sqlc.arg(sweep_kind)
      AND seen.scope_key = sqlc.arg(scope_key)
      AND seen.entity_key = cached.entity_key
)
ORDER BY cached.entity_key;

-- name: ListSweepRepositories :many
SELECT id, gh_id, full_name
FROM repos
WHERE installation_id = sqlc.arg(installation_id)
  AND tombstoned_at IS NULL
ORDER BY id;

-- name: ReapOrphanedRepositorySweepCursors :execrows
-- Q16: repository-scoped cursors use mutable names to call GitHub. Remove
-- scopes whose current live repository identity no longer owns that name;
-- child pages and seen keys follow via ON DELETE CASCADE.
DELETE FROM sweep_cursors AS cursor
WHERE cursor.installation_id = sqlc.arg(installation_id)
  AND cursor.sweep_kind IN ('stacks', 'pull_requests')
  AND NOT EXISTS (
      SELECT 1
      FROM repos
      WHERE repos.installation_id = cursor.installation_id
        AND repos.full_name = cursor.scope_key
        AND repos.tombstoned_at IS NULL
  );

-- name: ListLiveRepositoryNames :many
SELECT full_name
FROM repos
WHERE installation_id = sqlc.arg(installation_id)
  AND tombstoned_at IS NULL
ORDER BY full_name;

-- name: ListLiveStackNumbers :many
SELECT stacks.number
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.full_name = sqlc.arg(repo_full_name)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
ORDER BY stacks.number;

-- name: ListLiveOpenPullRequestNumbers :many
SELECT pull_requests.number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.full_name = sqlc.arg(repo_full_name)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state = 'open'
ORDER BY pull_requests.number;

-- name: ListStaleOpenStacks :many
SELECT repos.full_name AS repo_full_name, stacks.number,
       stacks.last_checked_at
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
  AND stacks.open
  AND stacks.last_checked_at <= sqlc.arg(stale_before)
ORDER BY stacks.last_checked_at, stacks.repo_id, stacks.number;

-- name: ListStaleClosedStacks :many
SELECT repos.full_name AS repo_full_name, stacks.number,
       stacks.last_checked_at
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
  AND NOT stacks.open
  AND stacks.display_until > clock_timestamp()
  AND stacks.last_checked_at <= sqlc.arg(stale_before)
ORDER BY stacks.last_checked_at, stacks.repo_id, stacks.number;

-- name: ListStaleOpenPullRequests :many
SELECT repos.full_name AS repo_full_name, pull_requests.number,
       pull_requests.last_checked_at
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state = 'open'
  AND pull_requests.last_checked_at <= sqlc.arg(stale_before)
ORDER BY pull_requests.last_checked_at,
         pull_requests.repo_id,
         pull_requests.number;

-- name: ListStaleClosedPullRequests :many
SELECT repos.full_name AS repo_full_name, pull_requests.number,
       pull_requests.last_checked_at
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state <> 'open'
  AND pull_requests.display_until > clock_timestamp()
  AND pull_requests.last_checked_at <= sqlc.arg(stale_before)
ORDER BY pull_requests.last_checked_at,
         pull_requests.repo_id,
         pull_requests.number;

-- name: ListStaleRepoRules :many
SELECT repos.full_name,
       COALESCE(
           repo_rule_sync_state.last_checked_at,
           repos.last_checked_at
       ) AS last_checked_at
FROM repos
LEFT JOIN repo_rule_sync_state ON repo_rule_sync_state.repo_id = repos.id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND COALESCE(
      repo_rule_sync_state.last_checked_at,
      repos.last_checked_at
  ) <= sqlc.arg(stale_before)
ORDER BY COALESCE(
    repo_rule_sync_state.last_checked_at,
    repos.last_checked_at
), repos.full_name;

-- name: ListExistingWebhookDeliveryGUIDs :many
SELECT delivery_guid
FROM webhook_deliveries
WHERE delivery_guid = ANY(sqlc.arg(delivery_guids)::text[]);

-- name: EnsureGapHealCursor :one
INSERT INTO gap_heal_cursors (installation_id)
VALUES (sqlc.arg(installation_id))
ON CONFLICT (installation_id) DO UPDATE
SET installation_id = EXCLUDED.installation_id
RETURNING *;

-- name: GetGapHealCursorForUpdate :one
SELECT *
FROM gap_heal_cursors
WHERE installation_id = sqlc.arg(installation_id)
FOR UPDATE;

-- name: StartGapHealCursor :one
UPDATE gap_heal_cursors
SET cursor = '',
    cutoff = sqlc.arg(cutoff),
    started_at = sqlc.arg(started_at),
    updated_at = sqlc.arg(started_at),
    completed_at = NULL
WHERE installation_id = sqlc.arg(installation_id)
RETURNING *;

-- name: AdvanceGapHealCursor :one
UPDATE gap_heal_cursors
SET cursor = sqlc.arg(next_cursor),
    updated_at = sqlc.arg(updated_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND cursor = sqlc.arg(expected_cursor)
  AND completed_at IS NULL
RETURNING *;

-- name: CompleteGapHealCursor :one
UPDATE gap_heal_cursors
SET cursor = '',
    updated_at = sqlc.arg(completed_at),
    completed_at = sqlc.arg(completed_at)
WHERE installation_id = sqlc.arg(installation_id)
  AND cursor = sqlc.arg(expected_cursor)
  AND completed_at IS NULL
RETURNING *;

-- name: PruneWebhookDeliveryPayloadBatch :execrows
WITH batch AS (
    SELECT candidate.delivery_guid
    FROM webhook_deliveries AS candidate
    WHERE candidate.received_at < sqlc.arg(cutoff)
      AND candidate.raw_body IS NOT NULL
      AND candidate.status = 'processed'
    ORDER BY candidate.received_at, candidate.delivery_guid
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
)
UPDATE webhook_deliveries AS delivery
SET raw_body = NULL,
    headers = '{}'::jsonb,
    traceparent = NULL,
    tracestate = NULL,
    payload_pruned_at = sqlc.arg(pruned_at)
FROM batch
WHERE delivery.delivery_guid = batch.delivery_guid;

-- name: DeleteCheckHistoryBatch :execrows
DELETE FROM check_history AS history
WHERE history.id IN (
    SELECT candidate.id
    FROM check_history AS candidate
    WHERE candidate.synced_at < sqlc.arg(cutoff)
    ORDER BY candidate.synced_at, candidate.id
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
);
