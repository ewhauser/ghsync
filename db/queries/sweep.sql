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
UPDATE sweep_cursors
SET cursor = sqlc.arg(first_cursor),
    seen_keys = '[]'::jsonb,
    started_at = sqlc.arg(started_at),
    updated_at = sqlc.arg(started_at),
    completed_at = NULL
WHERE installation_id = sqlc.arg(installation_id)
  AND sweep_kind = sqlc.arg(sweep_kind)
  AND scope_key = sqlc.arg(scope_key)
RETURNING *;

-- name: AdvanceSweepCursor :one
UPDATE sweep_cursors
SET cursor = sqlc.arg(next_cursor),
    seen_keys = sqlc.arg(seen_keys),
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
    seen_keys = sqlc.arg(seen_keys),
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
    entity_keys, last_checked_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(sweep_kind), sqlc.arg(scope_key),
    sqlc.arg(cursor), sqlc.arg(etag), sqlc.arg(next_cursor),
    sqlc.arg(entity_keys), sqlc.arg(last_checked_at)
)
ON CONFLICT (installation_id, sweep_kind, scope_key, cursor) DO UPDATE
SET etag = EXCLUDED.etag,
    next_cursor = EXCLUDED.next_cursor,
    entity_keys = EXCLUDED.entity_keys,
    last_checked_at = EXCLUDED.last_checked_at;

-- name: ListSweepRepositories :many
SELECT id, gh_id, full_name
FROM repos
WHERE installation_id = sqlc.arg(installation_id)
  AND tombstoned_at IS NULL
ORDER BY id;

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
SELECT repos.full_name AS repo_full_name, stacks.number
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
  AND stacks.open
  AND stacks.last_checked_at <= sqlc.arg(stale_before)
ORDER BY stacks.last_checked_at, repos.full_name, stacks.number;

-- name: ListStaleClosedStacks :many
SELECT repos.full_name AS repo_full_name, stacks.number
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
  AND NOT stacks.open
  AND stacks.last_checked_at <= sqlc.arg(stale_before)
ORDER BY stacks.last_checked_at, repos.full_name, stacks.number;

-- name: ListStaleOpenPullRequests :many
SELECT repos.full_name AS repo_full_name, pull_requests.number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state = 'open'
  AND pull_requests.last_checked_at <= sqlc.arg(stale_before)
ORDER BY pull_requests.last_checked_at, repos.full_name, pull_requests.number;

-- name: ListStaleClosedPullRequests :many
SELECT repos.full_name AS repo_full_name, pull_requests.number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.installation_id = sqlc.arg(installation_id)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state <> 'open'
  AND pull_requests.last_checked_at <= sqlc.arg(stale_before)
ORDER BY pull_requests.last_checked_at, repos.full_name, pull_requests.number;

-- name: ListStaleRepoRules :many
SELECT repos.full_name
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

-- name: TouchListedStacksCheckedAt :execrows
-- A 304 on the authoritative stack-list page validates the open stack
-- summaries on that page without downloading one response per stack.
UPDATE stacks
SET last_checked_at = GREATEST(
    last_checked_at,
    sqlc.arg(checked_at)
)
FROM repos
WHERE repos.id = stacks.repo_id
  AND repos.installation_id = sqlc.arg(installation_id)
  AND repos.full_name = sqlc.arg(repo_full_name)
  AND repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL
  AND stacks.open
  AND stacks.number = ANY(sqlc.arg(stack_numbers)::int[]);

-- name: TouchListedPullRequestsCheckedAt :execrows
-- C-R1/C-B4: the unchanged authoritative open-PR page is a successful
-- validation for every live PR summary it contains.
UPDATE pull_requests
SET last_checked_at = GREATEST(
    last_checked_at,
    sqlc.arg(checked_at)
)
FROM repos
WHERE repos.id = pull_requests.repo_id
  AND repos.installation_id = sqlc.arg(installation_id)
  AND repos.full_name = sqlc.arg(repo_full_name)
  AND repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state = 'open'
  AND pull_requests.number = ANY(sqlc.arg(pr_numbers)::int[]);

-- name: ListExistingWebhookDeliveryGUIDs :many
SELECT delivery_guid
FROM webhook_deliveries
WHERE delivery_guid = ANY(sqlc.arg(delivery_guids)::text[]);

-- name: PruneWebhookDeliveryPayloads :execrows
UPDATE webhook_deliveries
SET raw_body = NULL,
    headers = '{}'::jsonb,
    payload_pruned_at = sqlc.arg(pruned_at)
WHERE received_at < sqlc.arg(cutoff)
  AND raw_body IS NOT NULL;

-- name: DeleteCheckHistoryBefore :execrows
DELETE FROM check_history
WHERE synced_at < sqlc.arg(cutoff);
