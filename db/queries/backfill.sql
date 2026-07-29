-- name: EnsureInstallationBackfillCursor :one
INSERT INTO installation_backfill_cursors (
    installation_id, phase, page
) VALUES (
    sqlc.arg(installation_id), 'repositories', 1
)
ON CONFLICT (installation_id) DO UPDATE
SET updated_at = installation_backfill_cursors.updated_at
RETURNING *;

-- name: GetInstallationBackfillCursor :one
SELECT *
FROM installation_backfill_cursors
WHERE installation_id = sqlc.arg(installation_id);

-- name: GetInstallationBackfillCursorForUpdate :one
SELECT *
FROM installation_backfill_cursors
WHERE installation_id = sqlc.arg(installation_id)
FOR UPDATE;

-- name: AdvanceInstallationBackfillCursor :one
UPDATE installation_backfill_cursors
SET phase = sqlc.arg(next_phase),
    page = sqlc.arg(next_page),
    queue_name = sqlc.arg(queue_name),
    completed_at = CASE
        WHEN sqlc.arg(next_phase)::text = 'done' THEN now()
        ELSE NULL
    END,
    updated_at = now()
WHERE installation_id = sqlc.arg(installation_id)
  AND phase = sqlc.arg(expected_phase)
  AND page = sqlc.arg(expected_page)
RETURNING *;

-- name: EnsureBackfillCursor :one
INSERT INTO backfill_cursors (
    installation_id, repo_full_name, phase, page
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(repo_full_name), 'repository', 1
)
ON CONFLICT (installation_id, repo_full_name) DO UPDATE
SET updated_at = backfill_cursors.updated_at
RETURNING *;

-- name: GetBackfillCursor :one
SELECT *
FROM backfill_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND repo_full_name = sqlc.arg(repo_full_name);

-- name: GetBackfillCursorForUpdate :one
SELECT *
FROM backfill_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND repo_full_name = sqlc.arg(repo_full_name)
FOR UPDATE;

-- name: AdvanceBackfillCursor :one
UPDATE backfill_cursors
SET phase = sqlc.arg(next_phase),
    page = sqlc.arg(next_page),
    queue_name = sqlc.arg(queue_name),
    completed_at = CASE
        WHEN sqlc.arg(next_phase)::text = 'done' THEN now()
        ELSE NULL
    END,
    updated_at = now()
WHERE installation_id = sqlc.arg(installation_id)
  AND repo_full_name = sqlc.arg(repo_full_name)
  AND phase = sqlc.arg(expected_phase)
  AND page = sqlc.arg(expected_page)
RETURNING *;

-- name: UpsertBackfillChildren :exec
WITH input AS (
    SELECT element->>'kind' AS kind,
           element->>'refresh_key' AS refresh_key,
           (element->>'target_generation')::bigint AS target_generation
    FROM jsonb_array_elements(sqlc.arg(children)::jsonb) AS element
)
INSERT INTO backfill_children (
    installation_id, repo_full_name, kind, refresh_key, target_generation
)
SELECT sqlc.arg(installation_id), sqlc.arg(repo_full_name),
       input.kind, input.refresh_key, input.target_generation
FROM input
ON CONFLICT (installation_id, repo_full_name, kind, refresh_key) DO UPDATE
SET target_generation = GREATEST(
        backfill_children.target_generation,
        EXCLUDED.target_generation
    ),
    completed_at = CASE
        WHEN backfill_children.target_generation >= EXCLUDED.target_generation
        THEN backfill_children.completed_at
        ELSE NULL
    END;

-- name: MarkCompletedBackfillChildren :execrows
UPDATE backfill_children AS child
SET completed_at = now()
FROM refresh_intent_generations AS generation
WHERE child.installation_id = sqlc.arg(installation_id)
  AND child.repo_full_name = sqlc.arg(repo_full_name)
  AND child.completed_at IS NULL
  AND generation.kind = child.kind
  AND generation.refresh_key = child.refresh_key
  AND generation.completed_generation >= child.target_generation;

-- name: CountPendingBackfillChildren :one
SELECT count(*)
FROM backfill_children
WHERE installation_id = sqlc.arg(installation_id)
  AND repo_full_name = sqlc.arg(repo_full_name)
  AND completed_at IS NULL;

-- name: CountPendingInstallationRepos :one
SELECT count(*)
FROM backfill_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND phase <> 'done';
