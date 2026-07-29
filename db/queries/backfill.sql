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
