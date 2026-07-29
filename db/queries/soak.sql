-- name: DeleteSoakConsumerCursor :exec
DELETE FROM consumer_cursors
WHERE consumer = sqlc.arg(consumer)
  AND stream = sqlc.arg(stream);

-- name: GetSoakCacheSeedState :one
SELECT
    EXISTS (
        SELECT 1
        FROM installation_backfill_cursors
        WHERE installation_backfill_cursors.installation_id =
              sqlc.arg(installation_id)
          AND phase = 'done'
          AND completed_at IS NOT NULL
    ) AS installation_done,
    (
        SELECT count(*)
        FROM backfill_cursors
        WHERE backfill_cursors.installation_id = sqlc.arg(installation_id)
    ) AS repositories,
    (
        SELECT count(*)
        FROM backfill_cursors
        WHERE backfill_cursors.installation_id = sqlc.arg(installation_id)
          AND phase <> 'done'
    ) AS incomplete_repos,
    (
        SELECT count(*)
        FROM backfill_children
        WHERE backfill_children.installation_id = sqlc.arg(installation_id)
          AND completed_at IS NULL
    ) AS pending_children,
    (
        SELECT count(*)
        FROM river_job
        WHERE queue IN ('interactive', 'event', 'sweep', 'reconcile')
          AND (
              state IN ('available', 'pending', 'running')
              OR (
                  state IN ('retryable', 'scheduled')
                  AND scheduled_at <= now()
              )
          )
    ) AS cache_writers;

-- name: ListSoakCachedPullRequests :many
SELECT pull.number, pull.title
FROM pull_requests AS pull
JOIN repos AS repo ON repo.id = pull.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL;
