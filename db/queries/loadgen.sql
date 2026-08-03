-- name: GetLoadgenCacheSeedState :one
WITH params AS (
    SELECT sqlc.arg(installation_id)::bigint AS installation_id
)
SELECT
    EXISTS (
        SELECT 1
        FROM installation_backfill_cursors AS installation_cursor
        WHERE installation_cursor.installation_id = params.installation_id
          AND installation_cursor.phase = 'done'
          AND installation_cursor.completed_at IS NOT NULL
    ) AS installation_done,
    (
        SELECT count(*)
        FROM backfill_cursors AS repository_cursor
        WHERE repository_cursor.installation_id = params.installation_id
    ) AS repositories,
    (
        SELECT count(*)
        FROM backfill_cursors AS repository_cursor
        WHERE repository_cursor.installation_id = params.installation_id
          AND repository_cursor.phase <> 'done'
    ) AS incomplete_repos,
    (
        SELECT count(*)
        FROM backfill_children AS child
        WHERE child.installation_id = params.installation_id
          AND child.completed_at IS NULL
    ) AS pending_children,
    (
        SELECT count(*)
        FROM river_job AS job
        WHERE job.kind IN (
            'backfill_installation_page',
            'backfill_repo_page'
        )
          AND job.state IN (
              'available', 'pending', 'retryable', 'running', 'scheduled'
          )
    ) AS backfill_jobs
FROM params;

-- name: CountLoadgenRunDeliveries :one
SELECT
    count(*) FILTER (WHERE status = 'processed') AS completed,
    count(*) AS total
FROM webhook_deliveries
WHERE delivery_guid = ANY(sqlc.arg(delivery_guids)::text[]);

-- name: ListLoadgenDroppedDeliveries :many
SELECT delivery_guid, received_at, status
FROM webhook_deliveries
WHERE delivery_guid = ANY(sqlc.arg(delivery_guids)::text[]);

-- name: DeleteLoadgenConsumerCursor :exec
DELETE FROM consumer_cursors
WHERE consumer = sqlc.arg(consumer)
  AND stream = sqlc.arg(stream);

-- name: ListLoadgenCachedPullRequests :many
SELECT
    pull.gh_id,
    pull.node_id,
    pull.number,
    pull.title,
    pull.state,
    pull.draft,
    pull.author_login,
    pull.review_decision,
    pull.mergeable_state,
    pull.head_ref,
    pull.head_sha,
    pull.base_ref,
    pull.base_sha,
    pull.stack_number,
    pull.stack_position,
    pull.gh_updated_at
FROM pull_requests AS pull
JOIN repos AS repo ON repo.id = pull.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
ORDER BY pull.number;

-- name: ListLoadgenCachedPullRequestReviewRequests :many
SELECT
    request.pr_number,
    request.reviewer_kind,
    request.reviewer_gh_id,
    request.reviewer_node_id,
    request.reviewer_login,
    request.requested_at,
    request.head_sha
FROM pull_request_review_requests AS request
JOIN repos AS repo ON repo.id = request.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND request.tombstoned_at IS NULL
ORDER BY request.pr_number, request.reviewer_kind, request.reviewer_gh_id;

-- name: ListLoadgenCachedStacks :many
SELECT
    stack.gh_id,
    stack.node_id,
    stack.number,
    stack.base_ref,
    stack.base_sha,
    stack.open,
    stack.entries,
    stack.gh_updated_at,
    stack.head_sha
FROM stacks AS stack
JOIN repos AS repo ON repo.id = stack.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND stack.tombstoned_at IS NULL
ORDER BY stack.number;

-- name: ListLoadgenCachedCheckRuns :many
SELECT
    run.gh_id,
    run.node_id,
    run.head_sha,
    run.name,
    run.status,
    run.conclusion,
    run.details_url,
    run.app_slug,
    run.started_at,
    run.completed_at,
    run.gh_updated_at,
    run.semantic_version
FROM check_runs AS run
JOIN repos AS repo ON repo.id = run.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND run.tombstoned_at IS NULL
ORDER BY run.gh_id;

-- name: ListLoadgenCachedReviewThreads :many
SELECT
    thread.id,
    thread.pr_number,
    thread.is_resolved,
    thread.is_outdated,
    thread.path,
    thread.line,
    thread.comments,
    thread.gh_updated_at,
    thread.head_sha
FROM review_threads AS thread
JOIN repos AS repo ON repo.id = thread.repo_id
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND thread.tombstoned_at IS NULL
ORDER BY thread.id;
