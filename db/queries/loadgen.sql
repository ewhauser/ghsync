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
JOIN pull_requests AS pull
  ON pull.repo_id = request.repo_id
 AND pull.number = request.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND request.tombstoned_at IS NULL
  AND request.head_sha = pull.head_sha
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

-- name: ListLoadgenCachedPullRequestReviews :many
SELECT review.pr_number, review.gh_id, review.node_id,
       review.author_kind, review.author_node_id, review.author_login,
       review.state, review.submitted_at, review.commit_oid,
       review.gh_updated_at, review.head_sha
FROM pull_request_reviews AS review
JOIN repos AS repo ON repo.id = review.repo_id
JOIN pull_requests AS pull
  ON pull.repo_id = review.repo_id
 AND pull.number = review.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND review.tombstoned_at IS NULL
  AND review.head_sha = pull.head_sha
ORDER BY review.pr_number, review.node_id;

-- name: ListLoadgenCachedPullRequestComments :many
SELECT comment.pr_number, comment.gh_id, comment.node_id,
       comment.author_kind, comment.author_node_id, comment.author_login,
       comment.created_at, comment.gh_updated_at, comment.head_sha
FROM pull_request_comments AS comment
JOIN repos AS repo ON repo.id = comment.repo_id
JOIN pull_requests AS pull
  ON pull.repo_id = comment.repo_id
 AND pull.number = comment.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND comment.tombstoned_at IS NULL
  AND comment.head_sha = pull.head_sha
ORDER BY comment.pr_number, comment.node_id;

-- name: ListLoadgenCachedPullRequestChangeSnapshots :many
SELECT snapshot.pr_number, snapshot.base_sha, snapshot.head_sha,
       snapshot.files_total_count, snapshot.files_truncated,
       snapshot.codeowners_ref, snapshot.codeowners_sha,
       snapshot.codeowners_path, snapshot.codeowners_state,
       snapshot.codeowners_source, snapshot.codeowners_hash
FROM pull_request_change_snapshots AS snapshot
JOIN repos AS repo ON repo.id = snapshot.repo_id
JOIN pull_requests AS pull
  ON pull.repo_id = snapshot.repo_id
 AND pull.number = snapshot.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND snapshot.tombstoned_at IS NULL
  AND snapshot.base_sha = pull.base_sha
  AND snapshot.head_sha = pull.head_sha
ORDER BY snapshot.pr_number;

-- name: ListLoadgenCachedPullRequestChangedFiles :many
SELECT file.pr_number, file.path, file.previous_path, file.change_type,
       file.base_sha, file.head_sha
FROM pull_request_changed_files AS file
JOIN repos AS repo ON repo.id = file.repo_id
JOIN pull_requests AS pull
  ON pull.repo_id = file.repo_id
 AND pull.number = file.pr_number
JOIN pull_request_change_snapshots AS snapshot
  ON snapshot.repo_id = file.repo_id
 AND snapshot.pr_number = file.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND snapshot.tombstoned_at IS NULL
  AND file.tombstoned_at IS NULL
  AND snapshot.base_sha = pull.base_sha
  AND snapshot.head_sha = pull.head_sha
  AND file.base_sha = snapshot.base_sha
  AND file.head_sha = snapshot.head_sha
ORDER BY file.pr_number, file.path;

-- name: ListLoadgenCachedPullRequestFileOwners :many
SELECT owner.pr_number, owner.path, owner.owner_token, owner.owner_type,
       owner.owner_name, owner.resolution_state, owner.owner_gh_id,
       owner.owner_node_id, owner.owner_login, owner.source_pattern,
       owner.source_line, owner.base_sha, owner.head_sha
FROM pull_request_file_owners AS owner
JOIN repos AS repo ON repo.id = owner.repo_id
JOIN pull_requests AS pull
  ON pull.repo_id = owner.repo_id
 AND pull.number = owner.pr_number
JOIN pull_request_change_snapshots AS snapshot
  ON snapshot.repo_id = owner.repo_id
 AND snapshot.pr_number = owner.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND snapshot.tombstoned_at IS NULL
  AND owner.tombstoned_at IS NULL
  AND snapshot.base_sha = pull.base_sha
  AND snapshot.head_sha = pull.head_sha
  AND owner.base_sha = snapshot.base_sha
  AND owner.head_sha = snapshot.head_sha
ORDER BY owner.pr_number, owner.path, owner.owner_token;

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
JOIN pull_requests AS pull
  ON pull.repo_id = thread.repo_id
 AND pull.number = thread.pr_number
WHERE repo.full_name = sqlc.arg(repo_full_name)
  AND repo.tombstoned_at IS NULL
  AND pull.tombstoned_at IS NULL
  AND thread.tombstoned_at IS NULL
  AND thread.head_sha = pull.head_sha
ORDER BY thread.id;
