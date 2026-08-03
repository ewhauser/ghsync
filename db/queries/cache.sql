-- name: AcquireEntityAdvisoryLock :exec
-- C-C1 transaction-scoped serialization for direct writer calls.
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: AcquireEntitySessionLock :exec
-- Fetch workers use a dedicated connection and hold this lock from before
-- observation until after the state transaction commits.
SELECT pg_advisory_lock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: ReleaseEntitySessionLock :one
SELECT pg_advisory_unlock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: GetRepoByFullName :one
SELECT repos.*
FROM repo_aliases
JOIN repos ON repos.id = repo_aliases.repo_id
WHERE repo_aliases.full_name = $1;

-- name: GetRepoByGitHubID :one
SELECT *
FROM repos
WHERE gh_id = $1;

-- name: UpsertRepositoryWriteIfNewer :one
INSERT INTO repos (
    installation_id, org_id, gh_id, node_id, owner, name, full_name,
    default_branch, archived, gh_updated_at, head_sha, synced_at,
    last_checked_at, etag, sync_source, tombstoned_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(org_id), sqlc.arg(gh_id),
    sqlc.arg(node_id), sqlc.arg(owner), sqlc.arg(name), sqlc.arg(full_name),
    sqlc.arg(default_branch), sqlc.arg(archived),
    sqlc.narg(gh_updated_at), sqlc.arg(head_sha), sqlc.arg(synced_at),
    sqlc.arg(last_checked_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL
)
ON CONFLICT (gh_id) DO UPDATE
SET installation_id = EXCLUDED.installation_id,
    org_id = EXCLUDED.org_id,
    node_id = EXCLUDED.node_id,
    owner = EXCLUDED.owner,
    name = EXCLUDED.name,
    full_name = EXCLUDED.full_name,
    default_branch = EXCLUDED.default_branch,
    archived = EXCLUDED.archived,
    gh_updated_at = EXCLUDED.gh_updated_at,
    head_sha = CASE WHEN EXCLUDED.head_sha = '' THEN repos.head_sha
                    ELSE EXCLUDED.head_sha END,
    synced_at = EXCLUDED.synced_at,
    last_checked_at = EXCLUDED.last_checked_at,
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL
WHERE repos.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > repos.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at IS NOT DISTINCT FROM repos.gh_updated_at
       AND ROW(
           EXCLUDED.installation_id, EXCLUDED.org_id, EXCLUDED.node_id,
           EXCLUDED.owner, EXCLUDED.name, EXCLUDED.full_name,
           EXCLUDED.default_branch, EXCLUDED.archived,
           CASE WHEN EXCLUDED.head_sha = '' THEN repos.head_sha
                ELSE EXCLUDED.head_sha END
       ) IS DISTINCT FROM ROW(
           repos.installation_id, repos.org_id, repos.node_id,
           repos.owner, repos.name, repos.full_name,
           repos.default_branch, repos.archived, repos.head_sha
       )
   )
   OR (
       repos.tombstoned_at IS NOT NULL
       AND EXCLUDED.last_checked_at > repos.tombstoned_at
   )
RETURNING *;

-- name: TouchRepositoryCheckedAt :exec
UPDATE repos
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE gh_id = sqlc.arg(gh_id);

-- name: TombstoneRepository :one
-- C-R3 repository disappearance is authoritative only after the installation
-- listing omits it and this entity fetch confirms 404.
UPDATE repos
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(synced_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE gh_id = sqlc.arg(gh_id)
  AND tombstoned_at IS NULL
  AND last_checked_at <= sqlc.arg(tombstoned_at)
RETURNING *;

-- name: UpsertRepositoryAlias :exec
INSERT INTO repo_aliases (full_name, repo_id, first_seen_at, last_seen_at)
VALUES (
    sqlc.arg(full_name), sqlc.arg(repo_id),
    sqlc.arg(observed_at), sqlc.arg(observed_at)
)
ON CONFLICT (full_name) DO UPDATE
SET repo_id = EXCLUDED.repo_id,
    last_seen_at = GREATEST(repo_aliases.last_seen_at, EXCLUDED.last_seen_at)
WHERE EXCLUDED.last_seen_at >= repo_aliases.last_seen_at;

-- name: GetPullRequestByIdentity :one
SELECT pull_requests.*, repos.full_name AS repo_full_name
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.gh_id = sqlc.arg(repo_gh_id)
  AND pull_requests.number = sqlc.arg(pr_number);

-- name: GetPullRequestByKey :one
SELECT pull_requests.*, repos.full_name AS repo_full_name
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.number = sqlc.arg(pr_number);

-- name: GetPullRequestFetchMetadata :one
SELECT pull_requests.node_id, pull_requests.etag,
       pull_requests.stack_number, pull_requests.stack_position,
       pull_requests.head_sha, repos.gh_id AS repo_gh_id,
       repos.installation_id, repos.full_name AS repo_full_name
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.number = sqlc.arg(pr_number);

-- name: GetCheckRunsFetchMetadata :one
-- C-B4: the first-page validator is shared by every check row in one
-- repository/head-SHA listing. Prefer the newest observation when rows span
-- multiple refreshes, including a fully tombstoned listing.
SELECT repos.gh_id AS repo_gh_id, repos.installation_id,
       repos.full_name AS repo_full_name,
       COALESCE((
           SELECT check_runs.etag
           FROM check_runs
           WHERE check_runs.repo_id = repos.id
             AND check_runs.head_sha = sqlc.arg(head_sha)
           ORDER BY check_runs.last_checked_at DESC, check_runs.gh_id
           LIMIT 1
       ), ''::text)::text AS etag
FROM repos
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name);

-- name: UpsertPullRequestWriteIfNewer :one
INSERT INTO pull_requests (
    repo_id, gh_id, node_id, number, title, state, draft, author_login,
    head_ref, head_sha, base_ref, base_sha, review_decision, mergeable_state,
    stack_number, stack_position, gh_updated_at, synced_at, last_checked_at,
    etag, sync_source, tombstoned_at, display_until
) VALUES (
    sqlc.arg(repo_id), sqlc.arg(gh_id), sqlc.arg(node_id),
    sqlc.arg(pr_number), sqlc.arg(title), sqlc.arg(state), sqlc.arg(draft),
    sqlc.arg(author_login), sqlc.arg(head_ref), sqlc.arg(head_sha),
    sqlc.arg(base_ref), sqlc.arg(base_sha), sqlc.arg(review_decision),
    sqlc.arg(mergeable_state),
    CASE WHEN sqlc.arg(membership_known)::boolean
         THEN sqlc.narg(stack_number)::int ELSE NULL END,
    CASE WHEN sqlc.arg(membership_known)::boolean
         THEN sqlc.narg(stack_position)::int ELSE NULL END,
    sqlc.arg(gh_updated_at), sqlc.arg(synced_at),
    sqlc.arg(last_checked_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL,
    CASE WHEN sqlc.arg(state)::text = 'open'
         THEN NULL
         ELSE clock_timestamp() + make_interval(
             secs => sqlc.arg(display_window_seconds)::int
         )
    END
)
ON CONFLICT (repo_id, number) DO UPDATE
SET gh_id = EXCLUDED.gh_id,
    node_id = EXCLUDED.node_id,
    title = EXCLUDED.title,
    state = EXCLUDED.state,
    draft = EXCLUDED.draft,
    author_login = EXCLUDED.author_login,
    head_ref = EXCLUDED.head_ref,
    head_sha = EXCLUDED.head_sha,
    base_ref = EXCLUDED.base_ref,
    base_sha = EXCLUDED.base_sha,
    review_decision = EXCLUDED.review_decision,
    mergeable_state = EXCLUDED.mergeable_state,
    stack_number = CASE
        WHEN sqlc.arg(membership_known)::boolean
        THEN EXCLUDED.stack_number
        ELSE pull_requests.stack_number
    END,
    stack_position = CASE
        WHEN sqlc.arg(membership_known)::boolean
        THEN EXCLUDED.stack_position
        ELSE pull_requests.stack_position
    END,
    gh_updated_at = EXCLUDED.gh_updated_at,
    synced_at = EXCLUDED.synced_at,
    last_checked_at = EXCLUDED.last_checked_at,
    etag = CASE
        WHEN EXCLUDED.etag = '' THEN pull_requests.etag
        ELSE EXCLUDED.etag
    END,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL,
    display_until = CASE
        WHEN EXCLUDED.state = 'open' THEN NULL
        WHEN pull_requests.state = 'open'
        THEN clock_timestamp() + make_interval(
            secs => sqlc.arg(display_window_seconds)::int
        )
        ELSE pull_requests.display_until
    END
WHERE pull_requests.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > pull_requests.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at IS NOT DISTINCT FROM pull_requests.gh_updated_at
       AND ROW(
           EXCLUDED.gh_id, EXCLUDED.node_id, EXCLUDED.title, EXCLUDED.state,
           EXCLUDED.draft, EXCLUDED.author_login, EXCLUDED.head_ref,
           EXCLUDED.head_sha, EXCLUDED.base_ref, EXCLUDED.base_sha,
           EXCLUDED.review_decision, EXCLUDED.mergeable_state,
           CASE WHEN sqlc.arg(membership_known)::boolean
                THEN EXCLUDED.stack_number ELSE pull_requests.stack_number END,
           CASE WHEN sqlc.arg(membership_known)::boolean
                THEN EXCLUDED.stack_position ELSE pull_requests.stack_position END
       ) IS DISTINCT FROM ROW(
           pull_requests.gh_id, pull_requests.node_id, pull_requests.title,
           pull_requests.state, pull_requests.draft,
           pull_requests.author_login, pull_requests.head_ref,
           pull_requests.head_sha, pull_requests.base_ref,
           pull_requests.base_sha, pull_requests.review_decision,
           pull_requests.mergeable_state, pull_requests.stack_number,
           pull_requests.stack_position
       )
   )
   OR (
       pull_requests.tombstoned_at IS NOT NULL
       AND EXCLUDED.last_checked_at > pull_requests.tombstoned_at
   )
RETURNING *;

-- name: TouchPullRequestCheckedAt :exec
UPDATE pull_requests
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(pr_number);

-- name: TombstonePullRequest :one
UPDATE pull_requests
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(synced_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    display_until = NULL,
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL
  AND last_checked_at <= sqlc.arg(tombstoned_at)
RETURNING *;

-- name: GetStackByIdentity :one
SELECT stacks.*, repos.full_name AS repo_full_name
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.gh_id = sqlc.arg(repo_gh_id)
  AND stacks.number = sqlc.arg(stack_number);

-- name: GetStackByKey :one
SELECT stacks.*, repos.full_name AS repo_full_name
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND stacks.number = sqlc.arg(stack_number);

-- name: GetStackFetchMetadata :one
SELECT stacks.etag, stacks.entries, stacks.head_sha,
       repos.gh_id AS repo_gh_id, repos.installation_id,
       repos.full_name AS repo_full_name
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND stacks.number = sqlc.arg(stack_number);

-- name: UpsertStackWriteIfNewer :one
INSERT INTO stacks (
    repo_id, gh_id, node_id, number, base_ref, base_sha, open, entries,
    gh_updated_at, head_sha, synced_at, last_checked_at, etag, sync_source,
    tombstoned_at, display_until
) VALUES (
    sqlc.arg(repo_id), sqlc.arg(gh_id), sqlc.arg(node_id),
    sqlc.arg(stack_number), sqlc.arg(base_ref), sqlc.arg(base_sha),
    sqlc.arg(open), sqlc.arg(entries), sqlc.narg(gh_updated_at),
    sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(last_checked_at),
    sqlc.arg(etag), sqlc.arg(sync_source), NULL,
    CASE WHEN sqlc.arg(open)::boolean
         THEN NULL
         ELSE clock_timestamp() + make_interval(
             secs => sqlc.arg(display_window_seconds)::int
         )
    END
)
ON CONFLICT (repo_id, number) DO UPDATE
SET gh_id = EXCLUDED.gh_id,
    node_id = EXCLUDED.node_id,
    base_ref = EXCLUDED.base_ref,
    base_sha = EXCLUDED.base_sha,
    open = EXCLUDED.open,
    entries = EXCLUDED.entries,
    gh_updated_at = EXCLUDED.gh_updated_at,
    head_sha = EXCLUDED.head_sha,
    synced_at = EXCLUDED.synced_at,
    last_checked_at = EXCLUDED.last_checked_at,
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL,
    display_until = CASE
        WHEN EXCLUDED.open THEN NULL
        WHEN stacks.open THEN clock_timestamp() + make_interval(
            secs => sqlc.arg(display_window_seconds)::int
        )
        ELSE stacks.display_until
    END
WHERE stacks.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > stacks.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at IS NOT DISTINCT FROM stacks.gh_updated_at
       AND ROW(
           EXCLUDED.gh_id, EXCLUDED.node_id, EXCLUDED.base_ref,
           EXCLUDED.base_sha, EXCLUDED.open, EXCLUDED.entries,
           EXCLUDED.head_sha
       ) IS DISTINCT FROM ROW(
           stacks.gh_id, stacks.node_id, stacks.base_ref,
           stacks.base_sha, stacks.open, stacks.entries, stacks.head_sha
       )
   )
   OR (
       stacks.tombstoned_at IS NOT NULL
       AND EXCLUDED.last_checked_at > stacks.tombstoned_at
   )
RETURNING *;

-- name: TouchStackCheckedAt :exec
UPDATE stacks
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(stack_number);

-- name: TombstoneStack :one
UPDATE stacks
SET tombstoned_at = sqlc.arg(tombstoned_at),
    open = false,
    synced_at = sqlc.arg(synced_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    display_until = NULL,
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(stack_number)
  AND tombstoned_at IS NULL
  AND last_checked_at <= sqlc.arg(tombstoned_at)
RETURNING *;

-- name: ReplaceReviewThreads :many
WITH input AS (
    SELECT element->>'id' AS id,
           (element->>'is_resolved')::boolean AS is_resolved,
           (element->>'is_outdated')::boolean AS is_outdated,
           element->>'path' AS path,
           (element->>'line')::int AS line,
           element->'comments' AS comments,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at
    FROM jsonb_array_elements(sqlc.arg(threads)::jsonb) AS element
),
upserted AS (
    INSERT INTO review_threads (
        id, repo_id, pr_number, is_resolved, is_outdated, path, line,
        comments, gh_updated_at, head_sha, synced_at, last_checked_at,
        etag, sync_source, tombstoned_at
    )
    SELECT input.id, sqlc.arg(repo_id), sqlc.arg(pr_number),
           input.is_resolved, input.is_outdated, input.path, input.line,
           input.comments, input.gh_updated_at, sqlc.arg(head_sha),
           sqlc.arg(synced_at), sqlc.arg(last_checked_at),
           sqlc.arg(etag), sqlc.arg(sync_source), NULL
    FROM input
    ON CONFLICT (id) DO UPDATE
    SET repo_id = EXCLUDED.repo_id,
        pr_number = EXCLUDED.pr_number,
        is_resolved = EXCLUDED.is_resolved,
        is_outdated = EXCLUDED.is_outdated,
        path = EXCLUDED.path,
        line = EXCLUDED.line,
        comments = EXCLUDED.comments,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE review_threads.gh_updated_at IS NULL
       OR EXCLUDED.gh_updated_at > review_threads.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at IS NOT DISTINCT FROM review_threads.gh_updated_at
           AND ROW(
               EXCLUDED.repo_id, EXCLUDED.pr_number, EXCLUDED.is_resolved,
               EXCLUDED.is_outdated, EXCLUDED.path, EXCLUDED.line,
               EXCLUDED.comments, EXCLUDED.head_sha
           ) IS DISTINCT FROM ROW(
               review_threads.repo_id, review_threads.pr_number,
               review_threads.is_resolved, review_threads.is_outdated,
               review_threads.path, review_threads.line,
               review_threads.comments, review_threads.head_sha
           )
       )
       OR (
           review_threads.tombstoned_at IS NOT NULL
           AND EXCLUDED.last_checked_at > review_threads.tombstoned_at
       )
    RETURNING id
),
tombstoned AS (
    UPDATE review_threads
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND last_checked_at <= sqlc.arg(last_checked_at)
      AND NOT EXISTS (SELECT 1 FROM input WHERE input.id = review_threads.id)
    RETURNING id
)
SELECT id FROM upserted
UNION ALL
SELECT id FROM tombstoned;

-- name: TouchReviewThreadsCheckedAt :exec
UPDATE review_threads
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at))
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number);

-- name: ReplacePullRequestReviewRequests :many
-- The parent pull request's updated_at is the C-C2 observation version for
-- this timestamp-less GitHub connection. Equal-version replacements remain
-- eligible so a pure request-set change is not swallowed.
WITH input AS (
    SELECT element->>'kind' AS reviewer_kind,
           (element->>'gh_id')::bigint AS reviewer_gh_id,
           element->>'node_id' AS reviewer_node_id,
           element->>'login' AS reviewer_login,
           (element->>'requested_at')::timestamptz AS requested_at
    FROM jsonb_array_elements(sqlc.arg(review_requests)::jsonb) AS element
),
eligible AS (
    SELECT pull_requests.repo_id
    FROM pull_requests
    WHERE pull_requests.repo_id = sqlc.arg(repo_id)
      AND pull_requests.number = sqlc.arg(pr_number)
      AND pull_requests.tombstoned_at IS NULL
      AND (
          pull_requests.gh_updated_at IS NULL
          OR pull_requests.gh_updated_at <= sqlc.arg(gh_updated_at)
      )
),
upserted AS (
    INSERT INTO pull_request_review_requests (
        repo_id, pr_number, reviewer_kind, reviewer_gh_id,
        reviewer_node_id, reviewer_login, requested_at, first_seen_at,
        gh_updated_at, head_sha, synced_at, last_checked_at, etag,
        sync_source, tombstoned_at
    )
    SELECT sqlc.arg(repo_id), sqlc.arg(pr_number), input.reviewer_kind,
           input.reviewer_gh_id, input.reviewer_node_id,
           input.reviewer_login, input.requested_at,
           sqlc.arg(first_seen_at), sqlc.arg(gh_updated_at),
           sqlc.arg(head_sha), sqlc.arg(synced_at),
           sqlc.arg(last_checked_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL
    FROM input
    CROSS JOIN eligible
    ON CONFLICT (repo_id, pr_number, reviewer_kind, reviewer_gh_id)
    DO UPDATE
    SET reviewer_node_id = EXCLUDED.reviewer_node_id,
        reviewer_login = EXCLUDED.reviewer_login,
        requested_at = CASE
            WHEN pull_request_review_requests.tombstoned_at IS NOT NULL
            THEN EXCLUDED.requested_at
            ELSE COALESCE(
                EXCLUDED.requested_at,
                pull_request_review_requests.requested_at
            )
        END,
        first_seen_at = CASE
            WHEN pull_request_review_requests.tombstoned_at IS NOT NULL
            THEN EXCLUDED.first_seen_at
            ELSE pull_request_review_requests.first_seen_at
        END,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE pull_request_review_requests.tombstoned_at IS NOT NULL
       OR ROW(
           EXCLUDED.reviewer_node_id,
           EXCLUDED.reviewer_login,
           CASE
               WHEN pull_request_review_requests.tombstoned_at IS NOT NULL
               THEN EXCLUDED.requested_at
               ELSE COALESCE(
                   EXCLUDED.requested_at,
                   pull_request_review_requests.requested_at
               )
           END,
           EXCLUDED.head_sha
       ) IS DISTINCT FROM ROW(
           pull_request_review_requests.reviewer_node_id,
           pull_request_review_requests.reviewer_login,
           pull_request_review_requests.requested_at,
           pull_request_review_requests.head_sha
       )
    RETURNING reviewer_kind || ':' || reviewer_gh_id::text AS reviewer_key
),
tombstoned AS (
    UPDATE pull_request_review_requests
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND EXISTS (SELECT 1 FROM eligible)
      AND NOT EXISTS (
          SELECT 1
          FROM input
          WHERE input.reviewer_kind =
                    pull_request_review_requests.reviewer_kind
            AND input.reviewer_gh_id =
                    pull_request_review_requests.reviewer_gh_id
      )
    RETURNING reviewer_kind || ':' || reviewer_gh_id::text AS reviewer_key
)
SELECT reviewer_key FROM upserted
UNION ALL
SELECT reviewer_key FROM tombstoned;

-- name: TouchPullRequestReviewRequestsCheckedAt :exec
UPDATE pull_request_review_requests
SET last_checked_at = GREATEST(
        pull_request_review_requests.last_checked_at,
        sqlc.arg(checked_at)
    ),
    etag = CASE WHEN sqlc.arg(etag)::text = ''
                THEN pull_request_review_requests.etag
                ELSE sqlc.arg(etag)::text END
WHERE pull_request_review_requests.repo_id = sqlc.arg(repo_id)
  AND pull_request_review_requests.pr_number = sqlc.arg(pr_number)
  AND pull_request_review_requests.tombstoned_at IS NULL
  AND EXISTS (
      SELECT 1
      FROM pull_requests
      WHERE pull_requests.repo_id = sqlc.arg(repo_id)
        AND pull_requests.number = sqlc.arg(pr_number)
        AND pull_requests.tombstoned_at IS NULL
        AND (
            pull_requests.gh_updated_at IS NULL
            OR pull_requests.gh_updated_at <= sqlc.arg(gh_updated_at)
        )
  );

-- name: TombstonePullRequestReviewRequests :many
UPDATE pull_request_review_requests
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(
        last_checked_at,
        sqlc.arg(tombstoned_at)
    ),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL
RETURNING reviewer_kind || ':' || reviewer_gh_id::text AS reviewer_key;

-- name: ReplacePullRequestReviews :many
WITH input AS (
    SELECT NULLIF((element->>'gh_id')::bigint, 0) AS gh_id,
           element->>'node_id' AS node_id,
           element->>'author_kind' AS author_kind,
           NULLIF(element->>'author_node_id', '') AS author_node_id,
           NULLIF(element->>'author_login', '') AS author_login,
           element->>'state' AS state,
           (element->>'submitted_at')::timestamptz AS submitted_at,
           NULLIF(element->>'commit_oid', '') AS commit_oid,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at
    FROM jsonb_array_elements(sqlc.arg(reviews)::jsonb) AS element
),
eligible AS (
    SELECT pull_requests.repo_id
    FROM pull_requests
    WHERE pull_requests.repo_id = sqlc.arg(repo_id)
      AND pull_requests.number = sqlc.arg(pr_number)
      AND pull_requests.tombstoned_at IS NULL
      AND (
          pull_requests.gh_updated_at IS NULL
          OR pull_requests.gh_updated_at <= sqlc.arg(parent_gh_updated_at)
      )
),
upserted AS (
    INSERT INTO pull_request_reviews (
        gh_id, node_id, repo_id, pr_number, author_kind, author_node_id,
        author_login, state, submitted_at, commit_oid, gh_updated_at,
        head_sha, synced_at, etag, sync_source, tombstoned_at,
        last_checked_at
    )
    SELECT input.gh_id, input.node_id, sqlc.arg(repo_id),
           sqlc.arg(pr_number), input.author_kind, input.author_node_id,
           input.author_login, input.state, input.submitted_at,
           input.commit_oid, input.gh_updated_at, sqlc.arg(head_sha),
           sqlc.arg(synced_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL,
           sqlc.arg(last_checked_at)
    FROM input
    WHERE EXISTS (SELECT 1 FROM eligible)
    ON CONFLICT (node_id) DO UPDATE
    SET gh_id = EXCLUDED.gh_id,
        repo_id = EXCLUDED.repo_id,
        pr_number = EXCLUDED.pr_number,
        author_kind = EXCLUDED.author_kind,
        author_node_id = EXCLUDED.author_node_id,
        author_login = EXCLUDED.author_login,
        state = EXCLUDED.state,
        submitted_at = EXCLUDED.submitted_at,
        commit_oid = EXCLUDED.commit_oid,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE ROW(
              CASE
                  WHEN EXCLUDED.state = 'dismissed' THEN 2
                  WHEN EXCLUDED.submitted_at IS NOT NULL THEN 1
                  ELSE 0
              END,
              COALESCE(
                  EXCLUDED.submitted_at,
                  '-infinity'::timestamptz
              ),
              EXCLUDED.gh_updated_at
          ) > ROW(
              CASE
                  WHEN pull_request_reviews.state = 'dismissed' THEN 2
                  WHEN pull_request_reviews.submitted_at IS NOT NULL THEN 1
                  ELSE 0
              END,
              COALESCE(
                  pull_request_reviews.submitted_at,
                  '-infinity'::timestamptz
              ),
              pull_request_reviews.gh_updated_at
          )
       OR (
           ROW(
               CASE
                   WHEN EXCLUDED.state = 'dismissed' THEN 2
                   WHEN EXCLUDED.submitted_at IS NOT NULL THEN 1
                   ELSE 0
               END,
               COALESCE(
                   EXCLUDED.submitted_at,
                   '-infinity'::timestamptz
               ),
               EXCLUDED.gh_updated_at
           ) = ROW(
               CASE
                   WHEN pull_request_reviews.state = 'dismissed' THEN 2
                   WHEN pull_request_reviews.submitted_at IS NOT NULL THEN 1
                   ELSE 0
               END,
               COALESCE(
                   pull_request_reviews.submitted_at,
                   '-infinity'::timestamptz
               ),
               pull_request_reviews.gh_updated_at
           )
           AND (
               (
                   pull_request_reviews.tombstoned_at IS NULL
                   AND ROW(
                   EXCLUDED.gh_id, EXCLUDED.repo_id, EXCLUDED.pr_number,
                   EXCLUDED.author_kind, EXCLUDED.author_node_id,
                   EXCLUDED.author_login, EXCLUDED.state,
                   EXCLUDED.submitted_at, EXCLUDED.commit_oid,
                   EXCLUDED.head_sha
                   ) IS DISTINCT FROM ROW(
                   pull_request_reviews.gh_id,
                   pull_request_reviews.repo_id,
                   pull_request_reviews.pr_number,
                   pull_request_reviews.author_kind,
                   pull_request_reviews.author_node_id,
                   pull_request_reviews.author_login,
                   pull_request_reviews.state,
                   pull_request_reviews.submitted_at,
                   pull_request_reviews.commit_oid,
                       pull_request_reviews.head_sha
                   )
               )
               OR (
                   pull_request_reviews.tombstoned_at IS NOT NULL
                   AND EXCLUDED.last_checked_at >
                       pull_request_reviews.tombstoned_at
                   AND EXISTS (SELECT 1 FROM eligible)
               )
           )
       )
    RETURNING node_id
),
tombstoned AS (
    UPDATE pull_request_reviews
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND EXISTS (SELECT 1 FROM eligible)
      AND last_checked_at <= sqlc.arg(last_checked_at)
      AND NOT EXISTS (
          SELECT 1 FROM input
          WHERE input.node_id = pull_request_reviews.node_id
      )
    RETURNING node_id
)
SELECT node_id FROM upserted
UNION ALL
SELECT node_id FROM tombstoned;

-- name: TouchPullRequestReviewsCheckedAt :exec
UPDATE pull_request_reviews
SET last_checked_at = GREATEST(
        pull_request_reviews.last_checked_at,
        sqlc.arg(checked_at)
    ),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE pull_request_reviews.repo_id = sqlc.arg(repo_id)
  AND pull_request_reviews.pr_number = sqlc.arg(pr_number)
  AND pull_request_reviews.tombstoned_at IS NULL
  AND EXISTS (
      SELECT 1 FROM pull_requests
      WHERE pull_requests.repo_id = sqlc.arg(repo_id)
        AND pull_requests.number = sqlc.arg(pr_number)
        AND pull_requests.tombstoned_at IS NULL
        AND (
            pull_requests.gh_updated_at IS NULL
            OR pull_requests.gh_updated_at <= sqlc.arg(parent_gh_updated_at)
        )
  );

-- name: TombstonePullRequestReviews :many
UPDATE pull_request_reviews
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL
RETURNING node_id;

-- name: ReplacePullRequestComments :many
WITH input AS (
    SELECT NULLIF((element->>'gh_id')::bigint, 0) AS gh_id,
           element->>'node_id' AS node_id,
           element->>'author_kind' AS author_kind,
           NULLIF(element->>'author_node_id', '') AS author_node_id,
           NULLIF(element->>'author_login', '') AS author_login,
           (element->>'created_at')::timestamptz AS created_at,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at
    FROM jsonb_array_elements(sqlc.arg(comments)::jsonb) AS element
),
eligible AS (
    SELECT pull_requests.repo_id
    FROM pull_requests
    WHERE pull_requests.repo_id = sqlc.arg(repo_id)
      AND pull_requests.number = sqlc.arg(pr_number)
      AND pull_requests.tombstoned_at IS NULL
      AND (
          pull_requests.gh_updated_at IS NULL
          OR pull_requests.gh_updated_at <= sqlc.arg(parent_gh_updated_at)
      )
),
upserted AS (
    INSERT INTO pull_request_comments (
        gh_id, node_id, repo_id, pr_number, author_kind, author_node_id,
        author_login, created_at, gh_updated_at, head_sha, synced_at, etag,
        sync_source, tombstoned_at, last_checked_at
    )
    SELECT input.gh_id, input.node_id, sqlc.arg(repo_id),
           sqlc.arg(pr_number), input.author_kind, input.author_node_id,
           input.author_login, input.created_at, input.gh_updated_at,
           sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL, sqlc.arg(last_checked_at)
    FROM input
    WHERE EXISTS (SELECT 1 FROM eligible)
    ON CONFLICT (node_id) DO UPDATE
    SET gh_id = EXCLUDED.gh_id,
        repo_id = EXCLUDED.repo_id,
        pr_number = EXCLUDED.pr_number,
        author_kind = EXCLUDED.author_kind,
        author_node_id = EXCLUDED.author_node_id,
        author_login = EXCLUDED.author_login,
        created_at = EXCLUDED.created_at,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE EXCLUDED.gh_updated_at > pull_request_comments.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at = pull_request_comments.gh_updated_at
           AND (
               (
                   pull_request_comments.tombstoned_at IS NULL
                   AND ROW(
                   EXCLUDED.gh_id, EXCLUDED.repo_id, EXCLUDED.pr_number,
                   EXCLUDED.author_kind, EXCLUDED.author_node_id,
                   EXCLUDED.author_login, EXCLUDED.created_at,
                   EXCLUDED.head_sha
                   ) IS DISTINCT FROM ROW(
                   pull_request_comments.gh_id,
                   pull_request_comments.repo_id,
                   pull_request_comments.pr_number,
                   pull_request_comments.author_kind,
                   pull_request_comments.author_node_id,
                   pull_request_comments.author_login,
                   pull_request_comments.created_at,
                       pull_request_comments.head_sha
                   )
               )
               OR (
                   pull_request_comments.tombstoned_at IS NOT NULL
                   AND EXCLUDED.last_checked_at >
                       pull_request_comments.tombstoned_at
                   AND EXISTS (SELECT 1 FROM eligible)
               )
           )
       )
    RETURNING node_id
),
tombstoned AS (
    UPDATE pull_request_comments
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND EXISTS (SELECT 1 FROM eligible)
      AND last_checked_at <= sqlc.arg(last_checked_at)
      AND NOT EXISTS (
          SELECT 1 FROM input
          WHERE input.node_id = pull_request_comments.node_id
      )
    RETURNING node_id
)
SELECT node_id FROM upserted
UNION ALL
SELECT node_id FROM tombstoned;

-- name: TouchPullRequestCommentsCheckedAt :exec
UPDATE pull_request_comments
SET last_checked_at = GREATEST(
        pull_request_comments.last_checked_at,
        sqlc.arg(checked_at)
    ),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE pull_request_comments.repo_id = sqlc.arg(repo_id)
  AND pull_request_comments.pr_number = sqlc.arg(pr_number)
  AND pull_request_comments.tombstoned_at IS NULL
  AND EXISTS (
      SELECT 1 FROM pull_requests
      WHERE pull_requests.repo_id = sqlc.arg(repo_id)
        AND pull_requests.number = sqlc.arg(pr_number)
        AND pull_requests.tombstoned_at IS NULL
        AND (
            pull_requests.gh_updated_at IS NULL
            OR pull_requests.gh_updated_at <= sqlc.arg(parent_gh_updated_at)
        )
  );

-- name: TombstonePullRequestComments :many
UPDATE pull_request_comments
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL
RETURNING node_id;

-- name: UpsertPullRequestChangeSnapshot :one
-- C-C2 parent freshness plus explicit base/head identity gates the current
-- changed-file and ownership snapshot. Equal parent versions remain eligible
-- so a base-branch CODEOWNERS push or a later complete listing can heal facts
-- without relying on pull_request.updatedAt changing.
WITH eligible AS (
    SELECT pull_requests.repo_id
    FROM pull_requests
    WHERE pull_requests.repo_id = sqlc.arg(repo_id)
      AND pull_requests.number = sqlc.arg(pr_number)
      AND pull_requests.tombstoned_at IS NULL
      AND pull_requests.head_sha = sqlc.arg(head_sha)
      AND pull_requests.base_sha = sqlc.arg(base_sha)
      AND (
          pull_requests.gh_updated_at IS NULL
          OR pull_requests.gh_updated_at <= sqlc.arg(parent_gh_updated_at)
      )
),
prior AS MATERIALIZED (
    SELECT base_sha, head_sha, files_total_count, files_truncated,
           codeowners_ref, codeowners_sha, codeowners_path,
           codeowners_state, codeowners_source, codeowners_hash,
           tombstoned_at
    FROM pull_request_change_snapshots
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
),
upserted AS (
    INSERT INTO pull_request_change_snapshots (
        repo_id, pr_number, base_sha, head_sha, files_total_count,
        files_truncated, codeowners_ref, codeowners_sha, codeowners_path,
        codeowners_state, codeowners_source, codeowners_hash,
        parent_gh_updated_at, synced_at, etag, sync_source, tombstoned_at,
        last_checked_at
    )
    SELECT sqlc.arg(repo_id), sqlc.arg(pr_number), sqlc.arg(base_sha),
           sqlc.arg(head_sha), sqlc.arg(files_total_count),
           sqlc.arg(files_truncated), sqlc.arg(codeowners_ref),
           sqlc.arg(codeowners_sha), sqlc.narg(codeowners_path),
           sqlc.arg(codeowners_state), sqlc.narg(codeowners_source),
           sqlc.arg(codeowners_hash), sqlc.arg(parent_gh_updated_at),
           sqlc.arg(synced_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL,
           sqlc.arg(last_checked_at)
    FROM eligible
    ON CONFLICT (repo_id, pr_number) DO UPDATE
    SET base_sha = EXCLUDED.base_sha,
        head_sha = EXCLUDED.head_sha,
        files_total_count = EXCLUDED.files_total_count,
        files_truncated = EXCLUDED.files_truncated,
        codeowners_ref = EXCLUDED.codeowners_ref,
        codeowners_sha = EXCLUDED.codeowners_sha,
        codeowners_path = EXCLUDED.codeowners_path,
        codeowners_state = EXCLUDED.codeowners_state,
        codeowners_source = EXCLUDED.codeowners_source,
        codeowners_hash = EXCLUDED.codeowners_hash,
        parent_gh_updated_at = EXCLUDED.parent_gh_updated_at,
        synced_at = CASE
            WHEN pull_request_change_snapshots.tombstoned_at IS NOT NULL
              OR ROW(
                     EXCLUDED.base_sha, EXCLUDED.head_sha,
                     EXCLUDED.files_total_count, EXCLUDED.files_truncated,
                     EXCLUDED.codeowners_ref, EXCLUDED.codeowners_sha,
                     EXCLUDED.codeowners_path, EXCLUDED.codeowners_state,
                     EXCLUDED.codeowners_source, EXCLUDED.codeowners_hash
                 ) IS DISTINCT FROM ROW(
                     pull_request_change_snapshots.base_sha,
                     pull_request_change_snapshots.head_sha,
                     pull_request_change_snapshots.files_total_count,
                     pull_request_change_snapshots.files_truncated,
                     pull_request_change_snapshots.codeowners_ref,
                     pull_request_change_snapshots.codeowners_sha,
                     pull_request_change_snapshots.codeowners_path,
                     pull_request_change_snapshots.codeowners_state,
                     pull_request_change_snapshots.codeowners_source,
                     pull_request_change_snapshots.codeowners_hash
                 )
            THEN EXCLUDED.synced_at
            ELSE pull_request_change_snapshots.synced_at
        END,
        etag = EXCLUDED.etag,
        sync_source = CASE
            WHEN pull_request_change_snapshots.tombstoned_at IS NOT NULL
              OR ROW(
                     EXCLUDED.base_sha, EXCLUDED.head_sha,
                     EXCLUDED.files_total_count, EXCLUDED.files_truncated,
                     EXCLUDED.codeowners_ref, EXCLUDED.codeowners_sha,
                     EXCLUDED.codeowners_path, EXCLUDED.codeowners_state,
                     EXCLUDED.codeowners_source, EXCLUDED.codeowners_hash
                 ) IS DISTINCT FROM ROW(
                     pull_request_change_snapshots.base_sha,
                     pull_request_change_snapshots.head_sha,
                     pull_request_change_snapshots.files_total_count,
                     pull_request_change_snapshots.files_truncated,
                     pull_request_change_snapshots.codeowners_ref,
                     pull_request_change_snapshots.codeowners_sha,
                     pull_request_change_snapshots.codeowners_path,
                     pull_request_change_snapshots.codeowners_state,
                     pull_request_change_snapshots.codeowners_source,
                     pull_request_change_snapshots.codeowners_hash
                 )
            THEN EXCLUDED.sync_source
            ELSE pull_request_change_snapshots.sync_source
        END,
        tombstoned_at = NULL,
        last_checked_at = EXCLUDED.last_checked_at
    WHERE EXCLUDED.parent_gh_updated_at >
              pull_request_change_snapshots.parent_gh_updated_at
       OR (
           EXCLUDED.parent_gh_updated_at =
               pull_request_change_snapshots.parent_gh_updated_at
           AND ROW(
               EXCLUDED.base_sha, EXCLUDED.head_sha,
               EXCLUDED.files_total_count, EXCLUDED.files_truncated,
               EXCLUDED.codeowners_ref, EXCLUDED.codeowners_sha,
               EXCLUDED.codeowners_path, EXCLUDED.codeowners_state,
               EXCLUDED.codeowners_source, EXCLUDED.codeowners_hash
           ) IS DISTINCT FROM ROW(
               pull_request_change_snapshots.base_sha,
               pull_request_change_snapshots.head_sha,
               pull_request_change_snapshots.files_total_count,
               pull_request_change_snapshots.files_truncated,
               pull_request_change_snapshots.codeowners_ref,
               pull_request_change_snapshots.codeowners_sha,
               pull_request_change_snapshots.codeowners_path,
               pull_request_change_snapshots.codeowners_state,
               pull_request_change_snapshots.codeowners_source,
               pull_request_change_snapshots.codeowners_hash
           )
       )
       OR (
           pull_request_change_snapshots.tombstoned_at IS NOT NULL
           AND EXCLUDED.last_checked_at >
               pull_request_change_snapshots.tombstoned_at
       )
    RETURNING repo_id
)
SELECT count(*)
FROM upserted
WHERE NOT EXISTS (SELECT 1 FROM prior)
   OR EXISTS (
       SELECT 1
       FROM prior
       WHERE prior.tombstoned_at IS NOT NULL
          OR ROW(
                 prior.base_sha, prior.head_sha, prior.files_total_count,
                 prior.files_truncated, prior.codeowners_ref,
                 prior.codeowners_sha, prior.codeowners_path,
                 prior.codeowners_state, prior.codeowners_source,
                 prior.codeowners_hash
             ) IS DISTINCT FROM ROW(
                 sqlc.arg(base_sha)::text, sqlc.arg(head_sha)::text,
                 sqlc.arg(files_total_count)::integer,
                 sqlc.arg(files_truncated)::boolean,
                 sqlc.arg(codeowners_ref)::text,
                 sqlc.arg(codeowners_sha)::text,
                 sqlc.narg(codeowners_path)::text,
                 sqlc.arg(codeowners_state)::text,
                 sqlc.narg(codeowners_source)::text,
                 sqlc.arg(codeowners_hash)::text
             )
   );

-- name: ReplacePullRequestChangedFiles :many
WITH input AS (
    SELECT element->>'path' AS path,
           NULLIF(element->>'previous_path', '') AS previous_path,
           element->>'change_type' AS change_type
    FROM jsonb_array_elements(sqlc.arg(changed_files)::jsonb) AS element
),
eligible AS (
    SELECT snapshot.repo_id
    FROM pull_request_change_snapshots AS snapshot
    WHERE snapshot.repo_id = sqlc.arg(repo_id)
      AND snapshot.pr_number = sqlc.arg(pr_number)
      AND snapshot.tombstoned_at IS NULL
      AND snapshot.base_sha = sqlc.arg(base_sha)
      AND snapshot.head_sha = sqlc.arg(head_sha)
      AND snapshot.parent_gh_updated_at <= sqlc.arg(parent_gh_updated_at)
),
upserted AS (
    INSERT INTO pull_request_changed_files (
        repo_id, pr_number, path, previous_path, change_type, base_sha,
        head_sha, synced_at, etag, sync_source, tombstoned_at,
        last_checked_at
    )
    SELECT sqlc.arg(repo_id), sqlc.arg(pr_number), input.path,
           input.previous_path, input.change_type, sqlc.arg(base_sha),
           sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL, sqlc.arg(last_checked_at)
    FROM input
    CROSS JOIN eligible
    ON CONFLICT (repo_id, pr_number, path) DO UPDATE
    SET previous_path = EXCLUDED.previous_path,
        change_type = EXCLUDED.change_type,
        base_sha = EXCLUDED.base_sha,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL,
        last_checked_at = EXCLUDED.last_checked_at
    WHERE ROW(
              EXCLUDED.previous_path, EXCLUDED.change_type,
              EXCLUDED.base_sha, EXCLUDED.head_sha
          ) IS DISTINCT FROM ROW(
              pull_request_changed_files.previous_path,
              pull_request_changed_files.change_type,
              pull_request_changed_files.base_sha,
              pull_request_changed_files.head_sha
          )
       OR pull_request_changed_files.tombstoned_at IS NOT NULL
    RETURNING path
),
tombstoned AS (
    UPDATE pull_request_changed_files
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND EXISTS (SELECT 1 FROM eligible)
      AND NOT EXISTS (
          SELECT 1 FROM input
          WHERE input.path = pull_request_changed_files.path
      )
    RETURNING path
)
SELECT path FROM upserted
UNION ALL
SELECT path FROM tombstoned;

-- name: ReplacePullRequestFileOwners :many
WITH input AS (
    SELECT element->>'path' AS path,
           element->>'owner_token' AS owner_token,
           element->>'owner_type' AS owner_type,
           element->>'owner_name' AS owner_name,
           element->>'resolution_state' AS resolution_state,
           NULLIF((element->>'owner_gh_id')::bigint, 0) AS owner_gh_id,
           NULLIF(element->>'owner_node_id', '') AS owner_node_id,
           NULLIF(element->>'owner_login', '') AS owner_login,
           element->>'source_pattern' AS source_pattern,
           (element->>'source_line')::integer AS source_line
    FROM jsonb_array_elements(sqlc.arg(file_owners)::jsonb) AS element
),
eligible AS (
    SELECT snapshot.repo_id
    FROM pull_request_change_snapshots AS snapshot
    WHERE snapshot.repo_id = sqlc.arg(repo_id)
      AND snapshot.pr_number = sqlc.arg(pr_number)
      AND snapshot.tombstoned_at IS NULL
      AND snapshot.base_sha = sqlc.arg(base_sha)
      AND snapshot.head_sha = sqlc.arg(head_sha)
      AND snapshot.parent_gh_updated_at <= sqlc.arg(parent_gh_updated_at)
),
upserted AS (
    INSERT INTO pull_request_file_owners (
        repo_id, pr_number, path, owner_token, owner_type, owner_name,
        resolution_state, owner_gh_id, owner_node_id, owner_login,
        source_pattern, source_line, base_sha, head_sha, synced_at, etag,
        sync_source, tombstoned_at, last_checked_at
    )
    SELECT sqlc.arg(repo_id), sqlc.arg(pr_number), input.path,
           input.owner_token, input.owner_type, input.owner_name,
           input.resolution_state,
           input.owner_gh_id, input.owner_node_id, input.owner_login,
           input.source_pattern, input.source_line, sqlc.arg(base_sha),
           sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL, sqlc.arg(last_checked_at)
    FROM input
    CROSS JOIN eligible
    ON CONFLICT (repo_id, pr_number, path, owner_token) DO UPDATE
    SET owner_type = EXCLUDED.owner_type,
        owner_name = EXCLUDED.owner_name,
        resolution_state = EXCLUDED.resolution_state,
        owner_gh_id = EXCLUDED.owner_gh_id,
        owner_node_id = EXCLUDED.owner_node_id,
        owner_login = EXCLUDED.owner_login,
        source_pattern = EXCLUDED.source_pattern,
        source_line = EXCLUDED.source_line,
        base_sha = EXCLUDED.base_sha,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL,
        last_checked_at = EXCLUDED.last_checked_at
    WHERE ROW(
              EXCLUDED.owner_type, EXCLUDED.owner_name,
              EXCLUDED.resolution_state,
              EXCLUDED.owner_gh_id, EXCLUDED.owner_node_id,
              EXCLUDED.owner_login, EXCLUDED.source_pattern,
              EXCLUDED.source_line, EXCLUDED.base_sha, EXCLUDED.head_sha
          ) IS DISTINCT FROM ROW(
              pull_request_file_owners.owner_type,
              pull_request_file_owners.owner_name,
              pull_request_file_owners.resolution_state,
              pull_request_file_owners.owner_gh_id,
              pull_request_file_owners.owner_node_id,
              pull_request_file_owners.owner_login,
              pull_request_file_owners.source_pattern,
              pull_request_file_owners.source_line,
              pull_request_file_owners.base_sha,
              pull_request_file_owners.head_sha
          )
       OR pull_request_file_owners.tombstoned_at IS NOT NULL
    RETURNING (path || ':' || owner_token)::text AS owner_key
),
tombstoned AS (
    UPDATE pull_request_file_owners
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND EXISTS (SELECT 1 FROM eligible)
      AND NOT EXISTS (
          SELECT 1 FROM input
          WHERE input.path = pull_request_file_owners.path
            AND input.owner_token = pull_request_file_owners.owner_token
      )
    RETURNING (path || ':' || owner_token)::text AS owner_key
)
SELECT owner_key FROM upserted
UNION ALL
SELECT owner_key FROM tombstoned;

-- name: TouchPullRequestChangeInputsCheckedAt :exec
WITH eligible AS (
    SELECT snapshot.repo_id
    FROM pull_request_change_snapshots AS snapshot
    WHERE snapshot.repo_id = sqlc.arg(repo_id)
      AND snapshot.pr_number = sqlc.arg(pr_number)
      AND snapshot.tombstoned_at IS NULL
      AND snapshot.parent_gh_updated_at <= sqlc.arg(parent_gh_updated_at)
)
UPDATE pull_request_change_snapshots AS snapshot
SET last_checked_at = GREATEST(snapshot.last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN snapshot.etag
                ELSE sqlc.arg(etag)::text END
WHERE snapshot.repo_id = sqlc.arg(repo_id)
  AND snapshot.pr_number = sqlc.arg(pr_number)
  AND EXISTS (SELECT 1 FROM eligible);

-- name: TouchPullRequestChangedFilesCheckedAt :exec
UPDATE pull_request_changed_files AS file
SET last_checked_at = GREATEST(file.last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN file.etag
                ELSE sqlc.arg(etag)::text END
WHERE file.repo_id = sqlc.arg(repo_id)
  AND file.pr_number = sqlc.arg(pr_number)
  AND file.tombstoned_at IS NULL
  AND EXISTS (
      SELECT 1 FROM pull_request_change_snapshots AS snapshot
      WHERE snapshot.repo_id = sqlc.arg(repo_id)
        AND snapshot.pr_number = sqlc.arg(pr_number)
        AND snapshot.tombstoned_at IS NULL
        AND snapshot.parent_gh_updated_at <= sqlc.arg(parent_gh_updated_at)
  );

-- name: TouchPullRequestFileOwnersCheckedAt :exec
UPDATE pull_request_file_owners AS owner
SET last_checked_at = GREATEST(owner.last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN owner.etag
                ELSE sqlc.arg(etag)::text END
WHERE owner.repo_id = sqlc.arg(repo_id)
  AND owner.pr_number = sqlc.arg(pr_number)
  AND owner.tombstoned_at IS NULL
  AND EXISTS (
      SELECT 1 FROM pull_request_change_snapshots AS snapshot
      WHERE snapshot.repo_id = sqlc.arg(repo_id)
        AND snapshot.pr_number = sqlc.arg(pr_number)
        AND snapshot.tombstoned_at IS NULL
        AND snapshot.parent_gh_updated_at <= sqlc.arg(parent_gh_updated_at)
  );

-- name: TombstonePullRequestChangeSnapshot :execrows
UPDATE pull_request_change_snapshots
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL;

-- name: TombstonePullRequestChangedFiles :execrows
UPDATE pull_request_changed_files
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL;

-- name: TombstonePullRequestFileOwners :execrows
UPDATE pull_request_file_owners
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(tombstoned_at),
    last_checked_at = GREATEST(last_checked_at, sqlc.arg(tombstoned_at)),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND pr_number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL;

-- name: ListCodeOwnerIdentities :many
WITH candidates AS (
    SELECT request.reviewer_kind AS owner_type,
           request.reviewer_gh_id AS owner_gh_id,
           request.reviewer_node_id AS owner_node_id,
           request.reviewer_login AS owner_login,
           request.last_checked_at
    FROM pull_request_review_requests AS request
    WHERE request.repo_id = sqlc.arg(repo_id)

    UNION ALL

    SELECT 'user'::text, NULL::bigint, review.author_node_id,
           review.author_login, review.last_checked_at
    FROM pull_request_reviews AS review
    WHERE review.repo_id = sqlc.arg(repo_id)
      AND review.author_kind = 'user'
      AND review.author_node_id IS NOT NULL
      AND review.author_login IS NOT NULL

    UNION ALL

    SELECT 'user'::text, NULL::bigint, comment.author_node_id,
           comment.author_login, comment.last_checked_at
    FROM pull_request_comments AS comment
    WHERE comment.repo_id = sqlc.arg(repo_id)
      AND comment.author_kind = 'user'
      AND comment.author_node_id IS NOT NULL
      AND comment.author_login IS NOT NULL
)
SELECT DISTINCT ON (owner_type, lower(owner_login))
       owner_type, COALESCE(owner_gh_id, 0)::bigint AS owner_gh_id,
       owner_node_id, owner_login
FROM candidates
ORDER BY owner_type, lower(owner_login),
         owner_gh_id IS NOT NULL DESC, last_checked_at DESC,
         owner_node_id;

-- name: ReplaceCheckRuns :many
WITH input AS (
    SELECT (element->>'gh_id')::bigint AS gh_id,
           element->>'node_id' AS node_id,
           element->>'name' AS name,
           element->>'status' AS status,
           element->>'conclusion' AS conclusion,
           element->>'details_url' AS details_url,
           element->>'app_slug' AS app_slug,
           (element->>'started_at')::timestamptz AS started_at,
           (element->>'completed_at')::timestamptz AS completed_at,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at,
           element->>'semantic_version' AS semantic_version,
           element->'observed' AS observed
    FROM jsonb_array_elements(sqlc.arg(check_runs)::jsonb) AS element
),
upserted AS (
    INSERT INTO check_runs (
        gh_id, repo_id, node_id, name, status, conclusion, details_url,
        app_slug, started_at, completed_at, gh_updated_at, semantic_version,
        head_sha, synced_at, last_checked_at, etag, sync_source, tombstoned_at
    )
    SELECT input.gh_id, sqlc.arg(repo_id), input.node_id, input.name,
           input.status, input.conclusion, input.details_url, input.app_slug,
           input.started_at, input.completed_at, input.gh_updated_at,
           input.semantic_version, sqlc.arg(head_sha), sqlc.arg(synced_at),
           sqlc.arg(last_checked_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL
    FROM input
    ON CONFLICT (gh_id) DO UPDATE
    SET repo_id = EXCLUDED.repo_id,
        node_id = EXCLUDED.node_id,
        name = EXCLUDED.name,
        status = EXCLUDED.status,
        conclusion = EXCLUDED.conclusion,
        details_url = EXCLUDED.details_url,
        app_slug = EXCLUDED.app_slug,
        started_at = EXCLUDED.started_at,
        completed_at = EXCLUDED.completed_at,
        gh_updated_at = EXCLUDED.gh_updated_at,
        semantic_version = EXCLUDED.semantic_version,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE (
           check_runs.gh_updated_at IS NULL
           AND EXCLUDED.gh_updated_at IS NOT NULL
       )
       OR EXCLUDED.gh_updated_at > check_runs.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at IS NOT DISTINCT FROM check_runs.gh_updated_at
           AND ROW(
               EXCLUDED.repo_id, EXCLUDED.node_id, EXCLUDED.name,
               EXCLUDED.status, EXCLUDED.conclusion, EXCLUDED.details_url,
               EXCLUDED.app_slug, EXCLUDED.started_at, EXCLUDED.completed_at,
               EXCLUDED.semantic_version, EXCLUDED.head_sha
           ) IS DISTINCT FROM ROW(
               check_runs.repo_id, check_runs.node_id, check_runs.name,
               check_runs.status, check_runs.conclusion,
               check_runs.details_url, check_runs.app_slug,
               check_runs.started_at, check_runs.completed_at,
               check_runs.semantic_version, check_runs.head_sha
           )
       )
       OR (
           check_runs.tombstoned_at IS NOT NULL
           AND EXCLUDED.last_checked_at > check_runs.tombstoned_at
       )
    RETURNING gh_id
),
tombstoned AS (
    UPDATE check_runs
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND head_sha = sqlc.arg(head_sha)
      AND tombstoned_at IS NULL
      AND last_checked_at <= sqlc.arg(last_checked_at)
      AND NOT EXISTS (
          SELECT 1 FROM input WHERE input.gh_id = check_runs.gh_id
      )
    RETURNING gh_id
)
SELECT gh_id FROM upserted
UNION ALL
SELECT gh_id FROM tombstoned;

-- name: TouchCheckRunsCheckedAt :exec
UPDATE check_runs
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at)),
    etag = CASE WHEN sqlc.arg(etag)::text = '' THEN etag
                ELSE sqlc.arg(etag)::text END
WHERE repo_id = sqlc.arg(repo_id)
  AND head_sha = sqlc.arg(head_sha);

-- name: AppendAcceptedCheckHistory :exec
WITH input AS (
    SELECT (element->>'gh_id')::bigint AS gh_id,
           element->>'name' AS name,
           element->>'status' AS status,
           element->>'conclusion' AS conclusion,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at,
           element->>'semantic_version' AS semantic_version,
           element->'observed' AS observed
    FROM jsonb_array_elements(sqlc.arg(check_runs)::jsonb) AS element
)
INSERT INTO check_history (
    check_run_gh_id, repo_id, name, status, conclusion, observed,
    gh_updated_at, semantic_version, head_sha, synced_at, etag, sync_source,
    tombstoned_at
)
SELECT input.gh_id, sqlc.arg(repo_id), input.name, input.status,
       input.conclusion, input.observed, input.gh_updated_at,
       input.semantic_version, sqlc.arg(head_sha), sqlc.arg(synced_at),
       sqlc.arg(etag), sqlc.arg(sync_source), NULL
FROM input
JOIN check_runs ON check_runs.gh_id = input.gh_id
WHERE check_runs.repo_id = sqlc.arg(repo_id)
  AND check_runs.head_sha = sqlc.arg(head_sha)
  AND check_runs.synced_at = sqlc.arg(synced_at)
  AND check_runs.status = input.status
  AND check_runs.conclusion = input.conclusion
  AND check_runs.semantic_version = input.semantic_version;

-- name: GetRepoRulesFetchMetadata :one
SELECT repos.id AS repo_id, repos.gh_id AS repo_gh_id,
       repos.installation_id, repos.full_name,
       COALESCE(repo_rule_sync_state.etag, '') AS etag
FROM repos
LEFT JOIN repo_rule_sync_state
    ON repo_rule_sync_state.repo_id = repos.id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name);

-- name: ReplaceRepoRules :many
WITH input AS (
    SELECT element->>'rule_key' AS rule_key,
           element->'rule' AS rule,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at,
           element->>'head_sha' AS head_sha
    FROM jsonb_array_elements(sqlc.arg(rules)::jsonb) AS element
),
upserted AS (
    INSERT INTO repo_rules (
        repo_id, rule_key, rule, gh_updated_at, head_sha, synced_at,
        last_checked_at, etag, sync_source, tombstoned_at
    )
    SELECT sqlc.arg(repo_id), input.rule_key, input.rule,
           input.gh_updated_at, input.head_sha, sqlc.arg(synced_at),
           sqlc.arg(last_checked_at), sqlc.arg(etag),
           sqlc.arg(sync_source), NULL
    FROM input
    ON CONFLICT (repo_id, rule_key) DO UPDATE
    SET rule = EXCLUDED.rule,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        last_checked_at = EXCLUDED.last_checked_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE (
           repo_rules.gh_updated_at IS NULL
           AND EXCLUDED.gh_updated_at IS NOT NULL
       )
       OR EXCLUDED.gh_updated_at > repo_rules.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at IS NOT DISTINCT FROM repo_rules.gh_updated_at
           AND ROW(EXCLUDED.rule, EXCLUDED.head_sha)
               IS DISTINCT FROM ROW(repo_rules.rule, repo_rules.head_sha)
       )
       OR (
           repo_rules.tombstoned_at IS NOT NULL
           AND EXCLUDED.last_checked_at > repo_rules.tombstoned_at
       )
    RETURNING rule_key
),
tombstoned AS (
    UPDATE repo_rules
    SET tombstoned_at = sqlc.arg(last_checked_at),
        synced_at = sqlc.arg(synced_at),
        last_checked_at = sqlc.arg(last_checked_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND tombstoned_at IS NULL
      AND last_checked_at <= sqlc.arg(last_checked_at)
      AND NOT EXISTS (
          SELECT 1 FROM input WHERE input.rule_key = repo_rules.rule_key
      )
    RETURNING rule_key
)
SELECT rule_key FROM upserted
UNION ALL
SELECT rule_key FROM tombstoned;

-- name: TouchRepoRulesCheckedAt :exec
UPDATE repo_rules
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at))
WHERE repo_id = sqlc.arg(repo_id);

-- name: UpsertRepoRuleSyncState :exec
INSERT INTO repo_rule_sync_state (repo_id, etag, last_checked_at)
VALUES (sqlc.arg(repo_id), sqlc.arg(etag), sqlc.arg(checked_at))
ON CONFLICT (repo_id) DO UPDATE
SET etag = CASE
        WHEN EXCLUDED.etag = '' THEN repo_rule_sync_state.etag
        ELSE EXCLUDED.etag
    END,
    last_checked_at = GREATEST(
        repo_rule_sync_state.last_checked_at,
        EXCLUDED.last_checked_at
    );

-- name: MarkDerivationDirty :exec
INSERT INTO derivation_dirty (scope_key, marked_at)
SELECT DISTINCT dirty.scope_key, sqlc.arg(marked_at)::timestamptz
FROM unnest(sqlc.arg(scope_keys)::text[]) AS dirty(scope_key)
ON CONFLICT (scope_key) DO UPDATE
SET marked_at = GREATEST(derivation_dirty.marked_at, EXCLUDED.marked_at);

-- name: InsertChangeEvent :one
INSERT INTO change_events (
    stream, kind, entity_key, occurred_at, payload
) VALUES (
    sqlc.arg(stream), sqlc.arg(kind), sqlc.arg(entity_key),
    sqlc.arg(occurred_at), sqlc.arg(payload)
)
RETURNING seq;

-- name: ListRepositoryDerivationScopes :many
SELECT scopes.scope_key::text
FROM (
    SELECT 'stack:' || repos.installation_id::text || ':' ||
           repos.gh_id::text || ':' || stacks.number::text AS scope_key
    FROM stacks
    JOIN repos ON repos.id = stacks.repo_id
    WHERE repos.id = sqlc.arg(repo_id)
      AND stacks.tombstoned_at IS NULL
    UNION
    SELECT 'pr:' || repos.installation_id::text || ':' ||
           repos.gh_id::text || ':' || pull_requests.number::text AS scope_key
    FROM pull_requests
    JOIN repos ON repos.id = pull_requests.repo_id
    WHERE repos.id = sqlc.arg(repo_id)
      AND pull_requests.tombstoned_at IS NULL
      AND pull_requests.stack_number IS NULL
) AS scopes
ORDER BY scope_key;

-- name: ListPRsAffectedByBranch :many
SELECT DISTINCT pull_requests.number, pull_requests.stack_number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.tombstoned_at IS NULL
  AND pull_requests.state = 'open'
  AND (
      pull_requests.head_ref = sqlc.arg(branch)
      OR pull_requests.base_ref = sqlc.arg(branch)
  )
ORDER BY pull_requests.number;

-- name: ListStacksAffectedByBranch :many
SELECT DISTINCT stacks.number
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND stacks.tombstoned_at IS NULL
  AND (
      stacks.base_ref = sqlc.arg(branch)
      OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements(stacks.entries) AS entry
          WHERE entry->>'head_ref' = sqlc.arg(branch)
      )
  )
ORDER BY stacks.number;

-- name: ListPRScopesByHeadSHA :many
SELECT pull_requests.number, pull_requests.stack_number,
       repos.gh_id AS repo_gh_id, repos.installation_id
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.gh_id = sqlc.arg(repo_gh_id)
  AND pull_requests.head_sha = sqlc.arg(head_sha)
  AND pull_requests.tombstoned_at IS NULL
ORDER BY pull_requests.number;

-- name: ListCachedPRMemberships :many
SELECT pull_requests.number, pull_requests.stack_number
FROM pull_requests
WHERE pull_requests.repo_id = sqlc.arg(repo_id)
  AND pull_requests.number = ANY(sqlc.arg(pr_numbers)::int[])
ORDER BY pull_requests.number;
