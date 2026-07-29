-- name: AcquireEntityAdvisoryLock :exec
-- C-C1: transaction-scoped serialization survives process crashes.
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: GetRepoByFullName :one
SELECT *
FROM repos
WHERE full_name = $1;

-- name: UpsertRepositoryWriteIfNewer :one
-- C-C2: equal versions are idempotent; only a strictly newer version may
-- resurrect a tombstone.
INSERT INTO repos (
    installation_id, org_id, gh_id, node_id, owner, name, full_name,
    default_branch, archived, gh_updated_at, head_sha, synced_at, etag,
    sync_source, tombstoned_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(org_id), sqlc.arg(gh_id),
    sqlc.arg(node_id), sqlc.arg(owner), sqlc.arg(name), sqlc.arg(full_name),
    sqlc.arg(default_branch), sqlc.arg(archived),
    sqlc.narg(gh_updated_at), sqlc.arg(head_sha), sqlc.arg(synced_at),
    sqlc.arg(etag), sqlc.arg(sync_source), NULL
)
ON CONFLICT (full_name) DO UPDATE
SET installation_id = EXCLUDED.installation_id,
    org_id = EXCLUDED.org_id,
    gh_id = EXCLUDED.gh_id,
    node_id = EXCLUDED.node_id,
    owner = EXCLUDED.owner,
    name = EXCLUDED.name,
    default_branch = EXCLUDED.default_branch,
    archived = EXCLUDED.archived,
    gh_updated_at = EXCLUDED.gh_updated_at,
    head_sha = EXCLUDED.head_sha,
    synced_at = EXCLUDED.synced_at,
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL
WHERE repos.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > repos.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at = repos.gh_updated_at
       AND EXCLUDED.head_sha = repos.head_sha
       AND EXCLUDED.synced_at >= repos.synced_at
       AND repos.tombstoned_at IS NULL
   )
RETURNING *;

-- name: GetPullRequestByKey :one
SELECT pull_requests.*, repos.full_name AS repo_full_name
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.number = sqlc.arg(pr_number);

-- name: GetPullRequestFetchMetadata :one
SELECT pull_requests.node_id, pull_requests.etag, pull_requests.stack_number,
       pull_requests.stack_position, pull_requests.head_sha
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.number = sqlc.arg(pr_number);

-- name: UpsertPullRequestWriteIfNewer :one
-- C-C2: (gh_updated_at, head_sha) is the monotonic PR version.
INSERT INTO pull_requests (
    repo_id, gh_id, node_id, number, title, state, draft, author_login,
    head_ref, head_sha, base_ref, base_sha, review_decision, mergeable_state,
    stack_number, stack_position, gh_updated_at, synced_at, etag, sync_source,
    tombstoned_at
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
    sqlc.arg(gh_updated_at),
    sqlc.arg(synced_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL
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
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL
WHERE pull_requests.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > pull_requests.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at = pull_requests.gh_updated_at
       AND EXCLUDED.head_sha = pull_requests.head_sha
       AND EXCLUDED.synced_at >= pull_requests.synced_at
       AND pull_requests.tombstoned_at IS NULL
   )
RETURNING *;

-- name: TombstonePullRequest :one
UPDATE pull_requests
SET tombstoned_at = sqlc.arg(tombstoned_at),
    synced_at = sqlc.arg(synced_at),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(pr_number)
  AND tombstoned_at IS NULL
  AND synced_at <= sqlc.arg(tombstoned_at)
RETURNING *;

-- name: GetStackByKey :one
SELECT stacks.*, repos.full_name AS repo_full_name
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND stacks.number = sqlc.arg(stack_number);

-- name: GetStackFetchMetadata :one
SELECT stacks.etag, stacks.entries, stacks.head_sha
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND stacks.number = sqlc.arg(stack_number);

-- name: UpsertStackWriteIfNewer :one
INSERT INTO stacks (
    repo_id, gh_id, node_id, number, base_ref, base_sha, open, entries,
    gh_updated_at, head_sha, synced_at, etag, sync_source, tombstoned_at
) VALUES (
    sqlc.arg(repo_id), sqlc.arg(gh_id), sqlc.arg(node_id),
    sqlc.arg(stack_number), sqlc.arg(base_ref), sqlc.arg(base_sha),
    sqlc.arg(open), sqlc.arg(entries), sqlc.narg(gh_updated_at),
    sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
    sqlc.arg(sync_source), NULL
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
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL
WHERE stacks.gh_updated_at IS NULL
   OR EXCLUDED.gh_updated_at > stacks.gh_updated_at
   OR (
       EXCLUDED.gh_updated_at = stacks.gh_updated_at
       AND EXCLUDED.head_sha = stacks.head_sha
       AND EXCLUDED.synced_at >= stacks.synced_at
       AND stacks.tombstoned_at IS NULL
   )
RETURNING *;

-- name: TombstoneStack :one
UPDATE stacks
SET tombstoned_at = sqlc.arg(tombstoned_at),
    open = false,
    synced_at = sqlc.arg(synced_at),
    etag = '',
    sync_source = sqlc.arg(sync_source)
WHERE repo_id = sqlc.arg(repo_id)
  AND number = sqlc.arg(stack_number)
  AND tombstoned_at IS NULL
  AND synced_at <= sqlc.arg(tombstoned_at)
RETURNING *;

-- name: ReplaceReviewThreads :many
-- C-P3: all threads returned for one PR are applied as a set.
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
        comments, gh_updated_at, head_sha, synced_at, etag, sync_source,
        tombstoned_at
    )
    SELECT input.id, sqlc.arg(repo_id), sqlc.arg(pr_number),
           input.is_resolved, input.is_outdated, input.path, input.line,
           input.comments, input.gh_updated_at, sqlc.arg(head_sha),
           sqlc.arg(synced_at), sqlc.arg(etag), sqlc.arg(sync_source), NULL
    FROM input
    ON CONFLICT (id) DO UPDATE
    SET is_resolved = EXCLUDED.is_resolved,
        is_outdated = EXCLUDED.is_outdated,
        path = EXCLUDED.path,
        line = EXCLUDED.line,
        comments = EXCLUDED.comments,
        gh_updated_at = EXCLUDED.gh_updated_at,
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE review_threads.gh_updated_at IS NULL
       OR EXCLUDED.gh_updated_at > review_threads.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at = review_threads.gh_updated_at
           AND EXCLUDED.head_sha = review_threads.head_sha
           AND EXCLUDED.synced_at >= review_threads.synced_at
           AND review_threads.tombstoned_at IS NULL
       )
    RETURNING id
),
tombstoned AS (
    UPDATE review_threads
    SET tombstoned_at = sqlc.arg(synced_at),
        synced_at = sqlc.arg(synced_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND pr_number = sqlc.arg(pr_number)
      AND tombstoned_at IS NULL
      AND synced_at <= sqlc.arg(synced_at)
      AND NOT EXISTS (SELECT 1 FROM input WHERE input.id = review_threads.id)
    RETURNING id
)
SELECT id FROM upserted
UNION ALL
SELECT id FROM tombstoned;

-- name: ReplaceCheckRuns :many
-- C-P3: one unnest-like json recordset upsert replaces row-at-a-time writes.
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
           element->'observed' AS observed
    FROM jsonb_array_elements(sqlc.arg(check_runs)::jsonb) AS element
),
upserted AS (
    INSERT INTO check_runs (
        gh_id, repo_id, node_id, name, status, conclusion, details_url,
        app_slug, started_at, completed_at, gh_updated_at, head_sha,
        synced_at, etag, sync_source, tombstoned_at
    )
    SELECT input.gh_id, sqlc.arg(repo_id), input.node_id, input.name,
           input.status, input.conclusion, input.details_url, input.app_slug,
           input.started_at, input.completed_at, input.gh_updated_at,
           sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
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
        head_sha = EXCLUDED.head_sha,
        synced_at = EXCLUDED.synced_at,
        etag = EXCLUDED.etag,
        sync_source = EXCLUDED.sync_source,
        tombstoned_at = NULL
    WHERE check_runs.gh_updated_at IS NULL
       OR EXCLUDED.gh_updated_at > check_runs.gh_updated_at
       OR (
           EXCLUDED.gh_updated_at = check_runs.gh_updated_at
           AND EXCLUDED.head_sha = check_runs.head_sha
           AND EXCLUDED.synced_at >= check_runs.synced_at
           AND check_runs.tombstoned_at IS NULL
       )
    RETURNING gh_id
),
tombstoned AS (
    UPDATE check_runs
    SET tombstoned_at = sqlc.arg(synced_at),
        synced_at = sqlc.arg(synced_at),
        etag = sqlc.arg(etag),
        sync_source = sqlc.arg(sync_source)
    WHERE repo_id = sqlc.arg(repo_id)
      AND head_sha = sqlc.arg(head_sha)
      AND tombstoned_at IS NULL
      AND synced_at <= sqlc.arg(synced_at)
      AND NOT EXISTS (
          SELECT 1 FROM input WHERE input.gh_id = check_runs.gh_id
      )
    RETURNING gh_id
)
SELECT gh_id FROM upserted
UNION ALL
SELECT gh_id FROM tombstoned;

-- name: AppendCheckHistory :exec
-- check_history is append-only raw material for later flake-rate derivation.
WITH input AS (
    SELECT (element->>'gh_id')::bigint AS gh_id,
           element->>'name' AS name,
           element->>'status' AS status,
           element->>'conclusion' AS conclusion,
           (element->>'gh_updated_at')::timestamptz AS gh_updated_at,
           element->'observed' AS observed
    FROM jsonb_array_elements(sqlc.arg(check_runs)::jsonb) AS element
)
INSERT INTO check_history (
    check_run_gh_id, repo_id, name, status, conclusion, observed,
    gh_updated_at, head_sha, synced_at, etag, sync_source, tombstoned_at
)
SELECT input.gh_id, sqlc.arg(repo_id), input.name, input.status,
       input.conclusion, input.observed, input.gh_updated_at,
       sqlc.arg(head_sha), sqlc.arg(synced_at), sqlc.arg(etag),
       sqlc.arg(sync_source), NULL
FROM input
ON CONFLICT DO NOTHING;

-- name: MarkDerivationDirty :exec
-- C-D2: the caller supplies only stack or loose-PR scope keys.
INSERT INTO derivation_dirty (scope_key, marked_at)
SELECT DISTINCT dirty.scope_key, sqlc.arg(marked_at)::timestamptz
FROM unnest(sqlc.arg(scope_keys)::text[]) AS dirty(scope_key)
ON CONFLICT (scope_key) DO UPDATE
SET marked_at = GREATEST(derivation_dirty.marked_at, EXCLUDED.marked_at);

-- name: InsertChangeEvent :one
-- C-C3/C-S1: called inside the same transaction as the accepted entity write.
INSERT INTO change_events (
    stream, kind, entity_key, occurred_at, payload
) VALUES (
    sqlc.arg(stream), sqlc.arg(kind), sqlc.arg(entity_key),
    sqlc.arg(occurred_at), sqlc.arg(payload)
)
RETURNING seq;

-- name: ListPRsAffectedByBranch :many
SELECT DISTINCT pull_requests.number, pull_requests.stack_number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.tombstoned_at IS NULL
  AND (
      pull_requests.head_ref = sqlc.arg(branch)
      OR pull_requests.base_ref = sqlc.arg(branch)
  )
ORDER BY pull_requests.number;

-- name: ListStacksAffectedByBranch :many
SELECT DISTINCT stacks.number
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
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
SELECT pull_requests.number, pull_requests.stack_number
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.head_sha = sqlc.arg(head_sha)
  AND pull_requests.tombstoned_at IS NULL
ORDER BY pull_requests.number;

-- name: ListCachedPRMemberships :many
SELECT pull_requests.number, pull_requests.stack_number
FROM pull_requests
WHERE pull_requests.repo_id = (
        SELECT id FROM repos WHERE full_name = sqlc.arg(repo_full_name)
      )
  AND pull_requests.number = ANY(sqlc.arg(pr_numbers)::int[])
ORDER BY pull_requests.number;
