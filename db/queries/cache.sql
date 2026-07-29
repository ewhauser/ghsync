-- name: AcquireEntityAdvisoryLock :exec
-- C-C1 transaction-scoped serialization for direct writer calls.
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: AcquireEntitySessionLock :exec
-- Fetch workers use a dedicated connection and hold this lock from before
-- observation until after the state transaction commits.
SELECT pg_advisory_lock(hashtextextended(sqlc.arg(entity_key)::text, 0));

-- name: ReleaseEntitySessionLock :exec
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
    head_sha = EXCLUDED.head_sha,
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
           EXCLUDED.default_branch, EXCLUDED.archived, EXCLUDED.head_sha
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
         ELSE clock_timestamp() + interval '30 days'
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
    etag = EXCLUDED.etag,
    sync_source = EXCLUDED.sync_source,
    tombstoned_at = NULL,
    display_until = CASE
        WHEN EXCLUDED.state = 'open' THEN NULL
        WHEN pull_requests.state = 'open'
        THEN clock_timestamp() + interval '30 days'
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
         ELSE clock_timestamp() + interval '30 days'
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
        WHEN stacks.open THEN clock_timestamp() + interval '30 days'
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
SET last_checked_at = GREATEST(last_checked_at, sqlc.arg(checked_at))
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
JOIN repo_aliases ON repo_aliases.repo_id = repos.id
WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
  AND pull_requests.head_sha = sqlc.arg(head_sha)
  AND pull_requests.tombstoned_at IS NULL
ORDER BY pull_requests.number;

-- name: ListCachedPRMemberships :many
SELECT pull_requests.number, pull_requests.stack_number
FROM pull_requests
WHERE pull_requests.repo_id = sqlc.arg(repo_id)
  AND pull_requests.number = ANY(sqlc.arg(pr_numbers)::int[])
ORDER BY pull_requests.number;
