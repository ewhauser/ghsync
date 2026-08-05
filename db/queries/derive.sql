-- name: ClaimDerivationDirtyScopes :many
SELECT scope_key
FROM derivation_dirty
ORDER BY marked_at, scope_key
LIMIT sqlc.arg(dirty_cap)
FOR UPDATE SKIP LOCKED;

-- name: ApplyDerivedWorkItemBatch :many
WITH claimed(scope_key) AS (
    SELECT unnest(sqlc.arg(scope_keys)::text[])
),
input AS (
    SELECT element->>'scope_key' AS scope_key,
           element->>'identity_key' AS identity_key,
           (element->>'org_id')::bigint AS org_id,
           element->'payload' AS payload
    FROM jsonb_array_elements(sqlc.arg(items)::jsonb) AS element
),
upserted AS (
    INSERT INTO work_items (
        scope_key, identity_key, org_id, payload, updated_at
    )
    SELECT scope_key, identity_key, org_id, payload, clock_timestamp()
    FROM input
    ON CONFLICT (identity_key) DO UPDATE
    SET scope_key = EXCLUDED.scope_key,
        org_id = EXCLUDED.org_id,
        payload = EXCLUDED.payload,
        updated_at = EXCLUDED.updated_at
    WHERE ROW(
            work_items.scope_key,
            work_items.org_id,
            work_items.payload
        )
        IS DISTINCT FROM
        ROW(
            EXCLUDED.scope_key,
            EXCLUDED.org_id,
            EXCLUDED.payload
        )
    RETURNING scope_key, identity_key
),
removed AS (
    DELETE FROM work_items AS prior
    USING claimed
    WHERE prior.scope_key = claimed.scope_key
      AND NOT EXISTS (
          SELECT 1
          FROM input
          WHERE input.identity_key = prior.identity_key
      )
    RETURNING prior.scope_key, prior.identity_key
),
events AS (
    SELECT scope_key, identity_key, sqlc.arg(changed_kind)::text AS kind
    FROM upserted
    UNION ALL
    SELECT scope_key, identity_key, sqlc.arg(removed_kind)::text AS kind
    FROM removed
)
INSERT INTO change_events (
    stream, kind, entity_key, occurred_at, payload
)
SELECT sqlc.arg(stream), kind, identity_key, clock_timestamp(),
       jsonb_build_object(
           'version', 1,
           'identity_key', identity_key,
           'scope_key', scope_key
       )
FROM events
ORDER BY identity_key, kind
RETURNING seq;

-- name: ClearDerivationDirtyScopes :exec
DELETE FROM derivation_dirty
WHERE scope_key = ANY(sqlc.arg(scope_keys)::text[]);

-- name: LoadDerivationSnapshot :many
WITH requested AS (
    SELECT element->>'scope_key' AS scope_key,
           element->>'kind' AS kind,
           (element->>'installation_id')::bigint AS installation_id,
           (element->>'repo_id')::bigint AS repo_id,
           (element->>'number')::int AS number
    FROM jsonb_array_elements(sqlc.arg(scopes)::jsonb) AS element
),
requested_repos AS (
    SELECT requested.*,
           repos.id AS local_repo_id,
           COALESCE(repos.org_id, 0)::bigint AS org_id,
           to_jsonb(repos) AS repository
    FROM requested
    LEFT JOIN repos
      ON repos.installation_id = requested.installation_id
     AND repos.gh_id = requested.repo_id
     AND repos.tombstoned_at IS NULL
),
selected_prs AS (
    SELECT requested_repos.scope_key, pull_requests.*
    FROM requested_repos
    JOIN pull_requests
      ON pull_requests.repo_id = requested_repos.local_repo_id
    WHERE pull_requests.tombstoned_at IS NULL
      AND (
          (
              requested_repos.kind = 'stack'
              AND pull_requests.stack_number = requested_repos.number
          ) OR (
              requested_repos.kind = 'pr'
              AND pull_requests.number = requested_repos.number
              AND pull_requests.stack_number IS NULL
          )
      )
),
pull_requests_by_scope AS (
    SELECT selected_prs.scope_key,
           jsonb_agg(
               to_jsonb(selected_prs) - 'scope_key'
               ORDER BY selected_prs.number
           ) AS rows
    FROM selected_prs
    GROUP BY selected_prs.scope_key
),
selected_pr_keys AS (
    SELECT DISTINCT scope_key, repo_id, number, head_sha
    FROM selected_prs
),
review_threads_by_scope AS (
    SELECT selected_pr_keys.scope_key,
           jsonb_agg(
               to_jsonb(review_threads)
               ORDER BY review_threads.id
           ) AS rows
    FROM selected_pr_keys
    JOIN review_threads
      ON review_threads.repo_id = selected_pr_keys.repo_id
     AND review_threads.pr_number = selected_pr_keys.number
     AND review_threads.tombstoned_at IS NULL
    GROUP BY selected_pr_keys.scope_key
),
selected_heads AS (
    SELECT DISTINCT scope_key, repo_id, head_sha
    FROM selected_pr_keys
),
check_runs_by_scope AS (
    SELECT selected_heads.scope_key,
           jsonb_agg(
               to_jsonb(check_runs)
               ORDER BY check_runs.gh_id
           ) AS rows
    FROM selected_heads
    JOIN check_runs
      ON check_runs.repo_id = selected_heads.repo_id
     AND check_runs.head_sha = selected_heads.head_sha
     AND check_runs.tombstoned_at IS NULL
    GROUP BY selected_heads.scope_key
),
requested_repo_ids AS (
    SELECT DISTINCT local_repo_id
    FROM requested_repos
    WHERE local_repo_id IS NOT NULL
),
repo_rules_by_repo AS (
    SELECT repo_rules.repo_id,
           jsonb_agg(
               to_jsonb(repo_rules)
               ORDER BY repo_rules.rule_key
           ) AS rows
    FROM requested_repo_ids
    JOIN repo_rules ON repo_rules.repo_id = requested_repo_ids.local_repo_id
    WHERE repo_rules.tombstoned_at IS NULL
    GROUP BY repo_rules.repo_id
)
SELECT requested_repos.scope_key::text AS scope_key,
       requested_repos.org_id,
       requested_repos.repo_id,
       jsonb_build_object(
           'version', 1,
           'scope', jsonb_build_object(
               'kind', requested_repos.kind,
               'number', requested_repos.number
           ),
           'repository', requested_repos.repository,
           'repo_rules', COALESCE(repo_rules_by_repo.rows, '[]'::jsonb),
           'stack', to_jsonb(stacks),
           'pull_requests', COALESCE(
               pull_requests_by_scope.rows,
               '[]'::jsonb
           ),
           'review_threads', COALESCE(
               review_threads_by_scope.rows,
               '[]'::jsonb
           ),
           'check_runs', COALESCE(
               check_runs_by_scope.rows,
               '[]'::jsonb
           )
       ) AS data
FROM requested_repos
LEFT JOIN stacks
  ON requested_repos.kind = 'stack'
 AND stacks.repo_id = requested_repos.local_repo_id
 AND stacks.number = requested_repos.number
 AND stacks.tombstoned_at IS NULL
LEFT JOIN repo_rules_by_repo
  ON repo_rules_by_repo.repo_id = requested_repos.local_repo_id
LEFT JOIN pull_requests_by_scope
  ON pull_requests_by_scope.scope_key = requested_repos.scope_key
LEFT JOIN review_threads_by_scope
  ON review_threads_by_scope.scope_key = requested_repos.scope_key
LEFT JOIN check_runs_by_scope
  ON check_runs_by_scope.scope_key = requested_repos.scope_key
ORDER BY requested_repos.scope_key;
