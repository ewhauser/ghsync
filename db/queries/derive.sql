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
selected_prs AS (
    SELECT requested.scope_key, pull_requests.*
    FROM requested
    JOIN repos
      ON repos.installation_id = requested.installation_id
     AND repos.gh_id = requested.repo_id
     AND repos.tombstoned_at IS NULL
    JOIN pull_requests ON pull_requests.repo_id = repos.id
    WHERE pull_requests.tombstoned_at IS NULL
      AND (
          (
              requested.kind = 'stack'
              AND pull_requests.stack_number = requested.number
          ) OR (
              requested.kind = 'pr'
              AND pull_requests.number = requested.number
              AND pull_requests.stack_number IS NULL
          )
      )
)
SELECT requested.scope_key::text AS scope_key,
       COALESCE(repos.org_id, 0)::bigint AS org_id,
       requested.repo_id,
       jsonb_build_object(
           'version', 1,
           'scope', jsonb_build_object(
               'kind', requested.kind,
               'number', requested.number
           ),
           'repository', to_jsonb(repos),
           'repo_rules', COALESCE((
               SELECT jsonb_agg(to_jsonb(repo_rules)
                                ORDER BY repo_rules.rule_key)
               FROM repo_rules
               WHERE repo_rules.repo_id = repos.id
                 AND repo_rules.tombstoned_at IS NULL
           ), '[]'::jsonb),
           'stack', (
               SELECT to_jsonb(stacks)
               FROM stacks
               WHERE requested.kind = 'stack'
                 AND stacks.repo_id = repos.id
                 AND stacks.number = requested.number
                 AND stacks.tombstoned_at IS NULL
           ),
           'pull_requests', COALESCE((
               SELECT jsonb_agg(to_jsonb(selected_prs) - 'scope_key'
                                ORDER BY selected_prs.number)
               FROM selected_prs
               WHERE selected_prs.scope_key = requested.scope_key
           ), '[]'::jsonb),
           'review_threads', COALESCE((
               SELECT jsonb_agg(to_jsonb(review_threads)
                                ORDER BY review_threads.id)
               FROM review_threads
               WHERE review_threads.repo_id = repos.id
                 AND review_threads.tombstoned_at IS NULL
                 AND EXISTS (
                     SELECT 1
                     FROM selected_prs
                     WHERE selected_prs.scope_key = requested.scope_key
                       AND selected_prs.number = review_threads.pr_number
                 )
           ), '[]'::jsonb),
           'check_runs', COALESCE((
               SELECT jsonb_agg(to_jsonb(check_runs)
                                ORDER BY check_runs.gh_id)
               FROM check_runs
               WHERE check_runs.repo_id = repos.id
                 AND check_runs.tombstoned_at IS NULL
                 AND EXISTS (
                     SELECT 1
                     FROM selected_prs
                     WHERE selected_prs.scope_key = requested.scope_key
                       AND selected_prs.head_sha = check_runs.head_sha
                 )
           ), '[]'::jsonb)
       ) AS data
FROM requested
LEFT JOIN repos
  ON repos.installation_id = requested.installation_id
 AND repos.gh_id = requested.repo_id
 AND repos.tombstoned_at IS NULL
ORDER BY requested.scope_key;
