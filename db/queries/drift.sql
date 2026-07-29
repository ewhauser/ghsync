-- name: SampleCachedEntities :many
-- C-O3 samples semantic entity snapshots only. Validation/provenance columns
-- are deliberately excluded from comparison.
WITH entities AS (
    SELECT
        'repository'::text AS entity_kind,
        ('repo:' || repos.full_name || ':metadata')::text AS entity_key,
        jsonb_build_object(
            'id', repos.gh_id,
            'node_id', repos.node_id,
            'owner', repos.owner,
            'name', repos.name,
            'full_name', repos.full_name,
            'default_branch', repos.default_branch,
            'archived', repos.archived
        ) AS cache_snapshot
    FROM repos
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL

    UNION ALL

    SELECT
        'pull_request'::text,
        ('pr:' || repos.full_name || ':' || pull_requests.number)::text,
        jsonb_build_object(
            'id', pull_requests.gh_id,
            'node_id', pull_requests.node_id,
            'number', pull_requests.number,
            'title', pull_requests.title,
            'state', pull_requests.state,
            'draft', pull_requests.draft,
            'author_login', pull_requests.author_login,
            'head_ref', pull_requests.head_ref,
            'head_sha', pull_requests.head_sha,
            'base_ref', pull_requests.base_ref,
            'base_sha', pull_requests.base_sha,
            'review_decision', pull_requests.review_decision,
            'mergeable_state', pull_requests.mergeable_state,
            'stack_number', pull_requests.stack_number,
            'stack_position', pull_requests.stack_position
        )
    FROM pull_requests
    JOIN repos ON repos.id = pull_requests.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
      AND pull_requests.tombstoned_at IS NULL

    UNION ALL

    SELECT
        'stack'::text,
        ('stack:' || repos.full_name || ':' || stacks.number)::text,
        jsonb_build_object(
            'id', stacks.gh_id,
            'node_id', stacks.node_id,
            'number', stacks.number,
            'base_ref', stacks.base_ref,
            'base_sha', stacks.base_sha,
            'open', stacks.open,
            'entries', stacks.entries
        )
    FROM stacks
    JOIN repos ON repos.id = stacks.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
      AND stacks.tombstoned_at IS NULL

    UNION ALL

    SELECT
        'repo_rules'::text,
        ('repo_rules:' || repos.full_name || ':rules')::text,
        jsonb_build_object(
            'rules',
            COALESCE(
                jsonb_agg(repo_rules.rule ORDER BY repo_rules.rule_key)
                    FILTER (WHERE repo_rules.rule_key IS NOT NULL),
                '[]'::jsonb
            )
        )
    FROM repos
    LEFT JOIN repo_rules
      ON repo_rules.repo_id = repos.id
     AND repo_rules.tombstoned_at IS NULL
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
    GROUP BY repos.id, repos.full_name

    UNION ALL

    SELECT
        'checks'::text,
        ('checks:' || repos.full_name || ':' || check_runs.head_sha)::text,
        jsonb_build_object(
            'runs',
            jsonb_agg(
                jsonb_build_object(
                    'id', check_runs.gh_id,
                    'node_id', check_runs.node_id,
                    'name', check_runs.name,
                    'status', check_runs.status,
                    'conclusion', check_runs.conclusion,
                    'details_url', check_runs.details_url,
                    'app_slug', check_runs.app_slug
                )
                ORDER BY check_runs.gh_id
            )
        )
    FROM check_runs
    JOIN repos ON repos.id = check_runs.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
      AND check_runs.tombstoned_at IS NULL
    GROUP BY repos.id, repos.full_name, check_runs.head_sha
)
SELECT entity_kind, entity_key, cache_snapshot
FROM entities
ORDER BY random()
LIMIT sqlc.arg(sample_size);

-- name: InsertDriftFinding :one
INSERT INTO drift_findings (
    installation_id, entity_kind, entity_key, detected_at, cache_snapshot,
    upstream_snapshot, diff, refresh_enqueued_at
) VALUES (
    sqlc.arg(installation_id), sqlc.arg(entity_kind),
    sqlc.arg(entity_key), sqlc.arg(detected_at),
    sqlc.arg(cache_snapshot), sqlc.arg(upstream_snapshot),
    sqlc.arg(diff), sqlc.arg(refresh_enqueued_at)
)
RETURNING *;

-- name: GetDriftFinding :one
SELECT *
FROM drift_findings
WHERE id = sqlc.arg(id);

-- name: CountDriftFindings :one
SELECT count(*)
FROM drift_findings;
