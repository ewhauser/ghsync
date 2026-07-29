-- C-O3: per-member updated_at is freshness metadata, not stack semantics.
-- Dispatcher rules deliberately do not fan every member-PR event out to a
-- stack refresh (reviews and comments bump the member's updated_at without
-- changing stack composition or heads), so a cached stack legitimately lags
-- upstream on that field. Comparing it made stack drift findings guaranteed
-- noise under member churn: each pass recorded a new finding that could
-- never heal. Strip updated_at from the drift projection only — the stored
-- stack entries keep it for consumers. The upstream projection in
-- internal/drift/drift.go drops the same field.
CREATE OR REPLACE VIEW drift_entities AS
SELECT repos.installation_id,
       'repository'::text AS entity_kind,
       repos.id AS source_id,
       ('repo:' || repos.full_name || ':metadata')::text AS entity_key,
       ('repo:' || repos.installation_id || ':' || repos.gh_id)::text
           AS lock_key,
       jsonb_build_object(
           'id', repos.gh_id,
           'node_id', repos.node_id,
           'owner', repos.owner,
           'name', repos.name,
           'full_name', repos.full_name,
           'default_branch', repos.default_branch,
           'archived', repos.archived
       ) AS cache_snapshot,
       repos.last_checked_at
FROM repos
WHERE repos.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'pull_request'::text,
       pull_requests.id,
       ('pr:' || repos.full_name || ':' || pull_requests.number)::text,
       ('pr:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        pull_requests.number)::text,
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
       ),
       pull_requests.last_checked_at
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'stack'::text,
       stacks.id,
       ('stack:' || repos.full_name || ':' || stacks.number)::text,
       ('stack:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        stacks.number)::text,
       jsonb_build_object(
           'id', stacks.gh_id,
           'node_id', stacks.node_id,
           'number', stacks.number,
           'base_ref', stacks.base_ref,
           'base_sha', stacks.base_sha,
           'open', stacks.open,
           'entries', (
               SELECT COALESCE(
                   jsonb_agg(
                       entry.value - 'updated_at'
                       ORDER BY entry.ordinality
                   ),
                   '[]'::jsonb
               )
               FROM jsonb_array_elements(stacks.entries)
                   WITH ORDINALITY AS entry(value, ordinality)
           )
       ),
       stacks.last_checked_at
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'repo_rules'::text,
       repos.id,
       ('repo_rules:' || repos.full_name || ':rules')::text,
       ('repo_rules:' || repos.installation_id || ':' || repos.gh_id)::text,
       jsonb_build_object(
           'rules_by_id',
           COALESCE(
               jsonb_object_agg(repo_rules.rule_key, repo_rules.rule)
                   FILTER (WHERE repo_rules.rule_key IS NOT NULL),
               '{}'::jsonb
           )
       ),
       COALESCE(
           repo_rule_sync_state.last_checked_at,
           repos.last_checked_at
       )
FROM repos
LEFT JOIN repo_rules
  ON repo_rules.repo_id = repos.id
 AND repo_rules.tombstoned_at IS NULL
LEFT JOIN repo_rule_sync_state
  ON repo_rule_sync_state.repo_id = repos.id
WHERE repos.tombstoned_at IS NULL
GROUP BY repos.id, repo_rule_sync_state.last_checked_at

UNION ALL

SELECT repos.installation_id,
       'review_threads'::text,
       pull_requests.id,
       ('review_threads:' || repos.full_name || ':' ||
        pull_requests.number)::text,
       ('pr:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        pull_requests.number)::text,
       jsonb_build_object(
           'threads',
           COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'id', review_threads.id,
                       'is_resolved', review_threads.is_resolved,
                       'is_outdated', review_threads.is_outdated,
                       'path', review_threads.path,
                       'line', review_threads.line,
                       'comments', review_threads.comments
                   )
                   ORDER BY review_threads.id
               ) FILTER (WHERE review_threads.id IS NOT NULL),
               '[]'::jsonb
           )
       ),
       GREATEST(
           pull_requests.last_checked_at,
           COALESCE(
               max(review_threads.last_checked_at),
               pull_requests.last_checked_at
           )
       )
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
LEFT JOIN review_threads
  ON review_threads.repo_id = pull_requests.repo_id
 AND review_threads.pr_number = pull_requests.number
 AND review_threads.tombstoned_at IS NULL
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL
GROUP BY repos.id, pull_requests.id

UNION ALL

SELECT repos.installation_id,
       'checks'::text,
       min(check_runs.gh_id) AS source_id,
       ('checks:' || repos.full_name || ':' || check_runs.head_sha)::text,
       ('checks:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        check_runs.head_sha)::text,
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
       ),
       max(check_runs.last_checked_at)
FROM check_runs
JOIN repos ON repos.id = check_runs.repo_id
WHERE repos.tombstoned_at IS NULL
  AND check_runs.tombstoned_at IS NULL
GROUP BY repos.id, check_runs.head_sha;
