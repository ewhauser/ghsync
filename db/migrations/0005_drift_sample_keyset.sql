-- C-O3 drift sampler: make the rotating sample sublinear in cache size.
--
-- SampleCachedEntitiesAfter reads drift_entities with an ORDER BY, which
-- forces PostgreSQL to plan every UNION ALL arm of the view for full
-- retrieval (set_subquery_pathlist refuses to pass the outer LIMIT's tuple
-- fraction down past a sortClause). The 'checks' arm therefore built a jsonb
-- snapshot for every (repo, head_sha) group on every sample, and its
-- source_id was min(check_runs.gh_id) -- an aggregate, so the keyset
-- predicate degraded to a HAVING filter that could prune nothing.
--
-- Two changes fix that without altering a single returned row:
--
--   1. The 'checks' arm now picks the group's representative row with a
--      NOT EXISTS anti-join instead of GROUP BY, so source_id is the plain
--      column check_runs.gh_id and keyset predicates reach the index. The
--      snapshot moves into a LATERAL aggregate evaluated per surviving row.
--      The representative is still the group's smallest live gh_id, so
--      source_id, entity_key, lock_key, cache_snapshot and last_checked_at
--      are identical to the GROUP BY form.
--
--   2. drift_entity_keys projects the same (installation_id, entity_kind,
--      source_id) triples without any snapshot. The sampler orders and
--      limits over that cheap view, then looks the chosen source_ids back up
--      in drift_entities through an ARRAY() InitPlan, which IS pushed into
--      the UNION ALL arms as an indexable qual.

CREATE INDEX check_runs_live_group_idx
    ON check_runs USING btree (repo_id, head_sha, gh_id)
    WHERE (tombstoned_at IS NULL);

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

-- head is the group's representative: the smallest live gh_id sharing its
-- (repo_id, head_sha). Exposing source_id as a plain column is what lets the
-- sampler's keyset and lookup predicates reach check_runs_live_group_idx.
SELECT repos.installation_id,
       'checks'::text,
       head.gh_id AS source_id,
       ('checks:' || repos.full_name || ':' || head.head_sha)::text,
       ('checks:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        head.head_sha)::text,
       jsonb_build_object('runs', snapshot.runs),
       snapshot.last_checked_at
FROM check_runs AS head
JOIN repos ON repos.id = head.repo_id
CROSS JOIN LATERAL (
    SELECT jsonb_agg(
               jsonb_build_object(
                   'id', run.gh_id,
                   'node_id', run.node_id,
                   'name', run.name,
                   'status', run.status,
                   'conclusion', run.conclusion,
                   'details_url', run.details_url,
                   'app_slug', run.app_slug
               )
               ORDER BY run.gh_id
           ) AS runs,
           max(run.last_checked_at) AS last_checked_at
    FROM check_runs AS run
    WHERE run.repo_id = head.repo_id
      AND run.head_sha = head.head_sha
      AND run.tombstoned_at IS NULL
) AS snapshot
WHERE repos.tombstoned_at IS NULL
  AND head.tombstoned_at IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM check_runs AS earlier
      WHERE earlier.repo_id = head.repo_id
        AND earlier.head_sha = head.head_sha
        AND earlier.tombstoned_at IS NULL
        AND earlier.gh_id < head.gh_id
  );

-- drift_entity_keys must stay row-for-row identical to
-- "SELECT installation_id, entity_kind, source_id FROM drift_entities";
-- TestDriftEntityKeysMatchDriftEntities pins that invariant.
CREATE VIEW drift_entity_keys AS
SELECT repos.installation_id,
       'repository'::text AS entity_kind,
       repos.id AS source_id
FROM repos
WHERE repos.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'pull_request'::text,
       pull_requests.id
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'stack'::text,
       stacks.id
FROM stacks
JOIN repos ON repos.id = stacks.repo_id
WHERE repos.tombstoned_at IS NULL
  AND stacks.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'repo_rules'::text,
       repos.id
FROM repos
WHERE repos.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'review_threads'::text,
       pull_requests.id
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL

UNION ALL

SELECT repos.installation_id,
       'checks'::text,
       min(check_runs.gh_id)
FROM check_runs
JOIN repos ON repos.id = check_runs.repo_id
WHERE repos.tombstoned_at IS NULL
  AND check_runs.tombstoned_at IS NULL
GROUP BY repos.id, check_runs.head_sha;
