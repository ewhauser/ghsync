-- Public current-state review-request projection for pull requests.
--
-- GitHub's GraphQL reviewRequests connection identifies the current requested
-- users and teams but does not expose the time at which the request was made.
-- requested_at is therefore nullable for a future authoritative timestamp;
-- first_seen_at is the observation time for the current uninterrupted request.
-- A tombstoned row that is requested again starts a new first_seen_at interval.
CREATE TABLE pull_request_review_requests (
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    reviewer_kind text NOT NULL,
    reviewer_gh_id bigint NOT NULL,
    reviewer_node_id text NOT NULL,
    reviewer_login text NOT NULL,
    requested_at timestamp with time zone,
    first_seen_at timestamp with time zone NOT NULL,
    gh_updated_at timestamp with time zone NOT NULL,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_review_requests_pkey PRIMARY KEY (
        repo_id, pr_number, reviewer_kind, reviewer_gh_id
    ),
    CONSTRAINT pull_request_review_requests_pull_request_fkey
        FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_requests(repo_id, number),
    CONSTRAINT pull_request_review_requests_pr_number_check
        CHECK (pr_number > 0),
    CONSTRAINT pull_request_review_requests_reviewer_kind_check
        CHECK (reviewer_kind = ANY (ARRAY['user'::text, 'team'::text])),
    CONSTRAINT pull_request_review_requests_reviewer_gh_id_check
        CHECK (reviewer_gh_id > 0),
    CONSTRAINT pull_request_review_requests_reviewer_node_id_check
        CHECK (reviewer_node_id <> ''::text),
    CONSTRAINT pull_request_review_requests_reviewer_login_check
        CHECK (reviewer_login <> ''::text),
    CONSTRAINT pull_request_review_requests_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

-- Current-set lookup and the pull-request drift LATERAL aggregate both use
-- this partial key without visiting tombstoned history.
CREATE INDEX pull_request_review_requests_live_pr_idx
    ON pull_request_review_requests USING btree (
        repo_id, pr_number, reviewer_kind, reviewer_gh_id
    ) INCLUDE (
        reviewer_node_id, reviewer_login, requested_at, first_seen_at,
        gh_updated_at, head_sha, synced_at, etag, sync_source,
        last_checked_at
    )
    WHERE tombstoned_at IS NULL;

-- Preserve 0005's sublinear sampling shape. The cheap drift_entity_keys view
-- selects source IDs first; only selected pull requests execute this LATERAL
-- review-request snapshot.
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
           'stack_position', pull_requests.stack_position,
           'review_requests', review_request_snapshot.requests
       ),
       GREATEST(
           pull_requests.last_checked_at,
           review_request_snapshot.last_checked_at
       )
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'kind', request.reviewer_kind,
                       'id', request.reviewer_gh_id,
                       'node_id', request.reviewer_node_id,
                       'login', request.reviewer_login,
                       'head_sha', request.head_sha
                   )
                   ORDER BY request.reviewer_kind, request.reviewer_gh_id
               ),
               '[]'::jsonb
           ) AS requests,
           COALESCE(
               max(request.last_checked_at),
               pull_requests.last_checked_at
           ) AS last_checked_at
    FROM pull_request_review_requests AS request
    WHERE request.repo_id = pull_requests.repo_id
      AND request.pr_number = pull_requests.number
      AND request.tombstoned_at IS NULL
) AS review_request_snapshot
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
-- (repo_id, head_sha). The selected key is resolved before this LATERAL
-- snapshot, preserving 0005's sublinear checks sampler.
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
