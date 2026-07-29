-- M4 review fixes. Applied migrations 0001-0010 remain immutable.

-- A page validator proves list membership only. Name that timestamp for what
-- it is so it cannot be confused with an entity-detail freshness proof.
ALTER TABLE sweep_pages
    RENAME COLUMN last_checked_at TO list_seen_at;

-- Mutable REST listings are scanned in overlapping passes until a complete
-- pass discovers no new entity keys.
ALTER TABLE sweep_cursors
    ADD COLUMN pass_new_count INT NOT NULL DEFAULT 0
        CHECK (pass_new_count >= 0);

-- Delivery-gap scans resume the exact opaque API cursor and fixed time-window
-- cutoff after a page cap or process restart.
CREATE TABLE gap_heal_cursors (
    installation_id BIGINT PRIMARY KEY,
    cursor           TEXT NOT NULL DEFAULT '',
    cutoff           TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    CHECK (
        (started_at IS NULL AND cutoff IS NULL)
        OR (started_at IS NOT NULL AND cutoff IS NOT NULL)
    )
);

-- Reconciliation deadlines stay outside River args so ByArgs uniqueness
-- remains keyed only by the durable work pointer.
ALTER TABLE refresh_intent_generations
    ADD COLUMN deadline_at TIMESTAMPTZ;

-- Persistent drift keeps one open finding per semantic diff. The first
-- observation schedules one heal; a mismatch that remains after that
-- generation completes is escalated rather than re-enqueued forever.
ALTER TABLE drift_findings
    ADD COLUMN diff_hash TEXT,
    ADD COLUMN first_seen_at TIMESTAMPTZ,
    ADD COLUMN last_seen_at TIMESTAMPTZ,
    ADD COLUMN occurrence_count BIGINT NOT NULL DEFAULT 1
        CHECK (occurrence_count > 0),
    ADD COLUMN heal_generation BIGINT NOT NULL DEFAULT 0
        CHECK (heal_generation >= 0),
    ADD COLUMN escalated_at TIMESTAMPTZ,
    ADD COLUMN resolved_at TIMESTAMPTZ;

UPDATE drift_findings
SET diff_hash = md5(diff::text),
    first_seen_at = detected_at,
    last_seen_at = detected_at;

ALTER TABLE drift_findings
    ALTER COLUMN diff_hash SET NOT NULL,
    ALTER COLUMN first_seen_at SET NOT NULL,
    ALTER COLUMN last_seen_at SET NOT NULL;

WITH duplicates AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY installation_id, entity_kind, entity_key, diff_hash
               ORDER BY detected_at DESC, id DESC
           ) AS duplicate_number
    FROM drift_findings
)
UPDATE drift_findings
SET resolved_at = detected_at
FROM duplicates
WHERE duplicates.id = drift_findings.id
  AND duplicates.duplicate_number > 1;

CREATE UNIQUE INDEX drift_findings_one_open_diff_idx
ON drift_findings (installation_id, entity_kind, entity_key, diff_hash)
WHERE resolved_at IS NULL;

CREATE INDEX drift_findings_resolved_at_idx
ON drift_findings (resolved_at)
WHERE resolved_at IS NOT NULL;

-- One cursor per semantic entity class provides bounded, rotating,
-- stratified drift coverage without ORDER BY random() over the whole cache.
CREATE TABLE drift_sample_cursors (
    installation_id BIGINT NOT NULL,
    entity_kind     TEXT NOT NULL,
    source_id       BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (installation_id, entity_kind)
);

CREATE VIEW drift_entities AS
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
       ) AS cache_snapshot
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
       )
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
           'entries', stacks.entries
       )
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
       )
FROM repos
LEFT JOIN repo_rules
  ON repo_rules.repo_id = repos.id
 AND repo_rules.tombstoned_at IS NULL
WHERE repos.tombstoned_at IS NULL
GROUP BY repos.id

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
       )
FROM check_runs
JOIN repos ON repos.id = check_runs.repo_id
WHERE repos.tombstoned_at IS NULL
  AND check_runs.tombstoned_at IS NULL
GROUP BY repos.id, check_runs.head_sha;

-- Retention scans touch old append-only delivery ranges. BRIN keeps this
-- compact while allowing each pruning transaction to select a bounded batch.
CREATE INDEX webhook_deliveries_received_at_brin_idx
ON webhook_deliveries USING BRIN (received_at);
