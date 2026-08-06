-- name: BeginBranchReconciliation :one
WITH repository AS MATERIALIZED (
    SELECT repos.id
    FROM repos
    JOIN repo_aliases ON repo_aliases.repo_id = repos.id
    WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
      AND repos.tombstoned_at IS NULL
    LIMIT 1
), advanced AS (
    INSERT INTO branch_reconciliations (
        repo_id, branch, generation, before_sha, after_sha,
        transition_known, deleted, forced, delivery_guid, received_at,
        applied_at, target_count, page_count, completed_at
    )
    SELECT repository.id, sqlc.arg(branch), 1, sqlc.arg(before_sha),
           sqlc.arg(after_sha), sqlc.arg(transition_known),
           sqlc.arg(deleted), sqlc.arg(forced), sqlc.arg(delivery_guid),
           sqlc.arg(received_at), sqlc.arg(applied_at), 0, 0, NULL
    FROM repository
    ON CONFLICT (repo_id, branch) DO UPDATE
    SET generation = branch_reconciliations.generation + 1,
        before_sha = EXCLUDED.before_sha,
        after_sha = EXCLUDED.after_sha,
        transition_known = EXCLUDED.transition_known,
        deleted = EXCLUDED.deleted,
        forced = EXCLUDED.forced,
        delivery_guid = EXCLUDED.delivery_guid,
        received_at = EXCLUDED.received_at,
        applied_at = EXCLUDED.applied_at,
        target_count = 0,
        page_count = 0,
        completed_at = NULL
    WHERE branch_reconciliations.delivery_guid <>
          EXCLUDED.delivery_guid
    RETURNING repo_id, generation
), superseded AS (
    UPDATE branch_reconciliation_pages AS page
    SET status = 'superseded',
        superseded_targets = page.target_count,
        completed_at = sqlc.arg(applied_at)
    FROM advanced
    WHERE page.repo_id = advanced.repo_id
      AND page.branch = sqlc.arg(branch)
      AND page.generation < advanced.generation
      AND page.status = 'pending'
    RETURNING page.page_number
)
SELECT advanced.repo_id, advanced.generation,
       (SELECT count(*) FROM superseded)::bigint AS superseded_pages
FROM advanced;

-- name: ListBranchReconciliationGenerationKeys :many
WITH repository AS MATERIALIZED (
    SELECT repos.id, repos.installation_id, repos.gh_id,
           repos.default_branch
    FROM repos
    JOIN repo_aliases ON repo_aliases.repo_id = repos.id
    WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
      AND repos.tombstoned_at IS NULL
    LIMIT 1
), page_targets AS (
    SELECT 'refresh_pr'::text AS refresh_kind,
           ('pr:' || sqlc.arg(repo_full_name)::text || ':' ||
            pull_requests.number::text)::text AS refresh_key,
           ('pr:' || repository.installation_id::text || ':' ||
            repository.gh_id::text || ':' ||
            pull_requests.number::text)::text AS entity_key
    FROM repository
    JOIN pull_requests ON pull_requests.repo_id = repository.id
    WHERE pull_requests.tombstoned_at IS NULL
      AND pull_requests.state = 'open'
      AND (
          pull_requests.head_ref = sqlc.arg(branch)
          OR pull_requests.base_ref = sqlc.arg(branch)
      )

    UNION

    SELECT 'refresh_stack'::text,
           ('stack:' || sqlc.arg(repo_full_name)::text || ':' ||
            stacks.number::text)::text,
           ('stack:' || repository.installation_id::text || ':' ||
            repository.gh_id::text || ':' || stacks.number::text)::text
    FROM repository
    JOIN stacks ON stacks.repo_id = repository.id
    WHERE stacks.tombstoned_at IS NULL
      AND (
          stacks.base_ref = sqlc.arg(branch)
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(stacks.entries) AS entry
              WHERE entry->>'head_ref' = sqlc.arg(branch)
          )
      )
), generation_targets AS (
    SELECT page_targets.refresh_kind, page_targets.refresh_key
    FROM page_targets

    UNION

    SELECT 'refresh_repository'::text,
           ('repo:' || sqlc.arg(repo_full_name)::text || ':metadata')::text
    FROM repository
    WHERE repository.default_branch = sqlc.arg(branch)
)
SELECT generation_targets.refresh_kind, generation_targets.refresh_key
FROM generation_targets
ORDER BY generation_targets.refresh_kind, generation_targets.refresh_key;

-- name: ApplyBranchHintTransition :many
WITH repository AS MATERIALIZED (
    SELECT repos.id, repos.installation_id, repos.gh_id,
           repos.default_branch
    FROM repos
    JOIN repo_aliases ON repo_aliases.repo_id = repos.id
    WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
      AND repos.tombstoned_at IS NULL
    LIMIT 1
), updated_repository AS (
    UPDATE repos
    SET head_sha = sqlc.arg(after_sha),
        etag = ''
    WHERE repos.id = (SELECT id FROM repository)
      AND sqlc.arg(transition_known)::boolean
      AND repos.default_branch = sqlc.arg(branch)
      AND repos.head_sha = sqlc.arg(before_sha)
      AND repos.head_sha IS DISTINCT FROM sqlc.arg(after_sha)::text
    RETURNING repos.id
), updated_pulls AS (
    UPDATE pull_requests AS pull
    SET head_sha = CASE
            WHEN pull.head_ref = sqlc.arg(branch)
             AND pull.head_sha = sqlc.arg(before_sha)
            THEN sqlc.arg(after_sha)
            ELSE pull.head_sha
        END,
        base_sha = CASE
            WHEN pull.base_ref = sqlc.arg(branch)
             AND pull.base_sha = sqlc.arg(before_sha)
            THEN sqlc.arg(after_sha)
            ELSE pull.base_sha
        END,
        review_decision = '',
        mergeable_state = '',
        etag = ''
    FROM repository
    WHERE pull.repo_id = repository.id
      AND pull.tombstoned_at IS NULL
      AND pull.state = 'open'
      AND sqlc.arg(transition_known)::boolean
      AND (
          (pull.head_ref = sqlc.arg(branch)
           AND pull.head_sha = sqlc.arg(before_sha)
           AND pull.head_sha IS DISTINCT FROM sqlc.arg(after_sha)::text)
          OR
          (pull.base_ref = sqlc.arg(branch)
           AND pull.base_sha = sqlc.arg(before_sha)
           AND pull.base_sha IS DISTINCT FROM sqlc.arg(after_sha)::text)
      )
    RETURNING pull.repo_id, pull.number, pull.stack_number
), stack_candidates AS MATERIALIZED (
    SELECT stack.id,
           CASE
               WHEN stack.base_ref = sqlc.arg(branch)
                AND stack.base_sha = sqlc.arg(before_sha)
               THEN sqlc.arg(after_sha)::text
               ELSE stack.base_sha
           END AS new_base_sha,
           COALESCE((
               SELECT jsonb_agg(
                   CASE
                       WHEN entry.value->>'head_ref' = sqlc.arg(branch)
                        AND entry.value->>'head_sha' = sqlc.arg(before_sha)
                       THEN jsonb_set(
                           entry.value,
                           '{head_sha}',
                           to_jsonb(sqlc.arg(after_sha)::text),
                           false
                       )
                       ELSE entry.value
                   END
                   ORDER BY entry.ordinality
               )
               FROM jsonb_array_elements(stack.entries)
                   WITH ORDINALITY AS entry(value, ordinality)
           ), '[]'::jsonb) AS new_entries
    FROM stacks AS stack
    JOIN repository ON repository.id = stack.repo_id
    WHERE stack.tombstoned_at IS NULL
      AND sqlc.arg(transition_known)::boolean
      AND (
          (stack.base_ref = sqlc.arg(branch)
           AND stack.base_sha = sqlc.arg(before_sha))
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(stack.entries) AS entry
              WHERE entry->>'head_ref' = sqlc.arg(branch)
                AND entry->>'head_sha' = sqlc.arg(before_sha)
          )
      )
), updated_stacks AS (
    UPDATE stacks AS stack
    SET base_sha = candidate.new_base_sha,
        entries = candidate.new_entries,
        head_sha = CASE
            WHEN jsonb_array_length(candidate.new_entries) = 0
            THEN candidate.new_base_sha
            ELSE candidate.new_entries ->
                (jsonb_array_length(candidate.new_entries) - 1) ->>
                'head_sha'
        END,
        etag = ''
    FROM stack_candidates AS candidate
    WHERE stack.id = candidate.id
      AND ROW(stack.base_sha, stack.entries) IS DISTINCT FROM
          ROW(candidate.new_base_sha, candidate.new_entries)
    RETURNING stack.repo_id, stack.number
), dirty_scopes AS MATERIALIZED (
    SELECT DISTINCT CASE
        WHEN pull.stack_number IS NULL
        THEN 'pr:' || repository.installation_id::text || ':' ||
             repository.gh_id::text || ':' || pull.number::text
        ELSE 'stack:' || repository.installation_id::text || ':' ||
             repository.gh_id::text || ':' || pull.stack_number::text
        END AS scope_key
    FROM updated_pulls AS pull
    CROSS JOIN repository

    UNION

    SELECT 'stack:' || repository.installation_id::text || ':' ||
           repository.gh_id::text || ':' || stack.number::text
    FROM updated_stacks AS stack
    CROSS JOIN repository
), marked_dirty AS (
    INSERT INTO derivation_dirty (scope_key, marked_at)
    SELECT scope_key, sqlc.arg(applied_at)
    FROM dirty_scopes
    ON CONFLICT (scope_key) DO UPDATE
    SET marked_at = GREATEST(
        derivation_dirty.marked_at,
        EXCLUDED.marked_at
    )
    RETURNING scope_key
), event_inputs AS MATERIALIZED (
    SELECT 'repository.changed'::text AS kind,
           'repo:' || repository.installation_id::text || ':' ||
           repository.gh_id::text AS entity_key
    FROM updated_repository
    CROSS JOIN repository

    UNION ALL

    SELECT 'pull_request.changed',
           'pr:' || repository.installation_id::text || ':' ||
           repository.gh_id::text || ':' || pull.number::text
    FROM updated_pulls AS pull
    CROSS JOIN repository

    UNION ALL

    SELECT 'stack.changed',
           'stack:' || repository.installation_id::text || ':' ||
           repository.gh_id::text || ':' || stack.number::text
    FROM updated_stacks AS stack
    CROSS JOIN repository
), inserted_events AS (
    INSERT INTO change_events (
        stream, kind, entity_key, occurred_at, payload
    )
    SELECT 'entities', kind, entity_key, sqlc.arg(applied_at),
           '{"version":1}'::jsonb
    FROM event_inputs
    ORDER BY entity_key, kind
    RETURNING seq, kind, entity_key
)
SELECT inserted_events.seq, inserted_events.kind,
       inserted_events.entity_key
FROM inserted_events
-- Dependent base/head rows remain retained history. Every reader fences them
-- against the live PR, while reconciliation can still reuse base-only
-- CODEOWNERS source data when a head push did not invalidate it.
CROSS JOIN LATERAL (
    SELECT (SELECT count(*) FROM marked_dirty) AS ignored
) AS completed
ORDER BY inserted_events.seq;

-- name: AdvanceBranchReconciliationTargetGenerations :many
WITH repository AS MATERIALIZED (
    SELECT repos.id, repos.installation_id, repos.gh_id,
           repos.default_branch
    FROM repos
    JOIN repo_aliases ON repo_aliases.repo_id = repos.id
    WHERE repo_aliases.full_name = sqlc.arg(repo_full_name)
      AND repos.tombstoned_at IS NULL
    LIMIT 1
), page_targets AS (
    SELECT 'refresh_pr'::text AS refresh_kind,
           ('pr:' || sqlc.arg(repo_full_name)::text || ':' ||
            pull_requests.number::text)::text AS refresh_key,
           ('pr:' || repository.installation_id::text || ':' ||
            repository.gh_id::text || ':' ||
            pull_requests.number::text)::text AS entity_key,
           true AS page_target
    FROM repository
    JOIN pull_requests ON pull_requests.repo_id = repository.id
    WHERE pull_requests.tombstoned_at IS NULL
      AND pull_requests.state = 'open'
      AND (
          pull_requests.head_ref = sqlc.arg(branch)
          OR pull_requests.base_ref = sqlc.arg(branch)
      )

    UNION

    SELECT 'refresh_stack'::text,
           ('stack:' || sqlc.arg(repo_full_name)::text || ':' ||
            stacks.number::text)::text,
           ('stack:' || repository.installation_id::text || ':' ||
            repository.gh_id::text || ':' || stacks.number::text)::text,
           true
    FROM repository
    JOIN stacks ON stacks.repo_id = repository.id
    WHERE stacks.tombstoned_at IS NULL
      AND (
          stacks.base_ref = sqlc.arg(branch)
          OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(stacks.entries) AS entry
              WHERE entry->>'head_ref' = sqlc.arg(branch)
          )
      )
), generation_targets AS (
    SELECT page_targets.refresh_kind, page_targets.refresh_key
    FROM page_targets

    UNION

    SELECT 'refresh_repository'::text,
           ('repo:' || sqlc.arg(repo_full_name)::text || ':metadata')::text
    FROM repository
    WHERE repository.default_branch = sqlc.arg(branch)
), advanced AS (
    INSERT INTO refresh_intent_generations (
        kind, refresh_key, generation, completed_generation,
        deadline_at, event_received_at, updated_at
    )
    SELECT generation_targets.refresh_kind,
           generation_targets.refresh_key,
           1, 1, NULL, NULL, sqlc.arg(applied_at)
    FROM generation_targets
    ORDER BY generation_targets.refresh_kind,
             generation_targets.refresh_key
    ON CONFLICT (kind, refresh_key) DO UPDATE
    SET generation = refresh_intent_generations.generation + 1,
        completed_generation = refresh_intent_generations.generation + 1,
        deadline_at = NULL,
        event_received_at = NULL,
        updated_at = EXCLUDED.updated_at
    RETURNING kind, refresh_key, generation
)
SELECT page_targets.refresh_kind, page_targets.refresh_key,
       page_targets.entity_key, advanced.generation AS refresh_generation
FROM page_targets
JOIN advanced
  ON advanced.kind = page_targets.refresh_kind
 AND advanced.refresh_key = page_targets.refresh_key
ORDER BY page_targets.refresh_kind, page_targets.refresh_key;

-- name: RecordBranchReconciliationPages :exec
WITH input AS (
    SELECT (element->>'page_number')::integer AS page_number,
           (element->>'target_count')::integer AS target_count
    FROM jsonb_array_elements(sqlc.arg(pages)::jsonb) AS element
), inserted AS (
    INSERT INTO branch_reconciliation_pages (
        repo_id, branch, generation, page_number, target_count,
        created_at
    )
    SELECT sqlc.arg(repo_id), sqlc.arg(branch), sqlc.arg(generation),
           input.page_number, input.target_count, sqlc.arg(created_at)
    FROM input
    ON CONFLICT (repo_id, branch, generation, page_number) DO NOTHING
    RETURNING page_number
)
UPDATE branch_reconciliations AS current_reconciliation
SET target_count = sqlc.arg(target_count),
    page_count = sqlc.arg(page_count),
    completed_at = CASE
        WHEN sqlc.arg(page_count)::integer = 0 THEN sqlc.arg(created_at)
        ELSE NULL
    END
WHERE current_reconciliation.repo_id = sqlc.arg(repo_id)
  AND current_reconciliation.branch = sqlc.arg(branch)
  AND current_reconciliation.generation = sqlc.arg(generation)
  AND (SELECT count(*) FROM inserted) >= 0;

-- name: GetBranchReconciliationPage :one
SELECT status
FROM branch_reconciliation_pages
WHERE repo_id = sqlc.arg(repo_id)
  AND branch = sqlc.arg(branch)
  AND generation = sqlc.arg(generation)
  AND page_number = sqlc.arg(page_number);

-- name: StartBranchReconciliationPage :execrows
UPDATE branch_reconciliation_pages AS page
SET attempt_count = page.attempt_count + 1,
    last_started_at = clock_timestamp(),
    heartbeat_at = clock_timestamp()
WHERE page.repo_id = sqlc.arg(repo_id)
  AND page.branch = sqlc.arg(branch)
  AND page.generation = sqlc.arg(generation)
  AND page.page_number = sqlc.arg(page_number)
  AND page.status = 'pending'
  AND EXISTS (
      SELECT 1
      FROM branch_reconciliations AS reconciliation
      WHERE reconciliation.repo_id = page.repo_id
        AND reconciliation.branch = page.branch
        AND reconciliation.generation = page.generation
  );

-- name: HeartbeatBranchReconciliationPage :execrows
UPDATE branch_reconciliation_pages AS page
SET heartbeat_at = clock_timestamp()
WHERE page.repo_id = sqlc.arg(repo_id)
  AND page.branch = sqlc.arg(branch)
  AND page.generation = sqlc.arg(generation)
  AND page.page_number = sqlc.arg(page_number)
  AND page.status = 'pending'
  AND EXISTS (
      SELECT 1
      FROM branch_reconciliations AS reconciliation
      WHERE reconciliation.repo_id = page.repo_id
        AND reconciliation.branch = page.branch
        AND reconciliation.generation = page.generation
  );

-- name: GetBranchReconciliationGenerationForShare :one
SELECT generation
FROM branch_reconciliations
WHERE repo_id = sqlc.arg(repo_id)
  AND branch = sqlc.arg(branch)
FOR SHARE;

-- name: CompleteBranchReconciliationPage :one
WITH completed AS (
    UPDATE branch_reconciliation_pages AS page
SET status = 'completed',
    superseded_targets = sqlc.arg(superseded_targets),
    heartbeat_at = sqlc.arg(completed_at),
    completed_at = sqlc.arg(completed_at)
    WHERE page.repo_id = sqlc.arg(repo_id)
      AND page.branch = sqlc.arg(branch)
      AND page.generation = sqlc.arg(generation)
      AND page.page_number = sqlc.arg(page_number)
      AND page.status = 'pending'
    RETURNING page.repo_id
), finished AS (
    UPDATE branch_reconciliations AS reconciliation
    SET completed_at = sqlc.arg(completed_at)
    WHERE reconciliation.repo_id = sqlc.arg(repo_id)
      AND reconciliation.branch = sqlc.arg(branch)
      AND reconciliation.generation = sqlc.arg(generation)
      AND NOT EXISTS (
          SELECT 1
          FROM branch_reconciliation_pages AS page
          WHERE page.repo_id = reconciliation.repo_id
            AND page.branch = reconciliation.branch
            AND page.generation = reconciliation.generation
            AND page.status = 'pending'
      )
    RETURNING reconciliation.repo_id
)
SELECT count(*)::bigint AS completed_count FROM completed
WHERE (SELECT count(*) FROM finished) >= 0;

-- name: CollectBranchReconciliationBacklog :one
SELECT count(*)::bigint AS pending_pages,
       COALESCE(sum(page.target_count), 0)::bigint AS pending_targets
FROM branch_reconciliation_pages AS page
JOIN branch_reconciliations AS reconciliation
  ON reconciliation.repo_id = page.repo_id
 AND reconciliation.branch = page.branch
 AND reconciliation.generation = page.generation
WHERE page.status = 'pending';
