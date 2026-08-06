-- Webhook branch pushes are locally applied as SHA hints before bounded
-- authoritative reconciliation. This row is the monotonic branch fence and
-- retains the exact latest delivery provenance without changing the
-- authoritative observation provenance on cache rows.
CREATE TABLE branch_reconciliations (
    repo_id bigint NOT NULL,
    branch text NOT NULL,
    generation bigint NOT NULL,
    before_sha text NOT NULL,
    after_sha text NOT NULL,
    transition_known boolean NOT NULL,
    deleted boolean NOT NULL,
    forced boolean NOT NULL,
    delivery_guid text NOT NULL,
    received_at timestamp with time zone NOT NULL,
    applied_at timestamp with time zone NOT NULL,
    target_count integer DEFAULT 0 NOT NULL,
    page_count integer DEFAULT 0 NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT branch_reconciliations_pkey PRIMARY KEY (repo_id, branch),
    CONSTRAINT branch_reconciliations_repo_fkey
        FOREIGN KEY (repo_id) REFERENCES repos(id),
    CONSTRAINT branch_reconciliations_branch_check CHECK (branch <> ''),
    CONSTRAINT branch_reconciliations_generation_check CHECK (generation > 0),
    CONSTRAINT branch_reconciliations_delivery_guid_check
        CHECK (delivery_guid <> ''),
    CONSTRAINT branch_reconciliations_target_count_check
        CHECK (target_count >= 0),
    CONSTRAINT branch_reconciliations_page_count_check CHECK (page_count >= 0)
);

-- One row per bounded failure/retry unit. Older pending pages are marked
-- superseded by the next push in the same transaction that advances the
-- branch generation.
CREATE TABLE branch_reconciliation_pages (
    repo_id bigint NOT NULL,
    branch text NOT NULL,
    generation bigint NOT NULL,
    page_number integer NOT NULL,
    target_count integer NOT NULL,
    status text DEFAULT 'pending' NOT NULL,
    superseded_targets integer DEFAULT 0 NOT NULL,
    attempt_count bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    last_started_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    completed_at timestamp with time zone,
    CONSTRAINT branch_reconciliation_pages_pkey PRIMARY KEY (
        repo_id, branch, generation, page_number
    ),
    CONSTRAINT branch_reconciliation_pages_repo_fkey
        FOREIGN KEY (repo_id) REFERENCES repos(id),
    CONSTRAINT branch_reconciliation_pages_branch_check CHECK (branch <> ''),
    CONSTRAINT branch_reconciliation_pages_generation_check
        CHECK (generation > 0),
    CONSTRAINT branch_reconciliation_pages_page_number_check
        CHECK (page_number > 0),
    CONSTRAINT branch_reconciliation_pages_target_count_check
        CHECK (target_count > 0),
    CONSTRAINT branch_reconciliation_pages_superseded_targets_check CHECK (
        superseded_targets >= 0 AND superseded_targets <= target_count
    ),
    CONSTRAINT branch_reconciliation_pages_attempt_count_check CHECK (
        attempt_count >= 0
    ),
    CONSTRAINT branch_reconciliation_pages_status_check CHECK (
        status = ANY (ARRAY['pending'::text, 'completed'::text,
                            'superseded'::text])
    ),
    CONSTRAINT branch_reconciliation_pages_completion_check CHECK (
        (status = 'pending' AND completed_at IS NULL) OR
        (status <> 'pending' AND completed_at IS NOT NULL)
    )
);

CREATE INDEX branch_reconciliation_pages_pending_idx
    ON branch_reconciliation_pages (repo_id, branch, generation, page_number)
    WHERE status = 'pending';

CREATE INDEX branch_reconciliation_pages_pending_heartbeat_idx
    ON branch_reconciliation_pages (heartbeat_at)
    WHERE status = 'pending';

-- Bulk hints retain authoritative dependent observations as history. Readers
-- must fail closed when those rows do not match the live base/head fence. Keep
-- the drift projection's prior implementation as the source and strip only
-- invalid dependent observations at read time.
ALTER VIEW drift_entities RENAME TO drift_entities_without_branch_fence;

CREATE VIEW drift_entities AS
SELECT prior.installation_id,
       prior.entity_kind,
       prior.source_id,
       prior.entity_key,
       prior.lock_key,
       CASE
           WHEN prior.entity_kind <> 'pull_request'
           THEN prior.cache_snapshot
           ELSE jsonb_set(
               jsonb_set(
                   jsonb_set(
                       CASE
                           WHEN prior.cache_snapshot->'change_inputs' <>
                                'null'::jsonb
                            AND NOT COALESCE(
                                pull_fence.change_inputs_valid,
                                false
                            )
                           THEN jsonb_set(
                               prior.cache_snapshot,
                               '{change_inputs}',
                               'null'::jsonb
                           )
                           ELSE prior.cache_snapshot
                       END,
                       '{review_requests}',
                       COALESCE((
                           SELECT jsonb_agg(item.value ORDER BY item.ordinality)
                           FROM jsonb_array_elements(
                               COALESCE(
                                   NULLIF(
                                       prior.cache_snapshot->'review_requests',
                                       'null'::jsonb
                                   ),
                                   '[]'::jsonb
                               )
                           ) WITH ORDINALITY AS item(value, ordinality)
                           WHERE item.value->>'head_sha' = pull_fence.head_sha
                       ), '[]'::jsonb)
                   ),
                   '{reviews}',
                   COALESCE((
                       SELECT jsonb_agg(item.value ORDER BY item.ordinality)
                       FROM jsonb_array_elements(
                           COALESCE(
                               NULLIF(
                                   prior.cache_snapshot->'reviews',
                                   'null'::jsonb
                               ),
                               '[]'::jsonb
                           )
                       ) WITH ORDINALITY AS item(value, ordinality)
                       WHERE item.value->>'head_sha' = pull_fence.head_sha
                   ), '[]'::jsonb)
               ),
               '{comments}',
               COALESCE((
                   SELECT jsonb_agg(item.value ORDER BY item.ordinality)
                   FROM jsonb_array_elements(
                       COALESCE(
                           NULLIF(
                               prior.cache_snapshot->'comments',
                               'null'::jsonb
                           ),
                           '[]'::jsonb
                       )
                   ) WITH ORDINALITY AS item(value, ordinality)
                   WHERE item.value->>'head_sha' = pull_fence.head_sha
               ), '[]'::jsonb)
           )
       END::jsonb AS cache_snapshot,
       prior.last_checked_at
FROM drift_entities_without_branch_fence AS prior
LEFT JOIN LATERAL (
    SELECT pull.head_sha,
       snapshot.repo_id IS NOT NULL
       AND snapshot.base_sha = pull.base_sha
       AND snapshot.head_sha = pull.head_sha
       AND NOT EXISTS (
           SELECT 1
           FROM pull_request_changed_files AS file
           WHERE file.repo_id = snapshot.repo_id
             AND file.pr_number = snapshot.pr_number
             AND file.tombstoned_at IS NULL
             AND (
                 file.base_sha IS DISTINCT FROM snapshot.base_sha
                 OR file.head_sha IS DISTINCT FROM snapshot.head_sha
             )
       )
       AND NOT EXISTS (
           SELECT 1
           FROM pull_request_file_owners AS owner
           WHERE owner.repo_id = snapshot.repo_id
             AND owner.pr_number = snapshot.pr_number
             AND owner.tombstoned_at IS NULL
             AND (
                 owner.base_sha IS DISTINCT FROM snapshot.base_sha
                 OR owner.head_sha IS DISTINCT FROM snapshot.head_sha
             )
       ) AS change_inputs_valid
    FROM pull_requests AS pull
    LEFT JOIN pull_request_change_snapshots AS snapshot
      ON snapshot.repo_id = pull.repo_id
     AND snapshot.pr_number = pull.number
     AND snapshot.tombstoned_at IS NULL
    WHERE pull.id = prior.source_id
      AND pull.tombstoned_at IS NULL
) AS pull_fence ON prior.entity_kind = 'pull_request';
