-- name: CollectBudgetMetrics :many
SELECT class,
       COALESCE(remaining, -1)::bigint AS remaining,
       COALESCE(rate_limit, -1)::bigint AS rate_limit,
       COALESCE(backoff_until > clock_timestamp(), false)::boolean
           AS gate_closed
FROM installation_budgets
WHERE installation_id = sqlc.arg(installation_id)
ORDER BY class;

-- name: CollectRiverQueueDepthMetrics :many
SELECT queue, count(*) AS depth
FROM river_job
WHERE state IN (
    'available', 'pending', 'retryable', 'running', 'scheduled'
)
GROUP BY queue;

-- name: CollectDeliveryMetrics :one
-- Every aggregate here is per-status so that it rides
-- webhook_deliveries_unfinished_received_idx (status, received_at) WHERE
-- status IN ('pending','processing','parked'). Aggregate FILTER clauses
-- block both the min/max index optimisation and the partial index's
-- predicate proof, so the older single-pass form seq-scanned the whole
-- delivery table on every metrics scrape. LEAST() ignores NULLs, so
-- LEAST(min pending, min processing) is exactly the old
-- min(received_at) FILTER (WHERE status IN ('pending','processing')).
SELECT
    COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - LEAST(
        (
            SELECT min(received_at)
            FROM webhook_deliveries
            WHERE status = 'pending'
        ),
        (
            SELECT min(received_at)
            FROM webhook_deliveries
            WHERE status = 'processing'
        )
    ))), 0)::double precision AS oldest_unprocessed,
    (
        SELECT count(*)
        FROM webhook_deliveries
        WHERE status = 'parked'
    )::bigint AS parked,
    COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - (
        SELECT min(received_at)
        FROM webhook_deliveries
        WHERE status = 'parked'
    ))), 0)::double precision AS oldest_parked,
    (
        SELECT count(*)
        FROM refresh_intent_generations
        WHERE completed_generation < generation
    ) AS outstanding_generations,
    (
        SELECT COALESCE(EXTRACT(EPOCH FROM (
            clock_timestamp() - min(event_received_at)
        )), 0)::double precision
        FROM refresh_intent_generations
        WHERE completed_generation < generation
          AND event_received_at IS NOT NULL
    ) AS oldest_generation;

-- name: CollectCacheStalenessMetrics :many
WITH ages(entity_class, age_seconds) AS (
    SELECT 'repository',
           max(EXTRACT(EPOCH FROM (
               clock_timestamp() - repos.last_checked_at
           )))
    FROM repos
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
    UNION ALL
    SELECT 'open_stack',
           max(EXTRACT(EPOCH FROM (
               clock_timestamp() - stacks.last_checked_at
           )))
    FROM stacks
    JOIN repos ON repos.id = stacks.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
      AND stacks.tombstoned_at IS NULL
      AND stacks.open
    UNION ALL
    SELECT 'open_pr',
           max(EXTRACT(EPOCH FROM (
               clock_timestamp() - pull_requests.last_checked_at
           )))
    FROM pull_requests
    JOIN repos ON repos.id = pull_requests.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
      AND pull_requests.tombstoned_at IS NULL
      AND pull_requests.state = 'open'
    UNION ALL
    SELECT 'repo_rules',
           max(EXTRACT(EPOCH FROM (
               clock_timestamp() - state.last_checked_at
           )))
    FROM repo_rule_sync_state AS state
    JOIN repos ON repos.id = state.repo_id
    WHERE repos.installation_id = sqlc.arg(installation_id)
      AND repos.tombstoned_at IS NULL
    UNION ALL
    SELECT 'closed_displayed',
           max(age_seconds)
    FROM (
        SELECT EXTRACT(EPOCH FROM (
                   clock_timestamp() - stacks.last_checked_at
               )) AS age_seconds
        FROM stacks
        JOIN repos ON repos.id = stacks.repo_id
        WHERE repos.installation_id = sqlc.arg(installation_id)
          AND repos.tombstoned_at IS NULL
          AND stacks.tombstoned_at IS NULL
          AND NOT stacks.open
          AND stacks.display_until > clock_timestamp()
        UNION ALL
        SELECT EXTRACT(EPOCH FROM (
                   clock_timestamp() - pull_requests.last_checked_at
               ))
        FROM pull_requests
        JOIN repos ON repos.id = pull_requests.repo_id
        WHERE repos.installation_id = sqlc.arg(installation_id)
          AND repos.tombstoned_at IS NULL
          AND pull_requests.tombstoned_at IS NULL
          AND pull_requests.state <> 'open'
          AND pull_requests.display_until > clock_timestamp()
    ) AS closed
)
SELECT entity_class, COALESCE(age_seconds, 0)::double precision AS age_seconds
FROM ages
ORDER BY entity_class;

-- name: CollectTombstoneMetrics :many
SELECT entity_kind, count(*) AS tombstone_count
FROM (
    SELECT 'repository' AS entity_kind FROM repos
    WHERE tombstoned_at IS NOT NULL
    UNION ALL
    SELECT 'repo_rules' FROM repo_rules
    WHERE tombstoned_at IS NOT NULL
    UNION ALL
    SELECT 'stack' FROM stacks
    WHERE tombstoned_at IS NOT NULL
    UNION ALL
    SELECT 'pull_request' FROM pull_requests
    WHERE tombstoned_at IS NOT NULL
    UNION ALL
    SELECT 'review_thread' FROM review_threads
    WHERE tombstoned_at IS NOT NULL
    UNION ALL
    SELECT 'check_run' FROM check_runs
    WHERE tombstoned_at IS NOT NULL
) AS tombstones
GROUP BY entity_kind;

-- name: CollectSweepMetrics :many
SELECT sweep_kind,
       max(GREATEST(EXTRACT(EPOCH FROM (
           COALESCE(completed_at, clock_timestamp()) - started_at
       )), 0))::double precision AS duration_seconds
FROM sweep_cursors
WHERE installation_id = sqlc.arg(installation_id)
  AND started_at IS NOT NULL
GROUP BY sweep_kind;

-- name: CollectDriftMetrics :many
SELECT CASE WHEN resolved_at IS NULL THEN 'open' ELSE 'resolved' END
           AS finding_state,
       entity_kind,
       count(*) AS finding_count
FROM drift_findings
WHERE installation_id = sqlc.arg(installation_id)
GROUP BY 1, entity_kind;

-- name: CollectStreamWatermarkMetrics :one
SELECT watermark.safe_seq,
       COALESCE(max(events.seq), 0)::bigint AS max_seq,
       EXTRACT(EPOCH FROM (
           clock_timestamp() - watermark.updated_at
       ))::double precision AS age_seconds
FROM stream_watermark AS watermark
LEFT JOIN change_events AS events ON true
WHERE watermark.singleton
GROUP BY watermark.safe_seq, watermark.updated_at;

-- name: CollectPrunableOutboxMetrics :many
SELECT events.stream, count(*) AS prunable_count
FROM change_events AS events
CROSS JOIN stream_watermark AS watermark
WHERE events.occurred_at <
      clock_timestamp() - sqlc.arg(retention_age)::text::interval
  AND events.seq <= watermark.safe_seq
GROUP BY events.stream;

-- name: CollectConsumerStreamMetrics :many
SELECT cursor.consumer,
       cursor.stream,
       count(events.seq) AS outstanding_count,
       COALESCE(EXTRACT(EPOCH FROM (
           clock_timestamp() - min(events.occurred_at)
       )), 0)::double precision AS oldest_outstanding_age,
       cursor.resync_count
FROM consumer_cursors AS cursor
CROSS JOIN stream_watermark AS watermark
LEFT JOIN change_events AS events
  ON events.stream = cursor.stream
 AND events.seq > cursor.seq
 AND events.seq <= watermark.safe_seq
GROUP BY cursor.consumer, cursor.stream, cursor.resync_count
ORDER BY cursor.consumer, cursor.stream;

-- name: CountDerivationDirty :one
SELECT count(*)
FROM derivation_dirty;

-- name: CollectOperationHeartbeatMetrics :many
WITH expected AS (
    SELECT 'drift'::text AS component, 'detector'::text AS operation
    UNION ALL SELECT 'sweep', 'repositories'
    UNION ALL SELECT 'sweep', 'stacks'
    UNION ALL SELECT 'sweep', 'pull_requests'
    UNION ALL SELECT 'sweep', 'repo_rules'
    UNION ALL SELECT 'sweep', 'closed_tracked'
    UNION ALL SELECT 'watermarker', 'entities'
)
SELECT expected.component,
       expected.operation,
       COALESCE(heartbeat.success_count, 0)::bigint AS success_count,
       COALESCE(heartbeat.sample_count, 0)::bigint AS sample_count,
       CASE
           WHEN heartbeat.last_success_at IS NULL THEN -1
           ELSE EXTRACT(EPOCH FROM (
               clock_timestamp() - heartbeat.last_success_at
           ))::double precision
       END AS success_age_seconds,
       CASE
           WHEN heartbeat.last_sample_at IS NULL THEN -1
           ELSE EXTRACT(EPOCH FROM (
               clock_timestamp() - heartbeat.last_sample_at
           ))::double precision
       END AS sample_age_seconds
FROM expected
LEFT JOIN operation_heartbeats AS heartbeat
  ON heartbeat.installation_id = sqlc.arg(installation_id)
 AND heartbeat.component = expected.component
 AND heartbeat.operation = expected.operation
ORDER BY 1, 2;
