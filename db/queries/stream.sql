-- name: SetLocalLockTimeout :exec
SELECT set_config('lock_timeout', sqlc.arg(lock_timeout), true);

-- name: AcquireOutboxWriterFence :exec
SELECT pg_advisory_xact_lock_shared(sqlc.arg(lock_key));

-- name: AcquireOutboxWatermarkFence :exec
SELECT pg_advisory_xact_lock(sqlc.arg(lock_key));

-- name: ReadStreamWatermarkTarget :one
SELECT watermark.safe_seq,
       COALESCE((SELECT max(seq) FROM change_events), 0)::bigint AS target_seq,
       COALESCE(
           watermark.lease_token = sqlc.arg(lease_token)::text
               AND watermark.lease_until > clock_timestamp(),
           false
       )::boolean AS lease_valid
FROM stream_watermark AS watermark
WHERE watermark.singleton;

-- name: PublishStreamWatermark :one
UPDATE stream_watermark
SET safe_seq = sqlc.arg(safe_seq),
    candidate_seq = NULL,
    candidate_xid = NULL,
    updated_at = clock_timestamp()
WHERE singleton
  AND lease_token = sqlc.arg(lease_token)::text
  AND lease_until > clock_timestamp()
  AND safe_seq < sqlc.arg(safe_seq)
RETURNING safe_seq;

-- name: AcquireOrRenewStreamWatermarkLease :one
UPDATE stream_watermark
SET lease_token = sqlc.arg(lease_token)::text,
    lease_until = clock_timestamp() + sqlc.arg(lease_ttl)::text::interval
WHERE singleton
  AND (
      lease_token IS NULL
      OR lease_until <= clock_timestamp()
      OR lease_token = sqlc.arg(lease_token)::text
  )
RETURNING COALESCE(lease_token, '')::text;

-- name: ReleaseStreamWatermarkLease :exec
UPDATE stream_watermark
SET lease_token = NULL, lease_until = NULL
WHERE singleton
  AND lease_token = sqlc.arg(lease_token)::text;

-- name: PruneChangeEvents :execrows
WITH doomed AS MATERIALIZED (
    SELECT events.seq, events.stream
    FROM change_events AS events
    CROSS JOIN stream_watermark AS watermark
    WHERE events.occurred_at < sqlc.arg(cutoff)
      -- C-S2/C-S7: never publish a pruned horizon above the greatest sequence
      -- consumers were allowed to observe.
      AND events.seq <= watermark.safe_seq
    ORDER BY events.occurred_at, events.seq
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE OF events SKIP LOCKED
),
advanced AS (
    INSERT INTO stream_horizons (
        stream, pruned_through_seq, updated_at
    )
    SELECT stream, max(seq), clock_timestamp()
    FROM doomed
    GROUP BY stream
    ON CONFLICT (stream) DO UPDATE
    SET pruned_through_seq = GREATEST(
            stream_horizons.pruned_through_seq,
            EXCLUDED.pruned_through_seq
        ),
        updated_at = EXCLUDED.updated_at
    RETURNING stream
)
DELETE FROM change_events AS events
USING doomed
WHERE events.seq = doomed.seq
  -- Reference the data-changing CTE explicitly: horizon and delete are one
  -- atomic RESYNC boundary (C-S4/C-S7).
  AND (SELECT count(*) FROM advanced) >= 0;
