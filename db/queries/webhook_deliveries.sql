-- name: InsertWebhookDelivery :execrows
-- C-I3: duplicate deliveries are free no-ops.
INSERT INTO webhook_deliveries (delivery_guid, event, raw_body, headers)
VALUES ($1, $2, $3, $4)
ON CONFLICT (delivery_guid) DO NOTHING;

-- name: GetWebhookDelivery :one
SELECT * FROM webhook_deliveries WHERE delivery_guid = $1;

-- name: CountWebhookDeliveriesByStatus :one
SELECT count(*) FROM webhook_deliveries WHERE status = $1;

-- name: ClaimWebhookDeliveries :many
-- C-P2/C-O2: claim a bounded batch without blocking another dispatcher. The
-- processing transition and attempt increment remain in the caller's batch
-- transaction, so a crashed dispatcher rolls the claim back to pending.
WITH candidates AS (
    SELECT candidate.delivery_guid
    FROM webhook_deliveries AS candidate
    WHERE candidate.status = 'pending'
      AND candidate.next_attempt_at <= clock_timestamp()
    ORDER BY candidate.next_attempt_at,
             candidate.received_at,
             candidate.delivery_guid
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
)
UPDATE webhook_deliveries AS delivery
SET status = 'processing',
    attempts = delivery.attempts + 1,
    last_error = NULL
FROM candidates
WHERE delivery.delivery_guid = candidates.delivery_guid
RETURNING delivery.*;

-- name: SetWebhookDeliveryResults :execrows
-- C-P2: finish the entire claimed batch with one set-based status update.
UPDATE webhook_deliveries AS delivery
SET status = result.status,
    last_error = NULLIF(result.last_error, ''),
    next_attempt_at = CASE
        WHEN result.retry_delay_seconds IS NULL
        THEN delivery.next_attempt_at
        ELSE clock_timestamp() + make_interval(
            secs => result.retry_delay_seconds
        )
    END
FROM jsonb_to_recordset(sqlc.arg(results)::jsonb)
    AS result(
        delivery_guid text,
        status text,
        last_error text,
        retry_delay_seconds int
    )
WHERE delivery.delivery_guid = result.delivery_guid
  AND delivery.status = 'processing';

-- name: ListParkedWebhookDeliveries :many
-- C-I5: dead-letter deliveries are explicitly queryable for operations and
-- replay tooling.
SELECT *
FROM webhook_deliveries
WHERE status = 'parked'
ORDER BY received_at, delivery_guid
LIMIT sqlc.arg(result_limit);

-- name: RequeueParkedWebhookDeliveries :execrows
-- C-I5: replay is deliberately bounded to an explicit GUID set or one
-- event/error signature. Payload-pruned deliveries remain parked and receive
-- an explicit refusal reason; an explicit-GUID CLI replay consequently fails
-- its selected-vs-requeued count check instead of dispatching a nil body.
WITH selected AS MATERIALIZED (
    SELECT delivery_guid, raw_body, payload_pruned_at
    FROM webhook_deliveries
    WHERE status = 'parked'
      AND (
          (
              COALESCE(
                  cardinality(sqlc.arg(delivery_guids)::text[]),
                  0
              ) > 0
              AND delivery_guid = ANY(sqlc.arg(delivery_guids)::text[])
          )
          OR (
              COALESCE(
                  cardinality(sqlc.arg(delivery_guids)::text[]),
                  0
              ) = 0
              AND event = sqlc.arg(event)::text
              AND COALESCE(last_error, '') LIKE
                  '%' || sqlc.arg(error_contains)::text || '%'
          )
      )
    ORDER BY received_at, delivery_guid
    LIMIT 100
    FOR UPDATE SKIP LOCKED
),
refused AS (
    UPDATE webhook_deliveries AS delivery
    SET last_error = format(
        'operator requeue refused at %s: payload unavailable%s; prior error: %s',
        clock_timestamp(),
        CASE
            WHEN selected.payload_pruned_at IS NOT NULL
            THEN format(' (pruned at %s)', selected.payload_pruned_at)
            ELSE ' (raw body is null)'
        END,
        COALESCE(delivery.last_error, '<none>')
    )
    FROM selected
    WHERE delivery.delivery_guid = selected.delivery_guid
      AND (
          selected.payload_pruned_at IS NOT NULL
          OR selected.raw_body IS NULL
      )
    RETURNING delivery.delivery_guid
),
candidates AS (
    SELECT delivery_guid
    FROM selected
    WHERE payload_pruned_at IS NULL
      AND raw_body IS NOT NULL
)
UPDATE webhook_deliveries AS delivery
SET status = 'pending',
    attempts = 0,
    next_attempt_at = clock_timestamp(),
    last_error = format(
        'operator requeue at %s; attempts reset from %s; prior error: %s',
        clock_timestamp(),
        delivery.attempts,
        COALESCE(delivery.last_error, '<none>')
    )
FROM candidates
WHERE delivery.delivery_guid = candidates.delivery_guid
  AND (SELECT count(*) FROM refused) >= 0;
