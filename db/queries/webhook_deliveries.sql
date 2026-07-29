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
    SELECT delivery_guid
    FROM webhook_deliveries
    WHERE status = 'pending'
    ORDER BY received_at, delivery_guid
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
    last_error = NULLIF(result.last_error, '')
FROM jsonb_to_recordset(sqlc.arg(results)::jsonb)
    AS result(delivery_guid text, status text, last_error text)
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
-- event/error signature. There is no unscoped "all parked" path.
WITH candidates AS (
    SELECT delivery_guid
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
)
UPDATE webhook_deliveries AS delivery
SET status = 'pending',
    attempts = 0,
    last_error = format(
        'operator requeue at %s; attempts reset from %s; prior error: %s',
        clock_timestamp(),
        delivery.attempts,
        COALESCE(delivery.last_error, '<none>')
    )
FROM candidates
WHERE delivery.delivery_guid = candidates.delivery_guid;
