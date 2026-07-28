-- name: InsertWebhookDelivery :execrows
-- C-I3: duplicate deliveries are free no-ops.
INSERT INTO webhook_deliveries (delivery_guid, event, raw_body, headers)
VALUES ($1, $2, $3, $4)
ON CONFLICT (delivery_guid) DO NOTHING;

-- name: GetWebhookDelivery :one
SELECT * FROM webhook_deliveries WHERE delivery_guid = $1;

-- name: CountWebhookDeliveriesByStatus :one
SELECT count(*) FROM webhook_deliveries WHERE status = $1;
