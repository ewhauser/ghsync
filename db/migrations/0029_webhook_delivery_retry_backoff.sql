-- Poison deliveries must become due again before another dispatcher can
-- claim them. This prevents a tight Run loop from consuming the full attempt
-- budget back-to-back.
ALTER TABLE webhook_deliveries
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Replace the old pending-order index with the due-time order used by claims.
-- The processed-payload retention index from 0025 covers its separate path.
DROP INDEX webhook_deliveries_pending_received_guid_idx;

CREATE INDEX webhook_deliveries_pending_due_idx
ON webhook_deliveries (next_attempt_at, received_at, delivery_guid)
WHERE status = 'pending';
