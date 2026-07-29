-- M6 final review metric support. Migration 0021 may already be present in
-- local review databases, so this remains a separate forward-only change.

ALTER TABLE operation_heartbeats
    ADD COLUMN sample_count BIGINT NOT NULL DEFAULT 0
        CHECK (sample_count >= 0);

CREATE INDEX webhook_deliveries_unfinished_received_idx
ON webhook_deliveries (status, received_at)
WHERE status IN ('pending', 'processing', 'parked');

CREATE INDEX change_events_prunable_idx
ON change_events (occurred_at, seq, stream);
