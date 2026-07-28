-- Keep the dispatch claim poll proportional to pending work while preserving
-- the append-only ingress table's minimal write path (SYNC_ENGINE C-P1).
CREATE INDEX webhook_deliveries_pending_received_guid_idx
ON webhook_deliveries (received_at, delivery_guid)
WHERE status = 'pending';
