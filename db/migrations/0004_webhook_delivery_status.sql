-- Dispatcher lifecycle and poison parking (SYNC_ENGINE C-I5/C-P2). Keep the
-- ingress table's exactly-one-index invariant: claims deliberately use row
-- locking without adding a status index.
ALTER TABLE webhook_deliveries
ADD CONSTRAINT webhook_deliveries_status_check
CHECK (status IN ('pending', 'processing', 'processed', 'parked'));
