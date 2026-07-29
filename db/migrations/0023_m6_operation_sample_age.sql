-- Durable last-sample timestamps distinguish an idle/empty pass from a pass
-- that actually inspected or completed trust-critical work.

ALTER TABLE operation_heartbeats
    ADD COLUMN last_sample_at TIMESTAMPTZ;

UPDATE operation_heartbeats
SET last_sample_at = last_success_at
WHERE sample_count > 0;
