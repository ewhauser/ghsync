-- M6 durable operational state. Applied migrations 0001-0019 remain
-- immutable.

-- C-Q2/C-O4: event-to-cache latency must survive dispatch/fetch role
-- separation, process restarts, and River coalescing. The earliest outstanding
-- webhook timestamp is cleared only when its observed generation completes.
ALTER TABLE refresh_intent_generations
    ADD COLUMN event_received_at TIMESTAMPTZ;

-- C-S4/C-O4: resyncs are consumer-visible protocol events. Persist their
-- monotonic count beside the cursor so every role's metrics endpoint can
-- expose one authoritative value instead of a process-local approximation.
ALTER TABLE consumer_cursors
    ADD COLUMN resync_count BIGINT NOT NULL DEFAULT 0
        CHECK (resync_count >= 0),
    ADD COLUMN last_resync_at TIMESTAMPTZ;
