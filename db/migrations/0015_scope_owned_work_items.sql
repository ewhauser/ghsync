-- Forward-only scope ownership for M5 derived work. Migration 0013 is already
-- applied and remains immutable.
--
-- This migration contains only the metadata-only column addition so its ACCESS
-- EXCLUSIVE lock is released before the forward backfill in 0016. M5's shipped
-- NoopDeriver leaves this new table empty; an installation that populated it
-- with an experimental deriver should still deploy during a quiet interval.
SET LOCAL lock_timeout = '5s';

ALTER TABLE work_items
    ADD COLUMN scope_key TEXT;
