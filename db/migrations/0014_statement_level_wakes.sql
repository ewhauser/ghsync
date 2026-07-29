-- Forward-only M5 wake-trigger repair. Migration 0013 is already applied and
-- remains immutable.
--
-- These DDL statements need brief relation locks to replace trigger metadata,
-- but they neither scan nor rewrite table data. Production rollout should be
-- retried during a quieter write interval if the five-second lock budget is
-- exhausted; do not raise the timeout and turn deploy startup into an
-- unbounded writer outage.
SET LOCAL lock_timeout = '5s';

DROP TRIGGER change_events_notify ON change_events;

CREATE OR REPLACE FUNCTION frontier_notify_change_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Constant payloads coalesce within a transaction. The transition table
    -- makes this one trigger invocation for an arbitrarily large INSERT.
    PERFORM pg_notify('frontier_change_events', 'changed');
    RETURN NULL;
END
$$;

CREATE TRIGGER change_events_notify
AFTER INSERT ON change_events
REFERENCING NEW TABLE AS inserted_events
FOR EACH STATEMENT EXECUTE FUNCTION frontier_notify_change_event();

DROP TRIGGER derivation_dirty_notify ON derivation_dirty;

CREATE OR REPLACE FUNCTION frontier_notify_derivation_dirty()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- INSERT ... ON CONFLICT can run both statement trigger classes, but
    -- PostgreSQL coalesces their identical channel/payload pair at commit.
    PERFORM pg_notify('frontier_derivation_dirty', 'dirty');
    RETURN NULL;
END
$$;

CREATE TRIGGER derivation_dirty_notify_insert
AFTER INSERT ON derivation_dirty
REFERENCING NEW TABLE AS inserted_dirty
FOR EACH STATEMENT EXECUTE FUNCTION frontier_notify_derivation_dirty();

CREATE TRIGGER derivation_dirty_notify_update
AFTER UPDATE ON derivation_dirty
REFERENCING NEW TABLE AS updated_dirty
FOR EACH STATEMENT EXECUTE FUNCTION frontier_notify_derivation_dirty();
