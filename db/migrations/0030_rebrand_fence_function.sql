-- Rebrand: DB objects created by applied (immutable) migrations under the
-- legacy frontier_* names move to ghsync_*. Function renames are OID-safe
-- (triggers keep working); the notify functions are REPLACED because their
-- bodies embed the legacy channel names the Go side no longer listens on.

ALTER FUNCTION frontier_require_change_event_writer_fence()
    RENAME TO ghsync_require_change_event_writer_fence;

ALTER FUNCTION frontier_notify_change_event()
    RENAME TO ghsync_notify_change_event;
CREATE OR REPLACE FUNCTION ghsync_notify_change_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Constant payloads coalesce within a transaction. The transition table
    -- makes this one trigger invocation for an arbitrarily large INSERT.
    PERFORM pg_notify('ghsync_change_events', 'changed');
    RETURN NULL;
END
$$;

ALTER FUNCTION frontier_notify_derivation_dirty()
    RENAME TO ghsync_notify_derivation_dirty;
CREATE OR REPLACE FUNCTION ghsync_notify_derivation_dirty()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- INSERT ... ON CONFLICT can run both statement trigger classes, but
    -- PostgreSQL coalesces their identical channel/payload pair at commit.
    PERFORM pg_notify('ghsync_derivation_dirty', 'dirty');
    RETURN NULL;
END
$$;
