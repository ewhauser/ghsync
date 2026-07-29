-- Functions and triggers on top of the base tables.
--
-- The writer fence (ghsync_require_change_event_writer_fence) DB-enforces
-- C-S: change_events inserts must hold the shared advisory fence lock
-- (classid=1181904750, objid=1953064306, objsubid=1). The notify functions
-- publish statement-level wakes for the change stream and deriver.

-- ------------------------------------------------------------------
-- Functions
-- ------------------------------------------------------------------

-- ghsync_notify_change_event() (function)
CREATE FUNCTION ghsync_notify_change_event() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Constant payloads coalesce within a transaction. The transition table
    -- makes this one trigger invocation for an arbitrarily large INSERT.
    PERFORM pg_notify('ghsync_change_events', 'changed');
    RETURN NULL;
END
$$;

-- ghsync_notify_derivation_dirty() (function)
CREATE FUNCTION ghsync_notify_derivation_dirty() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- INSERT ... ON CONFLICT can run both statement trigger classes, but
    -- PostgreSQL coalesces their identical channel/payload pair at commit.
    PERFORM pg_notify('ghsync_derivation_dirty', 'dirty');
    RETURN NULL;
END
$$;

-- ghsync_require_change_event_writer_fence() (function)
CREATE FUNCTION ghsync_require_change_event_writer_fence() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND pid = pg_backend_pid()
          AND classid = 1181904750
          AND objid = 1953064306
          AND objsubid = 1
          AND mode = 'ShareLock'
          AND granted
    ) THEN
        RAISE EXCEPTION
            USING ERRCODE = '55000',
                  MESSAGE = 'change_events INSERT requires the shared writer fence';
    END IF;
    RETURN NEW;
END;
$$;

-- ------------------------------------------------------------------
-- Triggers
-- ------------------------------------------------------------------

-- change_events change_events_notify (trigger)
CREATE TRIGGER change_events_notify AFTER INSERT ON change_events REFERENCING NEW TABLE AS inserted_events FOR EACH STATEMENT EXECUTE FUNCTION ghsync_notify_change_event();

-- change_events change_events_require_writer_fence (trigger)
CREATE TRIGGER change_events_require_writer_fence BEFORE INSERT ON change_events FOR EACH ROW EXECUTE FUNCTION ghsync_require_change_event_writer_fence();

-- derivation_dirty derivation_dirty_notify_insert (trigger)
CREATE TRIGGER derivation_dirty_notify_insert AFTER INSERT ON derivation_dirty REFERENCING NEW TABLE AS inserted_dirty FOR EACH STATEMENT EXECUTE FUNCTION ghsync_notify_derivation_dirty();

-- derivation_dirty derivation_dirty_notify_update (trigger)
CREATE TRIGGER derivation_dirty_notify_update AFTER UPDATE ON derivation_dirty REFERENCING NEW TABLE AS updated_dirty FOR EACH STATEMENT EXECUTE FUNCTION ghsync_notify_derivation_dirty();
