-- C-S2/T1: allocating a change-event sequence without the shared writer
-- fence invalidates the visibility watermark. Enforce the protocol in
-- Postgres so new writers cannot accidentally bypass the Go helper.
CREATE FUNCTION frontier_require_change_event_writer_fence()
RETURNS TRIGGER
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

CREATE TRIGGER change_events_require_writer_fence
BEFORE INSERT ON change_events
FOR EACH ROW
EXECUTE FUNCTION frontier_require_change_event_writer_fence();
