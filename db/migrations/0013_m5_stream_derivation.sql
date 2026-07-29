-- M5 public change-stream contract and derivation seam. Applied migrations
-- 0001-0012 remain immutable.

-- C-S2: assign the transaction ID before allocating the public sequence.
-- Watermark candidates rely on this ordering to prove that every event at or
-- below a captured sequence belongs to a transaction older than the candidate
-- barrier XID.
CREATE FUNCTION frontier_next_change_event_seq()
RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    PERFORM pg_current_xact_id();
    RETURN nextval('change_events_seq_seq'::regclass);
END
$$;

ALTER TABLE change_events
    ALTER COLUMN seq SET DEFAULT frontier_next_change_event_seq(),
    ADD COLUMN outbox_txid XID8;

-- Existing events were committed before this migration obtained its table
-- lock and are therefore safe at the initialized watermark below.
UPDATE change_events
SET outbox_txid = pg_current_xact_id()
WHERE outbox_txid IS NULL;

ALTER TABLE change_events
    ALTER COLUMN outbox_txid SET DEFAULT pg_current_xact_id(),
    ALTER COLUMN outbox_txid SET NOT NULL,
    ADD CONSTRAINT change_events_stream_nonempty CHECK (stream <> ''),
    ADD CONSTRAINT change_events_kind_nonempty CHECK (kind <> ''),
    ADD CONSTRAINT change_events_entity_key_nonempty CHECK (entity_key <> '');

-- C-S7: pruning is time-based and independent of consumer cursors. The
-- ordered index gives each bounded delete a small, deterministic old range.
CREATE INDEX change_events_occurred_at_seq_idx
ON change_events (occurred_at, seq);

-- C-S2: the singleton row carries the published safe sequence and one pending
-- candidate. A candidate is promoted only after its barrier XID precedes the
-- current PostgreSQL snapshot xmin. Lease fields coordinate the ~100ms loop.
CREATE TABLE stream_watermark (
    singleton       BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    safe_seq        BIGINT NOT NULL DEFAULT 0 CHECK (safe_seq >= 0),
    candidate_seq   BIGINT CHECK (candidate_seq >= safe_seq),
    candidate_xid   XID8,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token     TEXT,
    lease_until     TIMESTAMPTZ,
    CHECK ((candidate_seq IS NULL) = (candidate_xid IS NULL)),
    CHECK (
        (lease_token IS NULL AND lease_until IS NULL)
        OR
        (lease_token IS NOT NULL AND lease_until IS NOT NULL)
    )
);

INSERT INTO stream_watermark (safe_seq, updated_at)
SELECT COALESCE(max(seq), 0), now()
FROM change_events;

-- C-S1/C-S4: service consumers own one durable cursor per stream. A cursor is
-- advanced in the same transaction as its handler's database effects.
CREATE TABLE consumer_cursors (
    consumer    TEXT NOT NULL CHECK (consumer <> ''),
    stream      TEXT NOT NULL CHECK (stream <> ''),
    seq         BIGINT NOT NULL DEFAULT 0 CHECK (seq >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, stream)
);

-- C-S4/C-S7: retaining a per-stream deleted horizon distinguishes an empty
-- stream from one whose entire retained history was pruned.
CREATE TABLE stream_horizons (
    stream              TEXT PRIMARY KEY CHECK (stream <> ''),
    pruned_through_seq  BIGINT NOT NULL DEFAULT 0
                        CHECK (pruned_through_seq >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- C-D3: identity_key is domain identity, not a generated row ID. The real
-- derivation engine may add fields later under the additive-only contract.
CREATE TABLE work_items (
    identity_key  TEXT PRIMARY KEY CHECK (identity_key <> ''),
    org_id        BIGINT NOT NULL,
    payload       JSONB NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX work_items_org_identity_idx
ON work_items (org_id, identity_key);

-- C-S5/C-S6: NOTIFY only lowers wake latency. The trigger fires at commit and
-- PostgreSQL coalesces identical channel/payload pairs within a transaction.
-- pkg/streamclient still polls, so notification delivery is never correctness
-- critical.
CREATE FUNCTION frontier_notify_change_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('frontier_change_events', NEW.stream);
    RETURN NULL;
END
$$;

CREATE TRIGGER change_events_notify
AFTER INSERT ON change_events
FOR EACH ROW EXECUTE FUNCTION frontier_notify_change_event();

-- C-P5: the same wake optimization lets the deriver drain a whole dirty set
-- promptly; its polling fallback remains the correctness path.
CREATE FUNCTION frontier_notify_derivation_dirty()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('frontier_derivation_dirty', NEW.scope_key);
    RETURN NULL;
END
$$;

CREATE TRIGGER derivation_dirty_notify
AFTER INSERT OR UPDATE ON derivation_dirty
FOR EACH ROW EXECUTE FUNCTION frontier_notify_derivation_dirty();
