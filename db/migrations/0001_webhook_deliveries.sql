-- Ingestion table (SYNC_ENGINE C-I1/C-I3/C-P1): append-only, minimal, and
-- exactly one index — the delivery GUID primary key. The ingress commit path
-- is a single-row insert into this table and nothing else.
CREATE TABLE webhook_deliveries (
    delivery_guid TEXT PRIMARY KEY,
    event         TEXT NOT NULL,
    raw_body      BYTEA NOT NULL,
    headers       JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INT NOT NULL DEFAULT 0,
    last_error    TEXT
);
