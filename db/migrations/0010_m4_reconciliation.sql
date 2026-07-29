-- M4 reconciliation, validation, drift, and bounded retention.
-- Applied migrations 0001-0009 remain immutable.

-- C-R2 restart authority for one authoritative listing scope. The opaque
-- cursor is either a GitHub cursor or a decimal REST page number. Page bodies
-- are not retained here; only the compact set of entity keys seen so far.
CREATE TABLE sweep_cursors (
    installation_id  BIGINT NOT NULL,
    sweep_kind       TEXT NOT NULL,
    scope_key        TEXT NOT NULL DEFAULT '',
    cursor            TEXT NOT NULL DEFAULT '',
    seen_keys         JSONB NOT NULL DEFAULT '[]'::jsonb,
    started_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    PRIMARY KEY (installation_id, sweep_kind, scope_key),
    CHECK (jsonb_typeof(seen_keys) = 'array'),
    CHECK (
        (started_at IS NULL AND completed_at IS NULL)
        OR started_at IS NOT NULL
    )
);

-- C-R2/C-B4 retains per-page validators and membership. A 304 can therefore
-- advance the persisted cursor and participate in disappearance detection
-- without downloading the unchanged page again.
CREATE TABLE sweep_pages (
    installation_id  BIGINT NOT NULL,
    sweep_kind       TEXT NOT NULL,
    scope_key        TEXT NOT NULL DEFAULT '',
    cursor            TEXT NOT NULL,
    etag              TEXT NOT NULL DEFAULT '',
    next_cursor       TEXT NOT NULL DEFAULT '',
    entity_keys       JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_checked_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (installation_id, sweep_kind, scope_key, cursor),
    FOREIGN KEY (installation_id, sweep_kind, scope_key)
        REFERENCES sweep_cursors(installation_id, sweep_kind, scope_key)
        ON DELETE CASCADE,
    CHECK (jsonb_typeof(entity_keys) = 'array')
);

-- C-O3 records every semantic divergence with both snapshots and an attached
-- field-level diff. Findings are evidence, not transient log messages.
CREATE TABLE drift_findings (
    id                    BIGSERIAL PRIMARY KEY,
    installation_id       BIGINT NOT NULL,
    entity_kind           TEXT NOT NULL,
    entity_key            TEXT NOT NULL,
    detected_at           TIMESTAMPTZ NOT NULL,
    cache_snapshot        JSONB NOT NULL,
    upstream_snapshot     JSONB NOT NULL,
    diff                  JSONB NOT NULL,
    refresh_enqueued_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX drift_findings_detected_at_idx
ON drift_findings (detected_at DESC);

-- The 90-day payload decision preserves the GUID/status/time skeleton used by
-- C-R4 while allowing the bulky body and headers to be discarded.
ALTER TABLE webhook_deliveries
    ALTER COLUMN raw_body DROP NOT NULL,
    ADD COLUMN payload_pruned_at TIMESTAMPTZ;

-- C-R1 staleness scans use validation time (last_checked_at), because a 304 is
-- a successful freshness proof without a semantic cache mutation.
CREATE INDEX repos_live_installation_checked_idx
ON repos (installation_id, last_checked_at)
WHERE tombstoned_at IS NULL;

CREATE INDEX stacks_live_open_checked_idx
ON stacks (repo_id, open, last_checked_at)
WHERE tombstoned_at IS NULL;

CREATE INDEX pull_requests_live_state_checked_idx
ON pull_requests (repo_id, state, last_checked_at)
WHERE tombstoned_at IS NULL;
