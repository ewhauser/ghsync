-- M3 review hardening. Applied migrations 0001-0008 remain immutable.

-- A successful conditional validation is provenance, but not a cache change.
-- Keep synced_at as the time the domain snapshot changed and record cheap
-- 304/identical-200 validation independently.
ALTER TABLE repos
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE repos SET last_checked_at = synced_at;
ALTER TABLE repos
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE repo_rules
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE repo_rules SET last_checked_at = synced_at;
ALTER TABLE repo_rules
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE stacks
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE stacks SET last_checked_at = synced_at;
ALTER TABLE stacks
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE pull_requests
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE pull_requests SET last_checked_at = synced_at;
ALTER TABLE pull_requests
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE review_threads
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE review_threads SET last_checked_at = synced_at;
ALTER TABLE review_threads
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE check_runs
    ADD COLUMN semantic_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN last_checked_at TIMESTAMPTZ;
UPDATE check_runs SET last_checked_at = synced_at;
ALTER TABLE check_runs
    ALTER COLUMN last_checked_at SET NOT NULL;

ALTER TABLE check_history
    ADD COLUMN semantic_version TEXT NOT NULL DEFAULT '';

-- The sync path appends history only for accepted transitions, so the old
-- domain-value uniqueness constraint incorrectly collapsed A -> B -> A.
DO $$
DECLARE
    history_unique_name TEXT;
BEGIN
    SELECT conname
    INTO history_unique_name
    FROM pg_constraint
    WHERE conrelid = 'check_history'::regclass
      AND contype = 'u';

    IF history_unique_name IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE check_history DROP CONSTRAINT %I',
            history_unique_name
        );
    END IF;
END
$$;

-- Retention is global by synced_at, not scoped to a repo/SHA. BRIN matches the
-- append order and keeps the 90-day pruning index compact.
CREATE INDEX check_history_synced_at_brin_idx
ON check_history USING BRIN (synced_at);

-- GitHub ID is the repository identity. Names are mutable aliases retained so
-- jobs/webhooks carrying an old name can still resolve after rename/transfer.
CREATE TABLE repo_aliases (
    full_name       TEXT PRIMARY KEY,
    repo_id         BIGINT NOT NULL REFERENCES repos(id),
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL
);

INSERT INTO repo_aliases (full_name, repo_id, first_seen_at, last_seen_at)
SELECT full_name, id, synced_at, last_checked_at
FROM repos;

ALTER TABLE repos
    DROP CONSTRAINT IF EXISTS repos_full_name_key,
    DROP CONSTRAINT IF EXISTS repos_installation_id_owner_name_key;

CREATE INDEX repos_full_name_idx ON repos (full_name);

-- Collection-level conditional metadata is required even when a repository
-- currently has zero rules and therefore no repo_rules row to carry an ETag.
CREATE TABLE repo_rule_sync_state (
    repo_id          BIGINT PRIMARY KEY REFERENCES repos(id),
    etag             TEXT NOT NULL DEFAULT '',
    last_checked_at  TIMESTAMPTZ NOT NULL
);

-- A generation is complete only after a worker has committed the
-- authoritative observation which covered it. Backfill children wait on this
-- value instead of treating enqueue as completion.
ALTER TABLE refresh_intent_generations
    ADD COLUMN completed_generation BIGINT NOT NULL DEFAULT 0
    CHECK (completed_generation >= 0 AND completed_generation <= generation);

ALTER TABLE backfill_cursors
    DROP CONSTRAINT IF EXISTS backfill_cursors_phase_check,
    DROP CONSTRAINT IF EXISTS backfill_cursors_check;

ALTER TABLE backfill_cursors
    ADD COLUMN queue_name TEXT NOT NULL DEFAULT 'interactive'
        CHECK (queue_name IN ('interactive', 'sweep')),
    ADD CONSTRAINT backfill_cursors_phase_check
        CHECK (phase IN (
            'repository',
            'stacks',
            'pull_requests',
            'waiting',
            'done'
        )),
    ADD CONSTRAINT backfill_cursors_phase_completed_at_check
        CHECK ((phase = 'done') = (completed_at IS NOT NULL));

CREATE TABLE installation_backfill_cursors (
    installation_id  BIGINT PRIMARY KEY,
    phase            TEXT NOT NULL
                     CHECK (phase IN ('repositories', 'waiting', 'done')),
    page             INT NOT NULL CHECK (page > 0),
    queue_name       TEXT NOT NULL DEFAULT 'interactive'
                     CHECK (queue_name IN ('interactive', 'sweep')),
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((phase = 'done') = (completed_at IS NOT NULL))
);

CREATE TABLE backfill_children (
    installation_id   BIGINT NOT NULL,
    repo_full_name    TEXT NOT NULL,
    kind              TEXT NOT NULL,
    refresh_key       TEXT NOT NULL,
    target_generation BIGINT NOT NULL CHECK (target_generation > 0),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    PRIMARY KEY (installation_id, repo_full_name, kind, refresh_key),
    FOREIGN KEY (installation_id, repo_full_name)
        REFERENCES backfill_cursors(installation_id, repo_full_name)
);

CREATE INDEX backfill_children_pending_idx
ON backfill_children (installation_id, repo_full_name)
WHERE completed_at IS NULL;
