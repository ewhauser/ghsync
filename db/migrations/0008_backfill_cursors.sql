-- C-O5 durable cursor for the ordinary River backfill job chain. The job
-- carries an expected phase/page; this row is the restart authority.
CREATE TABLE backfill_cursors (
    installation_id  BIGINT NOT NULL,
    repo_full_name   TEXT NOT NULL,
    phase            TEXT NOT NULL
                     CHECK (phase IN ('repository', 'stacks', 'pull_requests', 'done')),
    page             INT NOT NULL CHECK (page > 0),
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (installation_id, repo_full_name),
    CHECK ((phase = 'done') = (completed_at IS NOT NULL))
);
