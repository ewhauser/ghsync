-- M3 authoritative mirror and transactional entity-change seam.
-- Every mirror row carries C-C5 provenance, C-C2 version columns, and the
-- C-C4 tombstone marker. Consumers must treat tombstoned rows as retained
-- history, never as live entities.
CREATE TABLE repos (
    id                BIGSERIAL PRIMARY KEY,
    installation_id   BIGINT NOT NULL,
    org_id             BIGINT NOT NULL,
    gh_id              BIGINT NOT NULL UNIQUE,
    node_id            TEXT NOT NULL,
    owner               TEXT NOT NULL,
    name                TEXT NOT NULL,
    full_name           TEXT NOT NULL UNIQUE,
    default_branch      TEXT NOT NULL,
    archived            BOOLEAN NOT NULL DEFAULT false,
    gh_updated_at       TIMESTAMPTZ,
    head_sha            TEXT NOT NULL DEFAULT '',
    synced_at           TIMESTAMPTZ NOT NULL,
    etag                TEXT NOT NULL DEFAULT '',
    sync_source         TEXT NOT NULL
                        CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at       TIMESTAMPTZ,
    UNIQUE (installation_id, owner, name)
);

CREATE TABLE repo_rules (
    repo_id             BIGINT NOT NULL REFERENCES repos(id),
    rule_key            TEXT NOT NULL,
    rule                JSONB NOT NULL,
    gh_updated_at       TIMESTAMPTZ,
    head_sha            TEXT NOT NULL DEFAULT '',
    synced_at           TIMESTAMPTZ NOT NULL,
    etag                TEXT NOT NULL DEFAULT '',
    sync_source         TEXT NOT NULL
                        CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at       TIMESTAMPTZ,
    PRIMARY KEY (repo_id, rule_key)
);

CREATE TABLE stacks (
    id                BIGSERIAL PRIMARY KEY,
    repo_id           BIGINT NOT NULL REFERENCES repos(id),
    gh_id              BIGINT,
    node_id            TEXT NOT NULL DEFAULT '',
    number             INT NOT NULL CHECK (number > 0),
    base_ref           TEXT NOT NULL DEFAULT '',
    base_sha           TEXT NOT NULL DEFAULT '',
    open               BOOLEAN NOT NULL DEFAULT false,
    entries            JSONB NOT NULL DEFAULT '[]'::jsonb,
    gh_updated_at      TIMESTAMPTZ,
    head_sha           TEXT NOT NULL DEFAULT '',
    synced_at          TIMESTAMPTZ NOT NULL,
    etag               TEXT NOT NULL DEFAULT '',
    sync_source        TEXT NOT NULL
                       CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at      TIMESTAMPTZ,
    UNIQUE (repo_id, number),
    UNIQUE (repo_id, gh_id)
);

CREATE TABLE pull_requests (
    id                BIGSERIAL PRIMARY KEY,
    repo_id           BIGINT NOT NULL REFERENCES repos(id),
    gh_id              BIGINT,
    node_id            TEXT NOT NULL DEFAULT '',
    number             INT NOT NULL CHECK (number > 0),
    title              TEXT NOT NULL DEFAULT '',
    state              TEXT NOT NULL DEFAULT '',
    draft              BOOLEAN NOT NULL DEFAULT false,
    author_login       TEXT NOT NULL DEFAULT '',
    head_ref           TEXT NOT NULL DEFAULT '',
    head_sha           TEXT NOT NULL DEFAULT '',
    base_ref           TEXT NOT NULL DEFAULT '',
    base_sha           TEXT NOT NULL DEFAULT '',
    review_decision    TEXT NOT NULL DEFAULT '',
    mergeable_state    TEXT NOT NULL DEFAULT '',
    stack_number       INT,
    stack_position     INT,
    gh_updated_at      TIMESTAMPTZ,
    synced_at          TIMESTAMPTZ NOT NULL,
    etag               TEXT NOT NULL DEFAULT '',
    sync_source        TEXT NOT NULL
                       CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at      TIMESTAMPTZ,
    UNIQUE (repo_id, number),
    UNIQUE (repo_id, gh_id),
    CHECK ((stack_number IS NULL) = (stack_position IS NULL))
);

CREATE INDEX pull_requests_repo_head_ref_idx
ON pull_requests (repo_id, head_ref)
WHERE tombstoned_at IS NULL;

CREATE INDEX pull_requests_repo_base_ref_idx
ON pull_requests (repo_id, base_ref)
WHERE tombstoned_at IS NULL;

CREATE INDEX pull_requests_repo_stack_idx
ON pull_requests (repo_id, stack_number)
WHERE tombstoned_at IS NULL AND stack_number IS NOT NULL;

CREATE TABLE review_threads (
    id                TEXT PRIMARY KEY,
    repo_id           BIGINT NOT NULL REFERENCES repos(id),
    pr_number         INT NOT NULL CHECK (pr_number > 0),
    is_resolved       BOOLEAN NOT NULL DEFAULT false,
    is_outdated       BOOLEAN NOT NULL DEFAULT false,
    path              TEXT NOT NULL DEFAULT '',
    line              INT,
    comments          JSONB NOT NULL DEFAULT '[]'::jsonb,
    gh_updated_at     TIMESTAMPTZ,
    head_sha           TEXT NOT NULL DEFAULT '',
    synced_at          TIMESTAMPTZ NOT NULL,
    etag               TEXT NOT NULL DEFAULT '',
    sync_source        TEXT NOT NULL
                       CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at      TIMESTAMPTZ,
    FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_requests(repo_id, number)
);

CREATE INDEX review_threads_pr_idx
ON review_threads (repo_id, pr_number);

CREATE TABLE check_runs (
    gh_id              BIGINT PRIMARY KEY,
    repo_id            BIGINT NOT NULL REFERENCES repos(id),
    node_id            TEXT NOT NULL DEFAULT '',
    name               TEXT NOT NULL,
    status             TEXT NOT NULL,
    conclusion         TEXT NOT NULL DEFAULT '',
    details_url        TEXT NOT NULL DEFAULT '',
    app_slug           TEXT NOT NULL DEFAULT '',
    started_at         TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    gh_updated_at      TIMESTAMPTZ,
    head_sha           TEXT NOT NULL,
    synced_at          TIMESTAMPTZ NOT NULL,
    etag               TEXT NOT NULL DEFAULT '',
    sync_source        TEXT NOT NULL
                       CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at      TIMESTAMPTZ,
    UNIQUE (repo_id, head_sha, gh_id)
);

CREATE INDEX check_runs_repo_head_sha_idx
ON check_runs (repo_id, head_sha);

CREATE TABLE check_history (
    id                BIGSERIAL PRIMARY KEY,
    check_run_gh_id   BIGINT NOT NULL REFERENCES check_runs(gh_id),
    repo_id           BIGINT NOT NULL REFERENCES repos(id),
    name              TEXT NOT NULL,
    status            TEXT NOT NULL,
    conclusion        TEXT NOT NULL DEFAULT '',
    observed          JSONB NOT NULL,
    gh_updated_at     TIMESTAMPTZ,
    head_sha           TEXT NOT NULL,
    synced_at          TIMESTAMPTZ NOT NULL,
    etag               TEXT NOT NULL DEFAULT '',
    sync_source        TEXT NOT NULL
                       CHECK (sync_source IN ('webhook', 'reconcile', 'backfill', 'manual')),
    tombstoned_at      TIMESTAMPTZ,
    UNIQUE NULLS NOT DISTINCT (
        check_run_gh_id,
        status,
        conclusion,
        gh_updated_at,
        head_sha
    )
);

CREATE INDEX check_history_repo_head_sha_synced_idx
ON check_history (repo_id, head_sha, synced_at);

-- C-D2 dirty-set seam. M5 owns draining this table.
CREATE TABLE derivation_dirty (
    scope_key  TEXT PRIMARY KEY,
    marked_at  TIMESTAMPTZ NOT NULL
);

-- C-C3 entities outbox. M5 adds the visibility watermark and consumers.
CREATE TABLE change_events (
    seq          BIGSERIAL PRIMARY KEY,
    stream       TEXT NOT NULL,
    kind         TEXT NOT NULL,
    entity_key   TEXT NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload      JSONB NOT NULL
);

CREATE INDEX change_events_stream_seq_idx
ON change_events (stream, seq);
