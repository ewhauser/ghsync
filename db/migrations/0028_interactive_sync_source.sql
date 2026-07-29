-- Q14: queue class and cache provenance are different axes. Preserve a real
-- interactive provenance value instead of recording user-driven refreshes as
-- backfill. Backfill jobs continue to write the existing backfill value.
ALTER TABLE repos
    DROP CONSTRAINT repos_sync_source_check,
    ADD CONSTRAINT repos_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE repo_rules
    DROP CONSTRAINT repo_rules_sync_source_check,
    ADD CONSTRAINT repo_rules_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE stacks
    DROP CONSTRAINT stacks_sync_source_check,
    ADD CONSTRAINT stacks_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE pull_requests
    DROP CONSTRAINT pull_requests_sync_source_check,
    ADD CONSTRAINT pull_requests_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE review_threads
    DROP CONSTRAINT review_threads_sync_source_check,
    ADD CONSTRAINT review_threads_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE check_runs
    DROP CONSTRAINT check_runs_sync_source_check,
    ADD CONSTRAINT check_runs_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));

ALTER TABLE check_history
    DROP CONSTRAINT check_history_sync_source_check,
    ADD CONSTRAINT check_history_sync_source_check
        CHECK (sync_source IN (
            'webhook', 'reconcile', 'backfill', 'manual', 'interactive'
        ));
