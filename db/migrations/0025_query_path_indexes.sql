-- Wave B query-path corrections. Applied migrations 0001-0024 remain
-- immutable.

-- Q8: the check-runs UNIQUE (repo_id, head_sha, gh_id) already supports
-- repository/SHA lookups, and the original change-event prune index already
-- has the exact (occurred_at, seq) ordering used by the retention batch.
-- Keeping either duplicate adds write amplification without another plan.
DROP INDEX check_runs_repo_head_sha_idx;
DROP INDEX change_events_prunable_idx;

-- Q9: C-R1 scans one state class at a time and consume rows in global
-- last_checked_at order. Encode the state in a partial predicate and lead
-- with the ordering key; repo_id/number make the order deterministic and
-- support the repository join. EXPLAIN for each ListStale* query can therefore
-- use an ordered Index Scan (with no full-population Sort) before the repos
-- primary-key join.
DROP INDEX stacks_live_open_checked_idx;
DROP INDEX pull_requests_live_state_checked_idx;
DROP INDEX stacks_display_retained_checked_idx;
DROP INDEX pull_requests_display_retained_checked_idx;

CREATE INDEX stacks_stale_open_checked_idx
ON stacks (last_checked_at, repo_id, number)
WHERE tombstoned_at IS NULL AND open;

CREATE INDEX stacks_stale_closed_checked_idx
ON stacks (last_checked_at, repo_id, number)
INCLUDE (display_until)
WHERE tombstoned_at IS NULL AND NOT open;

CREATE INDEX pull_requests_stale_open_checked_idx
ON pull_requests (last_checked_at, repo_id, number)
WHERE tombstoned_at IS NULL AND state = 'open';

CREATE INDEX pull_requests_stale_closed_checked_idx
ON pull_requests (last_checked_at, repo_id, number)
INCLUDE (display_until)
WHERE tombstoned_at IS NULL AND state <> 'open';

-- Q10: BRIN is useful for range rejection but cannot produce the top-N order
-- used by the bounded payload/history batches. These btrees match each
-- ORDER BY exactly; the payload predicate also excludes rows the pruner can
-- never update. The old BRIN indexes are removed so comments and write cost
-- reflect the plan actually used.
DROP INDEX webhook_deliveries_received_at_brin_idx;
DROP INDEX check_history_synced_at_brin_idx;

CREATE INDEX webhook_deliveries_prunable_btree_idx
ON webhook_deliveries (received_at, delivery_guid)
WHERE raw_body IS NOT NULL AND status = 'processed';

CREATE INDEX check_history_prunable_btree_idx
ON check_history (synced_at, id);
