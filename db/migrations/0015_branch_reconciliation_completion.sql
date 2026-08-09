-- PostgreSQL data-modifying CTEs share one snapshot, so migration 0013's
-- sibling page and parent updates could leave a current reconciliation
-- incomplete after every declared page became terminal. Repair only current
-- generations with a complete manifest, preserving superseded old generations
-- that no longer have a parent row.
WITH terminal_pages AS (
    SELECT page.repo_id,
           page.branch,
           page.generation,
           count(*) AS page_count,
           max(page.completed_at) AS last_completed_at
    FROM branch_reconciliation_pages AS page
    GROUP BY page.repo_id, page.branch, page.generation
    HAVING count(*) FILTER (WHERE page.status = 'pending') = 0
       AND count(*) FILTER (WHERE page.completed_at IS NULL) = 0
)
UPDATE branch_reconciliations AS reconciliation
SET completed_at = terminal_pages.last_completed_at
FROM terminal_pages
WHERE reconciliation.repo_id = terminal_pages.repo_id
  AND reconciliation.branch = terminal_pages.branch
  AND reconciliation.generation = terminal_pages.generation
  AND reconciliation.completed_at IS NULL
  AND reconciliation.page_count > 0
  AND reconciliation.page_count = terminal_pages.page_count;
