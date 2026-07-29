-- Backfill scope ownership outside the metadata-lock transaction in 0015.
-- This takes row locks only. M5's default deriver means the table should be
-- empty; installations with experimental rows should monitor row-lock waits.
-- If this table ever grows beyond the initial M5 scale, operators should run
-- this same predicate in bounded identity-key batches before deploying 0016.

-- Existing v1 identities contain repository GitHub ID, kind, and number.
-- Resolve installation ownership through repos; malformed experimental rows
-- deliberately remain NULL so the forward constraint fails closed.
UPDATE work_items
SET scope_key = split_part(identity_key, ':', 3) || ':' ||
                repos.installation_id::text || ':' ||
                repos.gh_id::text || ':' ||
                split_part(identity_key, ':', 4)
FROM repos
WHERE split_part(work_items.identity_key, ':', 1) = 'repo'
  AND split_part(work_items.identity_key, ':', 2) = repos.gh_id::text
  AND split_part(work_items.identity_key, ':', 3) IN ('stack', 'pr')
  AND split_part(work_items.identity_key, ':', 4) ~ '^[1-9][0-9]*$';
