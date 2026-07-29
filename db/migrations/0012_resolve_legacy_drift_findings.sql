-- Pre-0011 rows have no trustworthy heal generation. Preserve them as
-- resolved history so the next observation creates one heal-bounded open
-- finding instead of touching an unhealable legacy row forever.
UPDATE drift_findings
SET resolved_at = detected_at,
    last_seen_at = GREATEST(last_seen_at, detected_at)
WHERE heal_generation = 0
  AND resolved_at IS NULL;
