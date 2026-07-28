-- name: EnsureInstallationBudget :exec
INSERT INTO installation_budgets (installation_id, class)
VALUES ($1, $2)
ON CONFLICT (installation_id, class) DO NOTHING;

-- name: LockInstallationBudgets :many
SELECT class, remaining, rate_limit, reset_at, lease_owner, lease_until,
       (
           lease_owner IS NOT NULL
           AND lease_owner <> sqlc.arg(owner)
           AND lease_until > now()
       )::boolean AS held_by_other
FROM installation_budgets
WHERE installation_id = sqlc.arg(installation_id)
ORDER BY class
FOR UPDATE;

-- name: AcquireInstallationBudgetLease :execrows
UPDATE installation_budgets
SET lease_owner = sqlc.arg(owner),
    lease_until = now() + sqlc.arg(ttl_seconds)::double precision * interval '1 second'
WHERE installation_id = sqlc.arg(installation_id);

-- name: RenewInstallationBudgetLease :execrows
UPDATE installation_budgets
SET lease_until = now() + sqlc.arg(ttl_seconds)::double precision * interval '1 second'
WHERE installation_id = sqlc.arg(installation_id)
  AND lease_owner = sqlc.arg(owner)
  AND lease_until > now();

-- name: SaveInstallationBudgetSnapshot :execrows
UPDATE installation_budgets
SET remaining = CASE class
        WHEN 'rest' THEN sqlc.narg(rest_remaining)::bigint
        WHEN 'graphql' THEN sqlc.narg(graphql_remaining)::bigint
    END,
    rate_limit = CASE class
        WHEN 'rest' THEN sqlc.narg(rest_limit)::bigint
        WHEN 'graphql' THEN sqlc.narg(graphql_limit)::bigint
    END,
    reset_at = CASE class
        WHEN 'rest' THEN sqlc.narg(rest_reset_at)::timestamptz
        WHEN 'graphql' THEN sqlc.narg(graphql_reset_at)::timestamptz
    END,
    updated_at = now()
WHERE installation_id = sqlc.arg(installation_id)
  AND lease_owner = sqlc.arg(owner)
  AND lease_until > now();

-- name: ReleaseInstallationBudgetLease :exec
UPDATE installation_budgets
SET lease_owner = NULL, lease_until = NULL
WHERE installation_id = $1 AND lease_owner = $2;
