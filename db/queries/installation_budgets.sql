-- name: EnsureInstallationBudget :exec
INSERT INTO installation_budgets (installation_id, class)
VALUES ($1, $2)
ON CONFLICT (installation_id, class) DO NOTHING;

-- name: AcquireInstallationBudgetLease :many
WITH locked AS MATERIALIZED (
    SELECT class
    FROM installation_budgets
    WHERE installation_id = sqlc.arg(installation_id)
    ORDER BY class
    FOR UPDATE
),
proposed AS MATERIALIZED (
    SELECT clock_timestamp()
           + sqlc.arg(ttl_seconds)::double precision * interval '1 second'
           AS lease_until
    FROM (SELECT count(*) FROM locked) AS after_lock
)
UPDATE installation_budgets AS budgets
SET lease_owner = sqlc.arg(lease_token)::text,
    lease_until = proposed.lease_until
FROM proposed
WHERE budgets.installation_id = sqlc.arg(installation_id)
  AND (
      budgets.lease_owner IS NULL
      OR budgets.lease_until <= clock_timestamp()
      OR budgets.lease_owner = sqlc.arg(lease_token)::text
  )
RETURNING budgets.class, budgets.remaining, budgets.rate_limit,
          budgets.reset_at, budgets.backoff_until, budgets.lease_until;

-- name: RenewInstallationBudgetLease :many
WITH locked AS MATERIALIZED (
    SELECT class
    FROM installation_budgets
    WHERE installation_id = sqlc.arg(installation_id)
    ORDER BY class
    FOR UPDATE
),
proposed AS MATERIALIZED (
    SELECT clock_timestamp()
           + sqlc.arg(ttl_seconds)::double precision * interval '1 second'
           AS lease_until
    FROM (SELECT count(*) FROM locked) AS after_lock
)
UPDATE installation_budgets AS budgets
SET lease_until = proposed.lease_until
FROM proposed
WHERE budgets.installation_id = sqlc.arg(installation_id)
  AND budgets.lease_owner = sqlc.arg(lease_token)::text
  AND budgets.lease_until > clock_timestamp()
RETURNING budgets.lease_until;

-- name: SaveInstallationBudgetSnapshot :execrows
WITH locked AS MATERIALIZED (
    SELECT class
    FROM installation_budgets
    WHERE installation_id = sqlc.arg(installation_id)
    ORDER BY class
    FOR UPDATE
),
observed AS MATERIALIZED (
    SELECT clock_timestamp() AS checked_at
    FROM (SELECT count(*) FROM locked) AS after_lock
)
UPDATE installation_budgets AS budgets
SET remaining = CASE budgets.class
        WHEN 'rest' THEN sqlc.narg(rest_remaining)::bigint
        WHEN 'graphql' THEN sqlc.narg(graphql_remaining)::bigint
    END,
    rate_limit = CASE budgets.class
        WHEN 'rest' THEN sqlc.narg(rest_limit)::bigint
        WHEN 'graphql' THEN sqlc.narg(graphql_limit)::bigint
    END,
    reset_at = CASE budgets.class
        WHEN 'rest' THEN sqlc.narg(rest_reset_at)::timestamptz
        WHEN 'graphql' THEN sqlc.narg(graphql_reset_at)::timestamptz
    END,
    backoff_until = sqlc.narg(backoff_until)::timestamptz,
    updated_at = observed.checked_at
FROM observed
WHERE budgets.installation_id = sqlc.arg(installation_id)
  AND budgets.lease_owner = sqlc.arg(lease_token)::text
  AND budgets.lease_until > observed.checked_at;

-- name: SaveInstallationBudgetBackoff :execrows
WITH locked AS MATERIALIZED (
    SELECT class
    FROM installation_budgets
    WHERE installation_id = sqlc.arg(installation_id)
    ORDER BY class
    FOR UPDATE
),
observed AS MATERIALIZED (
    SELECT clock_timestamp() AS checked_at
    FROM (SELECT count(*) FROM locked) AS after_lock
)
UPDATE installation_budgets AS budgets
SET backoff_until = CASE
        WHEN budgets.backoff_until IS NULL
          OR budgets.backoff_until < sqlc.arg(backoff_until)::timestamptz
        THEN sqlc.arg(backoff_until)::timestamptz
        ELSE budgets.backoff_until
    END,
    updated_at = observed.checked_at
FROM observed
WHERE budgets.installation_id = sqlc.arg(installation_id)
  AND budgets.lease_owner = sqlc.arg(lease_token)::text
  AND budgets.lease_until > observed.checked_at;

-- name: ReleaseInstallationBudgetLease :execrows
UPDATE installation_budgets
SET lease_owner = NULL, lease_until = NULL
WHERE installation_id = sqlc.arg(installation_id)
  AND lease_owner = sqlc.arg(lease_token)::text;
