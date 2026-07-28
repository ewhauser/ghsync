-- Per-installation GitHub budget state and leased singleton coordination
-- (SYNC_ENGINE C-B1/C-B5/C-O2/C-P6). "class" names the independently
-- accounted REST and GraphQL resources from the schema sketch in §5.
CREATE TABLE installation_budgets (
    installation_id BIGINT NOT NULL,
    class           TEXT NOT NULL CHECK (class IN ('rest', 'graphql')),
    remaining       BIGINT,
    rate_limit      BIGINT,
    reset_at        TIMESTAMPTZ,
    lease_owner     TEXT,
    lease_until     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (installation_id, class),
    CHECK (
        (remaining IS NULL AND rate_limit IS NULL AND reset_at IS NULL)
        OR
        (remaining >= 0 AND rate_limit > 0 AND reset_at IS NOT NULL)
    ),
    CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR
        (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    )
);
