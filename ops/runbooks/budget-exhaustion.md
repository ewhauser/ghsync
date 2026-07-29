# Budget exhaustion

## Symptoms

- `FrontierBudgetFloorBreached`, `FrontierBudgetClassStarved`, or
  `FrontierSecondaryGateClosed`.
- Event/sweep queues grow while interactive work continues.
- Sweep 304 ratio falls below 80 percent.

## Diagnosis

```sql
SELECT installation_id, class, remaining, rate_limit, reset_at,
       backoff_until, lease_owner, lease_until, updated_at
FROM installation_budgets ORDER BY installation_id, class;

SELECT queue, state, count(*), min(scheduled_at) AS oldest_scheduled
FROM river_job
WHERE state IN ('available','pending','retryable','running','scheduled')
GROUP BY queue, state ORDER BY queue, state;

SELECT sweep_kind, scope_key, started_at, updated_at, completed_at, cursor
FROM sweep_cursors WHERE completed_at IS NULL ORDER BY started_at;
```

Server headers are authoritative; do not calculate a replacement allowance.

## Remediation

Honor `backoff_until`; never clear it manually. Leave latency-critical roles
running while an avoidable sweep fan-out is repaired:

```sh
frontier-syncd serve --roles=ingress,dispatch,fetch,watermarker
```

After fixing ETags or request fan-out, restore background roles:

```sh
frontier-syncd serve --roles=sweep,drift,pruner
```

Never lower class floors to force work through.

## Escalation

Escalate if event work reaches its 10 percent floor, interactive work starves,
the gate remains closed beyond `Retry-After`, or steady state spends 10
percent or more of the hourly budget.
