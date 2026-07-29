# Webhook outage recovery

## Symptoms

- `GhsyncOldestDeliveryTooOld` after ingress recovers.
- Missing GUIDs followed by `ghsync_c_r4_gap_heal_requests_total`.
- C-R1 staleness grows while webhook volume is absent.

## Diagnosis

```sql
SELECT status, count(*), min(received_at) AS oldest, max(received_at) AS newest
FROM webhook_deliveries GROUP BY status ORDER BY status;

SELECT installation_id, cursor, cutoff, started_at, updated_at, completed_at
FROM gap_heal_cursors;

SELECT queue, state, count(*), min(scheduled_at) AS oldest_scheduled
FROM river_job
WHERE queue IN ('event', 'reconcile')
GROUP BY queue, state ORDER BY queue, state;
```

Confirm the App target reaches `POST /webhooks/github` and ingress has the
current `GITHUB_WEBHOOK_SECRET`.

## Remediation

Restore at least one instance of each required role through the deployment
platform. When starting roles manually, each foreground invocation below is a
separate shell/deployment action:

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=ingress
```

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=dispatch
```

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=fetch,sweep,drift
```

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=watermarker
```

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=metrics
```

The sweep role resumes `gap_heal_cursors` and requests missing GUID
redelivery. Do not delete cursor/delivery rows. If initial enrollment was never
completed:

```sh
ghsyncd backfill
```

## Escalation

Escalate if the App deliveries API cannot cover the comparison window,
redelivery fails repeatedly, any C-R1 bound is breached for two periods, or
drift becomes nonzero.
