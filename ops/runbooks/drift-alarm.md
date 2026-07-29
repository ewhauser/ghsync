# Drift alarm

## Symptoms

- `GhsyncDriftOpen`; the dashboard correctness verdict is red.
- A finding persists after its self-heal generation completed.

## Diagnosis

```sql
SELECT id, installation_id, entity_kind, entity_key, diff_hash,
       first_seen_at, last_seen_at, occurrence_count, heal_generation,
       escalated_at, resolved_at, diff
FROM drift_findings
WHERE resolved_at IS NULL
ORDER BY escalated_at NULLS LAST, first_seen_at;

SELECT kind, refresh_key, generation, completed_generation,
       event_received_at, deadline_at, updated_at
FROM refresh_intent_generations
WHERE completed_generation < generation
ORDER BY updated_at;
```

Compare the finding's cache/upstream snapshots and attached diff. A new
finding already enqueues exactly one heal.

## Remediation

Ensure detector and heal workers are present. When starting roles manually,
each foreground invocation is a separate shell/deployment action:

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

Fix the conversion, CAS, or missing event/sweep rule. The detector resolves
the row automatically after a matching full fetch.

## Escalation

Escalate on any persistent finding, multiple drifting classes, or a finding
still open after its heal generation completes. The cache is untrustworthy
while open drift is nonzero.
