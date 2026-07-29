# Watermark stalled

## Symptoms

- `FrontierWatermarkStalled`.
- Outbox depth rises while watermark progress stops.
- Consumers stay connected but receive no committed events.

## Diagnosis

```sql
SELECT safe_seq, updated_at, lease_token, lease_until
FROM stream_watermark WHERE singleton;

SELECT max(seq) AS max_seq, count(*) AS outbox_rows,
       min(occurred_at) AS oldest_event
FROM change_events;

SELECT pid, application_name, state, xact_start, wait_event_type, wait_event,
       left(query, 160) AS query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY xact_start NULLS LAST;

SELECT locktype, mode, granted, pid
FROM pg_locks WHERE locktype = 'advisory'
ORDER BY granted, pid;
```

The exclusive fence waits only for registered outbox writers; bootstrap
snapshots do not participate.

## Remediation

Gracefully replace the watermarker role so its lease fails over:

```sh
frontier-syncd serve --roles=watermarker
```

Terminate a stuck writer only under the database incident procedure. Never
update `safe_seq` manually.

## Escalation

Escalate if a current lease makes no step for 30 seconds, a writer exceeds its
job timeout, or role failover cannot advance through `max(change_events.seq)`.
