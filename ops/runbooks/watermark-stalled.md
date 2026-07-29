# Watermark stalled

## Symptoms

- `GhsyncWatermarkStalled`.
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

SELECT locks.pid, activity.application_name, locks.mode, locks.granted,
       pg_blocking_pids(locks.pid) AS blocking_pids,
       activity.state, activity.xact_start,
       activity.wait_event_type, activity.wait_event,
       left(activity.query, 160) AS query
FROM pg_locks AS locks
JOIN pg_stat_activity AS activity USING (pid)
WHERE locks.locktype = 'advisory'
  AND locks.classid = 1181904750
  AND locks.objid = 1953064306
ORDER BY locks.granted, locks.pid;
```

The exclusive fence waits only for outbox writers holding the required shared
lock; migration `0024` rejects an unfenced `change_events` insert. Its
acquisition has a local `lock_timeout`, and pooled connections enforce
`idle_in_transaction_session_timeout`; bootstrap snapshots do not
participate.

## Remediation

Gracefully replace the watermarker role so its lease fails over. If starting
the replacement manually, run the foreground process in a separate
shell/deployment action:

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=watermarker
```

A fence timeout is a retryable, metered step outcome; do not treat one timeout
as permission to alter the stream. Terminate a stuck writer only under the
database incident procedure. Never update `safe_seq` manually.

## Escalation

Escalate if a current lease makes no step for 30 seconds, a writer exceeds its
job timeout, or role failover cannot advance through `max(change_events.seq)`.
