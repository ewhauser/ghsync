# Resync storm

## Symptoms

- `GhsyncResyncStorm`.
- A consumer's `resync_count` rises repeatedly.
- The consumer receives typed `RESYNC_REQUIRED` after bootstrap.

## Diagnosis

```sql
SELECT consumer, stream, seq, updated_at, resync_count, last_resync_at
FROM consumer_cursors
ORDER BY resync_count DESC, consumer, stream;

SELECT stream, pruned_through_seq, updated_at
FROM stream_horizons ORDER BY stream;

SELECT safe_seq, updated_at FROM stream_watermark WHERE singleton;
```

Confirm the consumer commits `Bootstrap` before `Tail`, keeps one stable
consumer name, and does not hand-roll cursor updates.

## Remediation

`Bootstrap` must be run by the affected consumer as part of its resync loop.
The consumer must replace its projection from the public cache tables through
the returned snapshot transaction, then commit that projection replacement and
the cursor reset together. Only after that commit may it resume `Tail` from the
new cursor. See `cmd/stream-tail` for the reference detect, rebuild, and resume
loop.

Do **not** run `stream-tail --bootstrap` with another consumer's durable name.
That command is safe only with a throwaway consumer name owned by the logging
CLI. The previous instruction reset a real consumer's cursor to `safe_seq`
without replacing its projection. It would have silently discarded every
undelivered event through that watermark—in a retention resync, every surviving
event between the pruned horizon and the watermark—while leaving the real
projection stale.

After confirming the consumer follows the transactional replacement protocol,
keep the maintenance roles available. If starting them manually, run the
foreground process in a separate shell/deployment action:

```sh
# In a separate shell/deployment action:
ghsyncd serve --roles=watermarker,pruner
```

Do not extend retention to mask a consumer that cannot bootstrap.

## Escalation

Escalate after the same consumer resyncs twice after a successful bootstrap,
if several consumers resync together, or if the horizon violates seven-day
retention.
