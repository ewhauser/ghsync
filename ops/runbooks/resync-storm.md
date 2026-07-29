# Resync storm

## Symptoms

- `FrontierResyncStorm`.
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

Use the reference consumer's intentional replacement flow:

```sh
stream-tail --consumer=CONSUMER --stream=STREAM --bootstrap
frontier-syncd serve --roles=watermarker,pruner
```

Do not extend retention to mask a consumer that cannot bootstrap.

## Escalation

Escalate after the same consumer resyncs twice after a successful bootstrap,
if several consumers resync together, or if the horizon violates seven-day
retention.
