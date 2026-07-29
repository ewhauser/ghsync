-- name: EnsureConsumerCursor :exec
INSERT INTO consumer_cursors (consumer, stream, seq, updated_at)
VALUES (
    sqlc.arg(consumer), sqlc.arg(stream), 0, clock_timestamp()
)
ON CONFLICT (consumer, stream) DO NOTHING;

-- name: GetConsumerCursorForUpdate :one
SELECT seq
FROM consumer_cursors
WHERE consumer = sqlc.arg(consumer)
  AND stream = sqlc.arg(stream)
FOR UPDATE;

-- name: GetStreamSafeSequence :one
SELECT safe_seq
FROM stream_watermark
WHERE singleton;

-- name: UpdateConsumerCursor :exec
UPDATE consumer_cursors
SET seq = sqlc.arg(seq), updated_at = clock_timestamp()
WHERE consumer = sqlc.arg(consumer)
  AND stream = sqlc.arg(stream);

-- name: GetStreamPrunedThroughSequence :one
SELECT COALESCE((
    SELECT pruned_through_seq
    FROM stream_horizons
    WHERE stream = sqlc.arg(stream)
), 0)::bigint;

-- name: RecordConsumerResync :exec
UPDATE consumer_cursors
SET resync_count = resync_count + 1,
    last_resync_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE consumer = sqlc.arg(consumer)
  AND stream = sqlc.arg(stream);

-- name: PageChangeEvents :many
SELECT seq, stream, kind, entity_key, occurred_at, payload
FROM change_events
WHERE stream = sqlc.arg(stream)
  AND seq > sqlc.arg(after_seq)
  AND seq <= sqlc.arg(through_seq)
ORDER BY seq
LIMIT sqlc.arg(page_size);
