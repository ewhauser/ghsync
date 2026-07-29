-- name: RecordOperationHeartbeat :exec
INSERT INTO operation_heartbeats (
    installation_id, component, operation,
    success_count, sample_count, last_success_at, last_sample_at
)
VALUES (
    sqlc.arg(installation_id), sqlc.arg(component), sqlc.arg(operation),
    1, sqlc.arg(samples)::bigint, clock_timestamp(),
    CASE
        WHEN sqlc.arg(samples)::bigint > 0 THEN clock_timestamp()
        ELSE NULL
    END
)
ON CONFLICT (installation_id, component, operation) DO UPDATE
SET success_count = operation_heartbeats.success_count + 1,
    sample_count = operation_heartbeats.sample_count + EXCLUDED.sample_count,
    last_success_at = EXCLUDED.last_success_at,
    last_sample_at = COALESCE(
        EXCLUDED.last_sample_at,
        operation_heartbeats.last_sample_at
    );
