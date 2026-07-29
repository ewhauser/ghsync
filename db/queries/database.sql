-- name: ShowSynchronousCommit :one
SELECT current_setting('synchronous_commit')::text;

-- name: GetDatabaseClock :one
SELECT clock_timestamp()::timestamptz AS database_clock;
