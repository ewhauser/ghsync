-- name: GetEntityObservationVersion :one
SELECT version
FROM entity_observation_versions
WHERE entity_key = sqlc.arg(entity_key);

-- name: GetEntityObservationVersionForUpdate :one
SELECT version
FROM entity_observation_versions
WHERE entity_key = sqlc.arg(entity_key)
FOR UPDATE;

-- name: AdvanceEntityObservationVersion :one
INSERT INTO entity_observation_versions (entity_key, version)
VALUES (sqlc.arg(entity_key), 1)
ON CONFLICT (entity_key) DO UPDATE
SET version = entity_observation_versions.version + 1
RETURNING version;
