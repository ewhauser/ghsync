-- name: BumpRefreshIntentGenerations :many
-- Bump each coalesced key once in the same transaction that inserts its River
-- job. Workers snapshot it when they begin authoritative refresh work.
WITH intents AS (
    SELECT DISTINCT
        element->>'kind' AS kind,
        element->>'refresh_key' AS refresh_key
    FROM jsonb_array_elements(sqlc.arg(intents)::jsonb) AS element
)
INSERT INTO refresh_intent_generations (kind, refresh_key, generation)
SELECT kind, refresh_key, 1
FROM intents
ON CONFLICT (kind, refresh_key) DO UPDATE
SET generation = refresh_intent_generations.generation + 1,
    updated_at = now()
RETURNING kind, refresh_key, generation;

-- name: GetRefreshIntentGeneration :one
-- A running worker snapshots the generation before fetching authoritative
-- state. Signals coalesced before the fetch are thereby covered by that fetch.
SELECT generation
FROM refresh_intent_generations
WHERE kind = $1 AND refresh_key = $2;

-- name: GetRefreshIntentGenerationForUpdate :one
-- Workers lock the generation before transactionally completing their River
-- job. A concurrent dispatcher therefore bumps either before the recheck or
-- after completion, when it can insert a fresh job.
SELECT generation
FROM refresh_intent_generations
WHERE kind = $1 AND refresh_key = $2
FOR UPDATE;
