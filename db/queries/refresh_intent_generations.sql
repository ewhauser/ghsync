-- name: BumpRefreshIntentGenerations :many
-- Bump each coalesced key once in the same transaction that inserts its River
-- job. Workers snapshot it when they begin authoritative refresh work.
WITH intents AS (
    SELECT DISTINCT
        element->>'kind' AS kind,
        element->>'refresh_key' AS refresh_key,
        NULLIF(element->>'deadline_at', '')::timestamptz AS deadline_at,
        NULLIF(element->>'event_received_at', '')::timestamptz
            AS event_received_at
    FROM jsonb_array_elements(sqlc.arg(intents)::jsonb) AS element
    -- Upsert conflict-row locks must be acquired in the same order everywhere.
    ORDER BY kind, refresh_key
)
INSERT INTO refresh_intent_generations (
    kind, refresh_key, generation, deadline_at, event_received_at
)
SELECT kind, refresh_key, 1, deadline_at, event_received_at
FROM intents
ON CONFLICT (kind, refresh_key) DO UPDATE
SET generation = refresh_intent_generations.generation + 1,
    deadline_at = CASE
        WHEN EXCLUDED.deadline_at IS NULL
        THEN refresh_intent_generations.deadline_at
        WHEN refresh_intent_generations.deadline_at IS NULL
        THEN EXCLUDED.deadline_at
        ELSE LEAST(
            refresh_intent_generations.deadline_at,
            EXCLUDED.deadline_at
        )
    END,
    event_received_at = CASE
        WHEN EXCLUDED.event_received_at IS NULL
        THEN refresh_intent_generations.event_received_at
        WHEN refresh_intent_generations.event_received_at IS NULL
        THEN EXCLUDED.event_received_at
        ELSE LEAST(
            refresh_intent_generations.event_received_at,
            EXCLUDED.event_received_at
        )
    END,
    updated_at = now()
RETURNING kind, refresh_key, generation, deadline_at, event_received_at;

-- name: GetRefreshIntentState :one
SELECT generation, completed_generation, deadline_at, event_received_at
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

-- name: CompleteRefreshIntentGeneration :exec
UPDATE refresh_intent_generations
SET completed_generation = GREATEST(
        completed_generation,
        sqlc.arg(completed_generation)
    ),
    deadline_at = CASE
        WHEN generation = sqlc.arg(completed_generation)
        THEN NULL
        ELSE deadline_at
    END,
    event_received_at = CASE
        WHEN generation = sqlc.arg(completed_generation)
        THEN NULL
        ELSE event_received_at
    END,
    updated_at = now()
WHERE kind = sqlc.arg(kind)
  AND refresh_key = sqlc.arg(refresh_key)
  AND generation >= sqlc.arg(completed_generation);
