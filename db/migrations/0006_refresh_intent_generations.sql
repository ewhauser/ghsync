-- River requires running jobs to participate in uniqueness. This durable
-- generation lets a running refresh notice a coalesced signal and enqueue one
-- follow-up without changing River-owned uniqueness state.
CREATE TABLE refresh_intent_generations (
    kind        TEXT NOT NULL,
    refresh_key TEXT NOT NULL,
    generation  BIGINT NOT NULL CHECK (generation > 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, refresh_key)
);
