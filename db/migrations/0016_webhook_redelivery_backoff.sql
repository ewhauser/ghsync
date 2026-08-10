CREATE TABLE webhook_delivery_retries (
    installation_id bigint NOT NULL,
    delivery_guid text NOT NULL,
    latest_delivery_id bigint NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    outcome text NOT NULL,
    status_code integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    claim_token text,
    last_attempted_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    PRIMARY KEY (installation_id, delivery_guid),
    CONSTRAINT webhook_delivery_retries_attempt_count_check CHECK (
        attempt_count >= 0
    ),
    CONSTRAINT webhook_delivery_retries_delivery_id_check CHECK (
        latest_delivery_id > 0
    ),
    CONSTRAINT webhook_delivery_retries_outcome_check CHECK (
        outcome = ANY (ARRAY[
            'ready'::text,
            'claiming'::text,
            'pending'::text,
            'transient'::text,
            'terminal'::text
        ])
    ),
    CONSTRAINT webhook_delivery_retries_status_code_check CHECK (
        status_code >= 0 AND status_code <= 599
    ),
    CONSTRAINT webhook_delivery_retries_claim_check CHECK (
        (outcome = 'claiming') = (claim_token IS NOT NULL)
    )
);

CREATE INDEX webhook_delivery_retries_due_idx
    ON webhook_delivery_retries (
        installation_id,
        next_attempt_at,
        delivery_guid
    );
