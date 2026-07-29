-- M6 final review fixes. Applied migrations 0001-0020 remain immutable.

-- C-R1's 24-hour closed-entity bound applies only while a closed entity is
-- still eligible for display. Keep that eligibility explicit and finite
-- instead of treating all retained history as active cache.
ALTER TABLE pull_requests
    ADD COLUMN display_until TIMESTAMPTZ;

ALTER TABLE stacks
    ADD COLUMN display_until TIMESTAMPTZ;

UPDATE pull_requests
SET display_until = clock_timestamp() + interval '30 days'
WHERE state <> 'open'
  AND tombstoned_at IS NULL;

UPDATE stacks
SET display_until = clock_timestamp() + interval '30 days'
WHERE NOT open
  AND tombstoned_at IS NULL;

CREATE INDEX pull_requests_display_retained_checked_idx
ON pull_requests (repo_id, last_checked_at)
WHERE tombstoned_at IS NULL AND display_until IS NOT NULL;

CREATE INDEX stacks_display_retained_checked_idx
ON stacks (repo_id, last_checked_at)
WHERE tombstoned_at IS NULL AND display_until IS NOT NULL;

-- C-Q2 scrapes must find stuck event generations without scanning completed
-- history.
CREATE INDEX refresh_intent_outstanding_event_idx
ON refresh_intent_generations (event_received_at)
WHERE completed_generation < generation
  AND event_received_at IS NOT NULL;

-- C-O3/C-R2/C-S2: durable successful-pass evidence means a crashed or absent
-- role cannot turn into "no series means green".
CREATE TABLE operation_heartbeats (
    installation_id BIGINT NOT NULL,
    component       TEXT NOT NULL CHECK (component <> ''),
    operation       TEXT NOT NULL CHECK (operation <> ''),
    success_count   BIGINT NOT NULL DEFAULT 0 CHECK (success_count >= 0),
    last_success_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (installation_id, component, operation)
);
