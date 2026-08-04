ALTER TABLE gap_heal_cursors
    ADD COLUMN lease_token text,
    ADD COLUMN lease_until timestamp with time zone,
    ADD CONSTRAINT gap_heal_cursors_lease_check CHECK (
        (lease_token IS NULL) = (lease_until IS NULL)
    );
