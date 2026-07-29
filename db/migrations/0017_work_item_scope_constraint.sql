-- Install the scope invariant without scanning work_items. The brief metadata
-- lock has a bounded wait and is released before 0018 validates existing rows.
SET LOCAL lock_timeout = '5s';

ALTER TABLE work_items
    ADD CONSTRAINT work_items_scope_key_valid
    CHECK (scope_key IS NOT NULL AND scope_key <> '') NOT VALID;
