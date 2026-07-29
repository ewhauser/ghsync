-- Validate under SHARE UPDATE EXCLUSIVE rather than holding the ACCESS
-- EXCLUSIVE lock from constraint creation. Reads and ordinary writes continue;
-- deploy monitoring should still watch for long-running conflicting DDL.
SET LOCAL lock_timeout = '5s';

ALTER TABLE work_items
    VALIDATE CONSTRAINT work_items_scope_key_valid;
