-- The validated 0017 check lets PostgreSQL install NOT NULL without rescanning
-- work_items. This final metadata lock is brief and bounded.
SET LOCAL lock_timeout = '5s';

ALTER TABLE work_items
    ALTER COLUMN scope_key SET NOT NULL;
