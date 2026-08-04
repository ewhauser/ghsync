-- Keep the changed-files collection validator independent from the mirrored
-- CODEOWNERS content validator already stored in etag.
ALTER TABLE pull_request_change_snapshots
    ADD COLUMN files_etag text DEFAULT ''::text NOT NULL;
