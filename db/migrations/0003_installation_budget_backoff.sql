-- Persist installation-wide secondary-limit closure across process restarts
-- and budgeter lease handoff (SYNC_ENGINE C-B1/C-B2/C-O2).
ALTER TABLE installation_budgets
ADD COLUMN backoff_until TIMESTAMPTZ;
