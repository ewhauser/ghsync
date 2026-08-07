-- Optimistic entity observations replace session-level advisory locks. A
-- fetch snapshots one committed version without retaining a connection. The
-- short compare-and-write transaction serializes on the existing entity
-- advisory lock, verifies that version, and advances it only when it commits.
CREATE TABLE entity_observation_versions (
    entity_key text PRIMARY KEY,
    version bigint NOT NULL,
    CONSTRAINT entity_observation_versions_key_check CHECK (entity_key <> ''),
    CONSTRAINT entity_observation_versions_version_check CHECK (version > 0)
);
