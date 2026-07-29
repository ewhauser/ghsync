-- Q13: cumulative sweep membership is row-oriented so each entity key is
-- inserted once per sweep, rather than rewriting an ever-growing JSONB array
-- after every page. The child primary key supports the completion anti-join.
CREATE TABLE sweep_seen_keys (
    installation_id BIGINT NOT NULL,
    sweep_kind      TEXT NOT NULL,
    scope_key       TEXT NOT NULL DEFAULT '',
    entity_key      TEXT NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (installation_id, sweep_kind, scope_key, entity_key),
    FOREIGN KEY (installation_id, sweep_kind, scope_key)
        REFERENCES sweep_cursors(installation_id, sweep_kind, scope_key)
        ON DELETE CASCADE
);

INSERT INTO sweep_seen_keys (
    installation_id, sweep_kind, scope_key, entity_key, first_seen_at
)
SELECT sweep_cursors.installation_id,
       sweep_cursors.sweep_kind,
       sweep_cursors.scope_key,
       seen.entity_key,
       sweep_cursors.updated_at
FROM sweep_cursors
CROSS JOIN LATERAL jsonb_array_elements_text(
    sweep_cursors.seen_keys
) AS seen(entity_key)
ON CONFLICT DO NOTHING;

ALTER TABLE sweep_cursors
    DROP COLUMN seen_keys;
