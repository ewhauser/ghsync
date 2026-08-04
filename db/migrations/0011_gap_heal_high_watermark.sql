ALTER TABLE gap_heal_cursors
    ADD COLUMN high_watermark_at timestamp with time zone,
    ADD COLUMN pass_high_watermark_at timestamp with time zone,
    ADD COLUMN boundary_delivery_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN pass_boundary_delivery_id bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_deep_started_at timestamp with time zone,
    ADD COLUMN last_deep_completed_at timestamp with time zone,
    ADD COLUMN scan_mode text NOT NULL DEFAULT 'deep',
    ADD COLUMN cursor_version integer NOT NULL DEFAULT 0,
    ADD COLUMN lookback_duration_ns bigint NOT NULL DEFAULT 0,
    ADD COLUMN page_size integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT gap_heal_cursors_scan_mode_check CHECK (
        scan_mode IN ('incremental', 'deep')
    ),
    ADD CONSTRAINT gap_heal_cursors_cursor_version_check CHECK (
        cursor_version >= 0
    ),
    ADD CONSTRAINT gap_heal_cursors_boundary_delivery_id_check CHECK (
        boundary_delivery_id >= 0
    ),
    ADD CONSTRAINT gap_heal_cursors_pass_boundary_delivery_id_check CHECK (
        pass_boundary_delivery_id >= 0
    ),
    ADD CONSTRAINT gap_heal_cursors_lookback_duration_ns_check CHECK (
        lookback_duration_ns >= 0
    ),
    ADD CONSTRAINT gap_heal_cursors_page_size_check CHECK (
        page_size BETWEEN 0 AND 100
    );
