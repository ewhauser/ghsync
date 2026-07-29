-- Base schema: every table, sequence, constraint, and index.
--
-- This is a squashed baseline (the original 0001-0031 incremental history
-- predates any deployment). River's own tables (river_*) are managed by
-- rivermigrate and the schema_migrations ledger by the migration runner;
-- neither belongs in these files.
--
-- Layout follows pg_dump's canonical form: tables first, then defaults,
-- primary keys and other constraints, then secondary indexes.

-- ------------------------------------------------------------------
-- Sequence allocator (change_events.seq column default)
-- ------------------------------------------------------------------

-- Forces a transaction id before allocating so the C-S visibility
-- watermark can fence concurrent writers.
CREATE FUNCTION ghsync_next_change_event_seq() RETURNS bigint
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_current_xact_id();
    RETURN nextval('change_events_seq_seq'::regclass);
END
$$;

-- ------------------------------------------------------------------
-- Tables
-- ------------------------------------------------------------------

-- backfill_children (table)
CREATE TABLE backfill_children (
    installation_id bigint NOT NULL,
    repo_full_name text NOT NULL,
    kind text NOT NULL,
    refresh_key text NOT NULL,
    target_generation bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT backfill_children_target_generation_check CHECK ((target_generation > 0))
);

-- backfill_cursors (table)
CREATE TABLE backfill_cursors (
    installation_id bigint NOT NULL,
    repo_full_name text NOT NULL,
    phase text NOT NULL,
    page integer NOT NULL,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    queue_name text DEFAULT 'interactive'::text NOT NULL,
    CONSTRAINT backfill_cursors_page_check CHECK ((page > 0)),
    CONSTRAINT backfill_cursors_phase_check CHECK ((phase = ANY (ARRAY['repository'::text, 'stacks'::text, 'pull_requests'::text, 'waiting'::text, 'done'::text]))),
    CONSTRAINT backfill_cursors_phase_completed_at_check CHECK (((phase = 'done'::text) = (completed_at IS NOT NULL))),
    CONSTRAINT backfill_cursors_queue_name_check CHECK ((queue_name = ANY (ARRAY['interactive'::text, 'sweep'::text])))
);

-- change_events (table)
CREATE TABLE change_events (
    seq bigint DEFAULT ghsync_next_change_event_seq() NOT NULL,
    stream text NOT NULL,
    kind text NOT NULL,
    entity_key text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    payload jsonb NOT NULL,
    outbox_txid xid8 DEFAULT pg_current_xact_id() NOT NULL,
    CONSTRAINT change_events_entity_key_nonempty CHECK ((entity_key <> ''::text)),
    CONSTRAINT change_events_kind_nonempty CHECK ((kind <> ''::text)),
    CONSTRAINT change_events_stream_nonempty CHECK ((stream <> ''::text))
);

-- check_history (table)
CREATE TABLE check_history (
    id bigint NOT NULL,
    check_run_gh_id bigint NOT NULL,
    repo_id bigint NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    conclusion text DEFAULT ''::text NOT NULL,
    observed jsonb NOT NULL,
    gh_updated_at timestamp with time zone,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    semantic_version text DEFAULT ''::text NOT NULL,
    CONSTRAINT check_history_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- check_runs (table)
CREATE TABLE check_runs (
    gh_id bigint NOT NULL,
    repo_id bigint NOT NULL,
    node_id text DEFAULT ''::text NOT NULL,
    name text NOT NULL,
    status text NOT NULL,
    conclusion text DEFAULT ''::text NOT NULL,
    details_url text DEFAULT ''::text NOT NULL,
    app_slug text DEFAULT ''::text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    gh_updated_at timestamp with time zone,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    semantic_version text DEFAULT ''::text NOT NULL,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT check_runs_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- consumer_cursors (table)
CREATE TABLE consumer_cursors (
    consumer text NOT NULL,
    stream text NOT NULL,
    seq bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    resync_count bigint DEFAULT 0 NOT NULL,
    last_resync_at timestamp with time zone,
    CONSTRAINT consumer_cursors_consumer_check CHECK ((consumer <> ''::text)),
    CONSTRAINT consumer_cursors_resync_count_check CHECK ((resync_count >= 0)),
    CONSTRAINT consumer_cursors_seq_check CHECK ((seq >= 0)),
    CONSTRAINT consumer_cursors_stream_check CHECK ((stream <> ''::text))
);

-- derivation_dirty (table)
CREATE TABLE derivation_dirty (
    scope_key text NOT NULL,
    marked_at timestamp with time zone NOT NULL
);

-- drift_findings (table)
CREATE TABLE drift_findings (
    id bigint NOT NULL,
    installation_id bigint NOT NULL,
    entity_kind text NOT NULL,
    entity_key text NOT NULL,
    detected_at timestamp with time zone NOT NULL,
    cache_snapshot jsonb NOT NULL,
    upstream_snapshot jsonb NOT NULL,
    diff jsonb NOT NULL,
    refresh_enqueued_at timestamp with time zone NOT NULL,
    diff_hash text NOT NULL,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    occurrence_count bigint DEFAULT 1 NOT NULL,
    heal_generation bigint DEFAULT 0 NOT NULL,
    escalated_at timestamp with time zone,
    resolved_at timestamp with time zone,
    CONSTRAINT drift_findings_heal_generation_check CHECK ((heal_generation >= 0)),
    CONSTRAINT drift_findings_occurrence_count_check CHECK ((occurrence_count > 0))
);

-- drift_sample_cursors (table)
CREATE TABLE drift_sample_cursors (
    installation_id bigint NOT NULL,
    entity_kind text NOT NULL,
    source_id bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- gap_heal_cursors (table)
CREATE TABLE gap_heal_cursors (
    installation_id bigint NOT NULL,
    cursor text DEFAULT ''::text NOT NULL,
    cutoff timestamp with time zone,
    started_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT gap_heal_cursors_check CHECK ((((started_at IS NULL) AND (cutoff IS NULL)) OR ((started_at IS NOT NULL) AND (cutoff IS NOT NULL))))
);

-- installation_backfill_cursors (table)
CREATE TABLE installation_backfill_cursors (
    installation_id bigint NOT NULL,
    phase text NOT NULL,
    page integer NOT NULL,
    queue_name text DEFAULT 'interactive'::text NOT NULL,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT installation_backfill_cursors_check CHECK (((phase = 'done'::text) = (completed_at IS NOT NULL))),
    CONSTRAINT installation_backfill_cursors_page_check CHECK ((page > 0)),
    CONSTRAINT installation_backfill_cursors_phase_check CHECK ((phase = ANY (ARRAY['repositories'::text, 'waiting'::text, 'done'::text]))),
    CONSTRAINT installation_backfill_cursors_queue_name_check CHECK ((queue_name = ANY (ARRAY['interactive'::text, 'sweep'::text])))
);

-- installation_budgets (table)
CREATE TABLE installation_budgets (
    installation_id bigint NOT NULL,
    class text NOT NULL,
    remaining bigint,
    rate_limit bigint,
    reset_at timestamp with time zone,
    lease_owner text,
    lease_until timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    backoff_until timestamp with time zone,
    CONSTRAINT installation_budgets_check CHECK ((((remaining IS NULL) AND (rate_limit IS NULL) AND (reset_at IS NULL)) OR ((remaining >= 0) AND (rate_limit > 0) AND (reset_at IS NOT NULL)))),
    CONSTRAINT installation_budgets_check1 CHECK ((((lease_owner IS NULL) AND (lease_until IS NULL)) OR ((lease_owner IS NOT NULL) AND (lease_until IS NOT NULL)))),
    CONSTRAINT installation_budgets_class_check CHECK ((class = ANY (ARRAY['rest'::text, 'graphql'::text])))
);

-- operation_heartbeats (table)
CREATE TABLE operation_heartbeats (
    installation_id bigint NOT NULL,
    component text NOT NULL,
    operation text NOT NULL,
    success_count bigint DEFAULT 0 NOT NULL,
    last_success_at timestamp with time zone NOT NULL,
    sample_count bigint DEFAULT 0 NOT NULL,
    last_sample_at timestamp with time zone,
    CONSTRAINT operation_heartbeats_component_check CHECK ((component <> ''::text)),
    CONSTRAINT operation_heartbeats_operation_check CHECK ((operation <> ''::text)),
    CONSTRAINT operation_heartbeats_sample_count_check CHECK ((sample_count >= 0)),
    CONSTRAINT operation_heartbeats_success_count_check CHECK ((success_count >= 0))
);

-- pull_requests (table)
CREATE TABLE pull_requests (
    id bigint NOT NULL,
    repo_id bigint NOT NULL,
    gh_id bigint,
    node_id text DEFAULT ''::text NOT NULL,
    number integer NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    state text DEFAULT ''::text NOT NULL,
    draft boolean DEFAULT false NOT NULL,
    author_login text DEFAULT ''::text NOT NULL,
    head_ref text DEFAULT ''::text NOT NULL,
    head_sha text DEFAULT ''::text NOT NULL,
    base_ref text DEFAULT ''::text NOT NULL,
    base_sha text DEFAULT ''::text NOT NULL,
    review_decision text DEFAULT ''::text NOT NULL,
    mergeable_state text DEFAULT ''::text NOT NULL,
    stack_number integer,
    stack_position integer,
    gh_updated_at timestamp with time zone,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    display_until timestamp with time zone,
    CONSTRAINT pull_requests_check CHECK (((stack_number IS NULL) = (stack_position IS NULL))),
    CONSTRAINT pull_requests_number_check CHECK ((number > 0)),
    CONSTRAINT pull_requests_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- refresh_intent_generations (table)
CREATE TABLE refresh_intent_generations (
    kind text NOT NULL,
    refresh_key text NOT NULL,
    generation bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_generation bigint DEFAULT 0 NOT NULL,
    deadline_at timestamp with time zone,
    event_received_at timestamp with time zone,
    CONSTRAINT refresh_intent_generations_check CHECK (((completed_generation >= 0) AND (completed_generation <= generation))),
    CONSTRAINT refresh_intent_generations_generation_check CHECK ((generation > 0))
);

-- repo_aliases (table)
CREATE TABLE repo_aliases (
    full_name text NOT NULL,
    repo_id bigint NOT NULL,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL
);

-- repo_rule_sync_state (table)
CREATE TABLE repo_rule_sync_state (
    repo_id bigint NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    last_checked_at timestamp with time zone NOT NULL
);

-- repo_rules (table)
CREATE TABLE repo_rules (
    repo_id bigint NOT NULL,
    rule_key text NOT NULL,
    rule jsonb NOT NULL,
    gh_updated_at timestamp with time zone,
    head_sha text DEFAULT ''::text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT repo_rules_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- repos (table)
CREATE TABLE repos (
    id bigint NOT NULL,
    installation_id bigint NOT NULL,
    org_id bigint NOT NULL,
    gh_id bigint NOT NULL,
    node_id text NOT NULL,
    owner text NOT NULL,
    name text NOT NULL,
    full_name text NOT NULL,
    default_branch text NOT NULL,
    archived boolean DEFAULT false NOT NULL,
    gh_updated_at timestamp with time zone,
    head_sha text DEFAULT ''::text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT repos_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- review_threads (table)
CREATE TABLE review_threads (
    id text NOT NULL,
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    is_resolved boolean DEFAULT false NOT NULL,
    is_outdated boolean DEFAULT false NOT NULL,
    path text DEFAULT ''::text NOT NULL,
    line integer,
    comments jsonb DEFAULT '[]'::jsonb NOT NULL,
    gh_updated_at timestamp with time zone,
    head_sha text DEFAULT ''::text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT review_threads_pr_number_check CHECK ((pr_number > 0)),
    CONSTRAINT review_threads_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- stacks (table)
CREATE TABLE stacks (
    id bigint NOT NULL,
    repo_id bigint NOT NULL,
    gh_id bigint,
    node_id text DEFAULT ''::text NOT NULL,
    number integer NOT NULL,
    base_ref text DEFAULT ''::text NOT NULL,
    base_sha text DEFAULT ''::text NOT NULL,
    open boolean DEFAULT false NOT NULL,
    entries jsonb DEFAULT '[]'::jsonb NOT NULL,
    gh_updated_at timestamp with time zone,
    head_sha text DEFAULT ''::text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    display_until timestamp with time zone,
    CONSTRAINT stacks_number_check CHECK ((number > 0)),
    CONSTRAINT stacks_sync_source_check CHECK ((sync_source = ANY (ARRAY['webhook'::text, 'reconcile'::text, 'backfill'::text, 'manual'::text, 'interactive'::text])))
);

-- stream_horizons (table)
CREATE TABLE stream_horizons (
    stream text NOT NULL,
    pruned_through_seq bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stream_horizons_pruned_through_seq_check CHECK ((pruned_through_seq >= 0)),
    CONSTRAINT stream_horizons_stream_check CHECK ((stream <> ''::text))
);

-- stream_watermark (table)
CREATE TABLE stream_watermark (
    singleton boolean DEFAULT true NOT NULL,
    safe_seq bigint DEFAULT 0 NOT NULL,
    candidate_seq bigint,
    candidate_xid xid8,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_token text,
    lease_until timestamp with time zone,
    CONSTRAINT stream_watermark_check CHECK ((candidate_seq >= safe_seq)),
    CONSTRAINT stream_watermark_check1 CHECK (((candidate_seq IS NULL) = (candidate_xid IS NULL))),
    CONSTRAINT stream_watermark_check2 CHECK ((((lease_token IS NULL) AND (lease_until IS NULL)) OR ((lease_token IS NOT NULL) AND (lease_until IS NOT NULL)))),
    CONSTRAINT stream_watermark_safe_seq_check CHECK ((safe_seq >= 0)),
    CONSTRAINT stream_watermark_singleton_check CHECK (singleton)
);

-- sweep_cursors (table)
CREATE TABLE sweep_cursors (
    installation_id bigint NOT NULL,
    sweep_kind text NOT NULL,
    scope_key text DEFAULT ''::text NOT NULL,
    cursor text DEFAULT ''::text NOT NULL,
    started_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    pass_new_count integer DEFAULT 0 NOT NULL,
    CONSTRAINT sweep_cursors_check CHECK ((((started_at IS NULL) AND (completed_at IS NULL)) OR (started_at IS NOT NULL))),
    CONSTRAINT sweep_cursors_pass_new_count_check CHECK ((pass_new_count >= 0))
);

-- sweep_pages (table)
CREATE TABLE sweep_pages (
    installation_id bigint NOT NULL,
    sweep_kind text NOT NULL,
    scope_key text DEFAULT ''::text NOT NULL,
    cursor text NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    next_cursor text DEFAULT ''::text NOT NULL,
    entity_keys jsonb DEFAULT '[]'::jsonb NOT NULL,
    list_seen_at timestamp with time zone NOT NULL,
    CONSTRAINT sweep_pages_entity_keys_check CHECK ((jsonb_typeof(entity_keys) = 'array'::text))
);

-- sweep_seen_keys (table)
CREATE TABLE sweep_seen_keys (
    installation_id bigint NOT NULL,
    sweep_kind text NOT NULL,
    scope_key text DEFAULT ''::text NOT NULL,
    entity_key text NOT NULL,
    first_seen_at timestamp with time zone NOT NULL
);

-- webhook_deliveries (table)
CREATE TABLE webhook_deliveries (
    delivery_guid text NOT NULL,
    event text NOT NULL,
    raw_body bytea,
    headers jsonb NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    payload_pruned_at timestamp with time zone,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT webhook_deliveries_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, 'processed'::text, 'parked'::text])))
);

-- work_items (table)
CREATE TABLE work_items (
    identity_key text NOT NULL,
    org_id bigint NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    scope_key text NOT NULL,
    CONSTRAINT work_items_identity_key_check CHECK ((identity_key <> ''::text)),
    CONSTRAINT work_items_scope_key_valid CHECK (((scope_key IS NOT NULL) AND (scope_key <> ''::text)))
);

-- ------------------------------------------------------------------
-- Sequences
-- ------------------------------------------------------------------

-- change_events_seq_seq (sequence)
CREATE SEQUENCE change_events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- change_events_seq_seq (sequence owned by)
ALTER SEQUENCE change_events_seq_seq OWNED BY change_events.seq;

-- check_history_id_seq (sequence)
CREATE SEQUENCE check_history_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- check_history_id_seq (sequence owned by)
ALTER SEQUENCE check_history_id_seq OWNED BY check_history.id;

-- drift_findings_id_seq (sequence)
CREATE SEQUENCE drift_findings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- drift_findings_id_seq (sequence owned by)
ALTER SEQUENCE drift_findings_id_seq OWNED BY drift_findings.id;

-- pull_requests_id_seq (sequence)
CREATE SEQUENCE pull_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- pull_requests_id_seq (sequence owned by)
ALTER SEQUENCE pull_requests_id_seq OWNED BY pull_requests.id;

-- repos_id_seq (sequence)
CREATE SEQUENCE repos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- repos_id_seq (sequence owned by)
ALTER SEQUENCE repos_id_seq OWNED BY repos.id;

-- stacks_id_seq (sequence)
CREATE SEQUENCE stacks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- stacks_id_seq (sequence owned by)
ALTER SEQUENCE stacks_id_seq OWNED BY stacks.id;

-- ------------------------------------------------------------------
-- Column defaults
-- ------------------------------------------------------------------

-- check_history id (default)
ALTER TABLE ONLY check_history ALTER COLUMN id SET DEFAULT nextval('check_history_id_seq'::regclass);

-- drift_findings id (default)
ALTER TABLE ONLY drift_findings ALTER COLUMN id SET DEFAULT nextval('drift_findings_id_seq'::regclass);

-- pull_requests id (default)
ALTER TABLE ONLY pull_requests ALTER COLUMN id SET DEFAULT nextval('pull_requests_id_seq'::regclass);

-- repos id (default)
ALTER TABLE ONLY repos ALTER COLUMN id SET DEFAULT nextval('repos_id_seq'::regclass);

-- stacks id (default)
ALTER TABLE ONLY stacks ALTER COLUMN id SET DEFAULT nextval('stacks_id_seq'::regclass);

-- ------------------------------------------------------------------
-- Constraints
-- ------------------------------------------------------------------

-- backfill_children backfill_children_pkey (constraint)
ALTER TABLE ONLY backfill_children
    ADD CONSTRAINT backfill_children_pkey PRIMARY KEY (installation_id, repo_full_name, kind, refresh_key);

-- backfill_cursors backfill_cursors_pkey (constraint)
ALTER TABLE ONLY backfill_cursors
    ADD CONSTRAINT backfill_cursors_pkey PRIMARY KEY (installation_id, repo_full_name);

-- change_events change_events_pkey (constraint)
ALTER TABLE ONLY change_events
    ADD CONSTRAINT change_events_pkey PRIMARY KEY (seq);

-- check_history check_history_pkey (constraint)
ALTER TABLE ONLY check_history
    ADD CONSTRAINT check_history_pkey PRIMARY KEY (id);

-- check_runs check_runs_pkey (constraint)
ALTER TABLE ONLY check_runs
    ADD CONSTRAINT check_runs_pkey PRIMARY KEY (gh_id);

-- check_runs check_runs_repo_id_head_sha_gh_id_key (constraint)
ALTER TABLE ONLY check_runs
    ADD CONSTRAINT check_runs_repo_id_head_sha_gh_id_key UNIQUE (repo_id, head_sha, gh_id);

-- consumer_cursors consumer_cursors_pkey (constraint)
ALTER TABLE ONLY consumer_cursors
    ADD CONSTRAINT consumer_cursors_pkey PRIMARY KEY (consumer, stream);

-- derivation_dirty derivation_dirty_pkey (constraint)
ALTER TABLE ONLY derivation_dirty
    ADD CONSTRAINT derivation_dirty_pkey PRIMARY KEY (scope_key);

-- drift_findings drift_findings_pkey (constraint)
ALTER TABLE ONLY drift_findings
    ADD CONSTRAINT drift_findings_pkey PRIMARY KEY (id);

-- drift_sample_cursors drift_sample_cursors_pkey (constraint)
ALTER TABLE ONLY drift_sample_cursors
    ADD CONSTRAINT drift_sample_cursors_pkey PRIMARY KEY (installation_id, entity_kind);

-- gap_heal_cursors gap_heal_cursors_pkey (constraint)
ALTER TABLE ONLY gap_heal_cursors
    ADD CONSTRAINT gap_heal_cursors_pkey PRIMARY KEY (installation_id);

-- installation_backfill_cursors installation_backfill_cursors_pkey (constraint)
ALTER TABLE ONLY installation_backfill_cursors
    ADD CONSTRAINT installation_backfill_cursors_pkey PRIMARY KEY (installation_id);

-- installation_budgets installation_budgets_pkey (constraint)
ALTER TABLE ONLY installation_budgets
    ADD CONSTRAINT installation_budgets_pkey PRIMARY KEY (installation_id, class);

-- operation_heartbeats operation_heartbeats_pkey (constraint)
ALTER TABLE ONLY operation_heartbeats
    ADD CONSTRAINT operation_heartbeats_pkey PRIMARY KEY (installation_id, component, operation);

-- pull_requests pull_requests_pkey (constraint)
ALTER TABLE ONLY pull_requests
    ADD CONSTRAINT pull_requests_pkey PRIMARY KEY (id);

-- pull_requests pull_requests_repo_id_gh_id_key (constraint)
ALTER TABLE ONLY pull_requests
    ADD CONSTRAINT pull_requests_repo_id_gh_id_key UNIQUE (repo_id, gh_id);

-- pull_requests pull_requests_repo_id_number_key (constraint)
ALTER TABLE ONLY pull_requests
    ADD CONSTRAINT pull_requests_repo_id_number_key UNIQUE (repo_id, number);

-- refresh_intent_generations refresh_intent_generations_pkey (constraint)
ALTER TABLE ONLY refresh_intent_generations
    ADD CONSTRAINT refresh_intent_generations_pkey PRIMARY KEY (kind, refresh_key);

-- repo_aliases repo_aliases_pkey (constraint)
ALTER TABLE ONLY repo_aliases
    ADD CONSTRAINT repo_aliases_pkey PRIMARY KEY (full_name);

-- repo_rule_sync_state repo_rule_sync_state_pkey (constraint)
ALTER TABLE ONLY repo_rule_sync_state
    ADD CONSTRAINT repo_rule_sync_state_pkey PRIMARY KEY (repo_id);

-- repo_rules repo_rules_pkey (constraint)
ALTER TABLE ONLY repo_rules
    ADD CONSTRAINT repo_rules_pkey PRIMARY KEY (repo_id, rule_key);

-- repos repos_gh_id_key (constraint)
ALTER TABLE ONLY repos
    ADD CONSTRAINT repos_gh_id_key UNIQUE (gh_id);

-- repos repos_pkey (constraint)
ALTER TABLE ONLY repos
    ADD CONSTRAINT repos_pkey PRIMARY KEY (id);

-- review_threads review_threads_pkey (constraint)
ALTER TABLE ONLY review_threads
    ADD CONSTRAINT review_threads_pkey PRIMARY KEY (id);

-- stacks stacks_pkey (constraint)
ALTER TABLE ONLY stacks
    ADD CONSTRAINT stacks_pkey PRIMARY KEY (id);

-- stacks stacks_repo_id_gh_id_key (constraint)
ALTER TABLE ONLY stacks
    ADD CONSTRAINT stacks_repo_id_gh_id_key UNIQUE (repo_id, gh_id);

-- stacks stacks_repo_id_number_key (constraint)
ALTER TABLE ONLY stacks
    ADD CONSTRAINT stacks_repo_id_number_key UNIQUE (repo_id, number);

-- stream_horizons stream_horizons_pkey (constraint)
ALTER TABLE ONLY stream_horizons
    ADD CONSTRAINT stream_horizons_pkey PRIMARY KEY (stream);

-- stream_watermark stream_watermark_pkey (constraint)
ALTER TABLE ONLY stream_watermark
    ADD CONSTRAINT stream_watermark_pkey PRIMARY KEY (singleton);

-- sweep_cursors sweep_cursors_pkey (constraint)
ALTER TABLE ONLY sweep_cursors
    ADD CONSTRAINT sweep_cursors_pkey PRIMARY KEY (installation_id, sweep_kind, scope_key);

-- sweep_pages sweep_pages_pkey (constraint)
ALTER TABLE ONLY sweep_pages
    ADD CONSTRAINT sweep_pages_pkey PRIMARY KEY (installation_id, sweep_kind, scope_key, cursor);

-- sweep_seen_keys sweep_seen_keys_pkey (constraint)
ALTER TABLE ONLY sweep_seen_keys
    ADD CONSTRAINT sweep_seen_keys_pkey PRIMARY KEY (installation_id, sweep_kind, scope_key, entity_key);

-- webhook_deliveries webhook_deliveries_pkey (constraint)
ALTER TABLE ONLY webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_pkey PRIMARY KEY (delivery_guid);

-- work_items work_items_pkey (constraint)
ALTER TABLE ONLY work_items
    ADD CONSTRAINT work_items_pkey PRIMARY KEY (identity_key);

-- backfill_children backfill_children_installation_id_repo_full_name_fkey (fk constraint)
ALTER TABLE ONLY backfill_children
    ADD CONSTRAINT backfill_children_installation_id_repo_full_name_fkey FOREIGN KEY (installation_id, repo_full_name) REFERENCES backfill_cursors(installation_id, repo_full_name);

-- check_history check_history_check_run_gh_id_fkey (fk constraint)
ALTER TABLE ONLY check_history
    ADD CONSTRAINT check_history_check_run_gh_id_fkey FOREIGN KEY (check_run_gh_id) REFERENCES check_runs(gh_id);

-- check_history check_history_repo_id_fkey (fk constraint)
ALTER TABLE ONLY check_history
    ADD CONSTRAINT check_history_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- check_runs check_runs_repo_id_fkey (fk constraint)
ALTER TABLE ONLY check_runs
    ADD CONSTRAINT check_runs_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- pull_requests pull_requests_repo_id_fkey (fk constraint)
ALTER TABLE ONLY pull_requests
    ADD CONSTRAINT pull_requests_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- repo_aliases repo_aliases_repo_id_fkey (fk constraint)
ALTER TABLE ONLY repo_aliases
    ADD CONSTRAINT repo_aliases_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- repo_rule_sync_state repo_rule_sync_state_repo_id_fkey (fk constraint)
ALTER TABLE ONLY repo_rule_sync_state
    ADD CONSTRAINT repo_rule_sync_state_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- repo_rules repo_rules_repo_id_fkey (fk constraint)
ALTER TABLE ONLY repo_rules
    ADD CONSTRAINT repo_rules_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- review_threads review_threads_repo_id_fkey (fk constraint)
ALTER TABLE ONLY review_threads
    ADD CONSTRAINT review_threads_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- review_threads review_threads_repo_id_pr_number_fkey (fk constraint)
ALTER TABLE ONLY review_threads
    ADD CONSTRAINT review_threads_repo_id_pr_number_fkey FOREIGN KEY (repo_id, pr_number) REFERENCES pull_requests(repo_id, number);

-- stacks stacks_repo_id_fkey (fk constraint)
ALTER TABLE ONLY stacks
    ADD CONSTRAINT stacks_repo_id_fkey FOREIGN KEY (repo_id) REFERENCES repos(id);

-- sweep_pages sweep_pages_installation_id_sweep_kind_scope_key_fkey (fk constraint)
ALTER TABLE ONLY sweep_pages
    ADD CONSTRAINT sweep_pages_installation_id_sweep_kind_scope_key_fkey FOREIGN KEY (installation_id, sweep_kind, scope_key) REFERENCES sweep_cursors(installation_id, sweep_kind, scope_key) ON DELETE CASCADE;

-- sweep_seen_keys sweep_seen_keys_installation_id_sweep_kind_scope_key_fkey (fk constraint)
ALTER TABLE ONLY sweep_seen_keys
    ADD CONSTRAINT sweep_seen_keys_installation_id_sweep_kind_scope_key_fkey FOREIGN KEY (installation_id, sweep_kind, scope_key) REFERENCES sweep_cursors(installation_id, sweep_kind, scope_key) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

-- ------------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------------

-- backfill_children_pending_idx (index)
CREATE INDEX backfill_children_pending_idx ON backfill_children USING btree (installation_id, repo_full_name) WHERE (completed_at IS NULL);

-- change_events_occurred_at_seq_idx (index)
CREATE INDEX change_events_occurred_at_seq_idx ON change_events USING btree (occurred_at, seq);

-- change_events_stream_seq_idx (index)
CREATE INDEX change_events_stream_seq_idx ON change_events USING btree (stream, seq);

-- check_history_prunable_btree_idx (index)
CREATE INDEX check_history_prunable_btree_idx ON check_history USING btree (synced_at, id);

-- check_history_repo_head_sha_synced_idx (index)
CREATE INDEX check_history_repo_head_sha_synced_idx ON check_history USING btree (repo_id, head_sha, synced_at);

-- drift_findings_detected_at_idx (index)
CREATE INDEX drift_findings_detected_at_idx ON drift_findings USING btree (detected_at DESC);

-- drift_findings_one_open_diff_idx (index)
CREATE UNIQUE INDEX drift_findings_one_open_diff_idx ON drift_findings USING btree (installation_id, entity_kind, entity_key, diff_hash) WHERE (resolved_at IS NULL);

-- drift_findings_resolved_at_idx (index)
CREATE INDEX drift_findings_resolved_at_idx ON drift_findings USING btree (resolved_at) WHERE (resolved_at IS NOT NULL);

-- pull_requests_repo_base_ref_idx (index)
CREATE INDEX pull_requests_repo_base_ref_idx ON pull_requests USING btree (repo_id, base_ref) WHERE (tombstoned_at IS NULL);

-- pull_requests_repo_head_ref_idx (index)
CREATE INDEX pull_requests_repo_head_ref_idx ON pull_requests USING btree (repo_id, head_ref) WHERE (tombstoned_at IS NULL);

-- pull_requests_repo_stack_idx (index)
CREATE INDEX pull_requests_repo_stack_idx ON pull_requests USING btree (repo_id, stack_number) WHERE ((tombstoned_at IS NULL) AND (stack_number IS NOT NULL));

-- pull_requests_stale_closed_checked_idx (index)
CREATE INDEX pull_requests_stale_closed_checked_idx ON pull_requests USING btree (last_checked_at, repo_id, number) INCLUDE (display_until) WHERE ((tombstoned_at IS NULL) AND (state <> 'open'::text));

-- pull_requests_stale_open_checked_idx (index)
CREATE INDEX pull_requests_stale_open_checked_idx ON pull_requests USING btree (last_checked_at, repo_id, number) WHERE ((tombstoned_at IS NULL) AND (state = 'open'::text));

-- refresh_intent_outstanding_event_idx (index)
CREATE INDEX refresh_intent_outstanding_event_idx ON refresh_intent_generations USING btree (event_received_at) WHERE ((completed_generation < generation) AND (event_received_at IS NOT NULL));

-- repos_full_name_idx (index)
CREATE INDEX repos_full_name_idx ON repos USING btree (full_name);

-- repos_live_installation_checked_idx (index)
CREATE INDEX repos_live_installation_checked_idx ON repos USING btree (installation_id, last_checked_at) WHERE (tombstoned_at IS NULL);

-- review_threads_pr_idx (index)
CREATE INDEX review_threads_pr_idx ON review_threads USING btree (repo_id, pr_number);

-- stacks_stale_closed_checked_idx (index)
CREATE INDEX stacks_stale_closed_checked_idx ON stacks USING btree (last_checked_at, repo_id, number) INCLUDE (display_until) WHERE ((tombstoned_at IS NULL) AND (NOT open));

-- stacks_stale_open_checked_idx (index)
CREATE INDEX stacks_stale_open_checked_idx ON stacks USING btree (last_checked_at, repo_id, number) WHERE ((tombstoned_at IS NULL) AND open);

-- webhook_deliveries_pending_due_idx (index)
CREATE INDEX webhook_deliveries_pending_due_idx ON webhook_deliveries USING btree (next_attempt_at, received_at, delivery_guid) WHERE (status = 'pending'::text);

-- webhook_deliveries_prunable_btree_idx (index)
CREATE INDEX webhook_deliveries_prunable_btree_idx ON webhook_deliveries USING btree (received_at, delivery_guid) WHERE ((raw_body IS NOT NULL) AND (status = 'processed'::text));

-- webhook_deliveries_unfinished_received_idx (index)
CREATE INDEX webhook_deliveries_unfinished_received_idx ON webhook_deliveries USING btree (status, received_at) WHERE (status = ANY (ARRAY['pending'::text, 'processing'::text, 'parked'::text]));

-- work_items_org_identity_idx (index)
CREATE INDEX work_items_org_identity_idx ON work_items USING btree (org_id, identity_key);

-- ------------------------------------------------------------------
-- Seed rows
-- ------------------------------------------------------------------

-- C-S2: stream_watermark is a singleton the watermarker advances and
-- readers trust unconditionally; it must exist before the first pass.
INSERT INTO stream_watermark (safe_seq, updated_at) VALUES (0, now());
