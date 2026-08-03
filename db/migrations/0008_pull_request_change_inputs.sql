-- Bounded current changed-file and CODEOWNERS inputs for pull requests.
-- The parent row is the completeness and provenance fence for both replace
-- sets. A true files_truncated means the listed paths and owners are only the
-- known prefix/subset and consumers must not infer absence.
CREATE TABLE pull_request_change_snapshots (
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    base_sha text NOT NULL,
    head_sha text NOT NULL,
    files_total_count integer NOT NULL,
    files_truncated boolean NOT NULL,
    codeowners_ref text NOT NULL,
    codeowners_sha text NOT NULL,
    codeowners_path text,
    codeowners_state text NOT NULL,
    codeowners_source text,
    codeowners_hash text NOT NULL,
    parent_gh_updated_at timestamp with time zone NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_change_snapshots_pkey PRIMARY KEY (
        repo_id, pr_number
    ),
    CONSTRAINT pull_request_change_snapshots_pull_request_fkey
        FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_requests(repo_id, number),
    CONSTRAINT pull_request_change_snapshots_pr_number_check
        CHECK (pr_number > 0),
    CONSTRAINT pull_request_change_snapshots_file_count_check
        CHECK (files_total_count >= 0),
    CONSTRAINT pull_request_change_snapshots_codeowners_state_check
        CHECK (codeowners_state = ANY (ARRAY[
            'present'::text, 'missing'::text, 'oversized'::text,
            'unavailable'::text
        ])),
    CONSTRAINT pull_request_change_snapshots_codeowners_shape_check CHECK (
        (codeowners_state = 'present' AND codeowners_path IS NOT NULL AND
         codeowners_source IS NOT NULL)
        OR
        (codeowners_state = 'missing' AND codeowners_path IS NULL AND
         codeowners_source IS NULL)
        OR
        (codeowners_state = 'oversized' AND codeowners_path IS NOT NULL AND
         codeowners_source IS NULL)
        OR
        (codeowners_state = 'unavailable' AND codeowners_sha = '' AND
         codeowners_path IS NULL AND codeowners_source IS NULL)
    ),
    CONSTRAINT pull_request_change_snapshots_codeowners_hash_check
        CHECK (codeowners_hash <> ''::text),
    CONSTRAINT pull_request_change_snapshots_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

CREATE TABLE pull_request_changed_files (
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    path text NOT NULL,
    previous_path text,
    change_type text NOT NULL,
    base_sha text NOT NULL,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_changed_files_pkey PRIMARY KEY (
        repo_id, pr_number, path
    ),
    CONSTRAINT pull_request_changed_files_snapshot_fkey
        FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_request_change_snapshots(repo_id, pr_number),
    CONSTRAINT pull_request_changed_files_path_check CHECK (path <> ''::text),
    CONSTRAINT pull_request_changed_files_change_type_check CHECK (
        change_type = ANY (ARRAY[
            'added'::text, 'deleted'::text, 'renamed'::text,
            'copied'::text, 'modified'::text, 'changed'::text
        ])
    ),
    CONSTRAINT pull_request_changed_files_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

CREATE INDEX pull_request_changed_files_live_pr_idx
    ON pull_request_changed_files USING btree (repo_id, pr_number, path)
    INCLUDE (previous_path, change_type, base_sha, head_sha, last_checked_at)
    WHERE tombstoned_at IS NULL;

CREATE TABLE pull_request_file_owners (
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    path text NOT NULL,
    owner_token text NOT NULL,
    owner_type text NOT NULL,
    owner_name text NOT NULL,
    resolution_state text NOT NULL,
    owner_gh_id bigint,
    owner_node_id text,
    owner_login text,
    source_pattern text NOT NULL,
    source_line integer NOT NULL,
    base_sha text NOT NULL,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_file_owners_pkey PRIMARY KEY (
        repo_id, pr_number, path, owner_token
    ),
    CONSTRAINT pull_request_file_owners_changed_file_fkey
        FOREIGN KEY (repo_id, pr_number, path)
        REFERENCES pull_request_changed_files(repo_id, pr_number, path),
    CONSTRAINT pull_request_file_owners_owner_type_check CHECK (
        owner_type = ANY (ARRAY[
            'user'::text, 'team'::text, 'email'::text, 'malformed'::text
        ])
    ),
    CONSTRAINT pull_request_file_owners_resolution_state_check CHECK (
        resolution_state = ANY (ARRAY[
            'resolved'::text, 'unresolved'::text, 'deleted'::text
        ])
    ),
    CONSTRAINT pull_request_file_owners_identity_shape_check CHECK (
        (resolution_state = 'resolved' AND owner_node_id IS NOT NULL AND
         owner_login IS NOT NULL)
        OR
        (resolution_state <> 'resolved' AND owner_gh_id IS NULL AND
         owner_node_id IS NULL AND owner_login IS NULL)
    ),
    CONSTRAINT pull_request_file_owners_source_line_check
        CHECK (source_line > 0),
    CONSTRAINT pull_request_file_owners_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

CREATE INDEX pull_request_file_owners_live_pr_path_idx
    ON pull_request_file_owners USING btree (
        repo_id, pr_number, path, owner_type, owner_token
    ) INCLUDE (
        owner_name, resolution_state, owner_gh_id, owner_node_id, owner_login,
        source_pattern, source_line, base_sha, head_sha, last_checked_at
    )
    WHERE tombstoned_at IS NULL;

-- Keep drift_entity_keys on its existing cheap key-only dependency. Replace
-- only the payload-bearing pull_request arm so selected PRs build ownership
-- JSON after keyset selection.
ALTER VIEW drift_entities RENAME TO drift_entities_without_change_inputs;

CREATE VIEW drift_entities AS
SELECT prior.installation_id,
       prior.entity_kind,
       prior.source_id,
       prior.entity_key,
       prior.lock_key,
       prior.cache_snapshot,
       prior.last_checked_at
FROM drift_entities_without_change_inputs AS prior
WHERE prior.entity_kind <> 'pull_request'::text

UNION ALL

SELECT repos.installation_id,
       'pull_request'::text,
       pull_requests.id,
       ('pr:' || repos.full_name || ':' || pull_requests.number)::text,
       ('pr:' || repos.installation_id || ':' || repos.gh_id || ':' ||
        pull_requests.number)::text,
       jsonb_build_object(
           'id', pull_requests.gh_id,
           'node_id', pull_requests.node_id,
           'number', pull_requests.number,
           'title', pull_requests.title,
           'state', pull_requests.state,
           'draft', pull_requests.draft,
           'author_login', pull_requests.author_login,
           'head_ref', pull_requests.head_ref,
           'head_sha', pull_requests.head_sha,
           'base_ref', pull_requests.base_ref,
           'base_sha', pull_requests.base_sha,
           'review_decision', pull_requests.review_decision,
           'mergeable_state', pull_requests.mergeable_state,
           'stack_number', pull_requests.stack_number,
           'stack_position', pull_requests.stack_position,
           'review_requests', review_request_snapshot.requests,
           'reviews', review_snapshot.reviews,
           'comments', comment_snapshot.comments,
           'change_inputs', CASE
               WHEN change_snapshot.repo_id IS NULL THEN NULL
               ELSE jsonb_build_object(
                   'base_sha', change_snapshot.base_sha,
                   'head_sha', change_snapshot.head_sha,
                   'files_total_count', change_snapshot.files_total_count,
                   'files_truncated', change_snapshot.files_truncated,
                   'codeowners_ref', change_snapshot.codeowners_ref,
                   'codeowners_sha', change_snapshot.codeowners_sha,
                   'codeowners_path', change_snapshot.codeowners_path,
                   'codeowners_state', change_snapshot.codeowners_state,
                   'codeowners_hash', change_snapshot.codeowners_hash,
                   'files', changed_file_snapshot.files,
                   'owners', file_owner_snapshot.owners
               )
           END
       ),
       GREATEST(
           pull_requests.last_checked_at,
           review_request_snapshot.last_checked_at,
           review_snapshot.last_checked_at,
           comment_snapshot.last_checked_at,
           COALESCE(
               change_snapshot.last_checked_at,
               pull_requests.last_checked_at
           ),
           changed_file_snapshot.last_checked_at,
           file_owner_snapshot.last_checked_at
       )
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
LEFT JOIN pull_request_change_snapshots AS change_snapshot
  ON change_snapshot.repo_id = pull_requests.repo_id
 AND change_snapshot.pr_number = pull_requests.number
 AND change_snapshot.tombstoned_at IS NULL
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'kind', request.reviewer_kind,
                       'id', request.reviewer_gh_id,
                       'node_id', request.reviewer_node_id,
                       'login', request.reviewer_login,
                       'head_sha', request.head_sha
                   )
                   ORDER BY request.reviewer_kind, request.reviewer_gh_id
               ),
               '[]'::jsonb
           ) AS requests,
           COALESCE(max(request.last_checked_at), pull_requests.last_checked_at)
               AS last_checked_at
    FROM pull_request_review_requests AS request
    WHERE request.repo_id = pull_requests.repo_id
      AND request.pr_number = pull_requests.number
      AND request.tombstoned_at IS NULL
) AS review_request_snapshot
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'id', review.gh_id,
                       'node_id', review.node_id,
                       'author_kind', review.author_kind,
                       'author_node_id', review.author_node_id,
                       'author_login', review.author_login,
                       'state', review.state,
                       'submitted_at', CASE
                           WHEN review.submitted_at IS NULL THEN NULL
                           ELSE (extract(epoch FROM review.submitted_at) *
                                 1000000)::bigint
                       END,
                       'commit_oid', review.commit_oid,
                       'updated_at',
                           (extract(epoch FROM review.gh_updated_at) *
                            1000000)::bigint,
                       'head_sha', review.head_sha
                   )
                   ORDER BY review.node_id
               ),
               '[]'::jsonb
           ) AS reviews,
           COALESCE(max(review.last_checked_at), pull_requests.last_checked_at)
               AS last_checked_at
    FROM pull_request_reviews AS review
    WHERE review.repo_id = pull_requests.repo_id
      AND review.pr_number = pull_requests.number
      AND review.tombstoned_at IS NULL
) AS review_snapshot
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'id', comment.gh_id,
                       'node_id', comment.node_id,
                       'author_kind', comment.author_kind,
                       'author_node_id', comment.author_node_id,
                       'author_login', comment.author_login,
                       'created_at',
                           (extract(epoch FROM comment.created_at) *
                            1000000)::bigint,
                       'updated_at',
                           (extract(epoch FROM comment.gh_updated_at) *
                            1000000)::bigint,
                       'head_sha', comment.head_sha
                   )
                   ORDER BY comment.node_id
               ),
               '[]'::jsonb
           ) AS comments,
           COALESCE(max(comment.last_checked_at), pull_requests.last_checked_at)
               AS last_checked_at
    FROM pull_request_comments AS comment
    WHERE comment.repo_id = pull_requests.repo_id
      AND comment.pr_number = pull_requests.number
      AND comment.tombstoned_at IS NULL
) AS comment_snapshot
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'path', file.path,
                       'previous_path', file.previous_path,
                       'change_type', file.change_type
                   ) ORDER BY file.path
               ),
               '[]'::jsonb
           ) AS files,
           COALESCE(max(file.last_checked_at), pull_requests.last_checked_at)
               AS last_checked_at
    FROM pull_request_changed_files AS file
    WHERE file.repo_id = pull_requests.repo_id
      AND file.pr_number = pull_requests.number
      AND file.tombstoned_at IS NULL
) AS changed_file_snapshot
CROSS JOIN LATERAL (
    SELECT COALESCE(
               jsonb_agg(
                   jsonb_build_object(
                       'path', owner.path,
                       'owner_token', owner.owner_token,
                       'owner_type', owner.owner_type,
                       'owner_name', owner.owner_name,
                       'resolution_state', owner.resolution_state,
                       'owner_gh_id', owner.owner_gh_id,
                       'owner_node_id', owner.owner_node_id,
                       'owner_login', owner.owner_login,
                       'source_pattern', owner.source_pattern,
                       'source_line', owner.source_line
                   ) ORDER BY owner.path, owner.owner_token
               ),
               '[]'::jsonb
           ) AS owners,
           COALESCE(max(owner.last_checked_at), pull_requests.last_checked_at)
               AS last_checked_at
    FROM pull_request_file_owners AS owner
    WHERE owner.repo_id = pull_requests.repo_id
      AND owner.pr_number = pull_requests.number
      AND owner.tombstoned_at IS NULL
) AS file_owner_snapshot
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL;
