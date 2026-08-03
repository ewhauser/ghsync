-- Public identity-keyed participation facts for pull requests.
--
-- Reviews and comments are histories of stable GitHub nodes, not replace-set
-- identities. Comments advance on GitHub updatedAt; review versions combine
-- lifecycle state with submittedAt/updatedAt so terminal dismissal remains
-- monotonic even when GitHub does not bump updatedAt. A complete PR connection
-- observation tombstones nodes that GitHub no longer returns, but only when
-- that observation is at least as new as the cached PR.
CREATE TABLE pull_request_reviews (
    node_id text PRIMARY KEY,
    gh_id bigint,
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    author_kind text NOT NULL,
    author_node_id text,
    author_login text,
    state text NOT NULL,
    submitted_at timestamp with time zone,
    commit_oid text,
    gh_updated_at timestamp with time zone NOT NULL,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_reviews_gh_id_key UNIQUE (gh_id),
    CONSTRAINT pull_request_reviews_pull_request_fkey
        FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_requests(repo_id, number),
    CONSTRAINT pull_request_reviews_node_id_check CHECK (node_id <> ''::text),
    CONSTRAINT pull_request_reviews_gh_id_check CHECK (
        gh_id IS NULL OR gh_id > 0
    ),
    CONSTRAINT pull_request_reviews_pr_number_check CHECK (pr_number > 0),
    CONSTRAINT pull_request_reviews_author_kind_check
        CHECK (author_kind = ANY (ARRAY[
            'user'::text, 'bot'::text, 'mannequin'::text,
            'organization'::text, 'enterprise_user_account'::text,
            'unknown'::text, 'deleted'::text
        ])),
    CONSTRAINT pull_request_reviews_deleted_author_check CHECK (
        author_kind <> 'deleted'::text OR
        (author_node_id IS NULL AND author_login IS NULL)
    ),
    CONSTRAINT pull_request_reviews_state_check CHECK (
        state <> ''::text AND state = lower(state)
    ),
    CONSTRAINT pull_request_reviews_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

CREATE INDEX pull_request_reviews_live_pr_idx
    ON pull_request_reviews USING btree (repo_id, pr_number, node_id)
    INCLUDE (
        gh_id, author_kind, author_node_id, author_login, state,
        submitted_at, commit_oid, gh_updated_at, head_sha, synced_at,
        etag, sync_source, last_checked_at
    )
    WHERE tombstoned_at IS NULL;

CREATE TABLE pull_request_comments (
    node_id text PRIMARY KEY,
    gh_id bigint,
    repo_id bigint NOT NULL,
    pr_number integer NOT NULL,
    author_kind text NOT NULL,
    author_node_id text,
    author_login text,
    created_at timestamp with time zone NOT NULL,
    gh_updated_at timestamp with time zone NOT NULL,
    head_sha text NOT NULL,
    synced_at timestamp with time zone NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    sync_source text NOT NULL,
    tombstoned_at timestamp with time zone,
    last_checked_at timestamp with time zone NOT NULL,
    CONSTRAINT pull_request_comments_gh_id_key UNIQUE (gh_id),
    CONSTRAINT pull_request_comments_pull_request_fkey
        FOREIGN KEY (repo_id, pr_number)
        REFERENCES pull_requests(repo_id, number),
    CONSTRAINT pull_request_comments_node_id_check CHECK (node_id <> ''::text),
    CONSTRAINT pull_request_comments_gh_id_check CHECK (
        gh_id IS NULL OR gh_id > 0
    ),
    CONSTRAINT pull_request_comments_pr_number_check CHECK (pr_number > 0),
    CONSTRAINT pull_request_comments_author_kind_check
        CHECK (author_kind = ANY (ARRAY[
            'user'::text, 'bot'::text, 'mannequin'::text,
            'organization'::text, 'enterprise_user_account'::text,
            'unknown'::text, 'deleted'::text
        ])),
    CONSTRAINT pull_request_comments_deleted_author_check CHECK (
        author_kind <> 'deleted'::text OR
        (author_node_id IS NULL AND author_login IS NULL)
    ),
    CONSTRAINT pull_request_comments_sync_source_check
        CHECK (sync_source = ANY (ARRAY[
            'webhook'::text, 'reconcile'::text, 'backfill'::text,
            'manual'::text, 'interactive'::text
        ]))
);

CREATE INDEX pull_request_comments_live_pr_idx
    ON pull_request_comments USING btree (repo_id, pr_number, node_id)
    INCLUDE (
        gh_id, author_kind, author_node_id, author_login, created_at,
        gh_updated_at, head_sha, synced_at, etag, sync_source,
        last_checked_at
    )
    WHERE tombstoned_at IS NULL;

-- Keep 0005's key-first sampler shape. The previous view remains as a private
-- implementation layer for unchanged entity classes; its pull-request arm is
-- replaced below by a row-local LATERAL participation snapshot evaluated only
-- for sampled PR source IDs.
ALTER VIEW drift_entities RENAME TO drift_entities_without_participation;

CREATE VIEW drift_entities AS
SELECT prior.installation_id,
       prior.entity_kind,
       prior.source_id,
       prior.entity_key,
       prior.lock_key,
       prior.cache_snapshot,
       prior.last_checked_at
FROM drift_entities_without_participation AS prior
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
           'comments', comment_snapshot.comments
       ),
       GREATEST(
           pull_requests.last_checked_at,
           review_request_snapshot.last_checked_at,
           review_snapshot.last_checked_at,
           comment_snapshot.last_checked_at
       )
FROM pull_requests
JOIN repos ON repos.id = pull_requests.repo_id
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
           COALESCE(
               max(request.last_checked_at),
               pull_requests.last_checked_at
           ) AS last_checked_at
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
           COALESCE(
               max(review.last_checked_at),
               pull_requests.last_checked_at
           ) AS last_checked_at
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
           COALESCE(
               max(comment.last_checked_at),
               pull_requests.last_checked_at
           ) AS last_checked_at
    FROM pull_request_comments AS comment
    WHERE comment.repo_id = pull_requests.repo_id
      AND comment.pr_number = pull_requests.number
      AND comment.tombstoned_at IS NULL
) AS comment_snapshot
WHERE repos.tombstoned_at IS NULL
  AND pull_requests.tombstoned_at IS NULL;
