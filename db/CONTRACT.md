# ghsync Postgres delivery contract

Contract version: **v1**. The schema begins in the squashed baseline
migrations (`0001` tables, `0002` functions and the database-enforced
writer fence trigger, `0003` views) and is extended only by checksummed,
append-only migrations such as `0004` through `0008`.

Postgres is the ghsync sync engine’s public delivery interface. Consumers
read snapshot-consistent cache rows and follow reference events through
`pkg/streamclient`. The Go package is the reference implementation; consumers
must not reproduce the watermark, cursor, retention, or resync algorithms.
Direct Postgres access is the **only v1 transport**. The gRPC and SSE surfaces
in `docs/API_SPEC.md` belong to a future API project and are not implemented
by the sync engine.

## Consumer facts

The suggested database role is `ghsync_consumer`. Run this block once as a
role that may create roles and grant access, then grant `ghsync_consumer` to
each application login that runs `pkg/streamclient`:

```sql
CREATE ROLE ghsync_consumer NOLOGIN;

GRANT USAGE ON SCHEMA public TO ghsync_consumer;

GRANT SELECT ON TABLE
    repos,
    repo_rules,
    stacks,
    pull_requests,
    pull_request_review_requests,
    pull_request_reviews,
    pull_request_comments,
    pull_request_change_snapshots,
    pull_request_changed_files,
    pull_request_file_owners,
    review_threads,
    check_runs,
    check_history,
    work_items,
    change_events,
    stream_watermark,
    stream_horizons
TO ghsync_consumer;

GRANT SELECT, INSERT, UPDATE ON TABLE consumer_cursors
TO ghsync_consumer;
```

Run exactly **one tailer per `(consumer, stream)`**. Two processes must not
share that pair; elect one tailer and fan out locally, or use distinct consumer
names. `pkg/streamclient` locks the cursor row in a `REPEATABLE READ`
transaction so handler effects and cursor advancement remain atomic.
Concurrent tailers for the same pair therefore surface PostgreSQL
serialization errors by design.

`pkg/streamclient` classifies that serialization outcome as the typed
`ErrCursorContention`; consumers may use `IsRetryable` for bounded retry and
must still restore the one-tailer topology.

Event order is `seq` alone. `occurred_at` is transaction-start time and is
**not** monotonic with `seq`: a long writer can commit a high sequence with an
older `occurred_at` than a faster writer. Never order, deduplicate, or resume
by `occurred_at`. Retention intentionally prunes by `occurred_at`; when that
time order differs from sequence order, `stream_horizons.pruned_through_seq`
may force a resync earlier than the remaining rows alone would require. That
is the conservative direction: it can cause an extra snapshot, never a silent
gap.

## Public v1 schema

The manifest below is normative and is checked against a freshly migrated
Postgres schema. `Nullable` describes SQL nullability, not whether a text or
JSON value may be empty.

<!-- v1-schema:start -->
| Table | Column | PostgreSQL type | Nullable | Key or join role |
| --- | --- | --- | --- | --- |
| `repos` | `id` | `bigint` | no | primary key; local join key |
| `repos` | `installation_id` | `bigint` | no | external scope key |
| `repos` | `org_id` | `bigint` | no | organization filter |
| `repos` | `gh_id` | `bigint` | no | unique GitHub repository identity |
| `repos` | `node_id` | `text` | no | GitHub GraphQL identity |
| `repos` | `owner` | `text` | no | current owner |
| `repos` | `name` | `text` | no | current repository name |
| `repos` | `full_name` | `text` | no | current owner/name |
| `repos` | `default_branch` | `text` | no | — |
| `repos` | `archived` | `boolean` | no | — |
| `repos` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `repos` | `head_sha` | `text` | no | default-branch head |
| `repos` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `repos` | `etag` | `text` | no | HTTP validator |
| `repos` | `sync_source` | `text` | no | provenance enum |
| `repos` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `repos` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `repo_rules` | `repo_id` | `bigint` | no | primary key part; references repos.id |
| `repo_rules` | `rule_key` | `text` | no | primary key part |
| `repo_rules` | `rule` | `jsonb` | no | current rule document |
| `repo_rules` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `repo_rules` | `head_sha` | `text` | no | repository head used for observation |
| `repo_rules` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `repo_rules` | `etag` | `text` | no | HTTP validator |
| `repo_rules` | `sync_source` | `text` | no | provenance enum |
| `repo_rules` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `repo_rules` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `stacks` | `id` | `bigint` | no | primary key; local join key |
| `stacks` | `repo_id` | `bigint` | no | references repos.id; unique with number |
| `stacks` | `gh_id` | `bigint` | yes | GitHub stack identity where supplied |
| `stacks` | `node_id` | `text` | no | GitHub GraphQL identity where supplied |
| `stacks` | `number` | `integer` | no | repository-local stack number |
| `stacks` | `base_ref` | `text` | no | — |
| `stacks` | `base_sha` | `text` | no | empty means GitHub reported the base ref but the SHA is unknown |
| `stacks` | `open` | `boolean` | no | — |
| `stacks` | `entries` | `jsonb` | no | ordered stack-entry references |
| `stacks` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `stacks` | `head_sha` | `text` | no | stack head |
| `stacks` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `stacks` | `etag` | `text` | no | HTTP validator |
| `stacks` | `sync_source` | `text` | no | provenance enum |
| `stacks` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `stacks` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `stacks` | `display_until` | `timestamp with time zone` | yes | closed-row display-retention boundary |
| `pull_requests` | `id` | `bigint` | no | primary key; local join key |
| `pull_requests` | `repo_id` | `bigint` | no | references repos.id; unique with number |
| `pull_requests` | `gh_id` | `bigint` | yes | GitHub pull-request identity |
| `pull_requests` | `node_id` | `text` | no | GitHub GraphQL identity |
| `pull_requests` | `number` | `integer` | no | repository-local PR number |
| `pull_requests` | `title` | `text` | no | — |
| `pull_requests` | `state` | `text` | no | — |
| `pull_requests` | `draft` | `boolean` | no | — |
| `pull_requests` | `author_login` | `text` | no | — |
| `pull_requests` | `head_ref` | `text` | no | — |
| `pull_requests` | `head_sha` | `text` | no | check-run join input |
| `pull_requests` | `base_ref` | `text` | no | — |
| `pull_requests` | `base_sha` | `text` | no | empty means GitHub reported the base ref but the SHA is unknown |
| `pull_requests` | `review_decision` | `text` | no | — |
| `pull_requests` | `mergeable_state` | `text` | no | — |
| `pull_requests` | `stack_number` | `integer` | yes | null means loose PR |
| `pull_requests` | `stack_position` | `integer` | yes | null exactly when stack_number is null |
| `pull_requests` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `pull_requests` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_requests` | `etag` | `text` | no | HTTP validator |
| `pull_requests` | `sync_source` | `text` | no | provenance enum |
| `pull_requests` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `pull_requests` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_requests` | `display_until` | `timestamp with time zone` | yes | closed-row display-retention boundary |
| `pull_request_change_snapshots` | `repo_id` | `bigint` | no | primary key part; references pull_requests(repo_id,number) |
| `pull_request_change_snapshots` | `pr_number` | `integer` | no | primary key part; repository-local PR number |
| `pull_request_change_snapshots` | `base_sha` | `text` | no | exact PR base fence; empty is the upstream-unknown sentinel |
| `pull_request_change_snapshots` | `head_sha` | `text` | no | exact PR head fence |
| `pull_request_change_snapshots` | `files_total_count` | `integer` | no | GitHub-reported changed-file total |
| `pull_request_change_snapshots` | `files_truncated` | `boolean` | no | true means the child file set is incomplete |
| `pull_request_change_snapshots` | `codeowners_ref` | `text` | no | PR base ref from which ownership applies |
| `pull_request_change_snapshots` | `codeowners_sha` | `text` | no | exact base commit read; empty when unavailable |
| `pull_request_change_snapshots` | `codeowners_path` | `text` | yes | effective source path selected by precedence |
| `pull_request_change_snapshots` | `codeowners_state` | `text` | no | present, missing, oversized, or unavailable |
| `pull_request_change_snapshots` | `codeowners_source` | `text` | yes | exact effective source; null unless present |
| `pull_request_change_snapshots` | `codeowners_hash` | `text` | no | source-state/path/content identity |
| `pull_request_change_snapshots` | `parent_gh_updated_at` | `timestamp with time zone` | no | parent-observation freshness fence |
| `pull_request_change_snapshots` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_change_snapshots` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_change_snapshots` | `sync_source` | `text` | no | provenance enum |
| `pull_request_change_snapshots` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means snapshot is not live |
| `pull_request_change_snapshots` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_request_changed_files` | `repo_id` | `bigint` | no | primary key part; snapshot join key |
| `pull_request_changed_files` | `pr_number` | `integer` | no | primary key part; snapshot join key |
| `pull_request_changed_files` | `path` | `text` | no | primary key part; current repository-relative path |
| `pull_request_changed_files` | `previous_path` | `text` | yes | prior path for a rename |
| `pull_request_changed_files` | `change_type` | `text` | no | added, deleted, renamed, copied, modified, or changed |
| `pull_request_changed_files` | `base_sha` | `text` | no | copied snapshot base fence |
| `pull_request_changed_files` | `head_sha` | `text` | no | copied snapshot head fence |
| `pull_request_changed_files` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_changed_files` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_changed_files` | `sync_source` | `text` | no | provenance enum |
| `pull_request_changed_files` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not in the current file set |
| `pull_request_changed_files` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_request_file_owners` | `repo_id` | `bigint` | no | primary key part; changed-file join key |
| `pull_request_file_owners` | `pr_number` | `integer` | no | primary key part; changed-file join key |
| `pull_request_file_owners` | `path` | `text` | no | primary key part; changed-file join key |
| `pull_request_file_owners` | `owner_token` | `text` | no | primary key part; exact source token |
| `pull_request_file_owners` | `owner_type` | `text` | no | user, team, email, or malformed |
| `pull_request_file_owners` | `owner_name` | `text` | no | normalized lookup name; may be empty when malformed |
| `pull_request_file_owners` | `resolution_state` | `text` | no | resolved, unresolved, or deleted |
| `pull_request_file_owners` | `owner_gh_id` | `bigint` | yes | stable database identity when known |
| `pull_request_file_owners` | `owner_node_id` | `text` | yes | stable GraphQL identity when known |
| `pull_request_file_owners` | `owner_login` | `text` | yes | known current user login or team slug |
| `pull_request_file_owners` | `source_pattern` | `text` | no | last matching CODEOWNERS pattern |
| `pull_request_file_owners` | `source_line` | `integer` | no | one-based source line |
| `pull_request_file_owners` | `base_sha` | `text` | no | copied snapshot base fence |
| `pull_request_file_owners` | `head_sha` | `text` | no | copied snapshot head fence |
| `pull_request_file_owners` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_file_owners` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_file_owners` | `sync_source` | `text` | no | provenance enum |
| `pull_request_file_owners` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means owner is not current for the path |
| `pull_request_file_owners` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_request_review_requests` | `repo_id` | `bigint` | no | primary key part; references pull_requests(repo_id,number) |
| `pull_request_review_requests` | `pr_number` | `integer` | no | primary key part; repository-local PR number |
| `pull_request_review_requests` | `reviewer_kind` | `text` | no | primary key part; user or team |
| `pull_request_review_requests` | `reviewer_gh_id` | `bigint` | no | primary key part; stable GitHub user or team identity |
| `pull_request_review_requests` | `reviewer_node_id` | `text` | no | stable GitHub GraphQL user or team identity |
| `pull_request_review_requests` | `reviewer_login` | `text` | no | current user login or team slug |
| `pull_request_review_requests` | `requested_at` | `timestamp with time zone` | yes | authoritative request time when exposed by GitHub |
| `pull_request_review_requests` | `first_seen_at` | `timestamp with time zone` | no | fallback age for the current uninterrupted request |
| `pull_request_review_requests` | `gh_updated_at` | `timestamp with time zone` | no | parent PR write-if-newer observation version |
| `pull_request_review_requests` | `head_sha` | `text` | no | PR head observed with this snapshot row |
| `pull_request_review_requests` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_review_requests` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_review_requests` | `sync_source` | `text` | no | provenance enum |
| `pull_request_review_requests` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not in the current request set |
| `pull_request_review_requests` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_request_reviews` | `node_id` | `text` | no | primary key; stable GitHub GraphQL review identity |
| `pull_request_reviews` | `gh_id` | `bigint` | yes | unique GitHub `fullDatabaseId` when exposed |
| `pull_request_reviews` | `repo_id` | `bigint` | no | references pull_requests(repo_id,number) |
| `pull_request_reviews` | `pr_number` | `integer` | no | repository-local PR number |
| `pull_request_reviews` | `author_kind` | `text` | no | normalized GraphQL actor kind or deleted |
| `pull_request_reviews` | `author_node_id` | `text` | yes | stable actor identity when exposed |
| `pull_request_reviews` | `author_login` | `text` | yes | actor login when exposed |
| `pull_request_reviews` | `state` | `text` | no | current normalized GitHub review state |
| `pull_request_reviews` | `submitted_at` | `timestamp with time zone` | yes | GitHub submission time; null for pending reviews |
| `pull_request_reviews` | `commit_oid` | `text` | yes | commit observed by the review when resolvable |
| `pull_request_reviews` | `gh_updated_at` | `timestamp with time zone` | no | per-review write-if-newer version |
| `pull_request_reviews` | `head_sha` | `text` | no | PR head observed with this fact |
| `pull_request_reviews` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_reviews` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_reviews` | `sync_source` | `text` | no | provenance enum |
| `pull_request_reviews` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means GitHub no longer returned the review |
| `pull_request_reviews` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `pull_request_comments` | `node_id` | `text` | no | primary key; stable GitHub GraphQL issue-comment identity |
| `pull_request_comments` | `gh_id` | `bigint` | yes | unique GitHub database identity when exposed |
| `pull_request_comments` | `repo_id` | `bigint` | no | references pull_requests(repo_id,number) |
| `pull_request_comments` | `pr_number` | `integer` | no | repository-local PR number |
| `pull_request_comments` | `author_kind` | `text` | no | normalized GraphQL actor kind or deleted |
| `pull_request_comments` | `author_node_id` | `text` | yes | stable actor identity when exposed |
| `pull_request_comments` | `author_login` | `text` | yes | actor login when exposed |
| `pull_request_comments` | `created_at` | `timestamp with time zone` | no | GitHub creation time |
| `pull_request_comments` | `gh_updated_at` | `timestamp with time zone` | no | GitHub updated_at and per-comment write-if-newer version |
| `pull_request_comments` | `head_sha` | `text` | no | PR head observed with this fact |
| `pull_request_comments` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `pull_request_comments` | `etag` | `text` | no | HTTP validator provenance |
| `pull_request_comments` | `sync_source` | `text` | no | provenance enum |
| `pull_request_comments` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means GitHub no longer returned the comment |
| `pull_request_comments` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `review_threads` | `id` | `text` | no | primary key; GitHub thread node ID |
| `review_threads` | `repo_id` | `bigint` | no | references repos.id |
| `review_threads` | `pr_number` | `integer` | no | references pull_requests(repo_id,number) |
| `review_threads` | `is_resolved` | `boolean` | no | — |
| `review_threads` | `is_outdated` | `boolean` | no | — |
| `review_threads` | `path` | `text` | no | — |
| `review_threads` | `line` | `integer` | yes | — |
| `review_threads` | `comments` | `jsonb` | no | ordered comment references |
| `review_threads` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `review_threads` | `head_sha` | `text` | no | observed PR head |
| `review_threads` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `review_threads` | `etag` | `text` | no | HTTP validator |
| `review_threads` | `sync_source` | `text` | no | provenance enum |
| `review_threads` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `review_threads` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `check_runs` | `gh_id` | `bigint` | no | primary key; GitHub check-run identity |
| `check_runs` | `repo_id` | `bigint` | no | references repos.id |
| `check_runs` | `node_id` | `text` | no | GitHub GraphQL identity |
| `check_runs` | `name` | `text` | no | — |
| `check_runs` | `status` | `text` | no | — |
| `check_runs` | `conclusion` | `text` | no | — |
| `check_runs` | `details_url` | `text` | no | — |
| `check_runs` | `app_slug` | `text` | no | — |
| `check_runs` | `started_at` | `timestamp with time zone` | yes | — |
| `check_runs` | `completed_at` | `timestamp with time zone` | yes | — |
| `check_runs` | `gh_updated_at` | `timestamp with time zone` | yes | write-if-newer input |
| `check_runs` | `head_sha` | `text` | no | PR lookup input |
| `check_runs` | `synced_at` | `timestamp with time zone` | no | domain-change time |
| `check_runs` | `etag` | `text` | no | HTTP validator |
| `check_runs` | `sync_source` | `text` | no | provenance enum |
| `check_runs` | `tombstoned_at` | `timestamp with time zone` | yes | non-null means not live |
| `check_runs` | `semantic_version` | `text` | no | write-if-newer input |
| `check_runs` | `last_checked_at` | `timestamp with time zone` | no | authoritative validation time |
| `check_history` | `id` | `bigint` | no | primary key |
| `check_history` | `check_run_gh_id` | `bigint` | no | references check_runs.gh_id |
| `check_history` | `repo_id` | `bigint` | no | references repos.id |
| `check_history` | `name` | `text` | no | — |
| `check_history` | `status` | `text` | no | — |
| `check_history` | `conclusion` | `text` | no | — |
| `check_history` | `observed` | `jsonb` | no | accepted check observation |
| `check_history` | `gh_updated_at` | `timestamp with time zone` | yes | source version |
| `check_history` | `head_sha` | `text` | no | — |
| `check_history` | `synced_at` | `timestamp with time zone` | no | accepted transition time |
| `check_history` | `etag` | `text` | no | HTTP validator |
| `check_history` | `sync_source` | `text` | no | provenance enum |
| `check_history` | `tombstoned_at` | `timestamp with time zone` | yes | retained provenance |
| `check_history` | `semantic_version` | `text` | no | accepted semantic version |
| `work_items` | `identity_key` | `text` | no | primary key; stable domain identity |
| `work_items` | `org_id` | `bigint` | no | organization filter |
| `work_items` | `payload` | `jsonb` | no | current derived value |
| `work_items` | `updated_at` | `timestamp with time zone` | no | materialization change time |
| `work_items` | `scope_key` | `text` | no | owning C-D2 derivation scope |
| `change_events` | `seq` | `bigint` | no | primary key; global event order |
| `change_events` | `stream` | `text` | no | entities or work_items |
| `change_events` | `kind` | `text` | no | event variant below |
| `change_events` | `entity_key` | `text` | no | immutable lookup reference |
| `change_events` | `occurred_at` | `timestamp with time zone` | no | source transaction event time |
| `change_events` | `payload` | `jsonb` | no | versioned reference payload |
| `stream_watermark` | `singleton` | `boolean` | no | primary key; always true |
| `stream_watermark` | `safe_seq` | `bigint` | no | greatest safe consumer sequence |
| `stream_watermark` | `updated_at` | `timestamp with time zone` | no | last publication time |
| `consumer_cursors` | `consumer` | `text` | no | primary key part |
| `consumer_cursors` | `stream` | `text` | no | primary key part |
| `consumer_cursors` | `seq` | `bigint` | no | last transactionally applied event |
| `consumer_cursors` | `updated_at` | `timestamp with time zone` | no | last cursor change |
| `consumer_cursors` | `resync_count` | `bigint` | no | monotonic C-S4 resync count |
| `consumer_cursors` | `last_resync_at` | `timestamp with time zone` | yes | latest RESYNC_REQUIRED time |
| `stream_horizons` | `stream` | `text` | no | primary key |
| `stream_horizons` | `pruned_through_seq` | `bigint` | no | greatest removed sequence |
| `stream_horizons` | `updated_at` | `timestamp with time zone` | no | last horizon change |
<!-- v1-schema:end -->

Use a `REPEATABLE READ` transaction when rows from more than one public table
must agree. Foreign keys use local surrogate IDs; durable external references
use GitHub IDs plus repository-local numbers.

### Provenance, freshness, and tombstones

`sync_source` is one of `webhook`, `reconcile`, `backfill`, `manual`, or
`interactive`.
`synced_at` records the last accepted domain change; `last_checked_at` records
the last authoritative validation, including 304 and identical responses. Do
not use `synced_at` as the freshness check after an unchanged response.

A non-null `tombstoned_at` means a mirror row is retained history and is not
live. Normal live reads include `tombstoned_at IS NULL`. A later authoritative
observation may resurrect the row through the monotonic writer.
For closed stacks and pull requests, `display_until > clock_timestamp()`
identifies the retained subset still eligible for display and therefore still
covered by the closed-entity C-R1 validation bound.
`check_history` is append-only transition history retained for at least 90
days. Other tombstoned mirror skeletons have no v1 expiry.

### Changed files and ownership inputs

`pull_request_change_snapshots` is the completeness and provenance fence for
two current replace sets. `pull_request_changed_files` mirrors the GraphQL PR
`files` connection through every cursor, bounded at GitHub's documented 3,000
file limit. The REST files listing supplies `previous_path` for renames because
GraphQL does not expose it. The parent and every child row carry the exact
`base_sha`/`head_sha` pair observed for the diff. A page-to-page or final REST
fence change rejects the observation and retries it; it never combines pages
from two heads.

`files_truncated = true` is explicit incomplete truth. It is set when GitHub
omits the connection, reports a total inconsistent with the returned set,
leaves a cursor beyond 3,000 files, or fails to supply a rename's previous
path. The stored rows remain useful positive facts, but consumers MUST NOT
infer that an absent path or owner is absent upstream. The next webhook,
backfill/reconciliation pass, or manual refresh replaces the set and may clear
the flag when GitHub returns a cursor-complete observation. There is no
out-of-band continuation beyond the cap.

CODEOWNERS is read from the PR base commit, not its head. At the exact
`codeowners_sha`, ghsync selects `.github/CODEOWNERS`, then root `CODEOWNERS`,
then `docs/CODEOWNERS`; the first present file is effective and its path,
base ref, SHA, source, and hash are retained. An effective file at least 3 MiB
is `oversized` and does not fall through. No file at any location is the
successful `missing` empty-ownership state. If GitHub reports the base ref but
not its SHA, `codeowners_sha = ''` and `codeowners_state = 'unavailable'`;
ghsync does not silently read the moving ref.

The pure resolver is case-sensitive and repository-root-relative. It applies
CODEOWNERS' gitignore-style pattern behavior, including root anchoring,
basename patterns, `*`, `?`, `**`, trailing-slash directory matches, escaped
spaces, and inline comments. The last matching rule wins as a whole. A later
matching rule with no owner tokens explicitly clears ownership for its matched
subtree. Negation (`!`), character ranges, and escaped leading `#` are not
CODEOWNERS features, so such pattern lines are ignored instead of being
interpreted as gitignore.
Duplicate owner tokens on the winning line collapse to one fact. Exact source
tokens are preserved: valid `@user`, `@org/team`, and email tokens receive a
syntactic type, while malformed tokens remain rows with `owner_type =
'malformed'`. Path matching is case-sensitive; source owner-token spelling is
also retained case-for-case, while user and team identity lookup is
case-insensitive to match GitHub login and slug identity.

User and team tokens resolve only from stable identities already mirrored for
the repository; ghsync makes no live per-owner lookup. `resolved` carries the
known node identity, login/slug, and database ID when available. A token with
no matching identity is `unresolved`; absence alone is never promoted to
`deleted`. `deleted` is a distinct reserved state for an explicit upstream
deletion fact, and carries null identity columns, matching participation's
deleted-actor policy. Email and malformed tokens are always explicit
unresolved facts. These are ownership inputs, not reviewer scores, workload
policy, or recommendations.

Consumers can join diff to ownership without Go or another GitHub read:

<!-- diff-to-owner-sql:start -->
```sql
SELECT snapshot.base_sha, snapshot.head_sha,
       snapshot.files_total_count, snapshot.files_truncated,
       snapshot.codeowners_ref, snapshot.codeowners_sha,
       snapshot.codeowners_path, snapshot.codeowners_state,
       file.path, file.previous_path, file.change_type,
       owner.owner_token, owner.owner_type, owner.resolution_state,
       owner.owner_gh_id, owner.owner_node_id, owner.owner_login,
       owner.source_pattern, owner.source_line
FROM pull_request_change_snapshots AS snapshot
JOIN pull_request_changed_files AS file
 ON file.repo_id = snapshot.repo_id
 AND file.pr_number = snapshot.pr_number
 AND file.base_sha = snapshot.base_sha
 AND file.head_sha = snapshot.head_sha
 AND file.tombstoned_at IS NULL
LEFT JOIN pull_request_file_owners AS owner
  ON owner.repo_id = file.repo_id
 AND owner.pr_number = file.pr_number
 AND owner.path = file.path
 AND owner.base_sha = snapshot.base_sha
 AND owner.head_sha = snapshot.head_sha
 AND owner.tombstoned_at IS NULL
WHERE snapshot.repo_id = $1
  AND snapshot.pr_number = $2
  AND snapshot.tombstoned_at IS NULL
ORDER BY file.path, owner.owner_token;
```
<!-- diff-to-owner-sql:end -->

ghsync deliberately does not mirror blame: line-by-line history is unbounded
in changed lines, history depth, and API cost. A lower-cost offline overlap
input is the fenced changed-path set above, combined with PR authorship,
identity-keyed review/comment participation below, and commit authorship from
the consumer's mirrored/local Git objects keyed by `head_sha`. A consumer can
restrict commits to its chosen recent window, intersect their touched paths
with `pull_request_changed_files.path`, and then apply its own recency,
workload, or ranking policy. `pull_request_reviews.commit_oid` supplies an
additional source-derived commit association for submitted reviewers. This
keeps the mirror bounded and the engine policy-free while avoiding blame or a
second independently timed GitHub snapshot.

The replace sets use the participation parent-observation gate: an observation
older than the current PR `gh_updated_at`, or whose base/head no longer equals
the parent row, cannot insert, update, tombstone, or merely freshen ownership
children. Equal parent versions remain eligible so a default-branch
CODEOWNERS change can update source facts even when GitHub does not change the
PR timestamp. Queue uniqueness coalesces repeated branch and PR refresh keys;
the fanout has no arbitrary count cutoff, so every cached open PR on the
affected branch remains covered. One accepted observation emits at most one
`pull_request.changed` reference if the snapshot, file set, or owner set
changes; an identical refresh only advances `last_checked_at` and emits none.

All consumed `pull_request` actions, including synchronize, force-push/base
change, reopen, and stacked previews, retain a direct PR refresh. Branch pushes
refresh the finite cached set of live open PRs whose head or base uses that
branch, including stacked PRs; closed retained rows are outside this fanout. A
default-branch push that adds, modifies, or removes one of the three effective
CODEOWNERS paths is also classified explicitly and coalesces onto that branch
refresh. Backfill, reconciliation, branch refresh, and webhook work all reach
the same GraphQL/REST hydration path.

`pull_request_review_requests` is the authoritative current request set for a
pull request. Live reads filter `tombstoned_at IS NULL` and distinguish users
from teams with `reviewer_kind`; `reviewer_gh_id` and `reviewer_node_id` remain
stable when `reviewer_login` changes. GitHub's `reviewRequests` connection does
not expose a request timestamp, so current GraphQL and REST observations write
`requested_at = NULL`. Consumers compute request age from
`COALESCE(requested_at, first_seen_at)`: `requested_at`, when non-null, is the
authoritative GitHub time; otherwise `first_seen_at` is only the time ghsync
first observed that uninterrupted current request. Identical refreshes preserve
`first_seen_at`; removal tombstones the row, and a later re-request starts a new
`first_seen_at`. This table is a current-state snapshot, not request history.

The v1 table intentionally represents only GraphQL `User` and `Team` union
members with complete database and node identities. `Bot`, `Mannequin`,
`EnterpriseTeam`, a null `requestedReviewer`, and future union variants are
excluded rather than mislabeled as users or teams. A consumer can list the
current supported requests for one open pull request with the partial live-set
index by using:

```sql
SELECT request.reviewer_kind,
       request.reviewer_gh_id,
       request.reviewer_node_id,
       request.reviewer_login,
       COALESCE(request.requested_at, request.first_seen_at) AS request_age_from,
       request.requested_at IS NOT NULL AS request_age_is_authoritative,
       request.head_sha
FROM pull_requests AS pull
JOIN pull_request_review_requests AS request
  ON request.repo_id = pull.repo_id
 AND request.pr_number = pull.number
WHERE pull.repo_id = $1
  AND pull.number = $2
  AND pull.state = 'open'
  AND pull.tombstoned_at IS NULL
  AND request.tombstoned_at IS NULL
ORDER BY request.reviewer_kind, request.reviewer_gh_id;
```

`pull_request_reviews` and `pull_request_comments` are identity-keyed fact
histories, not replace-set identities. `node_id` is the stable row identity;
`gh_id` carries GitHub's 64-bit `fullDatabaseId` when that identity is exposed;
the deprecated 32-bit `databaseId` is not used. Consumers must use `node_id`
as the stable cross-delivery identity because GitHub may omit the database ID.
A review's monotonic basis is lifecycle state plus timestamps: `dismissed` is
terminal for that review node, submitted rows outrank pending rows, and edits
within the same lifecycle advance on `submitted_at` and `gh_updated_at`. This
lets a dismissal replace an edited row even when GitHub leaves or regresses the
review's `updatedAt`, while preventing an older submitted snapshot from
resurrecting the review. A comment advances on GitHub `updatedAt`, stored as
`gh_updated_at`.

Every participation write is additionally fenced by the authoritative parent
PR observation. The complete connection is accepted only when its parent
`gh_updated_at` is at least as new as the cached PR. A delayed parent snapshot
therefore cannot insert, update, or tombstone child facts learned by a newer
observation. Within an accepted complete observation, absence is authoritative
and tombstones a missing node without comparing unrelated child and parent
timestamps. Dismissal is an update: the review remains live with
`state = 'dismissed'`; it is not tombstoned.

Review lifecycle values are normalized to lowercase. `pending` is an
unsubmitted draft and has no authoritative `submitted_at`. A submitted review
has a non-null `submitted_at` and normally has state `approved`,
`changes_requested`, or `commented`; `dismissed` means GitHub later invalidated
that already-submitted review while retaining its author, submission time, and
observed commit. Webhook action `submitted` is therefore a lifecycle action,
not a separate stored review state. Consumers should preserve unknown future
states as facts rather than classifying them.

Author policy is lossless across the two normalized tables. `author_kind` is
`user`, `bot`, `mannequin`, `organization`, `enterprise_user_account`,
`unknown`, or `deleted`. Non-user actors are retained because excluding them
would falsify participation. For a deleted author, `author_kind = 'deleted'`
and both identity columns are null. Other kinds retain GraphQL node ID and
login whenever GitHub supplies them; either identity column may be null when
upstream omits it. Ordinary issue-comment bodies are deliberately not stored.

To derive PR participants, union authorship, submitted review authors,
review-thread comment authors, and ordinary issue-comment authors. The query
below returns facts rather than a score or workflow classification; consumers
may group the result into their own participant set. Review-thread comments
are the existing embedded `review_threads.comments` facts, while ordinary PR
conversation is in `pull_request_comments`:

```sql
SELECT 'authored' AS participation, 'user' AS actor_kind,
       NULL::text AS actor_node_id, pull.author_login AS actor_login
FROM pull_requests AS pull
WHERE pull.repo_id = $1 AND pull.number = $2
  AND pull.tombstoned_at IS NULL
UNION ALL
SELECT 'reviewed', review.author_kind,
       review.author_node_id, review.author_login
FROM pull_request_reviews AS review
WHERE review.repo_id = $1 AND review.pr_number = $2
  AND review.tombstoned_at IS NULL
  AND review.submitted_at IS NOT NULL
UNION ALL
SELECT 'commented',
       CASE WHEN NULLIF(comment->>'author_login', '') IS NULL
            THEN 'deleted' ELSE 'user' END,
       NULL::text, NULLIF(comment->>'author_login', '')
FROM review_threads AS thread
CROSS JOIN LATERAL jsonb_array_elements(thread.comments) AS comment
WHERE thread.repo_id = $1 AND thread.pr_number = $2
  AND thread.tombstoned_at IS NULL
UNION ALL
SELECT 'commented', comment.author_kind,
       comment.author_node_id, comment.author_login
FROM pull_request_comments AS comment
WHERE comment.repo_id = $1 AND comment.pr_number = $2
  AND comment.tombstoned_at IS NULL;
```

When the parent PR closes, participation rows follow the existing
`pull_requests.display_until` display posture: they remain queryable facts,
while the parent controls whether the closed PR is still display-eligible.
Tombstoned participation skeletons have no v1 expiry, matching other mirror
rows; live reads always filter `tombstoned_at IS NULL`.

## Event-kind and entity-key grammar

This manifest is normative and schema-tested against the constants and key
constructors used by the entity writer and deriver.

<!-- v1-events:start -->
| Stream | Kind | Entity-key grammar | Public lookup target | Required payload |
| --- | --- | --- | --- | --- |
| `entities` | `repository.changed` | `repo:{installation_id}:{repo_gh_id}` | `repos(installation_id,gh_id)` | `{"version":1}` |
| `entities` | `repository.tombstoned` | `repo:{installation_id}:{repo_gh_id}` | `repos(installation_id,gh_id)` | `{"version":1}` |
| `entities` | `pull_request.changed` | `pr:{installation_id}:{repo_gh_id}:{pr_number}` | `pull_requests(repos.installation_id,repos.gh_id,number), pull_request_review_requests(repo_id,pr_number), pull_request_reviews(repo_id,pr_number), pull_request_comments(repo_id,pr_number), pull_request_change_snapshots(repo_id,pr_number), pull_request_changed_files(repo_id,pr_number), pull_request_file_owners(repo_id,pr_number)` | `{"version":1}` |
| `entities` | `pull_request.tombstoned` | `pr:{installation_id}:{repo_gh_id}:{pr_number}` | `pull_requests(repos.installation_id,repos.gh_id,number), pull_request_review_requests(repo_id,pr_number), pull_request_reviews(repo_id,pr_number), pull_request_comments(repo_id,pr_number), pull_request_change_snapshots(repo_id,pr_number), pull_request_changed_files(repo_id,pr_number), pull_request_file_owners(repo_id,pr_number)` | `{"version":1}` |
| `entities` | `stack.changed` | `stack:{installation_id}:{repo_gh_id}:{stack_number}` | `stacks(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `stack.tombstoned` | `stack:{installation_id}:{repo_gh_id}:{stack_number}` | `stacks(repos.installation_id,repos.gh_id,number)` | `{"version":1}` |
| `entities` | `checks.changed` | `checks:{installation_id}:{repo_gh_id}:{head_sha}` | `check_runs(repos.installation_id,repos.gh_id,head_sha)` | `{"version":1}` |
| `entities` | `repo_rules.changed` | `repo_rules:{installation_id}:{repo_gh_id}` | `repo_rules(repos.installation_id,repos.gh_id)` | `{"version":1}` |
| `work_items` | `work_item.changed` | `repo:{repo_gh_id}:{work_item_kind}:{number}` | `work_items(identity_key)` | `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}` |
| `work_items` | `work_item.removed` | `repo:{repo_gh_id}:{work_item_kind}:{number}` | `work_items(identity_key), absent after removal` | `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}` |
<!-- v1-events:end -->

Every braced numeric field is a positive base-10 integer with no delimiter.
`head_sha` is the authoritative Git object ID. `work_item_kind` is exactly
`stack` or `pr`. The owning scopes are
`stack:{installation_id}:{repo_gh_id}:{stack_number}` and
`pr:{installation_id}:{repo_gh_id}:{pr_number}`. A deriver must return exactly
one complete result set for every claimed scope; an empty result removes that
scope’s prior work item and emits `work_item.removed`.

Payloads are references, never row images or patches. The JSON shown above is
the complete required v1 shape; additive fields are allowed. Consumers fetch
current state from the lookup target, ignore unknown kinds and fields, and use
`seq` as the idempotency key for external effects.

### Internal key grammars and the shared advisory-lock namespace

Three key namespaces coexist. They are not interchangeable even where two
forms happen to contain the same text:

1. **Public v1 event/entity keys.** `internal/outbox` constructors, called by
   the entity writer and deriver, produce immutable GitHub-identity keys. The
   numbered cache-entity form is
   `{kind}:{installation_id}:{repo_gh_id}:{number}`; repository and repository
   rules omit the final number, while checks use `head_sha` there. The
   `v1-events` manifest above is the exact public grammar, including the
   separate work-item identity forms. These values are stable across repository
   rename.
2. **Internal refresh/job keys.** Dispatcher classification and
   fetch/backfill/sweep producers create path-addressed pointers consumed by
   `internal/queue` and parsed by `internal/fetch`:
   `{kind}:{owner}/{name}:{number_or_value}`. Concrete forms are
   `repo:{owner}/{name}:metadata`, `repo_rules:{owner}/{name}:rules`,
   `pr:{owner}/{name}:{number}`, `stack:{owner}/{name}:{number}`,
   `checks:{owner}/{name}:{head_sha}`, and
   `branch:{owner}/{name}:{branch}`. They are private, rename-sensitive work
   pointers, not public event identities.
3. **Drift-detector fetch and lock keys.** The SQL `drift_entities` view
   produces a path-addressed `entity_key` for the authoritative refresh and a
   separate immutable `lock_key` for serialization. Its fetch forms are
   `repo:{owner}/{name}:metadata`, `pr:{owner}/{name}:{number}`,
   `stack:{owner}/{name}:{number}`, `repo_rules:{owner}/{name}:rules`,
   `review_threads:{owner}/{name}:{number}`, and
   `checks:{owner}/{name}:{head_sha}`. Its lock forms are
   `repo:{installation_id}:{repo_gh_id}`,
   `repo_rules:{installation_id}:{repo_gh_id}`,
   `pr:{installation_id}:{repo_gh_id}:{number}` (also used for review
   threads), `stack:{installation_id}:{repo_gh_id}:{number}`, and
   `checks:{installation_id}:{repo_gh_id}:{head_sha}`.

All entity observation and transaction locks share one PostgreSQL advisory-lock
keyspace, derived with `hashtextextended(key, 0)`. Drift `lock_key` values and
the entity-writer keys therefore coordinate with each other.
`store.RepositoryDiscoveryKey` also mints
`repo-discovery:{installation_id}:{owner}/{name}` into this same keyspace while
an as-yet-unknown repository is fetched. Do not introduce a new producer or
change a grammar without checking every producer that participates in this
shared lock space.

When one operation must hold multiple distinct entity locks, it acquires their
complete key strings in ascending lexical order and releases them in reverse.
The two intentional nested paths are repository discovery followed by the
repository writer's transaction lock (`repo-discovery:...` then `repo:...`),
and the GraphQL PR coordinator's sorted PR observations followed by its short,
post-hydration repository apply (`pr:...` then `repo:...`). The coordinator
releases the repository observation before opening any PR writer transaction;
no production path acquires a PR observation while holding a repository
observation. Sweeps and backfills only enqueue those refreshes after their
network reads and never hold entity locks themselves.

## Writer fence and visibility watermark

Every transaction that inserts `change_events` has a mandatory obligation:
before allocating its first sequence, it calls the shared internal helper that
takes:

```sql
SELECT pg_advisory_xact_lock_shared(113698311597667);
```

The value is the ASCII bytes for `ghsync` interpreted as a signed `BIGINT`;
clients pass it as a parameter. All internal entity-writer and deriver paths use
`internal/outbox.AcquireWriterFence`. Migration `0002` adds a
`BEFORE INSERT` trigger that rejects any `change_events` write whose backend
does not already hold this shared fence, so the obligation is enforced by
Postgres rather than convention.

The watermarker briefly takes the exclusive transaction lock on that same key.
Once acquired, every earlier participating writer has committed or rolled
back, and no new writer can allocate a sequence. In that transaction the
watermarker publishes `max(change_events.seq)` and immediately commits,
releasing the fence. Unrelated transactions and read-only or Bootstrap
snapshots never take the fence and cannot stall watermark progress.
The exclusive attempt is a bounded global write barrier: a local
`lock_timeout` makes a busy fence a retryable, metered watermarker outcome,
and pooled connections set `idle_in_transaction_session_timeout` so an
abandoned writer cannot hold the shared side forever.

`change_events.outbox_txid` and
`stream_watermark.{candidate_seq,candidate_xid,lease_token,lease_until}` are
private compatibility/coordination columns. The candidate/XID columns are
retained for compatibility but are no longer the safety proof.

## Cursor paging and snapshot then stream

For `(consumer, stream)`, `consumer_cursors.seq` is the last event whose
handler effects committed. `resync_count` and `last_resync_at` are operational
evidence updated when the client detects a cursor below the pruned horizon;
they do not change cursor semantics. A page transaction uses `REPEATABLE READ`, locks
the cursor row, checks the pruned horizon, reads `safe_seq`, and selects:

```sql
SELECT seq, stream, kind, entity_key, occurred_at, payload
FROM change_events
WHERE stream = $stream
  AND seq > $cursor
  AND seq <= $safe_seq
ORDER BY seq
LIMIT $batch_size;
```

Horizon validation and the event selection must share that one snapshot so
retention cannot delete unseen events between them. Handler database effects
and cursor advancement commit in the same transaction. A rollback applies
neither. External I/O requires its own `seq`-keyed idempotency.

Bootstrap begins `REPEATABLE READ`, locks the cursor, reads watermark **W**,
replaces the consumer projection from public cache rows, sets the cursor to
**W**, and commits. Tailing resumes at `seq > W`. The snapshot can already
contain state referenced by a later event, so consumers apply by stable key,
not as patches.

**Bootstrap DISCARDS every undelivered event at or below the watermark for
that consumer.** The projection replacement and cursor reset must therefore
commit in the same snapshot transaction.

Product decision: server-side kind and entity-key filtering is deferred from
v1. Tail pages by `stream`; consumers apply any narrower filtering inside the
transactional handler.

`LISTEN ghsync_change_events` and its statement-level constant-payload
notification lower latency only. `pkg/streamclient` continues polling during a
listener failure and reconnects with bounded backoff.

## Retention and RESYNC_REQUIRED

Events are retained for at least seven days. Pruning is bounded, independent
of consumer cursors, and restricted to `seq <= stream_watermark.safe_seq`.
Each delete transaction advances the affected `stream_horizons` rows, so
`pruned_through_seq` can never exceed the safe watermark.

Before delivery:

- `cursor >= pruned_through_seq`: continue normally.
- `cursor < pruned_through_seq`: return `RESYNC_REQUIRED`.

There is no silent gap and no fallback to the oldest remaining row. On
`streamclient.ErrResyncRequired`, replace the projection through Bootstrap and
resume Tail.

## Evolution and private state

Public v1 evolution is additive: columns, streams, event kinds, and JSON fields
may be added; existing names, types, meanings, and identity grammars are not
changed or removed. A breaking change requires a versioned replacement and a
migration period.

All unlisted tables are private, including webhook, budget, queue, alias,
backfill, sweep, delivery-gap, drift, and `derivation_dirty` state, River
tables, and `schema_migrations`. Private state has no consumer compatibility
guarantee.
