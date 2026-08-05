package db_test

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestListCodeOwnerIdentitiesExcludesTombstonedParticipation(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	queries := dbgen.New(database.Pool)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repository := store.RepositoryRecord{
		InstallationID:  1,
		OrgID:           1,
		GitHubID:        35001,
		NodeID:          "repo-codeowner-identities",
		Owner:           "acme",
		Name:            "codeowner-identities",
		FullName:        "acme/codeowner-identities",
		DefaultBranch:   "main",
		DefaultHeadSHA:  "base-codeowner-identities",
		GitHubUpdatedAt: now,
	}
	pull := store.PullRequestRecord{
		Repository:          repository,
		GitHubID:            35002,
		NodeID:              "pr-codeowner-identities",
		Number:              35,
		Title:               "Exclude tombstoned CODEOWNER identities",
		State:               "open",
		AuthorLogin:         "author",
		HeadRef:             "codeowner-identity-index",
		HeadSHA:             "head-codeowner-identities",
		BaseRef:             "main",
		BaseSHA:             repository.DefaultHeadSHA,
		MembershipKnown:     true,
		GitHubUpdatedAt:     now,
		ReviewRequestsKnown: true,
		ReviewRequests: []store.ReviewRequestRecord{
			{
				Kind: store.ReviewRequestTeam, GitHubID: 35003,
				NodeID: "team-live", Login: "live-request-team",
			},
			{
				Kind: store.ReviewRequestUser, GitHubID: 35004,
				NodeID: "user-tombstoned", Login: "tombstoned-request",
			},
		},
		ReviewsKnown: true,
		Reviews: []store.PullRequestReviewRecord{
			{
				GitHubID: 35005, NodeID: "review-live",
				AuthorKind: "user", AuthorNodeID: "reviewer-live",
				AuthorLogin: "live-review", State: "approved",
				SubmittedAt: &now, GitHubUpdatedAt: now,
			},
			{
				GitHubID: 35006, NodeID: "review-tombstoned",
				AuthorKind: "user", AuthorNodeID: "reviewer-tombstoned",
				AuthorLogin: "tombstoned-review", State: "approved",
				SubmittedAt: &now, GitHubUpdatedAt: now,
			},
		},
		CommentsKnown: true,
		Comments: []store.PullRequestCommentRecord{
			{
				GitHubID: 35007, NodeID: "comment-live",
				AuthorKind: "user", AuthorNodeID: "commenter-live",
				AuthorLogin: "live-comment", CreatedAt: now,
				GitHubUpdatedAt: now,
			},
			{
				GitHubID: 35008, NodeID: "comment-tombstoned",
				AuthorKind: "user", AuthorNodeID: "commenter-tombstoned",
				AuthorLogin: "tombstoned-comment", CreatedAt: now,
				GitHubUpdatedAt: now,
			},
		},
		SyncedAt: now,
		Source:   store.SyncSourceReconcile,
	}
	if _, err := store.NewEntityWriter(database.Pool).ApplyPullRequest(
		t.Context(), pull,
	); err != nil {
		t.Fatalf("seed pull-request participation: %v", err)
	}
	repo, err := queries.GetRepoByGitHubID(t.Context(), repository.GitHubID)
	if err != nil {
		t.Fatalf("get seeded repository: %v", err)
	}

	before, err := queries.ListCodeOwnerIdentities(t.Context(), repo.ID)
	if err != nil {
		t.Fatalf("list identities before tombstoning: %v", err)
	}
	assertCodeOwnerIdentityLogins(t, before, []string{
		"team:live-request-team",
		"user:live-comment",
		"user:live-review",
		"user:tombstoned-comment",
		"user:tombstoned-request",
		"user:tombstoned-review",
	})

	for _, tombstone := range []struct {
		name  string
		query string
		key   string
	}{
		{
			name: "review request",
			query: `UPDATE pull_request_review_requests
			        SET tombstoned_at = $1 WHERE reviewer_node_id = $2`,
			key: "user-tombstoned",
		},
		{
			name: "review",
			query: `UPDATE pull_request_reviews
			        SET tombstoned_at = $1 WHERE node_id = $2`,
			key: "review-tombstoned",
		},
		{
			name: "comment",
			query: `UPDATE pull_request_comments
			        SET tombstoned_at = $1 WHERE node_id = $2`,
			key: "comment-tombstoned",
		},
	} {
		tag, err := database.Pool.Exec(t.Context(), tombstone.query, now, tombstone.key)
		if err != nil {
			t.Fatalf("tombstone %s: %v", tombstone.name, err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("tombstone %s affected %d rows, want 1", tombstone.name, tag.RowsAffected())
		}
	}

	after, err := queries.ListCodeOwnerIdentities(t.Context(), repo.ID)
	if err != nil {
		t.Fatalf("list identities after tombstoning: %v", err)
	}
	assertCodeOwnerIdentityLogins(t, after, []string{
		"team:live-request-team",
		"user:live-comment",
		"user:live-review",
	})

	resolved, err := store.NewEntityWriter(database.Pool).
		ResolveFileOwnerIdentities(
			t.Context(),
			repository.GitHubID,
			repository.Owner,
			[]store.FileOwnerRecord{
				{OwnerType: "user", OwnerName: "live-review"},
				{OwnerType: "user", OwnerName: "tombstoned-request"},
				{OwnerType: "user", OwnerName: "tombstoned-review"},
				{OwnerType: "user", OwnerName: "tombstoned-comment"},
			},
		)
	if err != nil {
		t.Fatalf("resolve file-owner identities: %v", err)
	}
	if len(resolved) != 4 {
		t.Fatalf("resolved file owners = %+v, want four", resolved)
	}
	if got := []string{
		resolved[0].ResolutionState,
		resolved[1].ResolutionState,
		resolved[2].ResolutionState,
		resolved[3].ResolutionState,
	}; strings.Join(got, ",") != "resolved,unresolved,unresolved,unresolved" {
		t.Fatalf("file-owner resolution states = %q", got)
	}

	rerequested := pull
	rerequested.ReviewsKnown = false
	rerequested.Reviews = nil
	rerequested.CommentsKnown = false
	rerequested.Comments = nil
	rerequested.SyncedAt = now.Add(time.Minute)
	if _, err := store.NewEntityWriter(database.Pool).ApplyPullRequest(
		t.Context(), rerequested,
	); err != nil {
		t.Fatalf("apply re-requested identity: %v", err)
	}
	afterRerequest, err := queries.ListCodeOwnerIdentities(t.Context(), repo.ID)
	if err != nil {
		t.Fatalf("list identities after re-request: %v", err)
	}
	assertCodeOwnerIdentityLogins(t, afterRerequest, []string{
		"team:live-request-team",
		"user:live-comment",
		"user:live-review",
		"user:tombstoned-request",
	})
}

func TestListCodeOwnerIdentitiesHasLiveRepositoryIndexes(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	tests := []struct {
		table         string
		index         string
		coveredFields []string
		branchQuery   string
	}{
		{
			table: "pull_request_review_requests",
			index: "pull_request_review_requests_live_pr_idx",
			coveredFields: []string{
				"reviewer_kind", "reviewer_gh_id", "reviewer_node_id",
				"reviewer_login", "last_checked_at",
			},
			branchQuery: `
				SELECT reviewer_kind, reviewer_gh_id, reviewer_node_id,
				       reviewer_login, last_checked_at
				FROM pull_request_review_requests
				WHERE repo_id = $1 AND tombstoned_at IS NULL
			`,
		},
		{
			table: "pull_request_reviews",
			index: "pull_request_reviews_live_pr_idx",
			coveredFields: []string{
				"author_kind", "author_node_id", "author_login",
				"last_checked_at",
			},
			branchQuery: `
				SELECT author_node_id, author_login, last_checked_at
				FROM pull_request_reviews
				WHERE repo_id = $1 AND tombstoned_at IS NULL
				  AND author_kind = 'user'
				  AND author_node_id IS NOT NULL
				  AND author_login IS NOT NULL
			`,
		},
		{
			table: "pull_request_comments",
			index: "pull_request_comments_live_pr_idx",
			coveredFields: []string{
				"author_kind", "author_node_id", "author_login",
				"last_checked_at",
			},
			branchQuery: `
				SELECT author_node_id, author_login, last_checked_at
				FROM pull_request_comments
				WHERE repo_id = $1 AND tombstoned_at IS NULL
				  AND author_kind = 'user'
				  AND author_node_id IS NOT NULL
				  AND author_login IS NOT NULL
			`,
		},
	}
	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			t.Parallel()
			var definition, predicate string
			if err := database.Pool.QueryRow(t.Context(), `
				SELECT pg_get_indexdef(indexes.indexrelid),
				       pg_get_expr(indexes.indpred, indexes.indrelid)
				FROM pg_index AS indexes
				JOIN pg_class AS tables ON tables.oid = indexes.indrelid
				JOIN pg_class AS names ON names.oid = indexes.indexrelid
				JOIN pg_namespace AS schemas ON schemas.oid = tables.relnamespace
				WHERE schemas.nspname = current_schema()
				  AND tables.relname = $1
				  AND names.relname = $2
			`, test.table, test.index).Scan(&definition, &predicate); err != nil {
				t.Fatalf("read %s definition: %v", test.index, err)
			}
			if !strings.Contains(definition, "USING btree (repo_id, pr_number") ||
				predicate != "(tombstoned_at IS NULL)" {
				t.Fatalf(
					"live repository index %s = %q with predicate %q",
					test.index, definition, predicate,
				)
			}
			for _, field := range test.coveredFields {
				if !strings.Contains(definition, field) {
					t.Errorf("live repository index %s does not cover %s: %q", test.index, field, definition)
				}
			}

			tx, err := database.Pool.Begin(t.Context())
			if err != nil {
				t.Fatalf("begin explain transaction: %v", err)
			}
			defer func() { _ = tx.Rollback(t.Context()) }()
			if _, err := tx.Exec(t.Context(), "SET LOCAL enable_seqscan = off"); err != nil {
				t.Fatalf("disable sequential scans: %v", err)
			}
			rows, err := tx.Query(
				t.Context(), "EXPLAIN (COSTS OFF) "+test.branchQuery, int64(1),
			)
			if err != nil {
				t.Fatalf("explain %s branch: %v", test.table, err)
			}
			var plan strings.Builder
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					rows.Close()
					t.Fatalf("scan %s plan: %v", test.table, err)
				}
				plan.WriteString(line)
				plan.WriteByte('\n')
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatalf("read %s plan: %v", test.table, err)
			}
			rows.Close()
			if !strings.Contains(plan.String(), test.index) {
				t.Fatalf("%s branch plan did not use %s:\n%s", test.table, test.index, plan.String())
			}
		})
	}
}

func assertCodeOwnerIdentityLogins(
	t *testing.T,
	identities []dbgen.ListCodeOwnerIdentitiesRow,
	want []string,
) {
	t.Helper()
	got := make([]string, 0, len(identities))
	for _, identity := range identities {
		got = append(got, identity.OwnerType+":"+identity.OwnerLogin)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("CODEOWNER identities = %q, want %q", got, want)
	}
}
