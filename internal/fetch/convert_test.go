package fetch

import (
	"testing"
	"time"

	"github.com/google/go-github/v88/github"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store"
)

func TestReviewRequestConvertersExcludeUnsupportedOrIncompleteReviewers(
	t *testing.T,
) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	restPull := gh.PullRequest{PullRequest: &github.PullRequest{
		ID:        new(int64(42)),
		NodeID:    new("PR_42"),
		Number:    new(42),
		UpdatedAt: &github.Timestamp{Time: now},
		RequestedReviewers: []*github.User{
			{
				ID: new(int64(5001)), NodeID: new("U_5001"),
				Login: new("alice"), Type: new("User"),
			},
			{
				ID: new(int64(5002)), NodeID: new("B_5002"),
				Login: new("automation"), Type: new("Bot"),
			},
			{
				ID: new(int64(5003)), Login: new("missing-node"),
				Type: new("User"),
			},
		},
		RequestedTeams: []*github.Team{
			{
				ID: new(int64(6001)), NodeID: new("T_6001"),
				Slug: new("platform"),
			},
			{ID: new(int64(6002)), Slug: new("missing-node")},
		},
	}}
	repository := store.RepositoryRecord{GitHubID: 1001}
	restRecord := pullRecordFromREST(
		&repository,
		&restPull,
		"",
		store.SyncSourceBackfill,
		now,
	)
	assertSupportedReviewRequests(t, restRecord.ReviewRequests)
	if !restRecord.ReviewRequestsKnown {
		t.Fatal("REST detail request set was not marked authoritative")
	}

	listRecords := pullRecordsFromList(
		&repository,
		[]gh.PullRequest{restPull},
		nil,
		store.SyncSourceBackfill,
		now,
	)
	if len(listRecords) != 1 || listRecords[0].ReviewRequestsKnown ||
		listRecords[0].ReviewRequests != nil {
		t.Fatalf("list-page request authority = %+v", listRecords)
	}

	node := &gh.PullRequestNode{UpdatedAt: now}
	addNodeReviewer := func(
		typename string,
		id int64,
		nodeID string,
		login string,
		slug string,
	) {
		request := gh.ReviewRequestNode{}
		request.RequestedReviewer.Typename = typename
		request.RequestedReviewer.DatabaseID = id
		request.RequestedReviewer.ID = nodeID
		request.RequestedReviewer.Login = login
		request.RequestedReviewer.Slug = slug
		node.ReviewRequests.Nodes = append(node.ReviewRequests.Nodes, request)
	}
	addNodeReviewer("User", 5001, "U_5001", "alice", "")
	addNodeReviewer("Team", 6001, "T_6001", "", "platform")
	addNodeReviewer("Bot", 5002, "B_5002", "automation", "")
	addNodeReviewer("Mannequin", 5003, "M_5003", "former-user", "")
	addNodeReviewer("User", 0, "U_missing_database_id", "incomplete", "")
	addNodeReviewer("", 0, "", "", "") // null requestedReviewer
	graphQLRecord := pullRecordFromNode(
		node,
		&pullBatchItem{startedAt: now, source: store.SyncSourceBackfill},
		1,
		1,
	)
	assertSupportedReviewRequests(t, graphQLRecord.ReviewRequests)
}

func assertSupportedReviewRequests(
	t *testing.T,
	requests []store.ReviewRequestRecord,
) {
	t.Helper()
	if len(requests) != 2 ||
		requests[0].Kind != store.ReviewRequestUser ||
		requests[0].GitHubID != 5001 ||
		requests[1].Kind != store.ReviewRequestTeam ||
		requests[1].GitHubID != 6001 {
		t.Fatalf("supported review requests = %+v", requests)
	}
}
