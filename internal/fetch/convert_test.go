package fetch

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-github/v88/github"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store"
)

func TestNullBaseSHAConvertersPreserveUnknown(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	repository := store.RepositoryRecord{GitHubID: 1001}
	var restPull gh.PullRequest
	if err := json.Unmarshal([]byte(`{
		"id":804810,
		"node_id":"PR_historical",
		"number":4810,
		"title":"Historical stack member",
		"state":"open",
		"user":{"login":"author"},
		"head":{"ref":"feature/historical","sha":"head-sha"},
		"base":{"ref":"deleted/pr-base","sha":null},
		"updated_at":"2026-08-02T12:00:00Z",
		"stack":{
			"id":9876543,
			"number":142,
			"size":2,
			"position":5,
			"base":{"ref":"deleted/stack-base","sha":null}
		}
	}`), &restPull); err != nil {
		t.Fatal(err)
	}
	restRecord := pullRecordFromREST(
		&repository,
		&restPull,
		`"rest-etag"`,
		store.SyncSourceWebhook,
		now,
	)
	if restRecord.BaseRef != "deleted/pr-base" || restRecord.BaseSHA != "" ||
		restRecord.StackSummary == nil ||
		restRecord.StackSummary.BaseRef != "deleted/stack-base" ||
		restRecord.StackSummary.BaseSHA != "" ||
		restRecord.StackSummary.Size != 2 ||
		restRecord.StackSummary.Position != 5 {
		t.Fatalf("REST null-SHA record = %+v", restRecord)
	}

	var restStack gh.Stack
	if err := json.Unmarshal([]byte(`{
		"id":9876543,
		"node_id":"S_historical",
		"number":142,
		"base":{"ref":"deleted/stack-base","sha":null},
		"open":false,
		"created_at":"2026-08-02T11:00:00Z",
		"updated_at":"2026-08-02T12:00:00Z",
		"pull_requests":[]
	}`), &restStack); err != nil {
		t.Fatal(err)
	}
	stackRecord := stackRecordFromREST(
		&repository,
		&restStack,
		`"stack-etag"`,
		store.SyncSourceWebhook,
		now,
	)
	if stackRecord.BaseRef != "deleted/stack-base" ||
		stackRecord.BaseSHA != "" {
		t.Fatalf("REST stack null-SHA record = %+v", stackRecord)
	}

	var node gh.PullRequestNode
	if err := json.Unmarshal([]byte(`{
		"id":"PR_historical",
		"databaseId":804810,
		"number":4810,
		"title":"Historical stack member",
		"state":"CLOSED",
		"updatedAt":"2026-08-02T12:00:00Z",
		"headRefName":"feature/historical",
		"headRefOid":"head-sha",
		"baseRefName":"deleted/pr-base",
		"baseRefOid":null
	}`), &node); err != nil {
		t.Fatal(err)
	}
	graphQLRecord := pullRecordFromNode(
		&node,
		&pullBatchItem{startedAt: now, source: store.SyncSourceBackfill},
		1,
		1,
	)
	if graphQLRecord.BaseRef != "deleted/pr-base" ||
		graphQLRecord.BaseSHA != "" {
		t.Fatalf("GraphQL null-SHA record = %+v", graphQLRecord)
	}
}

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

func TestActorIdentityRetainsEveryDocumentedKind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typename string
		kind     string
	}{
		{typename: "User", kind: "user"},
		{typename: "Bot", kind: "bot"},
		{typename: "Mannequin", kind: "mannequin"},
		{typename: "Organization", kind: "organization"},
		{
			typename: "EnterpriseUserAccount",
			kind:     "enterprise_user_account",
		},
		{typename: "FutureActor", kind: "unknown"},
	}
	for _, test := range tests {
		kind, nodeID, login := actorIdentity(&gh.ActorNode{
			Typename: test.typename,
			ID:       "actor-node",
			Login:    "actor-login",
		})
		if kind != test.kind || nodeID != "actor-node" ||
			login != "actor-login" {
			t.Fatalf(
				"%s actor identity = %q/%q/%q",
				test.typename,
				kind,
				nodeID,
				login,
			)
		}
	}
	if kind, nodeID, login := actorIdentity(nil); kind != "deleted" || nodeID != "" || login != "" {
		t.Fatalf("deleted actor identity = %q/%q/%q", kind, nodeID, login)
	}
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
