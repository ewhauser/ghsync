package fakegithub

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/conformance"
	"github.com/ewhauser/ghsync/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

func TestNullBaseSHARoundTripsAcrossRESTGraphQLAndWebhooks(t *testing.T) {
	t.Parallel()
	fixture := DefaultFixture()
	fixture.PullRequests[0].Stack.Base.SHA = ""
	fixture.Stacks[0].Base.SHA = ""
	fake := New(fixture, "secret")

	pullResponse := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/pulls/4810",
		nil,
	)
	defer func() { _ = pullResponse.Body.Close() }()
	if pullResponse.StatusCode != http.StatusOK {
		t.Fatalf("REST pull status = %d", pullResponse.StatusCode)
	}
	var pullWire map[string]any
	if err := json.NewDecoder(pullResponse.Body).Decode(&pullWire); err != nil {
		t.Fatal(err)
	}
	stackWire, ok := pullWire["stack"].(map[string]any)
	if !ok {
		t.Fatalf("REST pull stack = %#v", pullWire["stack"])
	}
	stackBase, ok := stackWire["base"].(map[string]any)
	if !ok || stackBase["ref"] != "main" || stackBase["sha"] != nil {
		t.Fatalf("REST pull stack base = %#v", stackWire["base"])
	}

	stackResponse := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks/142",
		nil,
	)
	defer func() { _ = stackResponse.Body.Close() }()
	if stackResponse.StatusCode != http.StatusOK {
		t.Fatalf("REST stack status = %d", stackResponse.StatusCode)
	}
	var authoritativeStack map[string]any
	if err := json.NewDecoder(stackResponse.Body).Decode(
		&authoritativeStack,
	); err != nil {
		t.Fatal(err)
	}
	authoritativeBase, ok := authoritativeStack["base"].(map[string]any)
	if !ok || authoritativeBase["sha"] != nil {
		t.Fatalf("REST authoritative stack base = %#v", authoritativeStack["base"])
	}

	payload, err := fake.PullRequestWebhookPayload("closed", 4810)
	if err != nil {
		t.Fatal(err)
	}
	webhookPull, err := payloadObject(payload, "pull_request")
	if err != nil {
		t.Fatal(err)
	}
	webhookStack, ok := webhookPull["stack"].(map[string]any)
	if !ok {
		t.Fatalf("webhook stack = %#v", webhookPull["stack"])
	}
	webhookBase, ok := webhookStack["base"].(map[string]any)
	if !ok || webhookBase["sha"] != nil {
		t.Fatalf("webhook stack base = %#v", webhookStack["base"])
	}
	payloadBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := conformance.NewWebhookSchemaValidator().Validate(
		"pull_request",
		payloadBody,
	); err != nil {
		t.Fatalf("null stack base SHA webhook schema: %v", err)
	}

	fixture.PullRequests[0].Base.SHA = ""
	fake.SetFixture(fixture)
	pullResponse = serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/pulls/4810",
		nil,
	)
	defer func() { _ = pullResponse.Body.Close() }()
	pullWire = nil
	if err := json.NewDecoder(pullResponse.Body).Decode(&pullWire); err != nil {
		t.Fatal(err)
	}
	pullBase, ok := pullWire["base"].(map[string]any)
	if !ok || pullBase["ref"] != "main" || pullBase["sha"] != nil {
		t.Fatalf("REST pull base = %#v", pullWire["base"])
	}
	payload, err = fake.PullRequestWebhookPayload("closed", 4810)
	if err != nil {
		t.Fatal(err)
	}
	webhookPull, err = payloadObject(payload, "pull_request")
	if err != nil {
		t.Fatal(err)
	}
	webhookPullBase, ok := webhookPull["base"].(map[string]any)
	if !ok || webhookPullBase["sha"] != "" {
		t.Fatalf("webhook pull base = %#v", webhookPull["base"])
	}
	requestBody := bytes.NewBufferString(`{
		"query":"query($ids:[ID!]!){nodes(ids:$ids){... on PullRequest{baseRefOid}}}",
		"variables":{"ids":["PR_kwDOABCDEF4810"]}
	}`)
	graphQLResponse := serve(
		fake,
		http.MethodPost,
		"http://fake.test/graphql",
		requestBody,
	)
	defer func() { _ = graphQLResponse.Body.Close() }()
	if graphQLResponse.StatusCode != http.StatusOK {
		t.Fatalf("GraphQL status = %d", graphQLResponse.StatusCode)
	}
	var graphQLWire struct {
		Data struct {
			Nodes []map[string]any `json:"nodes"`
		} `json:"data"`
	}
	if err := json.NewDecoder(graphQLResponse.Body).Decode(
		&graphQLWire,
	); err != nil {
		t.Fatal(err)
	}
	if len(graphQLWire.Data.Nodes) != 1 ||
		graphQLWire.Data.Nodes[0]["baseRefOid"] != nil {
		t.Fatalf("GraphQL baseRefOid = %#v", graphQLWire.Data.Nodes)
	}
}

func TestHistoricalStackPositionRoundTripsAcrossRESTAndWebhook(t *testing.T) {
	t.Parallel()
	fixture := DefaultFixture()
	pull := &fixture.PullRequests[4]
	if pull.State != "open" || pull.Stack == nil {
		t.Fatalf("historical fixture PR = %+v, want open stacked PR", pull)
	}
	pull.Stack.Size = 2
	pull.Stack.Position = 5
	pull.Stack.Base.SHA = ""
	fake := New(fixture, "secret")

	response := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/pulls/4820",
		nil,
	)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("REST pull status = %d", response.StatusCode)
	}
	var wirePull map[string]any
	if err := json.NewDecoder(response.Body).Decode(&wirePull); err != nil {
		t.Fatal(err)
	}
	wireStack, ok := wirePull["stack"].(map[string]any)
	if !ok || wirePull["state"] != "open" ||
		wireStack["size"] != float64(2) ||
		wireStack["position"] != float64(5) {
		t.Fatalf("REST historical stack = %#v", wirePull["stack"])
	}
	wireBase, ok := wireStack["base"].(map[string]any)
	if !ok || wireBase["ref"] != "main" || wireBase["sha"] != nil {
		t.Fatalf("REST historical stack base = %#v", wireStack["base"])
	}

	payload, err := fake.PullRequestWebhookPayload("synchronize", pull.Number)
	if err != nil {
		t.Fatal(err)
	}
	webhookPull, err := payloadObject(payload, "pull_request")
	if err != nil {
		t.Fatal(err)
	}
	webhookStack, ok := webhookPull["stack"].(map[string]any)
	if !ok || webhookPull["state"] != "open" ||
		webhookStack["size"] != 2 || webhookStack["position"] != 5 {
		t.Fatalf("webhook historical stack = %#v", webhookPull["stack"])
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := conformance.NewWebhookSchemaValidator().Validate(
		"pull_request",
		body,
	); err != nil {
		t.Fatalf("historical stack webhook schema: %v", err)
	}
}

func TestControlEmitRecordsAndSignsLoopbackWebhook(t *testing.T) {
	received := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received <- r.Clone(r.Context())
			w.WriteHeader(http.StatusOK)
		},
	))
	defer target.Close()
	fakeServer := New(DefaultFixture(), "secret")
	fake := httptest.NewServer(fakeServer)
	defer fake.Close()
	payload, err := fakeServer.PullRequestWebhookPayload(
		"synchronize",
		4812,
	)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"target_url": target.URL,
		"event":      "pull_request",
		"guid":       "control-guid",
		"payload":    payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		fake.URL+ControlEmitPath,
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("control emit status = %d: %s", response.StatusCode, message)
	}
	select {
	case request := <-received:
		if request.Header.Get("X-GitHub-Delivery") != "control-guid" ||
			request.Header.Get("X-GitHub-Event") != "pull_request" ||
			request.Header.Get("X-Hub-Signature-256") == "" {
			t.Fatalf("emitted headers = %#v", request.Header)
		}
	case <-time.After(time.Second):
		t.Fatal("control emit did not deliver webhook")
	}
	if deliveries := fakeServer.Deliveries(); len(deliveries) != 1 ||
		deliveries[0].GUID != "control-guid" {
		t.Fatalf("recorded deliveries = %#v", deliveries)
	}
}

func TestControlTruthReportsFullMutatedPullState(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	fixture := DefaultFixture()
	pull := fixture.PullRequests[1]
	pull.Title = "Replay revision 77"
	pull.UpdatedAt = pull.UpdatedAt.Add(time.Minute)
	if err := fake.applyTruthMutation(truthPullMutation(fixture, pull)); err != nil {
		t.Fatal(err)
	}
	response := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+ControlTruthPath,
		nil,
	)
	defer func() { _ = response.Body.Close() }()
	var truth TruthSnapshot
	if err := json.NewDecoder(response.Body).Decode(&truth); err != nil {
		t.Fatal(err)
	}
	if len(truth.Repositories) != 1 ||
		truth.Repositories[0].Repository.FullName != "acme/monolith" ||
		len(truth.Repositories[0].PullRequests) != 5 ||
		truth.Repositories[0].PullRequests[1].Number != 4812 ||
		truth.Repositories[0].PullRequests[1].Title != "Replay revision 77" {
		t.Fatalf("truth = %+v", truth)
	}
}

func TestTruthMutationsCoverStacksChecksAndReviewThreads(t *testing.T) {
	now := time.Date(2026, 7, 30, 18, 0, 0, 0, time.UTC)
	repository := TruthRepository{
		ID:               7001,
		NodeID:           "R_truth",
		Owner:            "acme",
		Name:             "truth",
		DefaultBranch:    "main",
		DefaultBranchSHA: "base",
		UpdatedAt:        now,
	}
	fake := New(
		EmptyFixture(truthRepository(repository)),
		"secret",
	)
	mergedAt := now.Add(4 * time.Minute)
	pullStates := []TruthPullRequest{
		{
			ID:          8001,
			NodeID:      "PR_truth_1",
			Number:      1,
			Title:       "bottom",
			State:       "open",
			AuthorLogin: "author",
			Head:        TruthBranch{Ref: "bottom", SHA: "head-1"},
			Base:        TruthBranch{Ref: "main", SHA: "base"},
			CreatedAt:   now,
			UpdatedAt:   now.Add(time.Minute),
		},
		{
			ID:          8002,
			NodeID:      "PR_truth_2",
			Number:      2,
			Title:       "top",
			State:       "closed",
			Merged:      true,
			AuthorLogin: "author",
			Head:        TruthBranch{Ref: "top", SHA: "head-2"},
			Base:        TruthBranch{Ref: "bottom", SHA: "head-1"},
			CreatedAt:   now,
			UpdatedAt:   mergedAt,
			MergedAt:    &mergedAt,
		},
	}
	for index := range pullStates {
		mutation := TruthMutation{
			Kind:        "pull_request",
			Repository:  repository,
			PullRequest: &pullStates[index],
		}
		if err := fake.applyTruthMutation(mutation); err != nil {
			t.Fatal(err)
		}
	}
	line := 23
	pull := pullStates[0]
	pull.UpdatedAt = now.Add(5 * time.Minute)
	pullStates[0] = pull
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:        "review_thread",
		Repository:  repository,
		PullRequest: &pull,
		ReviewThread: &TruthReviewThread{
			ID:         "T_truth",
			IsResolved: true,
			Path:       "truth.go",
			Line:       &line,
			Comments: []TruthReviewComment{{
				ID:          9001,
				NodeID:      "C_truth",
				Body:        "resolved",
				Path:        "truth.go",
				Line:        &line,
				AuthorLogin: "reviewer",
				CreatedAt:   now,
				UpdatedAt:   now.Add(5 * time.Minute),
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:       "check_run",
		Repository: repository,
		CheckRun: &TruthCheckRun{
			ID:          10001,
			NodeID:      "CR_truth",
			HeadSHA:     "head-1",
			Name:        "unit",
			Status:      "completed",
			Conclusion:  "success",
			DetailsURL:  "https://example.test/check",
			AppSlug:     "actions",
			StartedAt:   &now,
			CompletedAt: &mergedAt,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:       "stack",
		Repository: repository,
		Stack: &TruthStack{
			ID:                11001,
			Number:            7,
			Base:              TruthBranch{Ref: "main", SHA: "base"},
			PullRequests:      []int{1, 2},
			PullRequestStates: pullStates,
		},
	}); err != nil {
		t.Fatal(err)
	}
	pullStates[1].Title = "top after stack"
	pullStates[1].UpdatedAt = now.Add(6 * time.Minute)
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:        "pull_request",
		Repository:  repository,
		PullRequest: &pullStates[1],
	}); err != nil {
		t.Fatal(err)
	}

	snapshot := snapshotFixture(fake.fixture)
	if len(snapshot.PullRequests) != 2 ||
		len(snapshot.Stacks) != 1 ||
		len(snapshot.CheckRuns) != 1 ||
		len(snapshot.ReviewThreads) != 1 {
		t.Fatalf("full truth snapshot = %+v", snapshot)
	}
	if snapshot.Stacks[0].PullRequests[1].MergedAt == nil ||
		!snapshot.Stacks[0].PullRequests[1].MergedAt.Equal(mergedAt) {
		t.Fatalf(
			"stack did not retain merged timestamp: %+v",
			snapshot.Stacks[0],
		)
	}
	if !snapshot.Stacks[0].UpdatedAt.Equal(pullStates[1].UpdatedAt) ||
		!snapshot.Stacks[0].PullRequests[1].UpdatedAt.Equal(
			pullStates[1].UpdatedAt,
		) {
		t.Fatalf(
			"post-stack pull mutation did not refresh stack truth: %+v",
			snapshot.Stacks[0],
		)
	}
	if snapshot.PullRequests[0].Stack == nil ||
		snapshot.PullRequests[0].Stack.Position != 1 ||
		snapshot.ReviewThreads[0].Comments[0].ID != "C_truth" {
		t.Fatalf("mutated fixture details = %+v", snapshot)
	}
}

func TestTruthMutationKindsMatchReplayCompilerAndRejectUnknowns(t *testing.T) {
	now := time.Date(2026, 7, 30, 20, 0, 0, 0, time.UTC)
	repository := TruthRepository{
		ID:               12001,
		NodeID:           "R_kinds",
		Owner:            "acme",
		Name:             "kinds",
		DefaultBranch:    "main",
		DefaultBranchSHA: "initial",
		UpdatedAt:        now,
	}
	pull := func(id int64, number int, head string) TruthPullRequest {
		return TruthPullRequest{
			ID:          id,
			NodeID:      fmt.Sprintf("PR_kinds_%d", number),
			Number:      number,
			Title:       fmt.Sprintf("pull %d", number),
			State:       "open",
			AuthorLogin: "author",
			Head:        TruthBranch{Ref: head, SHA: head + "-sha"},
			Base:        TruthBranch{Ref: "main", SHA: "initial"},
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}
	first := pull(13001, 1, "first")
	second := pull(13002, 2, "second")
	fixture := EmptyFixture(truthRepository(repository))
	fixture.PullRequests = []PullRequest{
		truthPullRequest(&first),
		truthPullRequest(&second),
	}
	fake := New(fixture, "secret")
	line := 9
	started := now.Add(time.Second)
	completed := now.Add(2 * time.Second)
	thread := TruthReviewThread{
		ID:   "T_kinds",
		Path: "kinds.go",
		Line: &line,
		Comments: []TruthReviewComment{{
			ID:          14001,
			NodeID:      "C_kinds",
			Body:        "before edit",
			Path:        "kinds.go",
			Line:        &line,
			AuthorLogin: "reviewer",
			CreatedAt:   now,
			UpdatedAt:   now,
		}},
	}
	mutations := []TruthMutation{
		{Kind: "repository", Repository: repository},
		{
			Kind:       "commit",
			Repository: repository,
			Commit: &TruthCommit{
				SHA:           "commit-head",
				Ref:           "refs/heads/main",
				CommittedAt:   now,
				DefaultBranch: true,
			},
		},
		{
			Kind:       "push",
			Repository: repository,
			Push: &TruthPush{
				Ref:           "refs/heads/main",
				Before:        "commit-head",
				After:         "push-head",
				DefaultBranch: true,
				PushedAt:      now.Add(time.Second),
			},
		},
		{Kind: "pull_request", Repository: repository, PullRequest: &first},
		{
			Kind:        "pull_request_review",
			Repository:  repository,
			PullRequest: &first,
			Review: &TruthReview{
				ID:          15001,
				NodeID:      "REV_kinds",
				State:       "approved",
				AuthorLogin: "reviewer",
				SubmittedAt: now,
			},
		},
		{
			Kind:        "issue_comment",
			Action:      "created",
			Repository:  repository,
			PullRequest: &first,
			IssueComment: &TruthIssueComment{
				ID:           15501,
				NodeID:       "IC_kinds",
				AuthorKind:   "bot",
				AuthorNodeID: "BOT_kinds",
				AuthorLogin:  "participant[bot]",
				Body:         "fixture-only body",
				CreatedAt:    now,
				UpdatedAt:    now.Add(time.Second),
			},
		},
		{
			Kind:         "review_thread",
			Repository:   repository,
			PullRequest:  &first,
			ReviewThread: &thread,
		},
		{
			Kind:        "review_comment",
			Repository:  repository,
			PullRequest: &first,
			ReviewComment: &TruthReviewComment{
				ID:          14001,
				NodeID:      "C_kinds",
				Body:        "after edit",
				Path:        "kinds.go",
				Line:        &line,
				AuthorLogin: "reviewer",
				CreatedAt:   now,
				UpdatedAt:   now.Add(time.Minute),
			},
		},
		{
			Kind:       "check_suite",
			Repository: repository,
			CheckSuite: &TruthCheckSuite{
				ID:        16001,
				NodeID:    "CS_kinds",
				HeadSHA:   first.Head.SHA,
				Status:    "queued",
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
		{
			Kind:       "check_run",
			Repository: repository,
			CheckRun: &TruthCheckRun{
				ID:          17001,
				NodeID:      "CR_kinds",
				HeadSHA:     first.Head.SHA,
				Name:        "unit",
				Status:      "completed",
				Conclusion:  "success",
				DetailsURL:  "https://example.test/check",
				StartedAt:   &started,
				CompletedAt: &completed,
			},
		},
		{
			Kind:       "stack",
			Repository: repository,
			Stack: &TruthStack{
				ID:                18001,
				Number:            3,
				Base:              TruthBranch{Ref: "main", SHA: "push-head"},
				PullRequests:      []int{1, 2},
				PullRequestStates: []TruthPullRequest{first, second},
			},
		},
	}
	for _, mutation := range mutations {
		if err := fake.applyTruthMutation(mutation); err != nil {
			t.Fatalf("apply %s mutation: %v", mutation.Kind, err)
		}
	}
	snapshot := snapshotFixture(fake.fixture)
	if snapshot.Repository.DefaultBranchSHA != "push-head" {
		t.Fatalf(
			"later mutations reset mutable repository truth to %q",
			snapshot.Repository.DefaultBranchSHA,
		)
	}
	if len(fake.fixture.Repositories) != 1 ||
		fake.fixture.Repositories[0].DefaultBranchSHA != "push-head" ||
		!fake.fixture.Repositories[0].PushedAt.Equal(now.Add(time.Second)) {
		t.Fatal("repository-list truth diverged from repository endpoint truth")
	}
	if got := snapshot.ReviewThreads[0].Comments[0].Body; got != "after edit" {
		t.Fatalf("review-comment mutation body = %q", got)
	}
	if len(snapshot.PullRequests[0].Reviews) != 1 ||
		len(snapshot.PullRequests[0].Comments) != 1 ||
		snapshot.PullRequests[0].Comments[0].Author.Kind != "bot" {
		t.Fatalf("participation mutation snapshot = %+v", snapshot.PullRequests[0])
	}
	before := cloneFixture(&fake.fixture)
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:       "future_compiler_kind",
		Repository: repository,
	}); err == nil {
		t.Fatal("unknown compiler mutation kind was silently accepted")
	}
	if !reflect.DeepEqual(before, fake.fixture) {
		t.Fatal("rejected mutation partially changed fixture truth")
	}
	additionalBefore := len(fake.additionalFixtures)
	unknownRepository := repository
	unknownRepository.ID++
	unknownRepository.Name = "unknown"
	if err := fake.applyTruthMutation(TruthMutation{
		Kind:       "stack",
		Repository: unknownRepository,
		Stack: &TruthStack{
			ID:           19001,
			Number:       4,
			PullRequests: []int{999},
		},
	}); err == nil {
		t.Fatal("invalid new-repository stack mutation was accepted")
	}
	if len(fake.additionalFixtures) != additionalBefore {
		t.Fatal("rejected mutation created a partial repository fixture")
	}
}

func TestControlEmitValidatesDeliveriesBeforeCommittingTruth(t *testing.T) {
	fixture := DefaultFixture()
	fake := New(fixture, "secret")
	pull := fixture.PullRequests[0]
	pull.Title = "must not commit"
	requestBody, err := json.Marshal(map[string]any{
		"target_url": "http://127.0.0.1:1/webhook",
		"mutation":   truthPullMutation(fixture, pull),
		"deliveries": []map[string]any{{
			"event":   "",
			"payload": json.RawMessage(`{"invalid":true}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := serve(
		fake,
		http.MethodPost,
		"http://fake.test"+ControlEmitPath,
		bytes.NewReader(requestBody),
	)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("control status = %d, want 400", response.StatusCode)
	}
	if fake.fixture.PullRequests[0].Title == "must not commit" {
		t.Fatal("invalid control delivery partially committed truth")
	}
}

func truthPullRequest(pull *TruthPullRequest) PullRequest {
	return PullRequest{
		ID:             pull.ID,
		NodeID:         pull.NodeID,
		Number:         pull.Number,
		Title:          pull.Title,
		State:          pull.State,
		Draft:          pull.Draft,
		AuthorLogin:    pull.AuthorLogin,
		ReviewDecision: pull.ReviewDecision,
		MergeableState: pull.MergeableState,
		Head: PullRequestBranch{
			Ref: pull.Head.Ref,
			SHA: pull.Head.SHA,
		},
		Base: Base{
			Ref: pull.Base.Ref,
			SHA: pull.Base.SHA,
		},
		CreatedAt: pull.CreatedAt,
		UpdatedAt: pull.UpdatedAt,
		MergedAt:  cloneTime(pull.MergedAt),
	}
}

func TestControlFaultBurstApplies500And429(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	body := bytes.NewReader([]byte(
		`{"internal_errors":1,"rate_limits":1,"retry_after":1000000000}`,
	))
	response := serve(
		fake,
		http.MethodPost,
		"http://fake.test"+ControlFaultPath,
		body,
	)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("control fault status = %d", response.StatusCode)
	}
	first := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks",
		nil,
	)
	_ = first.Body.Close()
	second := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks",
		nil,
	)
	_ = second.Body.Close()
	third := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks",
		nil,
	)
	_ = third.Body.Close()
	if first.StatusCode != http.StatusInternalServerError ||
		second.StatusCode != http.StatusTooManyRequests ||
		second.Header.Get("Retry-After") != "1" ||
		third.StatusCode != http.StatusOK {
		t.Fatalf(
			"fault statuses = %d/%d/%d Retry-After=%q",
			first.StatusCode,
			second.StatusCode,
			third.StatusCode,
			second.Header.Get("Retry-After"),
		)
	}
	truth := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+ControlTruthPath,
		nil,
	)
	defer func() {
		_ = truth.Body.Close()
	}()
	var snapshot TruthSnapshot
	if err := json.NewDecoder(truth.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Faults != (TruthFaultSnapshot{
		Configured500: 1,
		Configured429: 1,
		Applied500:    1,
		Applied429:    1,
	}) {
		t.Fatalf("fault snapshot = %+v", snapshot.Faults)
	}
}

func TestServesStacksWithRateHeaders(t *testing.T) {
	resp := serve(
		New(DefaultFixture(), "secret"),
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks",
		nil,
	)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("x-ratelimit-remaining") == "" {
		t.Fatal("missing rate-limit headers")
	}
	var stacks []Stack
	if err := json.NewDecoder(resp.Body).Decode(&stacks); err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 1 || stacks[0].Number != 142 || len(stacks[0].PullRequests) != 5 {
		t.Fatalf("unexpected stacks payload: %+v", stacks)
	}
}

func TestServesPullRequestParticipationRESTEndpoints(t *testing.T) {
	t.Parallel()
	fake := New(DefaultFixture(), "secret")
	for _, test := range []struct {
		path       string
		wantNodeID string
	}{
		{
			path:       "/repos/acme/monolith/pulls/4812/reviews",
			wantNodeID: "PRR_kwDOABCDEF8101",
		},
		{
			path:       "/repos/acme/monolith/issues/4812/comments",
			wantNodeID: "IC_kwDOABCDEF8201",
		},
	} {
		response := serve(
			fake,
			http.MethodGet,
			"http://fake.test"+test.path,
			nil,
		)
		var rows []map[string]any
		decodeErr := json.NewDecoder(response.Body).Decode(&rows)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			t.Fatalf(
				"GET %s status/decode = %d/%v",
				test.path,
				response.StatusCode,
				decodeErr,
			)
		}
		if len(rows) != 2 || rows[0]["node_id"] != test.wantNodeID {
			t.Fatalf("GET %s rows = %#v", test.path, rows)
		}
	}
}

func TestUnknownRepoIs404(t *testing.T) {
	resp := serve(
		New(DefaultFixture(), "secret"),
		http.MethodGet,
		"http://fake.test/repos/acme/other/stacks",
		nil,
	)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestAPIRequiresFakeInstallationBearer(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	for _, request := range []struct {
		method string
		target string
		body   io.Reader
	}{
		{
			method: http.MethodGet,
			target: "http://fake.test/repos/acme/monolith/stacks",
		},
		{
			method: http.MethodPost,
			target: "http://fake.test/graphql",
			body:   strings.NewReader(`{"query":"query { rateLimit { cost } }"}`),
		},
	} {
		response := serveAuthorized(
			fake,
			request.method,
			request.target,
			request.body,
			"Bearer wrong-token-kind",
		)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf(
				"%s %s status = %d, want 401",
				request.method,
				request.target,
				response.StatusCode,
			)
		}
	}
}

func TestAuthorizationRecordingIsBounded(t *testing.T) {
	server := New(DefaultFixture(), "secret")
	for index := range maxRecordedAuthorizations + 10 {
		server.recordAuthorization(strconv.Itoa(index))
	}
	got := server.Authorizations()
	if len(got) != maxRecordedAuthorizations {
		t.Fatalf(
			"recorded authorizations = %d, want %d",
			len(got),
			maxRecordedAuthorizations,
		)
	}
	if got[0] != "10" ||
		got[len(got)-1] != strconv.Itoa(maxRecordedAuthorizations+9) {
		t.Fatalf("bounded authorization window = %q ... %q", got[0], got[len(got)-1])
	}
}

func TestSinglePullETagChecksAndScripted404(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	path := "/repos/acme/monolith/pulls/4812"
	first := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	etag := first.Header.Get("ETag")
	_ = first.Body.Close()
	if first.StatusCode != http.StatusOK || etag == "" {
		t.Fatalf("first PR status=%d etag=%q", first.StatusCode, etag)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://fake.test"+path, http.NoBody)
	request.Header.Set("If-None-Match", etag)
	request.Header.Set("Authorization", "Bearer fake-installation-test")
	recorder := httptest.NewRecorder()
	fake.ServeHTTP(recorder, request)
	notModified := recorder.Result()
	_ = notModified.Body.Close()
	if notModified.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional PR status=%d, want 304", notModified.StatusCode)
	}

	checks := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/commits/8f31c2d/check-runs",
		nil,
	)
	defer func() { _ = checks.Body.Close() }()
	checksETag := checks.Header.Get("ETag")
	var payload struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	if err := json.NewDecoder(checks.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.CheckRuns) != 2 {
		t.Fatalf("check runs = %d, want 2", len(payload.CheckRuns))
	}
	checksPath := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://fake.test"+checksPath, http.NoBody)
	request.Header.Set("If-None-Match", checksETag)
	request.Header.Set("Authorization", "Bearer fake-installation-test")
	recorder = httptest.NewRecorder()
	fake.ServeHTTP(recorder, request)
	notModified = recorder.Result()
	_ = notModified.Body.Close()
	if notModified.StatusCode != http.StatusNotModified ||
		fake.NotModifiedCount(http.MethodGet, checksPath) != 1 {
		t.Fatalf(
			"conditional checks status=%d count=%d, want 304/1",
			notModified.StatusCode,
			fake.NotModifiedCount(http.MethodGet, checksPath),
		)
	}

	fake.ScriptNotFound(http.MethodGet, path, 1)
	missing := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	_ = missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("scripted status=%d, want 404", missing.StatusCode)
	}
	restored := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	_ = restored.Body.Close()
	if restored.StatusCode != http.StatusOK {
		t.Fatalf("post-script status=%d, want 200", restored.StatusCode)
	}
}

func TestPullsGoldenResponseDecodesThroughClientContract(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	resp := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/pulls?state=all",
		nil,
	)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/list_pulls.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(body), bytes.TrimSpace(golden)) {
		t.Fatalf("pull response does not match golden\n got: %s\nwant: %s", body, golden)
	}

	var extended []PullRequest
	if err := json.Unmarshal(body, &extended); err != nil {
		t.Fatalf("decode preview extension: %v", err)
	}
	if len(extended) < 2 {
		t.Fatalf("preview extension returned %d pulls, want at least 2", len(extended))
	}
	if extended[1].Stack == nil || extended[1].Stack.Number != 142 {
		t.Fatalf("stack preview extension lost: %+v", extended[1].Stack)
	}

	pulls, status, err := listPullRequests(
		context.Background(),
		"http://fake.test/repos/acme/monolith/pulls?state=all",
		&http.Client{Transport: handlerRoundTripper{handler: fake}},
	)
	if err != nil {
		t.Fatalf("client list pulls: %v", err)
	}
	if status != http.StatusOK || len(pulls) != 5 {
		t.Fatalf("client response status=%d pulls=%d", status, len(pulls))
	}
	if got := pulls[1].Head.Ref; got != "refactor/bm25f-ranker" {
		t.Fatalf("head ref = %q", got)
	}
	if got := pulls[1].Head.SHA; got != "8f31c2d" {
		t.Fatalf("head sha = %q", got)
	}
	if got := pulls[1].Base.Ref; got != "refactor/tokenizer" {
		t.Fatalf("base ref = %q", got)
	}
}

func TestPullListHonorsSortDirectionAndPagination(t *testing.T) {
	fixture := DefaultFixture()
	for index := range fixture.PullRequests {
		fixture.PullRequests[index].UpdatedAt = time.Date(
			2026,
			7,
			28,
			12,
			index,
			0,
			0,
			time.UTC,
		)
	}
	fake := New(fixture, "secret")
	readNumbers := func(target string) []int {
		t.Helper()
		resp := serve(fake, http.MethodGet, target, nil)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", target, resp.StatusCode)
		}
		var pulls []PullRequest
		if err := json.NewDecoder(resp.Body).Decode(&pulls); err != nil {
			t.Fatal(err)
		}
		numbers := make([]int, 0, len(pulls))
		for _, pull := range pulls {
			numbers = append(numbers, pull.Number)
		}
		return numbers
	}
	ascending := readNumbers(
		"http://fake.test/repos/acme/monolith/pulls" +
			"?state=all&sort=updated&direction=asc&per_page=2&page=1",
	)
	descending := readNumbers(
		"http://fake.test/repos/acme/monolith/pulls" +
			"?state=all&sort=updated&direction=desc&per_page=2&page=1",
	)
	if !reflect.DeepEqual(ascending, []int{4810, 4812}) {
		t.Fatalf("ascending first page = %v", ascending)
	}
	if !reflect.DeepEqual(descending, []int{4820, 4816}) {
		t.Fatalf("descending first page = %v", descending)
	}
}

func TestConcurrentListPullsAndTruthMutationUseIsolatedFixtureData(
	t *testing.T,
) {
	fixture := DefaultFixture()
	fake := New(fixture, "secret")
	start := make(chan struct{})
	errs := make(chan error, 9)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			<-start
			for range 250 {
				response := serve(
					fake,
					http.MethodGet,
					"http://fake.test/repos/acme/monolith/pulls?state=all",
					nil,
				)
				var pulls []PullRequest
				err := json.NewDecoder(response.Body).Decode(&pulls)
				_ = response.Body.Close()
				if err != nil {
					errs <- err
					return
				}
				if response.StatusCode != http.StatusOK || len(pulls) != 5 {
					errs <- fmt.Errorf(
						"list pulls status/count = %d/%d",
						response.StatusCode,
						len(pulls),
					)
					return
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for revision := range 2_000 {
			pull := fixture.PullRequests[1]
			pull.Title = fmt.Sprintf("Replay revision %d", revision)
			pull.UpdatedAt = pull.UpdatedAt.Add(
				time.Duration(revision) * time.Nanosecond,
			)
			if err := fake.applyTruthMutation(
				truthPullMutation(fixture, pull),
			); err != nil {
				errs <- err
				return
			}
		}
	})
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

//nolint:gocritic // Concurrent mutation tests intentionally copy detached fixture snapshots.
func truthPullMutation(
	fixture Fixture,
	pull PullRequest,
) TruthMutation {
	return TruthMutation{
		Kind: "pull_request",
		Repository: TruthRepository{
			ID:               fixture.Repository.ID,
			NodeID:           fixture.Repository.NodeID,
			Owner:            fixture.Repository.Owner,
			Name:             fixture.Repository.Name,
			DefaultBranch:    fixture.Repository.DefaultBranch,
			DefaultBranchSHA: fixture.Repository.DefaultBranchSHA,
			UpdatedAt:        fixture.Repository.UpdatedAt,
		},
		PullRequest: &TruthPullRequest{
			ID:             pull.ID,
			NodeID:         pull.NodeID,
			Number:         pull.Number,
			Title:          pull.Title,
			State:          pull.State,
			Draft:          pull.Draft,
			AuthorLogin:    pull.AuthorLogin,
			ReviewDecision: pull.ReviewDecision,
			MergeableState: pull.MergeableState,
			Head: TruthBranch{
				Ref: pull.Head.Ref,
				SHA: pull.Head.SHA,
			},
			Base: TruthBranch{
				Ref: pull.Base.Ref,
				SHA: pull.Base.SHA,
			},
			CreatedAt: pull.CreatedAt,
			UpdatedAt: pull.UpdatedAt,
		},
	}
}

func TestSeparateFixedWindowRateBudgets(t *testing.T) {
	fake := New(DefaultFixture(), "secret", WithRateLimits(2, 1))

	graphQL := func() (int, map[string]any, http.Header) {
		t.Helper()
		resp := serve(
			fake,
			http.MethodPost,
			"http://fake.test/graphql",
			bytes.NewReader(nil),
		)
		defer func() { _ = resp.Body.Close() }()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body, resp.Header
	}
	rest := func() (int, http.Header) {
		t.Helper()
		resp := serve(
			fake,
			http.MethodGet,
			"http://fake.test/repos/acme/monolith/stacks",
			nil,
		)
		_ = resp.Body.Close()
		return resp.StatusCode, resp.Header
	}

	status, body, headers := graphQL()
	if status != http.StatusOK || headers.Get("X-RateLimit-Resource") != "graphql" {
		t.Fatalf("GraphQL status=%d resource=%q", status, headers.Get("X-RateLimit-Resource"))
	}
	if fake.GraphQLRemaining() != 0 || fake.Remaining() != 2 {
		t.Fatalf(
			"budgets after GraphQL: graphql=%d rest=%d",
			fake.GraphQLRemaining(),
			fake.Remaining(),
		)
	}
	rateLimit := body["data"].(map[string]any)["rateLimit"].(map[string]any)
	if rateLimit["remaining"] != float64(0) || rateLimit["limit"] != float64(1) {
		t.Fatalf("GraphQL rateLimit block = %#v", rateLimit)
	}

	status, firstRESTHeaders := rest()
	if status != http.StatusOK ||
		firstRESTHeaders.Get("X-RateLimit-Resource") != "core" ||
		firstRESTHeaders.Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first REST status=%d headers=%v", status, firstRESTHeaders)
	}

	status, body, _ = graphQL()
	if status != http.StatusOK || body["errors"] == nil {
		t.Fatalf("exhausted GraphQL status=%d body=%#v", status, body)
	}

	status, secondRESTHeaders := rest()
	if status != http.StatusOK || secondRESTHeaders.Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("second REST status=%d headers=%v", status, secondRESTHeaders)
	}
	if firstRESTHeaders.Get("X-RateLimit-Reset") !=
		secondRESTHeaders.Get("X-RateLimit-Reset") {
		t.Fatal("REST reset time slid between requests")
	}

	status, exhaustedRESTHeaders := rest()
	if status != http.StatusForbidden ||
		exhaustedRESTHeaders.Get("Retry-After") != "" ||
		exhaustedRESTHeaders.Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("exhausted REST status=%d headers=%v", status, exhaustedRESTHeaders)
	}
}

func TestSecondaryLimitModelsHeaderAndHeaderlessForms(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	fake := New(
		DefaultFixture(),
		"secret",
		WithRESTRateSteps(
			RateLimitStep{
				Limit:      100,
				Remaining:  80,
				ResetAt:    reset,
				StatusCode: http.StatusForbidden,
				RetryAfter: time.Second,
				Secondary:  true,
			},
			RateLimitStep{
				Limit:      100,
				Remaining:  79,
				ResetAt:    reset,
				StatusCode: http.StatusTooManyRequests,
				Secondary:  true,
			},
		),
	)
	for index, wantRetryAfter := range []bool{true, false} {
		resp := serve(
			fake,
			http.MethodGet,
			"http://fake.test/repos/acme/monolith/stacks",
			nil,
		)
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Header.Get("Retry-After") != ""; got != wantRetryAfter {
			t.Fatalf("step %d Retry-After presence = %v", index, got)
		}
		if !strings.Contains(strings.ToLower(string(body)), "secondary rate limit") {
			t.Fatalf("step %d body is not a secondary-limit shape: %s", index, body)
		}
	}
}

func TestInstallationTokenEndpointValidatesJWTAndReturnsCreated(t *testing.T) {
	now := time.Now().UTC()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fake := New(
		DefaultFixture(),
		"secret",
		WithAppAuthentication(99, &key.PublicKey),
		WithNow(func() time.Time { return now }),
	)
	sign := func(issuer string, privateKey *rsa.PrivateKey) string {
		t.Helper()
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
			Issuer:    issuer,
			IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
		})
		signed, signErr := token.SignedString(privateKey)
		if signErr != nil {
			t.Fatal(signErr)
		}
		return signed
	}
	call := func(appJWT string) *http.Response {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(),
			http.MethodPost,
			"http://fake.test/app/installations/1234/access_tokens",
			strings.NewReader("{}"),
		)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Authorization", "Bearer "+appJWT)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		recorder := httptest.NewRecorder()
		fake.ServeHTTP(recorder, req)
		return recorder.Result()
	}

	valid := call(sign(strconv.FormatInt(99, 10), key))
	_ = valid.Body.Close()
	if valid.StatusCode != http.StatusCreated {
		t.Fatalf("valid token status = %d, want 201", valid.StatusCode)
	}
	if calls := fake.TokenRequests(); calls != 1 {
		t.Fatalf("token request count = %d, want 1", calls)
	}
	if got := fake.MaxConcurrent(); got != 1 {
		t.Fatalf("token endpoint max concurrency = %d, want 1", got)
	}

	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		"not.a.jwt",
		sign("100", key),
		sign("99", wrongKey),
	} {
		resp := call(invalid)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("invalid JWT status = %d, want 401", resp.StatusCode)
		}
	}
	if calls := fake.TokenRequests(); calls != 1 {
		t.Fatalf("invalid JWTs reached token issuance: %d", calls)
	}
}

func TestEmitWebhookSignsAndDelivers(t *testing.T) {
	fake := New(DefaultFixture(), "secret")

	type received struct {
		event, guid, sig string
		body             []byte
	}
	got := make(chan received, 1)
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		got <- received{
			event: r.Header.Get("X-GitHub-Event"),
			guid:  r.Header.Get("X-GitHub-Delivery"),
			sig:   r.Header.Get("X-Hub-Signature-256"),
			body:  body,
		}
		return emptyResponse(http.StatusAccepted), nil
	})}

	payload, err := fake.PullRequestWebhookPayload("synchronize", 4815)
	if err != nil {
		t.Fatal(err)
	}
	guid, err := fake.EmitWebhook(
		context.Background(),
		"http://webhook.test",
		"pull_request",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := <-got
	if rec.event != "pull_request" || rec.guid != guid {
		t.Fatalf("delivery headers wrong: %+v", rec)
	}
	if matched, err := regexp.MatchString(
		`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		guid,
	); err != nil || !matched {
		t.Fatalf("delivery GUID %q is not a UUIDv4", guid)
	}
	if !gh.VerifySignature([]byte("secret"), rec.body, rec.sig) {
		t.Fatal("signature does not verify")
	}
	if gh.VerifySignature([]byte("wrong"), rec.body, rec.sig) {
		t.Fatal("signature verified with wrong secret")
	}
}

func TestEmitWebhookWithGUIDOverride(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	got := make(chan string, 2)
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got <- r.Header.Get("X-GitHub-Delivery")
		return emptyResponse(http.StatusAccepted), nil
	})}

	const guid = "intentional-redelivery-guid"
	payload, err := fake.PullRequestWebhookPayload("synchronize", 4812)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		returned, err := fake.EmitWebhookWithGUID(
			context.Background(),
			"http://webhook.test",
			"pull_request",
			guid,
			payload,
		)
		if err != nil {
			t.Fatal(err)
		}
		if returned != guid {
			t.Fatalf("returned GUID = %q, want %q", returned, guid)
		}
	}
	if first, second := <-got, <-got; first != guid || second != guid {
		t.Fatalf("override headers = %q, %q", first, second)
	}
}

func TestEmitWebhookFailsOnNon2xx(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return emptyResponse(http.StatusServiceUnavailable), nil
	})}
	payload, err := fake.PushWebhookPayload(
		"refs/heads/main",
		"aaaa000",
		"bbbb000",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fake.EmitWebhook(
		context.Background(),
		"http://webhook.test",
		"push",
		payload,
	); err == nil {
		t.Fatal("expected error on 503 target")
	}
}

func TestAppHookDeliveriesPaginateAndRedeliver(t *testing.T) {
	now := time.Now().UTC()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	appJWT := signAppJWT(t, now, "99", key)
	var received atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received.Add(1)
			started <- struct{}{}
			<-release
			w.WriteHeader(http.StatusNoContent)
		},
	))
	defer target.Close()
	fake := New(
		DefaultFixture(),
		"secret",
		WithAppAuthentication(99, &key.PublicKey),
		WithNow(func() time.Time { return now }),
	)
	pushPayload, err := fake.PushWebhookPayload(
		"refs/heads/main",
		"aaaa000",
		"bbbb000",
	)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := fake.DropWebhook(
			target.URL,
			"push",
			pushPayload,
		); err != nil {
			t.Fatal(err)
		}
	}
	unauthorized := serve(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2",
		nil,
	)
	_ = unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf(
			"installation bearer on deliveries status = %d, want 401",
			unauthorized.StatusCode,
		)
	}
	first := serveAuthorized(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2",
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = first.Body.Close() }()
	link := first.Header.Get("Link")
	if first.StatusCode != http.StatusOK ||
		!strings.Contains(link, "cursor=") {
		t.Fatalf(
			"first page status/link = %d/%q",
			first.StatusCode,
			first.Header.Get("Link"),
		)
	}
	var deliveries []HookDelivery
	if err := json.NewDecoder(first.Body).Decode(&deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 || deliveries[0].ID <= deliveries[1].ID {
		t.Fatalf("first page deliveries = %+v", deliveries)
	}
	// A concurrent new delivery sorts ahead of page one. The opaque boundary
	// cursor must still resume after the last ID actually returned.
	if _, err := fake.DropWebhook(
		target.URL,
		"push",
		pushPayload,
	); err != nil {
		t.Fatal(err)
	}
	nextURL := strings.TrimSuffix(
		strings.TrimPrefix(strings.Split(link, ";")[0], "<"),
		">",
	)
	second := serveAuthorized(
		fake,
		http.MethodGet,
		nextURL,
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = second.Body.Close() }()
	var remaining []HookDelivery
	if err := json.NewDecoder(second.Body).Decode(&remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("second page deliveries = %+v", remaining)
	}
	redelivery := serveAuthorized(
		fake,
		http.MethodPost,
		fmt.Sprintf(
			"http://fake.test/app/hook/deliveries/%d/attempts",
			deliveries[0].ID,
		),
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = redelivery.Body.Close() }()
	if redelivery.StatusCode != http.StatusAccepted {
		t.Fatalf("redelivery status = %d", redelivery.StatusCode)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("asynchronous redelivery did not start")
	}
	if received.Load() != 1 || len(fake.RedeliveryRequests()) != 1 {
		t.Fatalf(
			"received=%d requests=%v deliveries=%+v",
			received.Load(),
			fake.RedeliveryRequests(),
			fake.Deliveries(),
		)
	}
	duplicate := serveAuthorized(
		fake,
		http.MethodPost,
		fmt.Sprintf(
			"http://fake.test/app/hook/deliveries/%d/attempts",
			deliveries[0].ID,
		),
		nil,
		"Bearer "+appJWT,
	)
	_ = duplicate.Body.Close()
	if duplicate.StatusCode != http.StatusAccepted ||
		received.Load() != 1 ||
		len(fake.RedeliveryRequests()) != 1 {
		t.Fatalf(
			"duplicate in-flight redelivery spawned work: "+
				"status=%d received=%d requests=%v",
			duplicate.StatusCode,
			received.Load(),
			fake.RedeliveryRequests(),
		)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fake.Deliveries()[0].Redelivery {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("redelivery was not recorded after asynchronous arrival")
}

// clientPullRequest is deliberately independent of the fake's response type.
// Its nested shape mirrors google/go-github's PullRequest and catches fixtures
// that would only decode through flat, fake-only fields.
type clientPullRequest struct {
	Number int                 `json:"number"`
	Head   clientPullReference `json:"head"`
	Base   clientPullReference `json:"base"`
}

type clientPullReference struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

func listPullRequests(
	ctx context.Context,
	endpoint string,
	client *http.Client,
) ([]clientPullRequest, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer fake-installation-test")
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var pulls []clientPullRequest
	if err := json.NewDecoder(resp.Body).Decode(&pulls); err != nil {
		return nil, resp.StatusCode, err
	}
	return pulls, resp.StatusCode, nil
}

func serve(handler http.Handler, method, target string, body io.Reader) *http.Response {
	return serveAuthorized(
		handler,
		method,
		target,
		body,
		"Bearer fake-installation-test",
	)
}

func serveAuthorized(
	handler http.Handler,
	method string,
	target string,
	body io.Reader,
	authorization string,
) *http.Response {
	req := httptest.NewRequestWithContext(
		context.Background(),
		method,
		target,
		body,
	)
	req.Header.Set("Authorization", authorization)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}

func signAppJWT(
	t *testing.T,
	now time.Time,
	issuer string,
	privateKey *rsa.PrivateKey,
) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer:    issuer,
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
	})
	signed, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

type handlerRoundTripper struct {
	handler http.Handler
}

func (rt handlerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	rt.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func emptyResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
	}
}
