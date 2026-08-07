package drift

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/fetch"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

type findingObserver struct {
	mu         sync.Mutex
	findings   []dbgen.DriftFinding
	persistent []dbgen.DriftFinding
}

func (o *findingObserver) PersistentDivergence(
	_ context.Context,
	finding *dbgen.DriftFinding,
) {
	o.mu.Lock()
	o.persistent = append(o.persistent, *finding)
	o.mu.Unlock()
}

func (o *findingObserver) Divergence(
	_ context.Context,
	finding *dbgen.DriftFinding,
) {
	o.mu.Lock()
	o.findings = append(o.findings, *finding)
	o.mu.Unlock()
}

type driftHarness struct {
	pool        *pgxpool.Pool
	fake        *fakegithub.Server
	fixture     fakegithub.Fixture
	service     *Service
	handler     *fetch.Handler
	riverClient *river.Client[pgx.Tx]
}

func newReadyDriftHarness(t *testing.T) *driftHarness {
	return newReadyDriftHarnessWithProjections(
		t,
		pullRequestResponseProjections{},
	)
}

func newReadyDriftHarnessWithRESTProjection(
	t *testing.T,
	projection func(map[string]any),
) *driftHarness {
	return newReadyDriftHarnessWithProjections(
		t,
		pullRequestResponseProjections{rest: projection},
	)
}

type pullRequestResponseProjections struct {
	rest    func(map[string]any)
	graphQL func(map[string]any) bool
}

func newReadyDriftHarnessWithProjections(
	t *testing.T,
	projections pullRequestResponseProjections,
) *driftHarness {
	t.Helper()
	pool := driftTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake := fakegithub.New(fixture, "drift-secret")
	var githubHandler http.Handler = fake
	if projections.rest != nil || projections.graphQL != nil {
		githubHandler = &projectedPullRequestHandler{
			next:        fake,
			restPath:    "/repos/acme/monolith/pulls/4810",
			projections: projections,
		}
	}
	server := httptest.NewServer(githubHandler)
	t.Cleanup(server.Close)
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := fetch.New(&fetch.Options{
		Pool:           pool,
		REST:           rest,
		GraphQL:        graphQL,
		InstallationID: 1,
		OrgID:          1,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Options{
		Pool:    pool,
		REST:    rest,
		GraphQL: graphQL,
		Config: Config{
			InstallationID:     1,
			Period:             time.Hour,
			SampleSize:         100,
			PageSize:           100,
			ResolvedRetention:  30 * 24 * time.Hour,
			RetentionBatchSize: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithRefreshHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetRiverClient(riverClient)
	service.SetRiverClient(riverClient)
	runCtx, cancel := context.WithCancel(context.Background())
	if err := riverClient.Start(runCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	})
	ctx := context.Background()
	for _, request := range []func() error{
		func() error {
			return handler.ResolveStackMembership(
				ctx,
				queue.RefreshRequest{
					Args: queue.NewResolveStackMembershipArgs(
						"pr:acme/monolith:4812",
					).RefreshArgs,
					Queue: queue.QueueSweep,
				},
			)
		},
		func() error {
			return handler.RefreshPR(
				ctx,
				queue.RefreshRequest{
					Args: queue.NewRefreshPRArgs(
						"pr:acme/monolith:4812",
					).RefreshArgs,
					Queue: queue.QueueSweep,
				},
			)
		},
		func() error {
			return handler.RefreshStack(
				ctx,
				queue.RefreshRequest{
					Args: queue.NewRefreshStackArgs(
						"stack:acme/monolith:142",
					).RefreshArgs,
					Queue: queue.QueueSweep,
				},
			)
		},
		func() error {
			return handler.RefreshChecks(
				ctx,
				queue.RefreshRequest{
					Args: queue.NewRefreshChecksArgs(
						"checks:acme/monolith:8f31c2d",
					).RefreshArgs,
					Queue: queue.QueueSweep,
				},
			)
		},
		func() error {
			return handler.RefreshRepoRules(
				ctx,
				queue.RefreshRequest{
					Args: queue.NewRefreshRepoRulesArgs(
						"repo_rules:acme/monolith:rules",
					).RefreshArgs,
					Queue: queue.QueueSweep,
				},
			)
		},
	} {
		if err := request(); err != nil {
			t.Fatal(err)
		}
	}
	for index := range fixture.PullRequests {
		pull := &fixture.PullRequests[index]
		if err := handler.RefreshPR(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
				).RefreshArgs,
				Queue: queue.QueueSweep,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	waitForCacheProducers(t, pool)
	if _, err := pool.Exec(ctx, `
		INSERT INTO installation_backfill_cursors (
		    installation_id, phase, page, completed_at
		) VALUES (1, 'done', 1, clock_timestamp())
		ON CONFLICT (installation_id) DO NOTHING
	`); err != nil {
		t.Fatal(err)
	}
	return &driftHarness{
		pool:        pool,
		fake:        fake,
		fixture:     fixture,
		service:     service,
		handler:     handler,
		riverClient: riverClient,
	}
}

type projectedPullRequestHandler struct {
	next        http.Handler
	restPath    string
	projections pullRequestResponseProjections
}

func (h *projectedPullRequestHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	isREST := h.projections.rest != nil &&
		r.Method == http.MethodGet && r.URL.Path == h.restPath
	isGraphQL := h.projections.graphQL != nil &&
		r.Method == http.MethodPost && r.URL.Path == "/graphql"
	if !isREST && !isGraphQL {
		h.next.ServeHTTP(w, r)
		return
	}
	recorder := httptest.NewRecorder()
	h.next.ServeHTTP(recorder, r)
	for name, values := range recorder.Header() {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	if recorder.Code != http.StatusOK {
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mutated := false
	if isREST {
		h.projections.rest(payload)
		mutated = true
	} else {
		mutated = h.projections.graphQL(payload)
	}
	if !mutated {
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
		return
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The projected representation no longer has the fake's entity tag. Do
	// not let a conditional response hide the exact source shape under test.
	w.Header().Del("Content-Length")
	w.Header().Del("Etag")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(encoded)
}

func (h *driftHarness) divergePullRequest() {
	h.fixture.PullRequests[1].Title = "mutated behind cache"
	h.fixture.PullRequests[1].UpdatedAt = h.fixture.PullRequests[1].
		UpdatedAt.Add(time.Minute)
	h.fake.SetFixture(h.fixture)
}

func TestDriftTreatsUnknownBaseSHAAsConvergedTruth(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_requests
		SET base_sha = ''
		FROM repos
		WHERE repos.id = pull_requests.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND pull_requests.number = 4812;

		UPDATE stacks
		SET base_sha = ''
		FROM repos
		WHERE repos.id = stacks.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND stacks.number = 142
	`); err != nil {
		t.Fatal(err)
	}
	harness.fixture.PullRequests[1].Base.SHA = ""
	if harness.fixture.PullRequests[1].Stack == nil {
		t.Fatal("drift fixture PR has no stack summary")
	}
	harness.fixture.PullRequests[1].Stack.Base.SHA = ""
	harness.fixture.Stacks[0].Base.SHA = ""
	harness.fake.SetFixture(harness.fixture)
	if err := harness.handler.RefreshPR(ctx, queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("unknown base SHA produced unhealable drift: %+v", findings)
	}
}

func TestDriftTreatsHistoricalStackPositionAsConvergedTruth(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	pull := &harness.fixture.PullRequests[0]
	if pull.Stack == nil {
		t.Fatal("drift fixture PR has no stack summary")
	}
	pull.Stack.Size = 2
	pull.Stack.Position = 5
	harness.fake.SetFixture(harness.fixture)
	if err := harness.handler.ResolveStackMembership(ctx, queue.RefreshRequest{
		Args: queue.NewResolveStackMembershipArgs(
			"pr:acme/monolith:4810",
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("historical stack position produced drift: %+v", findings)
	}
}

func TestDriftTreatsRESTAndGraphQLPullRequestValuesAsConverged(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarnessWithRESTProjection(
		t,
		func(payload map[string]any) {
			payload["state"] = "closed"
			payload["merged"] = true
			payload["mergeable_state"] = "clean"
			delete(payload, "review_decision")
		},
	)
	ctx := t.Context()
	pull := &harness.fixture.PullRequests[0]
	pull.State = "merged"
	pull.ReviewDecision = "APPROVED"
	pull.MergeableState = "MERGEABLE"
	pull.UpdatedAt = pull.UpdatedAt.Add(time.Minute)
	harness.fake.SetFixture(harness.fixture)
	if err := harness.handler.RefreshPR(ctx, queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4810",
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}
	waitForCacheProducers(t, harness.pool)

	var state, reviewDecision, mergeability string
	if err := harness.pool.QueryRow(ctx, `
		SELECT pull.state, pull.review_decision, pull.mergeable_state
		FROM pull_requests AS pull
		JOIN repos ON repos.id = pull.repo_id
		WHERE repos.full_name = 'acme/monolith'
		  AND pull.number = 4810
	`).Scan(&state, &reviewDecision, &mergeability); err != nil {
		t.Fatal(err)
	}
	if state != "merged" || reviewDecision != "APPROVED" ||
		mergeability != "MERGEABLE" {
		t.Fatalf(
			"GraphQL cache state/review/mergeability = %q/%q/%q",
			state,
			reviewDecision,
			mergeability,
		)
	}
	var generationBefore int64
	if err := harness.pool.QueryRow(ctx, `
		SELECT COALESCE((
			SELECT generation
			FROM refresh_intent_generations
			WHERE kind = 'refresh_pr'
			  AND refresh_key = 'pr:acme/monolith:4810'
		), 0)
	`).Scan(&generationBefore); err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("semantically equivalent PR produced drift: %+v", findings)
	}
	var findingCount int
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM drift_findings
		WHERE entity_kind = 'pull_request'
		  AND entity_key = 'pr:acme/monolith:4810'
	`).Scan(&findingCount); err != nil {
		t.Fatal(err)
	}
	if findingCount != 0 {
		t.Fatalf("semantically equivalent PR findings = %d, want 0", findingCount)
	}
	var generationAfter int64
	if err := harness.pool.QueryRow(ctx, `
		SELECT COALESCE((
			SELECT generation
			FROM refresh_intent_generations
			WHERE kind = 'refresh_pr'
			  AND refresh_key = 'pr:acme/monolith:4810'
		), 0)
	`).Scan(&generationAfter); err != nil {
		t.Fatal(err)
	}
	if generationAfter != generationBefore {
		t.Fatalf(
			"semantically equivalent PR heal generation = %d -> %d",
			generationBefore,
			generationAfter,
		)
	}
}

func TestDriftUsesPresentRESTDecisionWhenGraphQLDecisionIsAbsent(
	t *testing.T,
) {
	t.Parallel()
	var omitNextTargetDecision atomic.Bool
	harness := newReadyDriftHarnessWithProjections(
		t,
		pullRequestResponseProjections{
			graphQL: func(payload map[string]any) bool {
				if !omitNextTargetDecision.Load() {
					return false
				}
				data, ok := payload["data"].(map[string]any)
				if !ok {
					return false
				}
				nodes, ok := data["nodes"].([]any)
				if !ok {
					return false
				}
				for _, value := range nodes {
					node, ok := value.(map[string]any)
					if !ok {
						continue
					}
					number, ok := node["number"].(float64)
					if !ok || int(number) != 4812 {
						continue
					}
					if !omitNextTargetDecision.CompareAndSwap(true, false) {
						return false
					}
					delete(node, "reviewDecision")
					return true
				}
				return false
			},
		},
	)
	ctx := t.Context()
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_requests AS pull
		SET review_decision = 'APPROVED'
		FROM repos
		WHERE repos.id = pull.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND pull.number = 4812
	`); err != nil {
		t.Fatal(err)
	}

	// The first target GraphQL response belongs to drift fullFetch. The real
	// refresh_pr heal sees the fixture's subsequent CHANGES_REQUESTED value.
	omitNextTargetDecision.Store(true)
	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if omitNextTargetDecision.Load() {
		t.Fatal("target GraphQL review-decision projection was not exercised")
	}
	if len(findings) != 1 ||
		findings[0].EntityKey != "pr:acme/monolith:4812" ||
		!strings.Contains(string(findings[0].Diff), "review_decision") {
		t.Fatalf("REST review-decision drift findings = %+v", findings)
	}
	var upstream map[string]any
	if err := json.Unmarshal(findings[0].UpstreamSnapshot, &upstream); err != nil {
		t.Fatal(err)
	}
	if upstream["review_decision"] != "CHANGES_REQUESTED" {
		t.Fatalf(
			"fullFetch review_decision = %v, want REST CHANGES_REQUESTED",
			upstream["review_decision"],
		)
	}

	waitForCacheProducers(t, harness.pool)
	var healed string
	if err := harness.pool.QueryRow(ctx, `
		SELECT pull.review_decision
		FROM pull_requests AS pull
		JOIN repos ON repos.id = pull.repo_id
		WHERE repos.full_name = 'acme/monolith'
		  AND pull.number = 4812
	`).Scan(&healed); err != nil {
		t.Fatal(err)
	}
	if healed != "CHANGES_REQUESTED" {
		t.Fatalf("healed review_decision = %q", healed)
	}
	if findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	} else if len(findings) != 0 {
		t.Fatalf("post-heal REST review-decision findings = %+v", findings)
	}
}

func TestDriftDetectsAndHealsCanonicalPullRequestFieldDivergence(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name       string
		field      string
		corrupt    string
		wantHealed string
	}{
		{name: "state", field: "state", corrupt: "closed", wantHealed: "open"},
		{
			name:       "review_decision",
			field:      "review_decision",
			corrupt:    "APPROVED",
			wantHealed: "CHANGES_REQUESTED",
		},
		{
			name:       "mergeable_state",
			field:      "mergeable_state",
			corrupt:    "MERGEABLE",
			wantHealed: "CONFLICTING",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			harness := newReadyDriftHarness(t)
			ctx := t.Context()
			if _, err := harness.pool.Exec(ctx, `
				UPDATE pull_requests AS pull
				SET `+test.field+` = $1
				FROM repos
				WHERE repos.id = pull.repo_id
				  AND repos.full_name = 'acme/monolith'
				  AND pull.number = 4812
			`, test.corrupt); err != nil {
				t.Fatal(err)
			}

			findings, err := harness.service.Detect(ctx, DetectArgs{
				InstallationID: 1,
				SampleSize:     100,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(findings) != 1 ||
				findings[0].EntityKey != "pr:acme/monolith:4812" ||
				!strings.Contains(string(findings[0].Diff), test.field) {
				t.Fatalf("%s drift findings = %+v", test.field, findings)
			}
			waitForCacheProducers(t, harness.pool)

			var healed string
			if err := harness.pool.QueryRow(ctx, `
				SELECT `+test.field+`
				FROM pull_requests AS pull
				JOIN repos ON repos.id = pull.repo_id
				WHERE repos.full_name = 'acme/monolith'
				  AND pull.number = 4812
			`).Scan(&healed); err != nil {
				t.Fatal(err)
			}
			if healed != test.wantHealed {
				t.Fatalf("healed %s = %q, want %q", test.field, healed, test.wantHealed)
			}
			if findings, err := harness.service.Detect(ctx, DetectArgs{
				InstallationID: 1,
				SampleSize:     100,
			}); err != nil {
				t.Fatal(err)
			} else if len(findings) != 0 {
				t.Fatalf("post-heal %s findings = %+v", test.field, findings)
			}
		})
	}
}

func TestDriftDetectsAndHealsReviewRequestSetDivergence(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	// Use an identity that exists only in the review-request set and is not a
	// CODEOWNER. Tombstoning the fixture's original user request deliberately
	// changes its repository-wide ownership identity (reviews preserve the
	// node ID and login, but not the request's numeric GitHub ID), which would
	// turn this focused review-request test into a change-input drift test too.
	harness.fixture.PullRequests[1].ReviewRequests = append(
		harness.fixture.PullRequests[1].ReviewRequests,
		fakegithub.ReviewRequest{
			Kind: "user", ID: 5002, NodeID: "U_kwDOABCDEF5002",
			Login: "review-request-only",
		},
	)
	harness.fake.SetFixture(harness.fixture)
	if err := harness.handler.RefreshPR(ctx, queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_request_review_requests AS request
		SET tombstoned_at = clock_timestamp()
		FROM repos
		WHERE repos.id = request.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND request.pr_number = 4812
		  AND request.reviewer_gh_id = 5002
		  AND request.tombstoned_at IS NULL
	`); err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 ||
		findings[0].EntityKey != "pr:acme/monolith:4812" ||
		!strings.Contains(string(findings[0].Diff), "review_requests") {
		t.Fatalf("review-request drift findings = %+v", findings)
	}
	waitForCacheProducers(t, harness.pool)
	var live int
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pull_request_review_requests AS request
		JOIN repos ON repos.id = request.repo_id
		WHERE repos.full_name = 'acme/monolith'
		  AND request.pr_number = 4812
		  AND request.tombstoned_at IS NULL
	`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 3 {
		t.Fatalf("healed live review requests = %d, want 3", live)
	}
	if findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	} else if len(findings) != 0 {
		t.Fatalf("post-heal review-request findings = %+v", findings)
	}
}

func TestDriftDetectsAndHealsParticipationDivergence(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_request_reviews
		SET state = 'approved'
		WHERE node_id = 'PRR_kwDOABCDEF8101';
		UPDATE pull_request_comments
		SET author_kind = 'user', author_login = 'wrong-author'
		WHERE node_id = 'IC_kwDOABCDEF8201'
	`); err != nil {
		t.Fatal(err)
	}
	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 ||
		findings[0].EntityKey != "pr:acme/monolith:4812" ||
		(!strings.Contains(string(findings[0].Diff), "reviews") &&
			!strings.Contains(string(findings[0].Diff), "comments")) {
		t.Fatalf("participation drift findings = %+v", findings)
	}
	waitForCacheProducers(t, harness.pool)
	var state, authorKind, authorLogin string
	if err := harness.pool.QueryRow(ctx, `
		SELECT state FROM pull_request_reviews
		WHERE node_id = 'PRR_kwDOABCDEF8101'
	`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := harness.pool.QueryRow(ctx, `
		SELECT author_kind, author_login FROM pull_request_comments
		WHERE node_id = 'IC_kwDOABCDEF8201'
	`).Scan(&authorKind, &authorLogin); err != nil {
		t.Fatal(err)
	}
	if state != "changes_requested" || authorKind != "mannequin" ||
		authorLogin != "legacy-contributor" {
		t.Fatalf(
			"healed participation = state %q author %q/%q",
			state,
			authorKind,
			authorLogin,
		)
	}
	var deletedKind string
	var deletedIdentityIsNull bool
	var snapshot []byte
	if err := harness.pool.QueryRow(ctx, `
		SELECT author_kind,
		       author_node_id IS NULL AND author_login IS NULL
		FROM pull_request_comments
		WHERE node_id = 'IC_kwDOABCDEF8202'
	`).Scan(&deletedKind, &deletedIdentityIsNull); err != nil {
		t.Fatal(err)
	}
	if deletedKind != "deleted" || !deletedIdentityIsNull {
		t.Fatalf(
			"deleted-author cache row = %q/null:%v",
			deletedKind,
			deletedIdentityIsNull,
		)
	}
	if err := harness.pool.QueryRow(ctx, `
		SELECT cache_snapshot
		FROM drift_entities
		WHERE entity_kind = 'pull_request'
		  AND entity_key = 'pr:acme/monolith:4812'
	`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(snapshot), `"author_kind": "deleted"`) ||
		strings.Contains(string(snapshot), `"body"`) {
		t.Fatalf("participation drift snapshot = %s", snapshot)
	}
	findings, err = harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("deleted-author participation did not converge: %+v", findings)
	}
}

func TestDriftDetectsAndHealsChangeInputDivergence(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	const corruptSource = "* @corrupt-owner\n"
	corruptHash := fmt.Sprintf("%x", sha256.Sum256([]byte(
		gh.CodeownersPresent+"\x00.github/CODEOWNERS\x00"+corruptSource,
	)))
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_request_change_snapshots AS snapshot
		SET files_truncated = true,
		    codeowners_source = $1,
		    codeowners_hash = $2
		FROM repos
		WHERE repos.id = snapshot.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND snapshot.pr_number = 4812;
	`, corruptSource, corruptHash); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.pool.Exec(ctx, `
		UPDATE pull_request_changed_files AS file
		SET change_type = 'added'
		FROM repos
		WHERE repos.id = file.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND file.pr_number = 4812
		  AND file.path = 'internal/ranker.go';

		UPDATE pull_request_file_owners AS owner
		SET resolution_state = 'unresolved', owner_gh_id = NULL,
		    owner_node_id = NULL, owner_login = NULL
		FROM repos
		WHERE repos.id = owner.repo_id
		  AND repos.full_name = 'acme/monolith'
		  AND owner.pr_number = 4812
		  AND owner.owner_token = '@acme/search-platform'
	`); err != nil {
		t.Fatal(err)
	}
	codeownersPath := "/repos/acme/monolith/contents/.github/CODEOWNERS"
	beforeRequests := len(harness.fake.Requests())
	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 ||
		findings[0].EntityKey != "pr:acme/monolith:4812" ||
		!strings.Contains(string(findings[0].Diff), "change_inputs") {
		t.Fatalf("change-input drift findings = %+v", findings)
	}
	waitForCacheProducers(t, harness.pool)
	var ownershipFetches int
	for _, request := range harness.fake.Requests()[beforeRequests:] {
		if request.Method == "GET" && request.Path == codeownersPath &&
			request.RawQuery == "ref=bbbb001" {
			ownershipFetches++
		}
	}
	if ownershipFetches != 2 {
		t.Fatalf(
			"drift detection and healing CODEOWNERS fetches = %d, want 2",
			ownershipFetches,
		)
	}
	var changeType, resolution, nodeID, codeownersHash, codeownersSource string
	var truncated bool
	if err := harness.pool.QueryRow(ctx, `
		SELECT file.change_type, owner.resolution_state, owner.owner_node_id,
		       snapshot.files_truncated, snapshot.codeowners_hash,
		       snapshot.codeowners_source
		FROM pull_request_changed_files AS file
		JOIN pull_request_file_owners AS owner
		  ON owner.repo_id = file.repo_id
		 AND owner.pr_number = file.pr_number
		 AND owner.path = file.path
		JOIN pull_request_change_snapshots AS snapshot
		  ON snapshot.repo_id = file.repo_id
		 AND snapshot.pr_number = file.pr_number
		WHERE file.pr_number = 4812
		  AND file.path = 'internal/ranker.go'
		  AND owner.owner_token = '@acme/search-platform'
		  AND file.tombstoned_at IS NULL
		  AND owner.tombstoned_at IS NULL
	`).Scan(
		&changeType, &resolution, &nodeID, &truncated, &codeownersHash,
		&codeownersSource,
	); err != nil {
		t.Fatal(err)
	}
	if changeType != "modified" || resolution != "resolved" ||
		nodeID != "T_kwDOABCDEF6001" || truncated ||
		codeownersHash == corruptHash || codeownersSource == corruptSource {
		t.Fatalf(
			"healed change inputs = %q/%q/%q truncated=%v hash=%q source=%q",
			changeType, resolution, nodeID, truncated, codeownersHash,
			codeownersSource,
		)
	}
	if findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	} else if len(findings) != 0 {
		t.Fatalf("post-heal change-input findings = %+v", findings)
	}
}

func TestDriftTreatsTruncatedChangeSnapshotAsConvergedTruth(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	ctx := t.Context()
	pull := &harness.fixture.PullRequests[1]
	pull.ChangedFiles = make([]fakegithub.ChangedFile, 101)
	for index := range pull.ChangedFiles {
		pull.ChangedFiles[index] = fakegithub.ChangedFile{
			Path:       fmt.Sprintf("src/truncated-%03d.go", index),
			ChangeType: "modified",
		}
	}
	pull.ChangedFilesTotal = 102
	harness.fake.SetFixture(harness.fixture)
	if err := harness.handler.RefreshPR(ctx, queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}
	var total, files int
	var truncated bool
	if err := harness.pool.QueryRow(ctx, `
		SELECT snapshot.files_total_count, snapshot.files_truncated,
		       count(file.path)
		FROM pull_request_change_snapshots AS snapshot
		LEFT JOIN pull_request_changed_files AS file
		  ON file.repo_id = snapshot.repo_id
		 AND file.pr_number = snapshot.pr_number
		 AND file.tombstoned_at IS NULL
		WHERE snapshot.pr_number = 4812
		  AND snapshot.tombstoned_at IS NULL
		GROUP BY snapshot.files_total_count, snapshot.files_truncated
	`).Scan(&total, &truncated, &files); err != nil {
		t.Fatal(err)
	}
	if total != 102 || !truncated || files != 101 {
		t.Fatalf(
			"truncated snapshot total=%d truncated=%v files=%d",
			total, truncated, files,
		)
	}
	if findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	} else if len(findings) != 0 {
		t.Fatalf("truncated truth produced drift loop: %+v", findings)
	}
}

func TestDetectSamplesWithUnrelatedBusySweepQueue(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	harness.divergePullRequest()
	ctx := context.Background()
	job, err := harness.riverClient.Insert(
		ctx,
		queue.NewRefreshPRArgs("pr:acme/unrelated:9999"),
		queue.NewRefreshInsertOptsForQueue(
			queue.QueueSweep,
			time.Now().Add(time.Hour),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 ||
		findings[0].EntityKey != "pr:acme/monolith:4812" {
		t.Fatalf(
			"drift findings with unrelated queued sweep = %+v, want PR divergence",
			findings,
		)
	}
	var jobState string
	if err := harness.pool.QueryRow(ctx, `
		SELECT state FROM river_job WHERE id = $1
	`, job.Job.ID).Scan(&jobState); err != nil {
		t.Fatal(err)
	}
	if jobState != "scheduled" {
		t.Fatalf("unrelated sweep job state = %q, want scheduled", jobState)
	}
	var successes, samples int64
	if err := harness.pool.QueryRow(ctx, `
		SELECT success_count, sample_count
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector'
	`).Scan(&successes, &samples); err != nil {
		t.Fatal(err)
	}
	if successes != 1 || samples == 0 {
		t.Fatalf(
			"busy-queue heartbeat successes=%d samples=%d, want one sampled pass",
			successes,
			samples,
		)
	}
}

func TestDetectSkipsSampleWithOutstandingGeneration(t *testing.T) {
	t.Parallel()
	harness := newReadyDriftHarness(t)
	harness.divergePullRequest()
	ctx := context.Background()
	if _, err := harness.pool.Exec(ctx, `
		INSERT INTO refresh_intent_generations (
		    kind, refresh_key, generation, completed_generation
		) VALUES ('refresh_pr', 'pr:acme/monolith:4812', 1, 0)
		ON CONFLICT (kind, refresh_key) DO UPDATE
		SET generation = refresh_intent_generations.generation + 1
	`); err != nil {
		t.Fatal(err)
	}

	findings, err := harness.service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf(
			"findings with sampled-key refresh outstanding = %+v, want none",
			findings,
		)
	}
	var findingCount int64
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM drift_findings
		WHERE entity_key = 'pr:acme/monolith:4812'
	`).Scan(&findingCount); err != nil {
		t.Fatal(err)
	}
	if findingCount != 0 {
		t.Fatalf("false PR drift findings = %d, want 0", findingCount)
	}
	var successes, inspected, skipped int64
	if err := harness.pool.QueryRow(ctx, `
		SELECT success_count, sample_count
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector'
	`).Scan(&successes, &inspected); err != nil {
		t.Fatal(err)
	}
	if err := harness.pool.QueryRow(ctx, `
		SELECT sample_count
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = $1
	`, skippedSamplesOperation).Scan(&skipped); err != nil {
		t.Fatal(err)
	}
	if successes != 1 || inspected == 0 || skipped == 0 {
		t.Fatalf(
			"same-key heartbeat successes=%d inspected=%d skipped=%d",
			successes,
			inspected,
			skipped,
		)
	}
}

func TestDetectRecordsZeroSampleHeartbeat(t *testing.T) {
	t.Parallel()
	pool := driftTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	server := httptest.NewServer(fakegithub.New(fixture, "drift-secret"))
	defer server.Close()
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Options{
		Pool:    pool,
		REST:    rest,
		GraphQL: graphQL,
		Config: Config{
			InstallationID:     1,
			Period:             time.Hour,
			SampleSize:         len(driftEntityKinds),
			PageSize:           100,
			ResolvedRetention:  30 * 24 * time.Hour,
			RetentionBatchSize: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Detect(
		context.Background(),
		DetectArgs{
			InstallationID: 1,
			SampleSize:     len(driftEntityKinds),
		},
	); err != nil {
		t.Fatal(err)
	}
	var successes, samples int64
	var sampledAt bool
	if err := pool.QueryRow(context.Background(), `
		SELECT success_count, sample_count, last_sample_at IS NOT NULL
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector'
	`).Scan(&successes, &samples, &sampledAt); err != nil {
		t.Fatal(err)
	}
	if successes != 1 || samples != 0 || sampledAt {
		t.Fatalf(
			"zero-sample heartbeat successes=%d samples=%d sampled_at=%v",
			successes,
			samples,
			sampledAt,
		)
	}
}

func TestDetectSkipsWhileChildBackfillPending(t *testing.T) {
	t.Parallel()
	pool := driftTestDatabase(t)
	ctx := context.Background()
	// The installation cursor is 'done' (seeded by driftTestDatabase), but a
	// child seed is still in flight: sampling now would compare half-seeded
	// entities against upstream truth.
	if _, err := pool.Exec(ctx, `
		INSERT INTO backfill_cursors (
		    installation_id, repo_full_name, phase, page, completed_at
		) VALUES (1, 'acme/monolith', 'pull_requests', 1, NULL)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO backfill_children (
		    installation_id, repo_full_name, kind, refresh_key,
		    target_generation
		) VALUES (1, 'acme/monolith', 'refresh_pr', 'pr:acme/monolith:4812', 1)
	`); err != nil {
		t.Fatal(err)
	}
	service, err := New(Options{
		Pool:    pool,
		REST:    &gh.RESTClient{},
		GraphQL: &gh.GraphQLClient{},
		Config: Config{
			InstallationID:     1,
			Period:             time.Hour,
			SampleSize:         100,
			PageSize:           100,
			ResolvedRetention:  30 * 24 * time.Hour,
			RetentionBatchSize: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings during child backfill = %d, want 0", len(findings))
	}
	var skippedHeartbeats int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector_skipped'
	`).Scan(&skippedHeartbeats); err != nil {
		t.Fatal(err)
	}
	if skippedHeartbeats != 1 {
		t.Fatalf("detector_skipped heartbeats = %d, want 1", skippedHeartbeats)
	}
}

func TestStackDriftIgnoresMemberUpdatedAtChurn(t *testing.T) {
	t.Parallel()
	pool := driftTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake := fakegithub.New(fixture, "drift-secret")
	server := httptest.NewServer(fake)
	defer server.Close()
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := fetch.New(&fetch.Options{
		Pool:           pool,
		REST:           rest,
		GraphQL:        graphQL,
		InstallationID: 1,
		OrgID:          1,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Options{
		Pool:    pool,
		REST:    rest,
		GraphQL: graphQL,
		Config: Config{
			InstallationID:     1,
			Period:             time.Hour,
			SampleSize:         100,
			PageSize:           100,
			ResolvedRetention:  30 * 24 * time.Hour,
			RetentionBatchSize: 100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithRefreshHandler(handler),
		queue.WithWorkerRegistrar(service.RegisterWorker),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetRiverClient(riverClient)
	service.SetRiverClient(riverClient)
	ctx := context.Background()
	runCtx := t.Context()
	if err := riverClient.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	if err := handler.ResolveStackMembership(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewResolveStackMembershipArgs(
				"pr:acme/monolith:4812",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshPR(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshPRArgs(
				"pr:acme/monolith:4812",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshStack(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshStackArgs(
				"stack:acme/monolith:142",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	// Seed every PR-scoped connection before asserting that stack-only
	// updated_at churn is ignored. Missing change-input snapshots are genuine
	// drift now, not part of the unrelated-field tolerance under test.
	for index := range fixture.PullRequests {
		pull := &fixture.PullRequests[index]
		if err := handler.RefreshPR(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
				).RefreshArgs,
				Queue: queue.QueueSweep,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := handler.RefreshChecks(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshChecksArgs(
				"checks:acme/monolith:8f31c2d",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshRepoRules(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshRepoRulesArgs(
				"repo_rules:acme/monolith:rules",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	waitForCacheProducers(t, pool)

	// A review or comment on a member bumps only its updated_at upstream.
	// Dispatcher rules owe no stack refresh for that, so the cached stack
	// legitimately lags on the field; drift must not treat it as
	// divergence.
	fixture.PullRequests[1].UpdatedAt = fixture.PullRequests[1].UpdatedAt.Add(
		time.Minute,
	)
	fake.SetFixture(fixture)
	findings, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf(
			"member updated_at churn produced drift findings: %+v",
			findings,
		)
	}
}

func TestDriftDetectorRecordsDiffAndSelfHealsWithoutWebhook(
	t *testing.T,
) {
	t.Parallel()
	pool := driftTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake := fakegithub.New(fixture, "drift-secret")
	server := httptest.NewServer(fake)
	defer server.Close()
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-drift"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := fetch.New(&fetch.Options{
		Pool:           pool,
		REST:           rest,
		GraphQL:        graphQL,
		InstallationID: 1,
		OrgID:          1,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &findingObserver{}
	service, err := New(Options{
		Pool:    pool,
		REST:    rest,
		GraphQL: graphQL,
		Config: Config{
			InstallationID:     1,
			Period:             time.Hour,
			SampleSize:         100,
			PageSize:           100,
			ResolvedRetention:  30 * 24 * time.Hour,
			RetentionBatchSize: 100,
			Observer:           observer,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithRefreshHandler(handler),
		queue.WithWorkerRegistrar(service.RegisterWorker),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetRiverClient(riverClient)
	service.SetRiverClient(riverClient)
	ctx := context.Background()
	runCtx := t.Context()
	if err := riverClient.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	if err := handler.ResolveStackMembership(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewResolveStackMembershipArgs(
				"pr:acme/monolith:4812",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshPR(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshPRArgs(
				"pr:acme/monolith:4812",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshStack(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshStackArgs(
				"stack:acme/monolith:142",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	for index := range fixture.PullRequests {
		pull := &fixture.PullRequests[index]
		if err := handler.RefreshPR(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
				).RefreshArgs,
				Queue: queue.QueueSweep,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := handler.RefreshChecks(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshChecksArgs(
				"checks:acme/monolith:8f31c2d",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshRepoRules(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshRepoRulesArgs(
				"repo_rules:acme/monolith:rules",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	waitForCacheProducers(t, pool)
	if findings, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	} else if len(findings) != 0 {
		t.Fatalf("pre-backfill drift findings = %d, want 0", len(findings))
	}
	var preseedHeartbeats, preseedSamples int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(sum(sample_count), 0)
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector'
	`).Scan(&preseedHeartbeats, &preseedSamples); err != nil {
		t.Fatal(err)
	}
	if preseedHeartbeats != 1 || preseedSamples == 0 {
		t.Fatalf(
			"pre-backfill drift heartbeat rows=%d samples=%d, want one sampled pass",
			preseedHeartbeats,
			preseedSamples,
		)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO installation_backfill_cursors (
		    installation_id, phase, page, completed_at
		) VALUES (1, 'done', 1, clock_timestamp())
		ON CONFLICT (installation_id) DO NOTHING
	`); err != nil {
		t.Fatal(err)
	}

	// Model a sample selected before a concurrent legitimate cache refresh.
	// inspectSample must discard this stale snapshot and reread while holding
	// the same optimistic entity observation used by refresh writers.
	current, err := dbgen.New(pool).GetCachedEntitySnapshot(
		ctx,
		dbgen.GetCachedEntitySnapshotParams{
			InstallationID: 1,
			EntityKind:     "pull_request",
			EntityKey:      "pr:acme/monolith:4812",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	staleSampleFinding, recorded, skipped, err := service.inspectSample(
		ctx,
		&driftSample{
			EntityKind:    current.EntityKind,
			SourceID:      current.SourceID,
			EntityKey:     current.EntityKey,
			LockKey:       current.LockKey,
			CacheSnapshot: []byte(`{"stale_sample":true}`),
			LastCheckedAt: current.LastCheckedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if skipped {
		t.Fatal("stale sample was unexpectedly skipped")
	}
	if recorded {
		t.Fatalf(
			"stale pre-lock cache sample produced a false drift finding: %s",
			staleSampleFinding.Diff,
		)
	}

	// Change authoritative truth behind the cache with webhooks disabled.
	fixture.PullRequests[1].Title = "mutated behind cache"
	fixture.PullRequests[1].UpdatedAt = fixture.PullRequests[1].UpdatedAt.Add(
		time.Minute,
	)
	fake.SetFixture(fixture)
	findings, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("drift findings = %d, want 1: %+v", len(findings), findings)
	}
	if findings[0].EntityKey != "pr:acme/monolith:4812" ||
		!strings.Contains(string(findings[0].Diff), "title") {
		t.Fatalf("finding = %+v", findings[0])
	}
	observer.mu.Lock()
	observed := len(observer.findings)
	observer.mu.Unlock()
	if observed != 1 {
		t.Fatalf("divergence signals = %d, want 1", observed)
	}
	var sampledKinds int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM drift_sample_cursors
	`).Scan(&sampledKinds); err != nil {
		t.Fatal(err)
	}
	if sampledKinds != len(driftEntityKinds) {
		t.Fatalf(
			"stratified drift cursors = %d, want %d",
			sampledKinds,
			len(driftEntityKinds),
		)
	}
	var generation int64
	if err := pool.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr'
		  AND refresh_key = 'pr:acme/monolith:4812'
	`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation < 1 {
		t.Fatalf("self-heal generation = %d", generation)
	}

	deadline := time.Now().Add(10 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		row, rowErr := dbgen.New(pool).GetPullRequestByKey(
			ctx,
			dbgen.GetPullRequestByKeyParams{
				RepoFullName: "acme/monolith",
				PrNumber:     4812,
			},
		)
		if rowErr == nil && row.Title == "mutated behind cache" {
			converged = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !converged {
		t.Fatal("drift self-heal did not converge the cached PR")
	}
	for time.Now().Before(deadline) {
		var completedGeneration int64
		if err := pool.QueryRow(ctx, `
			SELECT completed_generation
			FROM refresh_intent_generations
			WHERE kind = 'refresh_pr'
			  AND refresh_key = 'pr:acme/monolith:4812'
		`).Scan(&completedGeneration); err != nil {
			t.Fatal(err)
		}
		if completedGeneration >= generation {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Reintroduce the exact same semantic mismatch after the heal generation
	// completed. It must escalate the one open finding without scheduling a
	// second heal loop.
	if _, err := pool.Exec(ctx, `
		UPDATE pull_requests
		SET title = 'BM25F ranker integration'
		WHERE number = 4812
	`); err != nil {
		t.Fatal(err)
	}
	var completedGeneration int64
	if err := pool.QueryRow(ctx, `
		SELECT completed_generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr'
		  AND refresh_key = 'pr:acme/monolith:4812'
	`).Scan(&completedGeneration); err != nil {
		t.Fatal(err)
	}
	if completedGeneration < generation {
		t.Fatalf(
			"completed generation = %d, want at least %d",
			completedGeneration,
			generation,
		)
	}
	waitForCacheProducers(t, pool)
	if _, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	}
	var findingCount int
	var occurrenceCount, generationAfter int64
	var escalated bool
	if err := pool.QueryRow(ctx, `
		SELECT count(*), max(occurrence_count),
		       bool_or(escalated_at IS NOT NULL)
		FROM drift_findings
		WHERE entity_key = 'pr:acme/monolith:4812'
	`).Scan(&findingCount, &occurrenceCount, &escalated); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr'
		  AND refresh_key = 'pr:acme/monolith:4812'
	`).Scan(&generationAfter); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	persistentSignals := len(observer.persistent)
	observer.mu.Unlock()
	if findingCount != 1 || occurrenceCount != 2 || !escalated ||
		generationAfter != generation || persistentSignals != 1 {
		t.Fatalf(
			"bounded self-heal count=%d occurrences=%d escalated=%v generation=%d->%d signals=%d",
			findingCount,
			occurrenceCount,
			escalated,
			generation,
			generationAfter,
			persistentSignals,
		)
	}

	if err := handler.RefreshPR(
		ctx,
		queue.RefreshRequest{
			Args: queue.NewRefreshPRArgs(
				"pr:acme/monolith:4812",
			).RefreshArgs,
			Queue: queue.QueueSweep,
		},
	); err != nil {
		t.Fatal(err)
	}
	waitForCacheProducers(t, pool)
	if _, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE drift_findings
		SET resolved_at = now() - interval '31 days'
		WHERE entity_key = 'pr:acme/monolith:4812'
		  AND resolved_at IS NOT NULL
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Detect(ctx, DetectArgs{
		InstallationID: 1,
		SampleSize:     100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM drift_findings
		WHERE entity_key = 'pr:acme/monolith:4812'
	`).Scan(&findingCount); err != nil {
		t.Fatal(err)
	}
	if findingCount != 0 {
		t.Fatalf("expired resolved drift findings = %d, want 0", findingCount)
	}
}

func waitForCacheProducers(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var activeJobs int64
		var outstandingGenerations int64
		if err := pool.QueryRow(context.Background(), `
			SELECT
			    (SELECT count(*)
			     FROM river_job
			     WHERE queue IN (
			         'interactive', 'event', 'sweep', 'reconcile'
			     )
			       AND state IN (
			           'available', 'pending', 'retryable',
			           'running', 'scheduled'
			       )),
			    (SELECT count(*)
			     FROM refresh_intent_generations
			     WHERE completed_generation < generation)
		`).Scan(&activeJobs, &outstandingGenerations); err != nil {
			t.Fatal(err)
		}
		if activeJobs == 0 && outstandingGenerations == 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-t.Context().Done():
			t.Fatalf(
				"cache producers did not quiesce before test cancellation: jobs=%d generations=%d",
				activeJobs, outstandingGenerations,
			)
		}
	}
}

func TestSemanticDiffNormalizesIDOrderedCollections(t *testing.T) {
	t.Parallel()
	cache := []byte(`{"runs":[{"id":10,"name":"ten"},{"id":2,"name":"two"}]}`)
	upstream := []byte(`{"runs":[{"name":"two","id":2},{"name":"ten","id":10}]}`)
	equal, diff, err := semanticDiff("checks", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("normalization-stable compare equal=%v diff=%s", equal, diff)
	}
}

func TestSemanticDiffNormalizesReviewRequestsInGo(t *testing.T) {
	t.Parallel()
	cache := []byte(`{"review_requests":[{"kind":"user","id":7},{"kind":"team","id":7}]}`)
	upstream := []byte(`{"review_requests":[{"id":7,"kind":"team"},{"id":7,"kind":"user"}]}`)
	equal, diff, err := semanticDiff("pull_request", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("Go-sorted request compare equal=%v diff=%s", equal, diff)
	}
}

func TestSemanticDiffNormalizesNullableDatabaseIDsByNodeIDInGo(
	t *testing.T,
) {
	t.Parallel()
	cache := []byte(`{
		"reviews":[
			{"id":null,"node_id":"review-Z","author_node_id":null},
			{"id":null,"node_id":"review-a","author_node_id":null}
		]
	}`)
	upstream := []byte(`{
		"reviews":[
			{"author_node_id":null,"node_id":"review-a","id":null},
			{"author_node_id":null,"node_id":"review-Z","id":null}
		]
	}`)
	equal, diff, err := semanticDiff("pull_request", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("node-ID normalized compare equal=%v diff=%s", equal, diff)
	}
}

func TestSemanticDiffNormalizesChangedFilesAndOwnersInGo(t *testing.T) {
	t.Parallel()
	cache := []byte(`{
		"change_inputs": {
			"files": [
				{"path":"z.go","previous_path":null},
				{"path":"a.go","previous_path":null}
			],
			"owners": [
				{"path":"z.go","owner_token":"@z","owner_gh_id":null},
				{"path":"a.go","owner_token":"@a","owner_gh_id":null}
			]
		}
	}`)
	upstream := []byte(`{
		"change_inputs": {
			"files": [
				{"previous_path":null,"path":"a.go"},
				{"previous_path":null,"path":"z.go"}
			],
			"owners": [
				{"owner_gh_id":null,"owner_token":"@a","path":"a.go"},
				{"owner_gh_id":null,"owner_token":"@z","path":"z.go"}
			]
		}
	}`)
	equal, diff, err := semanticDiff("pull_request", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("Go-sorted change-input compare equal=%v diff=%s", equal, diff)
	}
}

// The sampler resolves its keyset against drift_entity_keys and then reads
// the snapshots back out of drift_entities by source_id. That is only
// equivalent to ordering drift_entities directly while the two views project
// the same (installation_id, entity_kind, source_id) triples, so pin both the
// projection invariant and the window the sampler returns. The check group
// gets extra live runs plus a tombstoned one first: 'checks' is the only kind
// whose source_id is a group representative rather than a row id.
func TestDriftSampleKeysetMatchesDriftEntities(t *testing.T) {
	t.Parallel()
	pool := driftTestDatabase(t)
	ctx := context.Background()
	seedCheckRunGroup(t, pool)

	var mismatches int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
		    (SELECT installation_id, entity_kind, source_id FROM drift_entities
		     EXCEPT ALL
		     SELECT installation_id, entity_kind, source_id
		     FROM drift_entity_keys)
		    UNION ALL
		    (SELECT installation_id, entity_kind, source_id
		     FROM drift_entity_keys
		     EXCEPT ALL
		     SELECT installation_id, entity_kind, source_id FROM drift_entities)
		) AS diff
	`).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf(
			"drift_entity_keys diverges from drift_entities in %d rows",
			mismatches,
		)
	}

	queries := dbgen.New(pool)
	for _, kind := range driftEntityKinds {
		cursors, err := driftSourceIDs(ctx, pool, kind)
		if err != nil {
			t.Fatal(err)
		}
		// Guard against a vacuous pass: every kind must hold more rows than
		// one sample window, so the LIMIT and the keyset both bite.
		if len(cursors) < 3 {
			t.Fatalf("%s has %d cached rows, want at least 3", kind, len(cursors))
		}
		// Sample from before every row, from the first row, and from past
		// the end so both halves of the rotation are covered.
		probes := []int64{0}
		probes = append(probes, cursors...)
		for _, cursor := range probes {
			const sampleSize = 2
			want, err := driftEntityWindow(
				ctx, pool, kind, "source_id > $2", cursor, sampleSize,
			)
			if err != nil {
				t.Fatal(err)
			}
			got, err := queries.SampleCachedEntitiesAfter(
				ctx,
				dbgen.SampleCachedEntitiesAfterParams{
					InstallationID: 1,
					EntityKind:     kind,
					AfterSourceID:  cursor,
					SampleSize:     sampleSize,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			assertSampleMatches(t, kind, "after", cursor, want, got)

			want, err = driftEntityWindow(
				ctx, pool, kind, "source_id <= $2", cursor, sampleSize,
			)
			if err != nil {
				t.Fatal(err)
			}
			wrapped, err := queries.SampleCachedEntitiesThrough(
				ctx,
				dbgen.SampleCachedEntitiesThroughParams{
					InstallationID:  1,
					EntityKind:      kind,
					ThroughSourceID: cursor,
					SampleSize:      sampleSize,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			through := make([]dbgen.SampleCachedEntitiesAfterRow, 0, len(wrapped))
			for _, row := range wrapped {
				through = append(through, dbgen.SampleCachedEntitiesAfterRow(row))
			}
			assertSampleMatches(t, kind, "through", cursor, want, through)
		}
	}
}

// seedCheckRunGroup gives acme/monolith a check group with three live runs and
// one tombstoned run, so the 'checks' representative is the smallest LIVE
// gh_id and its snapshot must carry exactly the live runs in gh_id order.
func seedCheckRunGroup(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO repos (
		    id, installation_id, org_id, gh_id, node_id, owner, name,
		    full_name, default_branch, archived, synced_at, sync_source,
		    last_checked_at
		) VALUES
		    (9001, 1, 1, 9001, 'R_keyset', 'acme', 'monolith',
		     'acme/monolith', 'main', false, clock_timestamp(), 'backfill',
		     clock_timestamp()),
		    (9002, 1, 1, 9002, 'R_two', 'acme', 'tools', 'acme/tools',
		     'main', false, clock_timestamp(), 'backfill',
		     clock_timestamp()),
		    (9003, 1, 1, 9003, 'R_three', 'acme', 'docs', 'acme/docs',
		     'main', false, clock_timestamp(), 'backfill',
		     clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, head_sha, synced_at,
		    sync_source, last_checked_at, tombstoned_at
		) VALUES
		    (5003, 9001, 'CR_c', 'lint', 'completed', 'deadbeef',
		     clock_timestamp(), 'backfill', clock_timestamp(), NULL),
		    (5001, 9001, 'CR_a', 'build', 'completed', 'deadbeef',
		     clock_timestamp(), 'backfill', clock_timestamp(), NULL),
		    (5002, 9001, 'CR_b', 'test', 'completed', 'deadbeef',
		     clock_timestamp(), 'backfill', clock_timestamp(), NULL),
		    (5000, 9001, 'CR_dead', 'old', 'completed', 'deadbeef',
		     clock_timestamp(), 'backfill', clock_timestamp(),
		     clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	// A second check group, plus pull requests, stacks and review threads, so
	// every kind has more rows than one sample window holds and ordering is
	// observable.
	if _, err := pool.Exec(ctx, `
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, head_sha, synced_at,
		    sync_source, last_checked_at
		) VALUES
		    (4002, 9001, 'CR_e', 'build', 'completed', 'cafebabe',
		     clock_timestamp(), 'backfill', clock_timestamp()),
		    (4001, 9001, 'CR_d', 'test', 'completed', 'cafebabe',
		     clock_timestamp(), 'backfill', clock_timestamp()),
		    (3001, 9002, 'CR_f', 'build', 'completed', 'feedface',
		     clock_timestamp(), 'backfill', clock_timestamp());

		INSERT INTO pull_requests (
		    id, repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha, synced_at,
		    sync_source, last_checked_at
		) VALUES
		    (7001, 9001, 7001, 'PR_a', 11, 'first', 'open', false, 'ada',
		     'f1', 'aaa', 'main', 'bbb', clock_timestamp(), 'backfill',
		     clock_timestamp()),
		    (7002, 9001, 7002, 'PR_b', 12, 'second', 'open', false, 'ada',
		     'f2', 'ccc', 'main', 'bbb', clock_timestamp(), 'backfill',
		     clock_timestamp()),
		    (7003, 9001, 7003, 'PR_c', 13, 'third', 'closed', false, 'ada',
		     'f3', 'ddd', 'main', 'bbb', clock_timestamp(), 'backfill',
		     clock_timestamp());

		INSERT INTO stacks (
		    id, repo_id, gh_id, node_id, number, base_ref, base_sha, open,
		    entries, synced_at, sync_source, last_checked_at
		) VALUES
		    (8001, 9001, 8001, 'ST_a', 21, 'main', 'bbb', true,
		     '[]'::jsonb, clock_timestamp(), 'backfill', clock_timestamp()),
		    (8002, 9001, 8002, 'ST_b', 22, 'main', 'bbb', false,
		     '[]'::jsonb, clock_timestamp(), 'backfill', clock_timestamp()),
		    (8003, 9001, 8003, 'ST_c', 23, 'main', 'bbb', true,
		     '[]'::jsonb, clock_timestamp(), 'backfill', clock_timestamp());

		INSERT INTO review_threads (
		    id, repo_id, pr_number, is_resolved, is_outdated, path, line,
		    comments, synced_at, sync_source, last_checked_at
		) VALUES
		    ('RT_a', 9001, 11, false, false, 'a.go', 3, '[]'::jsonb,
		     clock_timestamp(), 'backfill', clock_timestamp()),
		    ('RT_b', 9001, 12, true, false, 'b.go', 9, '[]'::jsonb,
		     clock_timestamp(), 'backfill', clock_timestamp());
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO pull_request_review_requests (
		    repo_id, pr_number, reviewer_kind, reviewer_gh_id,
		    reviewer_node_id, reviewer_login, first_seen_at,
		    gh_updated_at, head_sha, synced_at, sync_source,
		    last_checked_at, tombstoned_at
		) VALUES
		    (9001, 11, 'user', 9101, 'U_keyset', 'ada',
		     clock_timestamp(), clock_timestamp(), 'aaa',
		     clock_timestamp(), 'backfill', clock_timestamp(), NULL),
		    (9001, 11, 'team', 9102, 'T_keyset', 'platform',
		     clock_timestamp(), clock_timestamp(), 'aaa',
		     clock_timestamp(), 'backfill', clock_timestamp(), NULL),
		    (9001, 11, 'user', 9103, 'U_dead', 'removed',
		     clock_timestamp(), clock_timestamp(), 'aaa',
		     clock_timestamp(), 'backfill', clock_timestamp(),
		     clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pull_request_reviews (
		    node_id, gh_id, repo_id, pr_number, author_kind,
		    author_node_id, author_login, state, submitted_at, commit_oid,
		    gh_updated_at, head_sha, synced_at, sync_source,
		    last_checked_at, tombstoned_at
		) VALUES
		    ('PRR_keyset', 9201, 9001, 11, 'user', 'U_participant',
		     'reviewer', 'approved', clock_timestamp(), 'aaa',
		     clock_timestamp(), 'aaa', clock_timestamp(), 'backfill',
		     clock_timestamp(), NULL),
		    ('PRR_dead', 9202, 9001, 11, 'user', 'U_removed',
		     'removed', 'commented', clock_timestamp(), 'aaa',
		     clock_timestamp(), 'aaa', clock_timestamp(), 'backfill',
		     clock_timestamp(), clock_timestamp());

		INSERT INTO pull_request_comments (
		    node_id, gh_id, repo_id, pr_number, author_kind,
		    author_node_id, author_login, created_at, gh_updated_at,
		    head_sha, synced_at, sync_source, last_checked_at, tombstoned_at
		) VALUES
		    ('IC_keyset', 9301, 9001, 11, 'deleted', NULL, NULL,
		     clock_timestamp(), clock_timestamp(), 'aaa', clock_timestamp(),
		     'backfill', clock_timestamp(), NULL),
		    ('IC_dead', 9302, 9001, 11, 'user', 'U_removed', 'removed',
		     clock_timestamp(), clock_timestamp(), 'aaa', clock_timestamp(),
		     'backfill', clock_timestamp(), clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}

	var sourceID int64
	var snapshot []byte
	if err := pool.QueryRow(ctx, `
		SELECT source_id, cache_snapshot
		FROM drift_entities
		WHERE installation_id = 1
		  AND entity_kind = 'checks'
		  AND entity_key = 'checks:acme/monolith:deadbeef'
	`).Scan(&sourceID, &snapshot); err != nil {
		t.Fatal(err)
	}
	if sourceID != 5001 {
		t.Fatalf("checks source_id = %d, want the smallest live gh_id 5001", sourceID)
	}
	if got := string(snapshot); !strings.Contains(got, `"id": 5001`) ||
		!strings.Contains(got, `"id": 5002`) ||
		!strings.Contains(got, `"id": 5003`) ||
		strings.Contains(got, `"id": 5000`) {
		t.Fatalf("checks snapshot = %s, want the three live runs only", got)
	}
	if err := pool.QueryRow(ctx, `
		SELECT cache_snapshot
		FROM drift_entities
		WHERE installation_id = 1
		  AND entity_kind = 'pull_request'
		  AND entity_key = 'pr:acme/monolith:11'
	`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if got := string(snapshot); !strings.Contains(got, `"id": 9101`) ||
		!strings.Contains(got, `"id": 9102`) ||
		strings.Contains(got, `"id": 9103`) ||
		!strings.Contains(got, `"node_id": "PRR_keyset"`) ||
		strings.Contains(got, `"node_id": "PRR_dead"`) ||
		!strings.Contains(got, `"node_id": "IC_keyset"`) ||
		strings.Contains(got, `"node_id": "IC_dead"`) ||
		strings.Contains(got, `"body"`) {
		t.Fatalf(
			"pull-request participation snapshot = %s, want live rows only",
			got,
		)
	}
}

func driftSourceIDs(
	ctx context.Context,
	pool *pgxpool.Pool,
	kind string,
) ([]int64, error) {
	rows, err := pool.Query(ctx, `
		SELECT source_id
		FROM drift_entities
		WHERE installation_id = 1 AND entity_kind = $1
		ORDER BY source_id
	`, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// driftEntityWindow is the pre-optimisation sampler: order drift_entities
// directly and LIMIT it.
func driftEntityWindow(
	ctx context.Context,
	pool *pgxpool.Pool,
	kind string,
	bound string,
	cursor int64,
	sampleSize int32,
) ([]dbgen.SampleCachedEntitiesAfterRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT entity_kind, source_id, entity_key, lock_key, cache_snapshot,
		       last_checked_at
		FROM drift_entities
		WHERE installation_id = 1
		  AND entity_kind = $1
		  AND `+bound+`
		ORDER BY source_id
		LIMIT $3
	`, kind, cursor, sampleSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	window := make([]dbgen.SampleCachedEntitiesAfterRow, 0, sampleSize)
	for rows.Next() {
		var row dbgen.SampleCachedEntitiesAfterRow
		if err := rows.Scan(
			&row.EntityKind,
			&row.SourceID,
			&row.EntityKey,
			&row.LockKey,
			&row.CacheSnapshot,
			&row.LastCheckedAt,
		); err != nil {
			return nil, err
		}
		window = append(window, row)
	}
	return window, rows.Err()
}

func assertSampleMatches(
	t *testing.T,
	kind string,
	half string,
	cursor int64,
	want []dbgen.SampleCachedEntitiesAfterRow,
	got []dbgen.SampleCachedEntitiesAfterRow,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf(
			"%s %s sample after %d returned %d rows, want %d",
			kind, half, cursor, len(got), len(want),
		)
	}
	for index := range want {
		if !reflect.DeepEqual(got[index], want[index]) {
			t.Fatalf(
				"%s %s sample after %d row %d = %+v, want %+v",
				kind, half, cursor, index, got[index], want[index],
			)
		}
	}
}

func driftTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t).Pool
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO installation_backfill_cursors (
		    installation_id, phase, page, completed_at
		) VALUES (1, 'done', 1, clock_timestamp())
		ON CONFLICT (installation_id) DO NOTHING
	`); err != nil {
		t.Fatal(err)
	}
	return pool
}
