package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/dispatch"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const fetchTestSecret = "fetch-test-secret"

func TestWriteRaceBothOrdersNewerWins(t *testing.T) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for index, order := range []string{"old-new", "new-old"} {
		repo := fmt.Sprintf("acme/write-race-%d", index)
		repository := testRepository(repo, int64(2000+index), baseTime)
		old := testPull(repository, baseTime, "old-head")
		newer := testPull(repository, baseTime.Add(time.Minute), "new-head")
		var sequence []store.PullRequestRecord
		if order == "old-new" {
			sequence = []store.PullRequestRecord{old, newer}
		} else {
			sequence = []store.PullRequestRecord{newer, old}
		}
		for _, pull := range sequence {
			if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
				t.Fatalf("%s apply: %v", order, err)
			}
		}
		row, err := dbgen.New(pool).GetPullRequestByKey(
			context.Background(),
			dbgen.GetPullRequestByKeyParams{
				RepoFullName: repo,
				PrNumber:     42,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if row.HeadSha != "new-head" ||
			!row.GhUpdatedAt.Valid ||
			!row.GhUpdatedAt.Time.Equal(newer.GitHubUpdatedAt) {
			t.Fatalf("%s final row = %+v, newer write did not win", order, row)
		}
	}
}

func TestEqualTimestampDomainChangeAndTombstoneResurrection(t *testing.T) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	updatedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/equal-version", 2100, updatedAt)
	first := testPull(repository, updatedAt, "head-one")
	second := testPull(repository, updatedAt, "head-two")
	second.Title = "equal timestamp changed truth"
	second.SyncedAt = first.SyncedAt.Add(time.Second)
	if _, err := writer.ApplyPullRequest(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	result, err := writer.ApplyPullRequest(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DomainChanged || result.NewHeadSHA != "head-two" {
		t.Fatalf("equal timestamp change result = %+v", result)
	}

	observation, err := writer.BeginObservation(
		context.Background(),
		store.PullRequestEntityKey(1, 2100, 42),
	)
	if err != nil {
		t.Fatal(err)
	}
	tombstonedAt := second.SyncedAt.Add(time.Second)
	if _, err := writer.TombstonePullRequestObserved(
		context.Background(),
		observation,
		repository,
		42,
		store.SyncSourceWebhook,
		tombstonedAt,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}
	resurrected := second
	resurrected.SyncedAt = tombstonedAt.Add(time.Second)
	observation, err = writer.BeginObservation(
		context.Background(),
		store.PullRequestEntityKey(1, 2100, 42),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ApplyPullRequestObserved(
		context.Background(),
		observation,
		resurrected,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.TombstonedAt.Valid || row.HeadSha != "head-two" {
		t.Fatalf("resurrected row = %+v", row)
	}
}

func TestRepositoryRenameKeepsAliasesImmutableEventsAndDirtyScopes(
	t *testing.T,
) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	oldRepository := testRepository("acme/old-name", 2200, baseTime)
	pull := testPull(oldRepository, baseTime, "rename-head")
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		context.Background(),
		`TRUNCATE derivation_dirty`,
	); err != nil {
		t.Fatal(err)
	}
	renamed := oldRepository
	renamed.Owner = "platform"
	renamed.Name = "new-name"
	renamed.FullName = "platform/new-name"
	renamed.GitHubUpdatedAt = baseTime.Add(time.Minute)
	if _, err := writer.ApplyRepository(
		context.Background(),
		renamed,
		store.SyncSourceWebhook,
		`"renamed"`,
		baseTime.Add(2*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"acme/old-name", "platform/new-name"} {
		got, err := writer.Repository(context.Background(), alias)
		if err != nil {
			t.Fatalf("resolve alias %s: %v", alias, err)
		}
		if got.GitHubID != 2200 || got.FullName != "platform/new-name" {
			t.Fatalf("alias %s resolved to %+v", alias, got)
		}
	}
	var repos, dirty, immutableEvents int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM repos WHERE gh_id = 2200`,
	).Scan(&repos); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM derivation_dirty
		WHERE scope_key = 'pr:1:2200:42'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'repository.changed'
		  AND entity_key = 'repo:1:2200'
	`).Scan(&immutableEvents); err != nil {
		t.Fatal(err)
	}
	if repos != 1 || dirty != 1 || immutableEvents < 1 {
		t.Fatalf(
			"rename repos=%d dirty=%d immutable_events=%d",
			repos,
			dirty,
			immutableEvents,
		)
	}

	if _, err := pool.Exec(
		context.Background(),
		`TRUNCATE derivation_dirty`,
	); err != nil {
		t.Fatal(err)
	}
	checksObservation, err := writer.BeginObservation(
		context.Background(),
		store.ChecksEntityKey(
			oldRepository.InstallationID,
			oldRepository.GitHubID,
			pull.HeadSHA,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer checksObservation.Close() //nolint:errcheck
	changed, err := writer.ApplyChecksObserved(
		context.Background(),
		checksObservation,
		store.ChecksRecord{
			// Model a queued job carrying the pre-rename alias. Dirty-scope
			// resolution must use immutable GitHub identity, not this name.
			Repository: oldRepository,
			HeadSHA:    pull.HeadSHA,
			Runs: []store.CheckRunRecord{{
				GitHubID:   22001,
				NodeID:     "rename-check",
				Name:       "unit",
				Status:     "completed",
				Conclusion: "success",
				Observed:   json.RawMessage(`{"id":22001}`),
			}},
			SyncedAt: baseTime.Add(3 * time.Minute),
			Source:   store.SyncSourceWebhook,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("stale-name check observation was not applied")
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM derivation_dirty
		WHERE scope_key = 'pr:1:2200:42'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("stale-name check dirty scopes = %d, want 1", dirty)
	}
}

func TestTimestampLessChecksOnlyAppendAcceptedTransitions(t *testing.T) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/check-transitions", 2300, baseTime)
	pull := testPull(repository, baseTime, "checks-head")
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	apply := func(status string, observedAt time.Time) bool {
		t.Helper()
		observation, err := writer.BeginObservation(
			context.Background(),
			store.ChecksEntityKey(1, 2300, "checks-head"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer observation.Close() //nolint:errcheck
		changed, err := writer.ApplyChecksObserved(
			context.Background(),
			observation,
			store.ChecksRecord{
				Repository: repository,
				HeadSHA:    "checks-head",
				Runs: []store.CheckRunRecord{{
					GitHubID:   9001,
					NodeID:     "check-node",
					Name:       "unit",
					Status:     status,
					DetailsURL: "https://example.test/check/9001",
					Observed:   json.RawMessage(`{"id":9001}`),
				}},
				SyncedAt: observedAt,
				Source:   store.SyncSourceWebhook,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if !apply("queued", baseTime.Add(time.Minute)) {
		t.Fatal("initial queued check was not accepted")
	}
	var initialSynced, initialChecked time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at, last_checked_at FROM check_runs WHERE gh_id = 9001
	`).Scan(&initialSynced, &initialChecked); err != nil {
		t.Fatal(err)
	}
	if apply("queued", baseTime.Add(2*time.Minute)) {
		t.Fatal("identical timestamp-less check was reported changed")
	}
	var afterSynced, afterChecked time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at, last_checked_at FROM check_runs WHERE gh_id = 9001
	`).Scan(&afterSynced, &afterChecked); err != nil {
		t.Fatal(err)
	}
	if !afterSynced.Equal(initialSynced) ||
		!afterChecked.After(initialChecked) {
		t.Fatalf(
			"identical check provenance synced=%s->%s checked=%s->%s",
			initialSynced,
			afterSynced,
			initialChecked,
			afterChecked,
		)
	}
	if !apply("in_progress", baseTime.Add(3*time.Minute)) {
		t.Fatal("timestamp-less status transition was rejected")
	}
	var history, events int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM check_history WHERE check_run_gh_id = 9001`,
	).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:2300:checks-head'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if history != 2 || events != 2 {
		t.Fatalf("accepted transition history=%d events=%d, want 2/2", history, events)
	}
}

func TestIdenticalPR200OnlyAdvancesLastCheckedAt(t *testing.T) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/recheck", 2400, baseTime)
	pull := testPull(repository, baseTime, "same-head")
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	initialSynced := row.SyncedAt.Time
	initialChecked := row.LastCheckedAt.Time
	pull.SyncedAt = pull.SyncedAt.Add(time.Minute)
	result, err := writer.ApplyPullRequest(context.Background(), pull)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied || result.DomainChanged {
		t.Fatalf("identical PR result = %+v", result)
	}
	row, err = dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !row.SyncedAt.Time.Equal(initialSynced) ||
		!row.LastCheckedAt.Time.After(initialChecked) {
		t.Fatalf(
			"identical PR synced=%s->%s checked=%s->%s",
			initialSynced,
			row.SyncedAt.Time,
			initialChecked,
			row.LastCheckedAt.Time,
		)
	}
	var events int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'pull_request.changed'
		  AND entity_key = 'pr:1:2400:42'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("identical PR change events = %d, want 1 initial event", events)
	}
}

func TestPullRequestBatchIsolatesPoisonEntity(t *testing.T) {
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	goodRepo := testRepository("acme/good-batch", 2500, baseTime)
	badRepo := testRepository("acme/bad-batch", 2501, baseTime)
	good := testPull(goodRepo, baseTime, "good-head")
	bad := testPull(badRepo, baseTime, "bad-head")
	bad.NodeID = ""
	outcomes := writer.ApplyPullRequestBatch(
		context.Background(),
		[]store.PullRequestApply{
			{Record: bad},
			{Record: good},
		},
	)
	goodKey := store.PullRequestEntityKey(1, 2500, 42)
	badKey := store.PullRequestEntityKey(1, 2501, 42)
	if outcomes[goodKey].Err != nil ||
		!outcomes[goodKey].Result.DomainChanged {
		t.Fatalf("healthy batch outcome = %+v", outcomes[goodKey])
	}
	if outcomes[badKey].Err == nil {
		t.Fatalf("poison batch outcome = %+v", outcomes[badKey])
	}
	if _, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: goodRepo.FullName,
			PrNumber:     42,
		},
	); err != nil {
		t.Fatalf("healthy batch row missing: %v", err)
	}
}

func TestCoordinatorReturnsPerKeyWriterErrors(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	for _, number := range []int{4812, 4815} {
		request := queue.RefreshRequest{
			Args: queue.NewResolveStackMembershipArgs(
				fmt.Sprintf("pr:acme/monolith:%d", number),
			).RefreshArgs,
			Queue: queue.QueueEvent,
		}
		if err := handler.ResolveStackMembership(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	fixture.PullRequests[1].Head.SHA = ""
	fixture.PullRequests[1].Title = "poison should not apply"
	fixture.PullRequests[2].Title = "healthy sibling applied"
	fake.SetFixture(fixture)
	baseline := fake.RequestCount(http.MethodPost, "/graphql")
	type result struct {
		number int
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, number := range []int{4812, 4815} {
		number := number
		go func() {
			<-start
			err := handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
			results <- result{number: number, err: err}
		}()
	}
	close(start)
	outcomes := map[int]error{}
	for range 2 {
		outcome := <-results
		outcomes[outcome.number] = outcome.err
	}
	if outcomes[4812] == nil {
		t.Fatal("poison entity unexpectedly succeeded")
	}
	if outcomes[4815] != nil {
		t.Fatalf("healthy sibling failed: %v", outcomes[4815])
	}
	if got := fake.RequestCount(
		http.MethodPost,
		"/graphql",
	) - baseline; got != 1 {
		t.Fatalf("coordinator GraphQL batches = %d, want 1", got)
	}
	healthy, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4815,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if healthy.Title != "healthy sibling applied" {
		t.Fatalf("healthy sibling row = %+v", healthy)
	}
}

func TestCoordinatorStampsEveryGangedItemEventContext(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	numbers := []int{4812, 4815, 4816}
	for _, number := range numbers {
		if err := handler.ResolveStackMembership(
			context.Background(),
			queue.RefreshRequest{
				Args: queue.NewResolveStackMembershipArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Title += " ganged latency"
			}
		}
	}
	fake.SetFixture(fixture)
	baseline := fake.RequestCount(http.MethodPost, "/graphql")
	eventContexts := make([]context.Context, len(numbers))
	results := make(chan error, len(numbers))
	start := make(chan struct{})
	for index, number := range numbers {
		eventContexts[index] = pipeline.WithEvent(
			context.Background(),
			time.Now().Add(-time.Duration(index+1)*time.Second),
		)
		ctx := eventContexts[index]
		number := number
		go func() {
			<-start
			results <- handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
		}()
	}
	close(start)
	for range numbers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	stamps := make(map[time.Time]struct{}, len(eventContexts))
	for index, eventCtx := range eventContexts {
		stamp := pipeline.CacheCommittedAt(eventCtx)
		if stamp.IsZero() {
			t.Fatalf("ganged item %d has no cache commit stamp", index)
		}
		stamps[stamp] = struct{}{}
	}
	if len(stamps) != len(eventContexts) {
		t.Fatalf(
			"ganged cache commit stamps = %d, want %d distinct item stamps",
			len(stamps),
			len(eventContexts),
		)
	}
	if got := fake.RequestCount(
		http.MethodPost,
		"/graphql",
	) - baseline; got != 1 {
		t.Fatalf("coordinator GraphQL batches = %d, want 1", got)
	}
}

func TestCoordinatorIsolatesReviewThreadTransportFailure(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	targetThread := fixture.PullRequests[1].ReviewThreads[0].ID
	comments := make([]fakegithub.ReviewComment, 101)
	for index := range comments {
		comments[index] = fakegithub.ReviewComment{
			ID:          fmt.Sprintf("comment-%03d", index),
			Body:        fmt.Sprintf("comment %d", index),
			UpdatedAt:   fixture.PullRequests[1].UpdatedAt,
			AuthorLogin: "reviewer",
		}
	}
	fixture.PullRequests[1].ReviewThreads[0].Comments = comments
	var injected atomic.Int64
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/graphql" {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				var request struct {
					Query     string                     `json:"query"`
					Variables map[string]json.RawMessage `json:"variables"`
				}
				if err := json.Unmarshal(body, &request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var nodeID string
				_ = json.Unmarshal(request.Variables["id"], &nodeID)
				if strings.Contains(
					request.Query,
					"GhsyncReviewThreadCommentsPage",
				) && nodeID == targetThread {
					injected.Add(1)
					http.Error(
						w,
						"injected review-thread transport failure",
						http.StatusBadGateway,
					)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	fake, server, handler, riverClient := newDirectHandlerWithMiddleware(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
		middleware,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	for _, number := range []int{4812, 4815} {
		if err := handler.ResolveStackMembership(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewResolveStackMembershipArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	fixture.PullRequests[1].Title = "poison transport update"
	fixture.PullRequests[2].Title = "healthy transport sibling"
	fake.SetFixture(fixture)
	type refreshResult struct {
		number int
		err    error
	}
	results := make(chan refreshResult, 2)
	start := make(chan struct{})
	for _, number := range []int{4812, 4815} {
		number := number
		go func() {
			<-start
			err := handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
			results <- refreshResult{number: number, err: err}
		}()
	}
	close(start)
	outcomes := make(map[int]error, 2)
	for range 2 {
		result := <-results
		outcomes[result.number] = result.err
	}
	if outcomes[4812] == nil {
		t.Fatal("transport-poisoned PR unexpectedly succeeded")
	}
	if outcomes[4815] != nil {
		t.Fatalf("healthy sibling failed: %v", outcomes[4815])
	}
	if injected.Load() < 2 {
		t.Fatalf(
			"injected transport failures = %d, want batch and isolated attempts",
			injected.Load(),
		)
	}
	healthy, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4815,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if healthy.Title != "healthy transport sibling" {
		t.Fatalf("healthy sibling row = %+v", healthy)
	}
	poisoned, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if poisoned.Title == "poison transport update" {
		t.Fatalf("transport-poisoned row was committed: %+v", poisoned)
	}
}

func TestPullRequestStateAndFollowupGenerationsCommitAtomically(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	_, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	repository := testRepository(
		"acme/monolith",
		1001,
		fixture.Repository.UpdatedAt,
	)
	repository.NodeID = fixture.Repository.NodeID
	repository.DefaultBranch = fixture.Repository.DefaultBranch
	repository.DefaultHeadSHA = fixture.Repository.DefaultBranchSHA
	repository.ETag = `"repo"`
	repository.LastCheckedAt = time.Now()
	if _, err := handler.writer.ApplyRepository(
		context.Background(),
		repository,
		store.SyncSourceWebhook,
		repository.ETag,
		repository.LastCheckedAt,
	); err != nil {
		t.Fatal(err)
	}
	pull := pullRecordFromREST(
		repository,
		toGHPullRequest(t, fixture.PullRequests[1]),
		`"pull"`,
		store.SyncSourceWebhook,
		time.Now(),
	)
	apply := func() error {
		observation, err := handler.writer.BeginObservation(
			context.Background(),
			store.PullRequestEntityKey(1, 1001, pull.Number),
		)
		if err != nil {
			return err
		}
		defer observation.Close() //nolint:errcheck
		_, err = handler.writer.ApplyPullRequestObserved(
			context.Background(),
			observation,
			pull,
			handler.pullRequestHook(
				repository.FullName,
				pull.Number,
				queue.QueueEvent,
			),
		)
		return err
	}
	if err := apply(); err == nil ||
		!strings.Contains(err.Error(), "river client missing") {
		t.Fatalf("missing River transaction error = %v", err)
	}
	var rolledBackRows int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pull_requests
	`).Scan(&rolledBackRows); err != nil {
		t.Fatal(err)
	}
	if rolledBackRows != 0 {
		t.Fatalf("cache row committed without followups: %d", rolledBackRows)
	}
	handler.SetRiverClient(riverClient)
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	var generations, jobs int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM refresh_intent_generations
		WHERE (kind = 'refresh_checks' AND refresh_key = 'checks:acme/monolith:8f31c2d')
		   OR (kind = 'refresh_stack' AND refresh_key = 'stack:acme/monolith:142')
	`).Scan(&generations); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM river_job
		WHERE kind IN ('refresh_checks', 'refresh_stack')
		  AND args->>'key' LIKE '%acme/monolith%'
	`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if generations != 2 || jobs != 2 {
		t.Fatalf("transactional followups generations=%d jobs=%d", generations, jobs)
	}
}

func TestBatchObservationLockBlocksConcurrentWorkerAndCommitsFollowupGeneration(
	t *testing.T,
) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
		fakegithub.WithResponseDelay(200*time.Millisecond),
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	resolveRequest := queue.RefreshRequest{
		Args: queue.NewResolveStackMembershipArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	fake.ScriptNotFound(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
		1,
	)
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	tombstoned, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !tombstoned.TombstonedAt.Valid {
		t.Fatal("pre-race authoritative 404 did not tombstone PR")
	}
	baselineREST := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
	)
	refreshRequest := queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- handler.RefreshPR(ctx, refreshRequest)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for fake.RequestCount(http.MethodPost, "/graphql") == 0 &&
		time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.RequestCount(http.MethodPost, "/graphql") == 0 {
		t.Fatal("GraphQL batch did not reach paused fake")
	}
	// The fake snapshots source truth after its configured delay, so this
	// equal-updated_at/head change lands while the authoritative request is in
	// flight and must resurrect the preceding tombstone.
	fixture.PullRequests[1].Head.SHA = "equal-time-new-head"
	fixture.PullRequests[1].Title = "equal timestamp concurrent truth"
	fake.SetFixture(fixture)
	resolveDone := make(chan error, 1)
	go func() {
		resolveDone <- handler.ResolveStackMembership(ctx, resolveRequest)
	}()
	time.Sleep(50 * time.Millisecond)
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
	); got != baselineREST {
		t.Fatalf(
			"concurrent REST worker fetched while batch held lock: %d -> %d",
			baselineREST,
			got,
		)
	}
	if err := <-refreshDone; err != nil {
		t.Fatal(err)
	}
	if err := <-resolveDone; err != nil {
		t.Fatal(err)
	}
	var generation int64
	if err := pool.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_checks'
		  AND refresh_key = 'checks:acme/monolith:equal-time-new-head'
	`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation < 1 {
		t.Fatalf("new-head follow-up generation = %d", generation)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.HeadSha != "equal-time-new-head" ||
		row.Title != "equal timestamp concurrent truth" {
		t.Fatalf("concurrent final PR = %+v", row)
	}
}

func TestPR404CreatesTombstoneAndEvent(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		100*time.Millisecond,
		100,
	)
	handler.SetRiverClient(riverClient)
	request := queue.RefreshRequest{
		Args: queue.NewResolveStackMembershipArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	path := "/repos/acme/monolith/pulls/4812"
	fake.ScriptNotFound(http.MethodGet, path, 1)
	if err := handler.ResolveStackMembership(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !row.TombstonedAt.Valid {
		t.Fatalf("PR was hard-deleted or left live: %+v", row)
	}
	var events int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM change_events
		WHERE kind = 'pull_request.tombstoned'
		  AND entity_key = 'pr:1:1001:4812'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("tombstone events = %d, want 1", events)
	}
	if got := fake.RequestCount(http.MethodGet, path); got != 2 {
		t.Fatalf("PR fetches = %d, want initial + scripted 404", got)
	}
	server.Close()
}

func TestRepositoryRulesLockedCASDirtyEventAndConditionalRecheck(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	stackRequest := queue.RefreshRequest{
		Args: queue.NewRefreshStackArgs(
			"stack:acme/monolith:142",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshStack(ctx, stackRequest); err != nil {
		t.Fatal(err)
	}
	rulesRequest := queue.RefreshRequest{
		Args: queue.NewRefreshRepoRulesArgs(
			"repo_rules:acme/monolith:rules",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshRepoRules(ctx, rulesRequest); err != nil {
		t.Fatal(err)
	}
	var ruleCount, eventCount int
	var firstChecked time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*), max(last_checked_at)
		FROM repo_rules
	`).Scan(&ruleCount, &firstChecked); err != nil {
		t.Fatal(err)
	}
	if ruleCount != 1 {
		t.Fatalf("repository rules = %d, want 1", ruleCount)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM change_events
		WHERE kind = 'repo_rules.changed'
		  AND entity_key = 'repo_rules:1:1001'
	`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("repository rule events = %d, want 1", eventCount)
	}
	if err := handler.RefreshRepoRules(ctx, rulesRequest); err != nil {
		t.Fatal(err)
	}
	var secondChecked time.Time
	if err := pool.QueryRow(ctx, `
		SELECT max(last_checked_at)
		FROM repo_rules
	`).Scan(&secondChecked); err != nil {
		t.Fatal(err)
	}
	if secondChecked.Before(firstChecked) {
		t.Fatalf(
			"conditional recheck moved checked_at backwards: %s -> %s",
			firstChecked,
			secondChecked,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'repo_rules.changed'
	`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("304 emitted repository rule event; count = %d", eventCount)
	}
	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM derivation_dirty
		WHERE scope_key = 'stack:1:1001:142'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("repository rule dirty scope count = %d, want 1", dirty)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/rulesets",
	); got != 2 {
		t.Fatalf("repository rules fetches = %d, want 2", got)
	}
}

func TestPRETagSurvivesGraphQLAndChecksRecheckUses304(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	prKey := "pr:acme/monolith:4812"
	resolveRequest := queue.RefreshRequest{
		Args:  queue.NewResolveStackMembershipArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	beforeGraphQL, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if beforeGraphQL.Etag == "" {
		t.Fatal("REST PR refresh stored an empty ETag")
	}
	if err := handler.RefreshPR(ctx, queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	afterGraphQL, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterGraphQL.Etag != beforeGraphQL.Etag {
		t.Fatalf(
			"GraphQL gang changed PR ETag %q -> %q",
			beforeGraphQL.Etag,
			afterGraphQL.Etag,
		)
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	prPath := "/repos/acme/monolith/pulls/4812"
	if got := fake.NotModifiedCount(http.MethodGet, prPath); got != 1 {
		t.Fatalf("conditional PR 304s = %d, want 1", got)
	}

	checksKey := "checks:acme/monolith:8f31c2d"
	checksRequest := queue.RefreshRequest{
		Args:  queue.NewRefreshChecksArgs(checksKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshChecks(ctx, checksRequest); err != nil {
		t.Fatal(err)
	}
	var runCount, emptyETags, historyBefore, eventsBefore int
	var syncedBefore, checkedBefore time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE etag = ''),
		       max(synced_at),
		       max(last_checked_at)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(
		&runCount,
		&emptyETags,
		&syncedBefore,
		&checkedBefore,
	); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 || emptyETags != 0 {
		t.Fatalf(
			"stored checks rows=%d empty_etags=%d, want 2/0",
			runCount,
			emptyETags,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&historyBefore); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:1001:8f31c2d'
	`).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshChecks(ctx, checksRequest); err != nil {
		t.Fatal(err)
	}
	checksPath := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	if got := fake.NotModifiedCount(http.MethodGet, checksPath); got != 1 {
		t.Fatalf("conditional checks 304s = %d, want 1", got)
	}
	var historyAfter, eventsAfter, tombstoned int
	var syncedAfter, checkedAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT max(synced_at),
		       max(last_checked_at),
		       count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&syncedAfter, &checkedAfter, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&historyAfter); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:1001:8f31c2d'
	`).Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if !syncedAfter.Equal(syncedBefore) ||
		!checkedAfter.After(checkedBefore) ||
		historyAfter != historyBefore ||
		eventsAfter != eventsBefore ||
		tombstoned != 0 {
		t.Fatalf(
			"checks 304 synced=%s->%s checked=%s->%s history=%d->%d events=%d->%d tombstoned=%d",
			syncedBefore,
			syncedAfter,
			checkedBefore,
			checkedAfter,
			historyBefore,
			historyAfter,
			eventsBefore,
			eventsAfter,
			tombstoned,
		)
	}
}

func TestChecksMidPagination404DoesNotReplaceOrTombstone(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	template := fixture.CheckRuns[0]
	fixture.CheckRuns = make([]fakegithub.CheckRun, 101)
	for index := range fixture.CheckRuns {
		run := template
		run.ID = int64(100_000 + index)
		run.NodeID = fmt.Sprintf("CR_page_%03d", index)
		run.Name = fmt.Sprintf("check-%03d", index)
		fixture.CheckRuns[index] = run
	}
	fake, server, handler, _ := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	ctx := context.Background()
	request := queue.RefreshRequest{
		Args: queue.NewRefreshChecksArgs(
			"checks:acme/monolith:8f31c2d",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshChecks(ctx, request); err != nil {
		t.Fatal(err)
	}
	path := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	baseline := fake.RequestCount(http.MethodGet, path)
	if baseline != 2 {
		t.Fatalf("initial checks pages = %d, want 2", baseline)
	}
	fixture.CheckRuns[0].Status = "in_progress"
	fixture.CheckRuns[0].Conclusion = ""
	fixture.CheckRuns[0].CompletedAt = nil
	fake.SetFixture(fixture)
	fake.ScriptNotFoundOnRequest(
		http.MethodGet,
		path,
		baseline+2,
	)
	err := handler.RefreshChecks(ctx, request)
	if err == nil ||
		!strings.Contains(err.Error(), "page 2") ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("mid-pagination 404 error = %v", err)
	}
	var total, live, tombstoned, history int
	var firstStatus string
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE tombstoned_at IS NULL),
		       count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&total, &live, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT status FROM check_runs WHERE gh_id = 100000
	`).Scan(&firstStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if total != 101 ||
		live != 101 ||
		tombstoned != 0 ||
		firstStatus != "completed" ||
		history != 101 {
		t.Fatalf(
			"post-404 checks total/live/tombstoned=%d/%d/%d first_status=%q history=%d",
			total,
			live,
			tombstoned,
			firstStatus,
			history,
		)
	}

	fake.ScriptNotFoundOnRequest(
		http.MethodGet,
		path,
		fake.RequestCount(http.MethodGet, path)+1,
	)
	if err := handler.RefreshChecks(ctx, request); err != nil {
		t.Fatalf("entry-listing 404: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&tombstoned); err != nil {
		t.Fatal(err)
	}
	if tombstoned != 101 {
		t.Fatalf(
			"entry-listing 404 tombstoned %d checks, want 101",
			tombstoned,
		)
	}
}

func TestBackfillResumesFromDurableCursor(t *testing.T) {
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		100*time.Millisecond,
		2,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := riverClient.Start(ctx); err != nil {
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
	cursor, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Phase != "repository" {
		t.Fatalf("initial cursor = %+v", cursor)
	}
	deadline := time.Now().Add(20 * time.Second)
	var completed dbgen.BackfillCursor
	for time.Now().Before(deadline) {
		completed, err = dbgen.New(pool).GetBackfillCursor(
			ctx,
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if completed.Phase != "done" || !completed.CompletedAt.Valid {
		t.Fatalf("backfill did not complete after children: %+v", completed)
	}
	resumed, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Phase != "done" || !resumed.CompletedAt.Valid {
		t.Fatalf("resume cursor = %+v, want completed", resumed)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith",
	); got != 1 {
		t.Fatalf("repository fetches = %d, want 1 across restart", got)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/stacks",
	); got != 1 {
		t.Fatalf("stack list fetches = %d, want 1", got)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf(
			"PR list fetches = %d, want two durable overlap passes",
			got,
		)
	}
	var refreshes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM (
			SELECT DISTINCT kind, args->>'key'
			FROM river_job
			WHERE kind IN ('refresh_pr', 'refresh_stack')
			  AND args->>'key' LIKE '%:acme/monolith:%'
		) AS distinct_refreshes
	`).Scan(&refreshes); err != nil {
		t.Fatal(err)
	}
	if refreshes != 5 {
		t.Fatalf("backfill refresh jobs = %d, want 1 stack + 4 open PRs", refreshes)
	}
	var backfilledPRs, emptyPRETags int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE pull_requests.etag = '')
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.full_name = 'acme/monolith'
	`).Scan(&backfilledPRs, &emptyPRETags); err != nil {
		t.Fatal(err)
	}
	if backfilledPRs != 5 || emptyPRETags != 0 {
		t.Fatalf(
			"backfilled PR rows/empty ETags = %d/%d, want 5/0",
			backfilledPRs,
			emptyPRETags,
		)
	}
	var pendingChildren int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM backfill_children
		WHERE installation_id = 1
		  AND repo_full_name = 'acme/monolith'
		  AND completed_at IS NULL
	`).Scan(&pendingChildren); err != nil {
		t.Fatal(err)
	}
	if pendingChildren != 0 {
		t.Fatalf("pending backfill children = %d, want 0", pendingChildren)
	}
}

func TestInstallationBackfillEnumeratesAndWaitsForRepoChildren(t *testing.T) {
	pool := fetchTestDatabase(t)
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := riverClient.Start(ctx); err != nil {
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
	if _, err := StartInstallationBackfill(ctx, pool, riverClient, 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var cursor dbgen.InstallationBackfillCursor
	var err error
	for time.Now().Before(deadline) {
		cursor, err = dbgen.New(pool).GetInstallationBackfillCursor(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if cursor.Phase != "done" || !cursor.CompletedAt.Valid {
		t.Fatalf("installation backfill did not complete: %+v", cursor)
	}
	repoCursor, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if repoCursor.Phase != "done" || !repoCursor.CompletedAt.Valid {
		t.Fatalf("repository child cursor = %+v", repoCursor)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/installation/repositories",
	); got != 1 {
		t.Fatalf("installation repository pages = %d, want 1", got)
	}
}

func TestBackfillStableCreatedSnapshotSurvivesMidScanUpdate(t *testing.T) {
	pool := fetchTestDatabase(t)
	var mutated atomic.Bool
	hook := fakegithub.WithRequestHook(func(
		method string,
		path string,
		count int,
		fixture *fakegithub.Fixture,
	) {
		if method != http.MethodGet ||
			path != "/repos/acme/monolith/pulls" ||
			count != 3 {
			return
		}
		fixture.PullRequests[1].UpdatedAt =
			fixture.PullRequests[1].UpdatedAt.Add(24 * time.Hour)
		mutated.Store(true)
	})
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
		hook,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := riverClient.Start(ctx); err != nil {
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
	if _, err := StartBackfill(
		ctx,
		pool,
		riverClient,
		1,
		"acme/monolith",
	); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cursor, err := dbgen.New(pool).GetBackfillCursor(
			ctx,
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !mutated.Load() {
		t.Fatal("mid-scan fake mutation did not run")
	}
	var discovered int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM backfill_children
		WHERE installation_id = 1
		  AND repo_full_name = 'acme/monolith'
		  AND kind = 'refresh_pr'
	`).Scan(&discovered); err != nil {
		t.Fatal(err)
	}
	if discovered != 4 {
		t.Fatalf(
			"stable snapshot discovered %d open PRs after mutation, want 4",
			discovered,
		)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf(
			"pull list calls = %d, want two durable overlap passes",
			got,
		)
	}
}

func TestBackfillCancelMidScanResumesFromDurablePage(t *testing.T) {
	pool := fetchTestDatabase(t)
	var pagesMu sync.Mutex
	var pages []int
	cancelPageTwo := make(chan context.CancelFunc, 1)
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet &&
				r.URL.Path == "/repos/acme/monolith/pulls" {
				page, err := strconv.Atoi(r.URL.Query().Get("page"))
				if err != nil {
					http.Error(w, "invalid page", http.StatusBadRequest)
					return
				}
				pagesMu.Lock()
				pages = append(pages, page)
				pagesMu.Unlock()
				if page == 2 {
					select {
					case cancel := <-cancelPageTwo:
						cancel()
						<-r.Context().Done()
						return
					default:
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	fake, server, handler, riverClient := newDirectHandlerWithMiddleware(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
		middleware,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	if _, err := StartBackfill(
		ctx,
		pool,
		riverClient,
		1,
		"acme/monolith",
	); err != nil {
		t.Fatal(err)
	}
	for _, args := range []queue.BackfillRepoPageArgs{
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"repository",
			1,
		),
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"stacks",
			1,
		),
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"pull_requests",
			1,
		),
	} {
		if err := handler.BackfillRepoPage(ctx, args); err != nil {
			t.Fatal(err)
		}
	}
	midScan, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	apiPage, passNewCount, _, err := decodePullBackfillCursor(
		int(midScan.Page),
	)
	if err != nil {
		t.Fatal(err)
	}
	if midScan.Phase != "pull_requests" ||
		apiPage != 2 ||
		passNewCount != 2 {
		t.Fatalf(
			"mid-scan cursor = %+v decoded page=%d pass_new_count=%d",
			midScan,
			apiPage,
			passNewCount,
		)
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelPageTwo <- cancel
	err = handler.BackfillRepoPage(
		cancelCtx,
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"pull_requests",
			int(midScan.Page),
		),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled mid-scan page error = %v", err)
	}
	afterCancel, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterCancel.Page != midScan.Page ||
		afterCancel.Phase != midScan.Phase {
		t.Fatalf(
			"cancel changed durable cursor: before=%+v after=%+v",
			midScan,
			afterCancel,
		)
	}
	events, unsubscribe := riverClient.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
	)
	defer unsubscribe()
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
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
	var completed dbgen.BackfillCursor
	for {
		completed, err = dbgen.New(pool).GetBackfillCursor(
			t.Context(),
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Phase == "done" {
			break
		}
		select {
		case <-events:
		case <-t.Context().Done():
			t.Fatalf("resumed backfill did not complete: %+v", completed)
		}
	}
	pagesMu.Lock()
	gotPages := append([]int(nil), pages...)
	pagesMu.Unlock()
	wantPages := []int{1, 2, 2, 1, 2}
	if !reflect.DeepEqual(gotPages, wantPages) {
		t.Fatalf(
			"pull page requests after restart = %v, want %v",
			gotPages,
			wantPages,
		)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf("successful pull page requests = %d, want 4", got)
	}
}

func TestPipelineWaitIdleIncludesKeylessBackfillJobs(t *testing.T) {
	harness := newPipelineHarness(t, "acme/keyless-idle")
	defer harness.close()
	if _, err := StartBackfill(
		t.Context(),
		harness.pool,
		harness.river,
		1,
		harness.repo,
	); err != nil {
		t.Fatal(err)
	}
	harness.waitIdle()
	cursor, err := dbgen.New(harness.pool).GetBackfillCursor(
		t.Context(),
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   harness.repo,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Phase != "done" || !cursor.CompletedAt.Valid {
		t.Fatalf("waitIdle returned before keyless backfill completed: %+v", cursor)
	}
}

func TestOrderIndependenceFinalCacheState(t *testing.T) {
	want := expectedOrderCacheSnapshot()
	for run := 0; run < 4; run++ {
		repo := "acme/order"
		harness := newPipelineHarness(t, repo)
		random := rand.New(rand.NewSource(int64(900 + run))) //nolint:gosec
		events := []pipelineEvent{
			{
				event: "pull_request",
				payload: map[string]any{
					"action": "synchronize",
					"number": 4812,
					"pull_request": map[string]any{
						"number": 4812,
						"stack":  map[string]any{"number": 142},
					},
				},
			},
			{
				event: "check_run",
				payload: map[string]any{
					"action":    "completed",
					"check_run": map[string]any{"head_sha": "8f31c2d"},
				},
			},
			{
				event: "push",
				payload: map[string]any{
					"ref":   "refs/heads/refactor/bm25f-ranker",
					"stack": map[string]any{"number": 142},
				},
			},
		}
		random.Shuffle(len(events), func(i, j int) {
			events[i], events[j] = events[j], events[i]
		})
		for index, event := range events {
			harness.emit(
				fmt.Sprintf("order-%d-%d", run, index),
				event,
			)
			if random.Intn(2) == 0 {
				harness.emit(
					fmt.Sprintf("order-%d-%d-duplicate", run, index),
					event,
				)
			}
		}
		harness.dispatchAll()
		harness.waitIdle()
		got := snapshotCache(t, harness.pool, repo)
		harness.close()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf(
				"run %d final cache differs\n got: %+v\nwant: %+v",
				run,
				got,
				want,
			)
		}
	}
}

func expectedOrderCacheSnapshot() cacheSnapshot {
	// This golden is deliberately authored independently of fakegithub.Fixture
	// and snapshotCache. It prevents an identically empty or consistently
	// malformed implementation from satisfying C-I4 by self-comparison.
	return cacheSnapshot{
		Repos:        `[{"gh_id": 2001, "node_id": "R_acme_order", "archived": false, "head_sha": "", "full_name": "acme/order", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T12:00:00Z", "default_branch": "main"}]`,
		RepoRules:    `[]`,
		Stacks:       `[{"open": true, "gh_id": 9876543, "number": 142, "entries": [{"draft": false, "state": "closed", "number": 4810, "head_ref": "refactor/tokenizer", "head_sha": "bbbb001", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4812, "head_ref": "refactor/bm25f-ranker", "head_sha": "8f31c2d", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4815, "head_ref": "feat/relevance-debug", "head_sha": "bbbb003", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4816, "head_ref": "feat/results-rewire", "head_sha": "bbbb004", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4820, "head_ref": "feat/relevance-telemetry", "head_sha": "bbbb005", "updated_at": "2026-07-28T12:00:00Z"}], "node_id": "S_kwDOABCDEF4AAAAA", "base_ref": "main", "base_sha": "aaaa000", "head_sha": "bbbb005", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T12:00:00Z"}]`,
		Pulls:        `[{"gh_id": 804810, "state": "closed", "title": "Tokenizer rewrite for query parser", "number": 4810, "node_id": "PR_kwDOABCDEF4810", "base_ref": "main", "base_sha": "aaaa000", "head_ref": "refactor/tokenizer", "head_sha": "bbbb001", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 1, "review_decision": "APPROVED"}, {"gh_id": 804812, "state": "open", "title": "BM25F ranker integration", "number": 4812, "node_id": "PR_kwDOABCDEF4812", "base_ref": "refactor/tokenizer", "base_sha": "bbbb001", "head_ref": "refactor/bm25f-ranker", "head_sha": "8f31c2d", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 2, "review_decision": "CHANGES_REQUESTED"}, {"gh_id": 804815, "state": "open", "title": "Relevance debug API endpoint", "number": 4815, "node_id": "PR_kwDOABCDEF4815", "base_ref": "refactor/bm25f-ranker", "base_sha": "8f31c2d", "head_ref": "feat/relevance-debug", "head_sha": "bbbb003", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 3, "review_decision": "REVIEW_REQUIRED"}, {"gh_id": 804816, "state": "open", "title": "Results page rewiring", "number": 4816, "node_id": "PR_kwDOABCDEF4816", "base_ref": "feat/relevance-debug", "base_sha": "bbbb003", "head_ref": "feat/results-rewire", "head_sha": "bbbb004", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 4, "review_decision": "REVIEW_REQUIRED"}, {"gh_id": 804820, "state": "open", "title": "Relevance telemetry dashboards", "number": 4820, "node_id": "PR_kwDOABCDEF4820", "base_ref": "feat/results-rewire", "base_sha": "bbbb004", "head_ref": "feat/relevance-telemetry", "head_sha": "bbbb005", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 5, "review_decision": "REVIEW_REQUIRED"}]`,
		Threads:      `[]`,
		Checks:       `[{"name": "unit", "gh_id": 99001, "status": "completed", "head_sha": "8f31c2d", "conclusion": "failure", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z"}, {"name": "lint", "gh_id": 99002, "status": "completed", "head_sha": "8f31c2d", "conclusion": "success", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z"}]`,
		CheckHistory: `[{"name": "unit", "status": "completed", "head_sha": "8f31c2d", "conclusion": "failure", "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z", "check_run_gh_id": 99001}, {"name": "lint", "status": "completed", "head_sha": "8f31c2d", "conclusion": "success", "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z", "check_run_gh_id": 99002}]`,
		Dirty:        `["pr:1:2001:4810", "pr:1:2001:4812", "pr:1:2001:4815", "pr:1:2001:4816", "pr:1:2001:4820", "stack:1:2001:142"]`,
	}
}

func TestStormAssertsFetchCount(t *testing.T) {
	harness := newPipelineHarness(t, "acme/storm")
	defer harness.close()
	warm := pipelineEvent{
		event: "push",
		payload: map[string]any{
			"ref":   "refs/heads/refactor/bm25f-ranker",
			"stack": map[string]any{"number": 142},
		},
	}
	harness.emit("storm-warm", warm)
	harness.dispatchAll()
	harness.waitIdle()
	path := "/repos/acme/storm/stacks/142"
	baseline := harness.fake.RequestCount(http.MethodGet, path)

	for index := 0; index < 20; index++ {
		harness.emit(fmt.Sprintf("storm-%02d", index), warm)
	}
	harness.dispatchAll()
	harness.waitIdle()
	delta := harness.fake.RequestCount(http.MethodGet, path) - baseline
	if delta != 1 {
		t.Fatalf(
			"20-event storm caused %d stack fetches, want one coalesced fetch",
			delta,
		)
	}
}

type pipelineEvent struct {
	event   string
	payload map[string]any
}

type pipelineHarness struct {
	t          *testing.T
	repo       string
	pool       *pgxpool.Pool
	fake       *fakegithub.Server
	fakeServer *httptest.Server
	ingress    *httptest.Server
	river      *river.Client[pgx.Tx]
	dispatcher *dispatch.Dispatcher
	cancel     context.CancelFunc
}

func newPipelineHarness(t *testing.T, repo string) *pipelineHarness {
	t.Helper()
	pool := fetchTestDatabase(t)
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	fake := fakegithub.New(fixture, fetchTestSecret)
	fakeServer := httptest.NewServer(fake)
	gate := budget.New(fakeServer.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		fakeServer.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		fakeServer.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(Options{
		Pool:           pool,
		REST:           rest,
		GraphQL:        graphQL,
		InstallationID: 1,
		OrgID:          1,
		BatchWindow:    5 * time.Millisecond,
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
	ctx, cancel := context.WithCancel(context.Background())
	if err := riverClient.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	ingressServer := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			fetchTestSecret,
			1<<20,
			5*time.Second,
		),
	))
	dispatcher, err := dispatch.New(pool, riverClient, dispatch.Config{
		BatchSize:    100,
		MaxAttempts:  3,
		Debounce:     time.Millisecond,
		PollInterval: time.Millisecond,
		Now:          time.Now,
		Classifier:   dispatch.DefaultClassifier(),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return &pipelineHarness{
		t:          t,
		repo:       repo,
		pool:       pool,
		fake:       fake,
		fakeServer: fakeServer,
		ingress:    ingressServer,
		river:      riverClient,
		dispatcher: dispatcher,
		cancel:     cancel,
	}
}

func (h *pipelineHarness) emit(guid string, event pipelineEvent) {
	h.t.Helper()
	payload := clonePayload(h.t, event.payload)
	payload["repository"] = map[string]any{"full_name": h.repo}
	if _, err := h.fake.EmitWebhookWithGUID(
		context.Background(),
		h.ingress.URL+ingress.WebhookPath,
		event.event,
		guid,
		payload,
	); err != nil {
		h.t.Fatal(err)
	}
}

func (h *pipelineHarness) dispatchAll() {
	h.t.Helper()
	for {
		count, err := h.dispatcher.DispatchBatch(context.Background())
		if err != nil {
			h.t.Fatal(err)
		}
		if count == 0 {
			return
		}
	}
}

func (h *pipelineHarness) waitIdle() {
	h.t.Helper()
	events, unsubscribe := h.river.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
	)
	defer unsubscribe()
	for {
		var count int
		if err := h.pool.QueryRow(h.t.Context(), `
			SELECT count(*)
			FROM river_job
			WHERE state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
		`).Scan(&count); err != nil {
			h.t.Fatal(err)
		}
		if count == 0 {
			return
		}
		select {
		case <-events:
		case <-h.t.Context().Done():
			var states string
			_ = h.pool.QueryRow(context.Background(), `
				SELECT string_agg(
					kind || ':' || state,
					', ' ORDER BY kind, state
				)
				FROM river_job
				WHERE state IN (
					'available', 'pending', 'retryable',
					'running', 'scheduled'
				)
			`).Scan(&states)
			h.t.Fatalf("pipeline did not quiesce: %s", states)
		}
	}
}

func (h *pipelineHarness) close() {
	h.ingress.Close()
	h.cancel()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.river.StopAndCancel(stopCtx)
	h.fakeServer.Close()
}

type cacheSnapshot struct {
	Repos        string
	RepoRules    string
	Stacks       string
	Pulls        string
	Threads      string
	Checks       string
	CheckHistory string
	Dirty        string
}

func snapshotCache(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
) cacheSnapshot {
	t.Helper()
	return cacheSnapshot{
		Repos: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data)), '[]'::jsonb)
			FROM (
				SELECT gh_id, node_id, full_name, default_branch, archived,
				       to_char(gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       head_sha, sync_source,
				       tombstoned_at IS NOT NULL AS tombstoned
				FROM repos WHERE full_name = $1
			) AS row_data
		`, repo),
		RepoRules: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY rule_key),
			                '[]'::jsonb)
			FROM (
				SELECT repo_rules.rule_key, repo_rules.rule,
				       to_char(repo_rules.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       repo_rules.head_sha, repo_rules.sync_source,
				       repo_rules.tombstoned_at IS NOT NULL AS tombstoned
				FROM repo_rules JOIN repos ON repos.id = repo_rules.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Stacks: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY number), '[]'::jsonb)
			FROM (
				SELECT stacks.number, stacks.gh_id, stacks.node_id,
				       stacks.base_ref, stacks.base_sha, stacks.open,
				       stacks.entries,
				       to_char(stacks.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       stacks.head_sha,
				       stacks.sync_source,
				       stacks.tombstoned_at IS NOT NULL AS tombstoned
				FROM stacks JOIN repos ON repos.id = stacks.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Pulls: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY number), '[]'::jsonb)
			FROM (
				SELECT pull_requests.number, pull_requests.gh_id,
				       pull_requests.node_id, pull_requests.title,
				       pull_requests.state, pull_requests.head_ref,
				       pull_requests.head_sha, pull_requests.base_ref,
				       pull_requests.base_sha, pull_requests.review_decision,
				       pull_requests.stack_number, pull_requests.stack_position,
				       to_char(pull_requests.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       pull_requests.sync_source,
				       pull_requests.tombstoned_at IS NOT NULL AS tombstoned
				FROM pull_requests JOIN repos ON repos.id = pull_requests.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Threads: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY id), '[]'::jsonb)
			FROM (
				SELECT review_threads.id, review_threads.pr_number,
				       review_threads.is_resolved, review_threads.is_outdated,
				       review_threads.path, review_threads.line,
				       review_threads.comments,
				       to_char(review_threads.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       review_threads.head_sha, review_threads.sync_source,
				       review_threads.tombstoned_at IS NOT NULL AS tombstoned
				FROM review_threads JOIN repos ON repos.id = review_threads.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Checks: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY gh_id), '[]'::jsonb)
			FROM (
				SELECT check_runs.gh_id, check_runs.name, check_runs.status,
				       check_runs.conclusion, check_runs.head_sha,
				       to_char(check_runs.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       check_runs.sync_source,
				       check_runs.tombstoned_at IS NOT NULL AS tombstoned
				FROM check_runs JOIN repos ON repos.id = check_runs.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		CheckHistory: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data)
				ORDER BY check_run_gh_id, status, conclusion), '[]'::jsonb)
			FROM (
				SELECT check_history.check_run_gh_id, check_history.name,
				       check_history.status, check_history.conclusion,
				       to_char(check_history.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       check_history.head_sha,
				       check_history.sync_source
				FROM check_history JOIN repos ON repos.id = check_history.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Dirty: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(scope_key ORDER BY scope_key), '[]'::jsonb)
			FROM derivation_dirty
			WHERE scope_key LIKE (
				SELECT '%:' || installation_id::text || ':' ||
				       gh_id::text || ':%'
				FROM repos WHERE full_name = $1
			)
		`, repo),
	}
}

func queryJSON(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	arg string,
) string {
	t.Helper()
	var value []byte
	if err := pool.QueryRow(context.Background(), query, arg).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func newDirectHandler(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture fakegithub.Fixture,
	batchWindow time.Duration,
	pageSize int,
	fakeOptions ...fakegithub.Option,
) (
	*fakegithub.Server,
	*httptest.Server,
	*Handler,
	*river.Client[pgx.Tx],
) {
	return newDirectHandlerWithMiddleware(
		t,
		pool,
		fixture,
		batchWindow,
		pageSize,
		nil,
		fakeOptions...,
	)
}

func newDirectHandlerWithMiddleware(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture fakegithub.Fixture,
	batchWindow time.Duration,
	pageSize int,
	middleware func(http.Handler) http.Handler,
	fakeOptions ...fakegithub.Option,
) (
	*fakegithub.Server,
	*httptest.Server,
	*Handler,
	*river.Client[pgx.Tx],
) {
	t.Helper()
	fake := fakegithub.New(fixture, fetchTestSecret, fakeOptions...)
	var serverHandler http.Handler = fake
	if middleware != nil {
		serverHandler = middleware(serverHandler)
	}
	server := httptest.NewServer(serverHandler)
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(Options{
		Pool:             pool,
		REST:             rest,
		GraphQL:          graphQL,
		InstallationID:   1,
		OrgID:            1,
		BatchWindow:      batchWindow,
		BackfillPageSize: pageSize,
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
	return fake, server, handler, riverClient
}

func fixtureForRepo(fixture fakegithub.Fixture, fullName string) fakegithub.Fixture {
	owner, name, _ := strings.Cut(fullName, "/")
	fixture.Owner = owner
	fixture.Repo = name
	fixture.Repository.Owner = owner
	fixture.Repository.Name = name
	fixture.Repository.FullName = fullName
	fixture.Repository.ID += int64(len(fullName) * 100)
	fixture.Repository.NodeID = fmt.Sprintf("R_%s_%s", owner, name)
	return fixture
}

func testRepository(
	fullName string,
	id int64,
	updatedAt time.Time,
) store.RepositoryRecord {
	owner, name, _ := strings.Cut(fullName, "/")
	return store.RepositoryRecord{
		InstallationID:  1,
		OrgID:           1,
		GitHubID:        id,
		NodeID:          fmt.Sprintf("repo-node-%d", id),
		Owner:           owner,
		Name:            name,
		FullName:        fullName,
		DefaultBranch:   "main",
		DefaultHeadSHA:  "base",
		GitHubUpdatedAt: updatedAt,
	}
}

func testPull(
	repository store.RepositoryRecord,
	updatedAt time.Time,
	headSHA string,
) store.PullRequestRecord {
	return store.PullRequestRecord{
		Repository:      repository,
		GitHubID:        4200,
		NodeID:          "pr-node-42",
		Number:          42,
		Title:           "race",
		State:           "open",
		HeadRef:         "feature",
		HeadSHA:         headSHA,
		BaseRef:         "main",
		BaseSHA:         "base",
		MembershipKnown: true,
		GitHubUpdatedAt: updatedAt,
		SyncedAt:        updatedAt.Add(time.Second),
		Source:          store.SyncSourceWebhook,
	}
}

func clonePayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func toGHPullRequest(
	t *testing.T,
	pull fakegithub.PullRequest,
) *gh.PullRequest {
	t.Helper()
	encoded, err := json.Marshal(pull)
	if err != nil {
		t.Fatal(err)
	}
	var converted gh.PullRequest
	if err := json.Unmarshal(encoded, &converted); err != nil {
		t.Fatal(err)
	}
	return &converted
}

func fetchTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := store.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	schema := fmt.Sprintf("ghsync_fetch_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		pool.Close()
		admin.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(
			dropCtx,
			"DROP SCHEMA "+identifier+" CASCADE",
		); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		admin.Close()
	})
	return pool
}
