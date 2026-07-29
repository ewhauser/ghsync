package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/dispatch"
	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/ingress"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
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
		  AND entity_key = 'pr:acme/monolith:4812'
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
	ctx := context.Background()
	cursor, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	first := queue.NewBackfillRepoPageArgs(
		1,
		"acme/monolith",
		cursor.Phase,
		int(cursor.Page),
	)
	if err := handler.BackfillRepoPage(ctx, first); err != nil {
		t.Fatal(err)
	}
	// A crashed/retried copy with the stale expected cursor is a no-op.
	if err := handler.BackfillRepoPage(ctx, first); err != nil {
		t.Fatal(err)
	}
	resumed, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Phase != "stacks" || resumed.Page != 1 {
		t.Fatalf("resume cursor = %+v, want stacks page 1", resumed)
	}
	for {
		current, err := dbgen.New(pool).GetBackfillCursor(
			ctx,
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if current.Phase == "done" {
			break
		}
		if err := handler.BackfillRepoPage(
			ctx,
			queue.NewBackfillRepoPageArgs(
				1,
				"acme/monolith",
				current.Phase,
				int(current.Page),
			),
		); err != nil {
			t.Fatal(err)
		}
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
	); got != 2 {
		t.Fatalf("PR list fetches = %d, want two resumable pages", got)
	}
	var refreshes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM river_job
		WHERE kind IN ('refresh_pr', 'refresh_stack')
		  AND args->>'key' LIKE '%:acme/monolith:%'
	`).Scan(&refreshes); err != nil {
		t.Fatal(err)
	}
	if refreshes != 5 {
		t.Fatalf("backfill refresh jobs = %d, want 1 stack + 4 open PRs", refreshes)
	}
}

func TestOrderIndependenceFinalCacheState(t *testing.T) {
	var want cacheSnapshot
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
		if run == 0 {
			want = got
			continue
		}
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
		gh.StaticToken("test-token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		fakeServer.URL,
		gate,
		gh.StaticToken("test-token"),
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
	ingressServer := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(pool),
			fetchTestSecret,
			1<<20,
			5*time.Second,
		).Mux(),
	)
	dispatcher := dispatch.New(pool, riverClient, dispatch.Config{
		BatchSize:    100,
		MaxAttempts:  3,
		Debounce:     time.Millisecond,
		PollInterval: time.Millisecond,
		Now:          time.Now,
		Classifier:   dispatch.DefaultClassifier(),
	})
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
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := h.pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM river_job
			WHERE state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
			  AND args->>'key' LIKE $1
		`, "%:"+h.repo+":%").Scan(&count); err != nil {
			h.t.Fatal(err)
		}
		if count == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	var states string
	_ = h.pool.QueryRow(context.Background(), `
		SELECT string_agg(kind || ':' || state, ', ' ORDER BY kind, state)
		FROM river_job
		WHERE args->>'key' LIKE $1
	`, "%:"+h.repo+":%").Scan(&states)
	h.t.Fatalf("pipeline did not quiesce: %s", states)
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
				       gh_updated_at, head_sha, sync_source,
				       tombstoned_at IS NOT NULL AS tombstoned
				FROM repos WHERE full_name = $1
			) AS row_data
		`, repo),
		Stacks: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY number), '[]'::jsonb)
			FROM (
				SELECT stacks.number, stacks.gh_id, stacks.node_id,
				       stacks.base_ref, stacks.base_sha, stacks.open,
				       stacks.entries, stacks.gh_updated_at, stacks.head_sha,
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
				       pull_requests.gh_updated_at, pull_requests.sync_source,
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
				       review_threads.comments, review_threads.gh_updated_at,
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
				       check_runs.gh_updated_at, check_runs.sync_source,
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
				       check_history.gh_updated_at, check_history.head_sha,
				       check_history.sync_source
				FROM check_history JOIN repos ON repos.id = check_history.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Dirty: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(scope_key ORDER BY scope_key), '[]'::jsonb)
			FROM derivation_dirty
			WHERE scope_key LIKE $1
		`, "%:"+repo+":%"),
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
) (
	*fakegithub.Server,
	*httptest.Server,
	*Handler,
	*river.Client[pgx.Tx],
) {
	t.Helper()
	fake := fakegithub.New(fixture, fetchTestSecret)
	server := httptest.NewServer(fake)
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("test-token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("test-token"),
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
	riverClient, err := queue.NewClient(pool)
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
	schema := fmt.Sprintf("frontier_fetch_%d", time.Now().UnixNano())
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
