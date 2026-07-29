package drift

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/fetch"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

type findingObserver struct {
	mu         sync.Mutex
	findings   []dbgen.DriftFinding
	persistent []dbgen.DriftFinding
}

func (o *findingObserver) PersistentDivergence(
	_ context.Context,
	finding dbgen.DriftFinding,
) {
	o.mu.Lock()
	o.persistent = append(o.persistent, finding)
	o.mu.Unlock()
}

func (o *findingObserver) Divergence(
	_ context.Context,
	finding dbgen.DriftFinding,
) {
	o.mu.Lock()
	o.findings = append(o.findings, finding)
	o.mu.Unlock()
}

func TestDriftDetectorRecordsDiffAndSelfHealsWithoutWebhook(
	t *testing.T,
) {
	pool := driftTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake := fakegithub.New(fixture, "drift-secret")
	server := httptest.NewServer(fake)
	defer server.Close()
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := fetch.New(fetch.Options{
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
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	var preseedHeartbeats int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'drift'
		  AND operation = 'detector'
	`).Scan(&preseedHeartbeats); err != nil {
		t.Fatal(err)
	}
	if preseedHeartbeats != 0 {
		t.Fatal("pre-backfill drift skip was recorded as a successful pass")
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO installation_backfill_cursors (
		    installation_id, phase, page, completed_at
		) VALUES (1, 'done', 1, clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}

	// Model a sample selected before a concurrent legitimate cache refresh.
	// inspectSample must discard this stale snapshot and reread while holding
	// the same entity observation lock used by refresh writers.
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
	staleSampleFinding, recorded, err := service.inspectSample(
		ctx,
		driftSample{
			EntityKind:    current.EntityKind,
			SourceID:      current.SourceID,
			EntityKey:     current.EntityKey,
			LockKey:       current.LockKey,
			CacheSnapshot: []byte(`{"stale_sample":true}`),
		},
	)
	if err != nil {
		t.Fatal(err)
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
	deadline := time.Now().Add(10 * time.Second)
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
		if time.Now().After(deadline) {
			t.Fatalf(
				"cache producers did not quiesce: jobs=%d generations=%d",
				activeJobs,
				outstandingGenerations,
			)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSemanticDiffNormalizesIDOrderedCollections(t *testing.T) {
	cache := []byte(`{"runs":[{"id":10,"name":"ten"},{"id":2,"name":"two"}]}`)
	upstream := []byte(`{"runs":[{"name":"two","id":2},{"name":"ten","id":10}]}`)
	equal, diff, err := semanticDiff(cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("normalization-stable compare equal=%v diff=%s", equal, diff)
	}
}

func driftTestDatabase(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("frontier_drift_%d", time.Now().UnixNano())
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
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
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
		dropCtx, dropCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
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
