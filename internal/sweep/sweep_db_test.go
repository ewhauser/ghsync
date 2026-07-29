package sweep

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/dispatch"
	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/fetch"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/ingress"
	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

const sweepTestSecret = "sweep-test-secret"

type observerRecorder struct {
	mu         sync.Mutex
	overruns   int
	redelivery int
	incomplete int
}

type simulatedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *simulatedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *simulatedClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type deadlineObserverRecorder struct {
	mu     sync.Mutex
	misses int
}

func (o *deadlineObserverRecorder) RefreshDeadlineMissed(
	context.Context,
	string,
	string,
	time.Time,
	time.Time,
) {
	o.mu.Lock()
	o.misses++
	o.mu.Unlock()
}

type blockSweepArgs struct {
	ID string `json:"id"`
}

func (blockSweepArgs) Kind() string { return "test_block_sweep_queue" }

type blockSweepWorker struct {
	river.WorkerDefaults[blockSweepArgs]
	started chan<- struct{}
	release <-chan struct{}
}

func (w *blockSweepWorker) Work(
	ctx context.Context,
	_ *river.Job[blockSweepArgs],
) error {
	w.started <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.release:
		return nil
	}
}

type simulatedPRFetch struct {
	queue.RefreshHandler
	pool      *pgxpool.Pool
	clock     *simulatedClock
	duration  time.Duration
	completed chan<- time.Time
}

func (h *simulatedPRFetch) RefreshPR(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	h.clock.Advance(h.duration)
	if err := h.RefreshHandler.RefreshPR(ctx, request); err != nil {
		return err
	}
	completedAt := h.clock.Now()
	if _, err := h.pool.Exec(ctx, `
		UPDATE pull_requests
		SET last_checked_at = $1
		WHERE number = 4812
	`, completedAt); err != nil {
		return err
	}
	h.completed <- completedAt
	return nil
}

func (o *observerRecorder) SweepOverrun(
	context.Context,
	string,
	string,
	time.Duration,
) {
	o.mu.Lock()
	o.overruns++
	o.mu.Unlock()
}

func (o *observerRecorder) GapRedelivery(
	context.Context,
	int64,
	string,
) {
	o.mu.Lock()
	o.redelivery++
	o.mu.Unlock()
}

func (o *observerRecorder) GapWindowIncomplete(
	context.Context,
	string,
	int,
) {
	o.mu.Lock()
	o.incomplete++
	o.mu.Unlock()
}

type sweepHarness struct {
	pool       *pgxpool.Pool
	fixture    fakegithub.Fixture
	fake       *fakegithub.Server
	server     *httptest.Server
	rest       *gh.RESTClient
	handler    *fetch.Handler
	service    *Service
	river      *river.Client[pgx.Tx]
	now        time.Time
	observer   *observerRecorder
	cancel     context.CancelFunc
	riverStart bool
}

func newSweepHarness(
	t *testing.T,
	pageSize int,
) *sweepHarness {
	t.Helper()
	pool := sweepTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	now := time.Now().UTC().Truncate(time.Second)
	appKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fake := fakegithub.New(
		fixture,
		sweepTestSecret,
		fakegithub.WithNow(func() time.Time { return now }),
		fakegithub.WithAppAuthentication(99, &appKey.PublicKey),
	)
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-sweep"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-sweep"),
	)
	if err != nil {
		t.Fatal(err)
	}
	appPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(appKey),
	})
	appTokens, err := gh.NewAppTokens(99, appPEM)
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := gh.NewDeliveriesClient(
		server.URL,
		gate,
		appTokens,
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
	observer := &observerRecorder{}
	service, err := New(Options{
		Pool:       pool,
		REST:       rest,
		Deliveries: deliveries,
		Config: Config{
			InstallationID:        1,
			OpenStackMaxStaleness: 5 * time.Minute,
			OpenPRMaxStaleness:    10 * time.Minute,
			RepoRulesMaxStaleness: time.Hour,
			ClosedMaxStaleness:    24 * time.Hour,
			RepositoryListPeriod:  time.Hour,
			PageSize:              pageSize,
			GapHealPeriod:         5 * time.Minute,
			GapWindow:             6 * time.Hour,
			GapPageSize:           pageSize,
			GapMaxPages:           10,
			RetentionPeriod:       24 * time.Hour,
			RetentionAge:          90 * 24 * time.Hour,
			RetentionBatchSize:    100,
			Now:                   func() time.Time { return now },
			Observer:              observer,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithQueues(
			queue.QueueInteractive,
			queue.QueueEvent,
			queue.QueueSweep,
			queue.QueueReconcile,
			queue.QueuePruner,
		),
		queue.WithRefreshHandler(handler),
		queue.WithWorkerRegistrar(service.RegisterReconciliationWorkers),
		queue.WithWorkerRegistrar(service.RegisterPrunerWorker),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetRiverClient(riverClient)
	service.SetRiverClient(riverClient)
	return &sweepHarness{
		pool:     pool,
		fixture:  fixture,
		fake:     fake,
		server:   server,
		rest:     rest,
		handler:  handler,
		service:  service,
		river:    riverClient,
		now:      now,
		observer: observer,
	}
}

func (h *sweepHarness) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	if err := h.river.Start(ctx); err != nil {
		t.Fatal(err)
	}
	h.riverStart = true
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = h.river.StopAndCancel(stopCtx)
	})
}

func TestC_R1EndToEndBoundIncludesCadenceQueueAndFetch(
	t *testing.T,
) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4812}, false, false)
	ctx := context.Background()
	plan := scheduleForBound(10 * time.Minute)
	if plan.Cadence >= plan.Bound ||
		plan.Cadence+plan.CompletionHeadroom >= plan.Bound {
		t.Fatalf("invalid C-R1 schedule plan: %+v", plan)
	}

	clock := &simulatedClock{now: h.now}
	h.service.config.Now = clock.Now
	lastChecked := h.now.Add(time.Microsecond)
	if _, err := h.pool.Exec(ctx, `
		UPDATE pull_requests
		SET last_checked_at = $1, synced_at = $1
		WHERE number = 4812
	`, lastChecked); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	completed := make(chan time.Time, 1)
	deadlines := &deadlineObserverRecorder{}
	handler := &simulatedPRFetch{
		RefreshHandler: h.handler,
		pool:           h.pool,
		clock:          clock,
		duration:       30 * time.Second,
		completed:      completed,
	}
	riverClient, err := queue.NewClient(
		h.pool,
		queue.WithQueues(queue.QueueSweep),
		queue.WithQueueMaxWorkers(queue.QueueSweep, 1),
		queue.WithRefreshHandler(handler),
		queue.WithDeadlineObserver(deadlines),
		queue.WithNow(clock.Now),
		queue.WithWorkerRegistrar(
			h.service.RegisterReconciliationWorkers,
		),
		queue.WithWorkerRegistrar(func(workers *river.Workers) {
			river.AddWorker(workers, &blockSweepWorker{
				started: blocked,
				release: release,
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	h.handler.SetRiverClient(riverClient)
	h.service.SetRiverClient(riverClient)
	if _, err := riverClient.Insert(
		ctx,
		blockSweepArgs{ID: "saturated"},
		&river.InsertOpts{Queue: queue.QueueSweep},
	); err != nil {
		t.Fatal(err)
	}
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
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep queue did not saturate")
	}

	// The just-after-tick entity is not due at the prior tick.
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	var early int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr'
		  AND refresh_key = 'pr:acme/monolith:4812'
	`).Scan(&early); err != nil {
		t.Fatal(err)
	}
	if early != 0 {
		t.Fatalf("just-after-tick entity was enqueued early: %d", early)
	}

	// At the next cadence, its deadline falls inside cadence + reserved
	// completion headroom and the scheduler must insert it.
	clock.Advance(plan.Cadence)
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	state, err := dbgen.New(h.pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       queue.KindRefreshPR,
			RefreshKey: "pr:acme/monolith:4812",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := lastChecked.Add(plan.Bound)
	if !state.DeadlineAt.Valid ||
		!state.DeadlineAt.Time.Equal(wantDeadline) {
		t.Fatalf(
			"refresh deadline = %v, want %s",
			state.DeadlineAt,
			wantDeadline,
		)
	}

	// Simulate a saturated queue consuming half the reserved headroom, then
	// an authoritative GraphQL fetch consuming another 30 seconds.
	clock.Advance(time.Minute)
	close(release)
	var completedAt time.Time
	select {
	case completedAt = <-completed:
	case <-time.After(10 * time.Second):
		t.Fatal("scheduled PR reconciliation did not complete")
	}
	if completedAt.After(wantDeadline) {
		t.Fatalf(
			"end-to-end freshness completed at %s after deadline %s",
			completedAt,
			wantDeadline,
		)
	}
	deadlines.mu.Lock()
	misses := deadlines.misses
	deadlines.mu.Unlock()
	if misses != 0 {
		t.Fatalf("deadline misses = %d, want 0", misses)
	}
}

func TestList304AdvancesOnlyListMembershipFreshness(t *testing.T) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4812}, false, false)
	ctx := context.Background()
	runListing := func() {
		t.Helper()
		if err := h.service.Kickoff(ctx, KickoffArgs{
			SweepKind:    KindPullRequests,
			Installation: 1,
		}); err != nil {
			t.Fatal(err)
		}
		// Mutable PR discovery requires one overlap pass with no new IDs.
		for range 2 {
			if err := h.service.ReconcilePage(ctx, ListPageArgs{
				SweepKind:    KindPullRequests,
				Installation: 1,
				ScopeKey:     "acme/monolith",
				Cursor:       "1",
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	runListing()

	memberChecked := h.now.Add(-11 * time.Minute)
	if _, err := h.pool.Exec(ctx, `
		UPDATE pull_requests
		SET last_checked_at = $1
		WHERE number = 4812
	`, memberChecked); err != nil {
		t.Fatal(err)
	}
	validatedAt := h.now.Add(time.Minute)
	h.service.config.Now = func() time.Time { return validatedAt }
	runListing()

	var afterMember, listSeen time.Time
	if err := h.pool.QueryRow(ctx, `
		SELECT last_checked_at
		FROM pull_requests
		WHERE number = 4812
	`).Scan(&afterMember); err != nil {
		t.Fatal(err)
	}
	if err := h.pool.QueryRow(ctx, `
		SELECT list_seen_at
		FROM sweep_pages
		WHERE installation_id = 1
		  AND sweep_kind = 'pull_requests'
		  AND scope_key = 'acme/monolith'
		  AND cursor = '1'
	`).Scan(&listSeen); err != nil {
		t.Fatal(err)
	}
	if !afterMember.Equal(memberChecked) {
		t.Fatalf(
			"list 304 advanced member freshness: %s -> %s",
			memberChecked,
			afterMember,
		)
	}
	if !listSeen.Equal(validatedAt) {
		t.Fatalf(
			"list membership freshness = %s, want %s",
			listSeen,
			validatedAt,
		)
	}
}

func TestPullListingOverlapPassRecoversConcurrentPageShift(t *testing.T) {
	h := newSweepHarness(t, 2)
	h.seedCache(t, nil, false, false)
	ctx := context.Background()
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.service.ReconcilePage(ctx, ListPageArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
		ScopeKey:     "acme/monolith",
		Cursor:       "1",
	}); err != nil {
		t.Fatal(err)
	}

	// Removing a page-one entity shifts PR 4816 from page two into an already
	// scanned page. A single mutable page-number pass would skip it.
	fixture := h.fixture
	fixture.PullRequests[1].State = "closed"
	h.fake.SetFixture(fixture)
	for _, cursor := range []string{"2", "1", "2", "1", "2"} {
		if err := h.service.ReconcilePage(ctx, ListPageArgs{
			SweepKind:    KindPullRequests,
			Installation: 1,
			ScopeKey:     "acme/monolith",
			Cursor:       cursor,
		}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := dbgen.New(h.pool).GetSweepCursor(
		ctx,
		dbgen.GetSweepCursorParams{
			InstallationID: 1,
			SweepKind:      KindPullRequests,
			ScopeKey:       "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !state.CompletedAt.Valid {
		t.Fatalf("overlap sweep did not stabilize: %+v", state)
	}
	var seen []string
	if err := h.pool.QueryRow(ctx, `
		SELECT COALESCE(
		    array_agg(entity_key ORDER BY entity_key),
		    ARRAY[]::text[]
		)
		FROM sweep_seen_keys
		WHERE installation_id = 1
		  AND sweep_kind = 'pull_requests'
		  AND scope_key = 'acme/monolith'
	`).Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(seen, "pr:acme/monolith:4816") {
		t.Fatalf("concurrent page shift skipped PR 4816: %v", seen)
	}
}

func TestSweepOnlyRefreshesStaleEntitiesWithinConfiguredBounds(
	t *testing.T,
) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4810, 4812}, true, true)
	ctx := context.Background()
	oldOpenStack := h.now.Add(-5*time.Minute - time.Second)
	oldOpenPR := h.now.Add(-10*time.Minute - time.Second)
	oldRules := h.now.Add(-time.Hour - time.Second)
	freshClosed := h.now.Add(-23 * time.Hour)
	for _, update := range []struct {
		query string
		args  []any
	}{
		{
			`UPDATE stacks SET last_checked_at = $1, synced_at = $1
			 WHERE open`,
			[]any{oldOpenStack},
		},
		{
			`UPDATE pull_requests
			 SET last_checked_at = CASE WHEN state = 'open'
			                            THEN $1::timestamptz
			                            ELSE $2::timestamptz END,
			     synced_at = CASE WHEN state = 'open'
			                      THEN $1::timestamptz
			                      ELSE $2::timestamptz END`,
			[]any{oldOpenPR, freshClosed},
		},
		{
			`UPDATE repo_rule_sync_state SET last_checked_at = $1`,
			[]any{oldRules},
		},
		{
			`UPDATE repo_rules SET last_checked_at = $1, synced_at = $1`,
			[]any{oldRules},
		},
	} {
		if _, err := h.pool.Exec(ctx, update.query, update.args...); err != nil {
			t.Fatal(err)
		}
	}
	var closedChecked time.Time
	if err := h.pool.QueryRow(ctx, `
		SELECT last_checked_at FROM pull_requests WHERE number = 4810
	`).Scan(&closedChecked); err != nil {
		t.Fatal(err)
	}
	fixture := h.fixture
	fixture.PullRequests[1].ReviewThreads[0].IsResolved = true
	h.fake.SetFixture(fixture)
	h.start(t)
	for _, kind := range []string{
		KindStacks,
		KindPullRequests,
		KindRepoRules,
	} {
		if err := h.service.Kickoff(ctx, KickoffArgs{
			SweepKind:    kind,
			Installation: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		var stack, pull, rules time.Time
		if err := h.pool.QueryRow(ctx, `
			SELECT last_checked_at FROM stacks WHERE number = 142
		`).Scan(&stack); err != nil {
			return false
		}
		if err := h.pool.QueryRow(ctx, `
			SELECT last_checked_at FROM pull_requests WHERE number = 4812
		`).Scan(&pull); err != nil {
			return false
		}
		if err := h.pool.QueryRow(ctx, `
			SELECT last_checked_at FROM repo_rule_sync_state
		`).Scan(&rules); err != nil {
			return false
		}
		return stack.After(oldOpenStack) &&
			pull.After(oldOpenPR) &&
			rules.After(oldRules)
	})
	var closedAfter time.Time
	if err := h.pool.QueryRow(ctx, `
		SELECT last_checked_at FROM pull_requests WHERE number = 4810
	`).Scan(&closedAfter); err != nil {
		t.Fatal(err)
	}
	if !closedAfter.Equal(closedChecked) {
		t.Fatalf(
			"closed PR fresher than 24h was swept: %s -> %s",
			closedChecked,
			closedAfter,
		)
	}
	staleClosed := h.now.Add(-24*time.Hour - time.Second)
	if _, err := h.pool.Exec(ctx, `
		UPDATE pull_requests
		SET last_checked_at = $1, synced_at = $1
		WHERE number = 4810
	`, staleClosed); err != nil {
		t.Fatal(err)
	}
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindClosed,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		var value time.Time
		err := h.pool.QueryRow(ctx, `
			SELECT last_checked_at
			FROM pull_requests WHERE number = 4810
		`).Scan(&value)
		return err == nil && value.After(staleClosed)
	})
	if got := h.fake.RequestCount(http.MethodPost, "/graphql"); got < 1 {
		t.Fatalf("PR GraphQL reconciliation requests = %d, want at least 1", got)
	}
	var synced, checked time.Time
	if err := h.pool.QueryRow(ctx, `
		SELECT synced_at, last_checked_at
		FROM pull_requests WHERE number = 4812
	`).Scan(&synced, &checked); err != nil {
		t.Fatal(err)
	}
	if checked.Before(synced) || !checked.After(oldOpenPR) {
		t.Fatalf("GraphQL provenance synced=%s checked=%s", synced, checked)
	}
	var resolved bool
	if err := h.pool.QueryRow(ctx, `
		SELECT is_resolved
		FROM review_threads
		WHERE pr_number = 4812
	`).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if !resolved {
		t.Fatal("periodic GraphQL PR reconciliation missed review-thread state")
	}
}

func TestSweepCursorResumesAfterMidSweepCrashAndSignalsOverrun(
	t *testing.T,
) {
	h := newSweepHarness(t, 2)
	h.seedCache(t, nil, false, false)
	ctx := context.Background()
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.service.ReconcilePage(ctx, ListPageArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
		ScopeKey:     "acme/monolith",
		Cursor:       "1",
	}); err != nil {
		t.Fatal(err)
	}
	cursor, err := dbgen.New(h.pool).GetSweepCursor(
		ctx,
		dbgen.GetSweepCursorParams{
			InstallationID: 1,
			SweepKind:      KindPullRequests,
			ScopeKey:       "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Cursor != "2" || cursor.CompletedAt.Valid {
		t.Fatalf("mid-sweep cursor = %+v, want page 2 in progress", cursor)
	}
	if _, err := h.pool.Exec(ctx, `
		UPDATE sweep_cursors
		SET started_at = $1
		WHERE installation_id = 1
		  AND sweep_kind = $2
		  AND scope_key = 'acme/monolith'
	`, h.now.Add(-11*time.Minute), KindPullRequests); err != nil {
		t.Fatal(err)
	}
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	h.observer.mu.Lock()
	overruns := h.observer.overruns
	h.observer.mu.Unlock()
	if overruns != 1 {
		t.Fatalf("overrun signals = %d, want 1", overruns)
	}
	for _, page := range []int{2, 1, 2} {
		if err := h.service.ReconcilePage(ctx, ListPageArgs{
			SweepKind:    KindPullRequests,
			Installation: 1,
			ScopeKey:     "acme/monolith",
			Cursor:       fmt.Sprint(page),
		}); err != nil {
			t.Fatal(err)
		}
	}
	cursor, err = dbgen.New(h.pool).GetSweepCursor(
		ctx,
		dbgen.GetSweepCursorParams{
			InstallationID: 1,
			SweepKind:      KindPullRequests,
			ScopeKey:       "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.CompletedAt.Valid || cursor.Cursor != "" {
		t.Fatalf("resumed sweep cursor = %+v, want completed", cursor)
	}
}

func TestRepositorySweepCursorsReapRenamedAndTombstonedScopes(
	t *testing.T,
) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4812}, false, false)
	ctx := context.Background()
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.service.ReconcilePage(ctx, ListPageArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
		ScopeKey:     "acme/monolith",
		Cursor:       "1",
	}); err != nil {
		t.Fatal(err)
	}
	var oldSeen int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sweep_seen_keys
		WHERE installation_id = 1
		  AND sweep_kind = 'pull_requests'
		  AND scope_key = 'acme/monolith'
	`).Scan(&oldSeen); err != nil {
		t.Fatal(err)
	}
	if oldSeen == 0 {
		t.Fatal("pre-rename sweep did not persist membership rows")
	}
	if _, err := h.pool.Exec(ctx, `
		UPDATE repos
		SET full_name = 'platform/renamed'
		WHERE installation_id = 1
		  AND gh_id = 1001
	`); err != nil {
		t.Fatal(err)
	}
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	var oldCursors, oldPages, oldKeys, renamedCursors int
	if err := h.pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM sweep_cursors
		     WHERE scope_key = 'acme/monolith'),
		    (SELECT count(*) FROM sweep_pages
		     WHERE scope_key = 'acme/monolith'),
		    (SELECT count(*) FROM sweep_seen_keys
		     WHERE scope_key = 'acme/monolith'),
		    (SELECT count(*) FROM sweep_cursors
		     WHERE scope_key = 'platform/renamed')
	`).Scan(
		&oldCursors,
		&oldPages,
		&oldKeys,
		&renamedCursors,
	); err != nil {
		t.Fatal(err)
	}
	if oldCursors != 0 || oldPages != 0 || oldKeys != 0 ||
		renamedCursors != 1 {
		t.Fatalf(
			"rename reap old cursors/pages/keys=%d/%d/%d renamed=%d",
			oldCursors,
			oldPages,
			oldKeys,
			renamedCursors,
		)
	}

	if _, err := h.pool.Exec(ctx, `
		UPDATE repos
		SET tombstoned_at = clock_timestamp()
		WHERE installation_id = 1
		  AND gh_id = 1001
	`); err != nil {
		t.Fatal(err)
	}
	if err := h.service.Kickoff(ctx, KickoffArgs{
		SweepKind:    KindPullRequests,
		Installation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sweep_cursors
		WHERE installation_id = 1
		  AND sweep_kind = 'pull_requests'
	`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("tombstoned repository sweep cursors = %d, want 0", remaining)
	}
}

func TestDisappearanceVerificationTombstonesPRStackAndRepository(
	t *testing.T,
) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4812}, true, false)
	fixture := h.fixture
	fixture.Repositories = []fakegithub.Repository{}
	fixture.PullRequests = nil
	fixture.Stacks = nil
	h.fake.SetFixture(fixture)
	h.fake.ScriptNotFound(
		http.MethodGet,
		"/repos/acme/monolith",
		1,
	)
	h.start(t)
	ctx := context.Background()
	for _, kind := range []string{
		KindRepositories,
		KindStacks,
		KindPullRequests,
	} {
		if err := h.service.Kickoff(ctx, KickoffArgs{
			SweepKind:    kind,
			Installation: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		var repo, stack, pull bool
		err := h.pool.QueryRow(ctx, `
			SELECT
			    repos.tombstoned_at IS NOT NULL,
			    stacks.tombstoned_at IS NOT NULL,
			    pull_requests.tombstoned_at IS NOT NULL
			FROM repos
			JOIN stacks ON stacks.repo_id = repos.id
			JOIN pull_requests ON pull_requests.repo_id = repos.id
			WHERE repos.gh_id = 1001
			  AND stacks.number = 142
			  AND pull_requests.number = 4812
		`).Scan(&repo, &stack, &pull)
		return err == nil && repo && stack && pull
	})
	var repositoryEvents int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'repository.tombstoned'
		  AND entity_key = 'repo:1:1001'
	`).Scan(&repositoryEvents); err != nil {
		t.Fatal(err)
	}
	if repositoryEvents != 1 {
		t.Fatalf(
			"repository tombstone events = %d, want 1",
			repositoryEvents,
		)
	}
}

func TestGapHealingRequestsRedeliveryAndCacheConverges(t *testing.T) {
	h := newSweepHarness(t, 100)
	h.seedCache(t, []int{4812}, false, false)
	ingressServer := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(h.pool),
			sweepTestSecret,
			1<<20,
			time.Second,
		).Mux(),
	)
	defer ingressServer.Close()
	fixture := h.fixture
	fixture.PullRequests[1].Title = "truth changed during outage"
	fixture.PullRequests[1].UpdatedAt = h.now.Add(time.Minute)
	h.fake.SetFixture(fixture)
	payload := map[string]any{
		"action": "edited",
		"number": 4812,
		"repository": map[string]any{
			"full_name": "acme/monolith",
		},
		"pull_request": map[string]any{
			"number": 4812,
		},
	}
	guid, err := h.fake.DropWebhook(
		ingressServer.URL+ingress.WebhookPath,
		"pull_request",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := dispatch.New(
		h.pool,
		h.river,
		dispatch.Config{
			BatchSize:    100,
			MaxAttempts:  3,
			Debounce:     time.Millisecond,
			PollInterval: time.Millisecond,
			Classifier:   dispatch.DefaultClassifier(),
		},
	)
	h.start(t)
	if err := h.service.HealDeliveryGaps(
		context.Background(),
		GapHealArgs{Installation: 1},
	); err != nil {
		t.Fatal(err)
	}
	if requests := h.fake.RedeliveryRequests(); len(requests) != 1 {
		t.Fatalf("redelivery requests = %v, want one", requests)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := dbgen.New(h.pool).GetWebhookDelivery(
			context.Background(),
			guid,
		)
		return err == nil
	})
	delivery, err := dbgen.New(h.pool).GetWebhookDelivery(
		context.Background(),
		guid,
	)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.DeliveryGuid != guid {
		t.Fatalf("healed delivery = %+v", delivery)
	}
	if _, err := dispatcher.DispatchBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		row, err := dbgen.New(h.pool).GetPullRequestByKey(
			context.Background(),
			dbgen.GetPullRequestByKeyParams{
				RepoFullName: "acme/monolith",
				PrNumber:     4812,
			},
		)
		return err == nil && row.Title == "truth changed during outage"
	})
}

func TestGapHealingSignalsCapAndResumesOpaqueCursorToCompletion(
	t *testing.T,
) {
	h := newSweepHarness(t, 1)
	h.service.config.GapMaxPages = 1
	ingressServer := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(h.pool),
			sweepTestSecret,
			1<<20,
			time.Second,
		).Mux(),
	)
	defer ingressServer.Close()
	for index := range 3 {
		if _, err := h.fake.DropWebhook(
			ingressServer.URL+ingress.WebhookPath,
			"push",
			map[string]any{
				"ref": fmt.Sprintf("refs/heads/gap-%d", index),
				"repository": map[string]any{
					"full_name": "acme/monolith",
				},
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if err := h.service.HealDeliveryGaps(
			context.Background(),
			GapHealArgs{Installation: 1},
		); err != nil {
			t.Fatal(err)
		}
	}
	if requests := h.fake.RedeliveryRequests(); len(requests) != 3 {
		t.Fatalf(
			"redelivery requests = %v, want every capped page",
			requests,
		)
	}
	var cursor string
	var completed bool
	if err := h.pool.QueryRow(context.Background(), `
		SELECT cursor, completed_at IS NOT NULL
		FROM gap_heal_cursors
		WHERE installation_id = 1
	`).Scan(&cursor, &completed); err != nil {
		t.Fatal(err)
	}
	if cursor != "" || !completed {
		t.Fatalf(
			"gap cursor cursor=%q completed=%v, want terminal",
			cursor,
			completed,
		)
	}
	h.observer.mu.Lock()
	incomplete := h.observer.incomplete
	h.observer.mu.Unlock()
	if incomplete != 2 {
		t.Fatalf("incomplete-window signals = %d, want 2", incomplete)
	}
}

func TestPrunerHonorsNinetyDayBoundaryAndPreservesGUIDSkeletons(
	t *testing.T,
) {
	h := newSweepHarness(t, 100)
	ctx := context.Background()
	cutoff := h.now.Add(-90 * 24 * time.Hour)
	for _, delivery := range []struct {
		guid   string
		at     time.Time
		status string
	}{
		{"older", cutoff.Add(-time.Nanosecond), "processed"},
		{"boundary", cutoff, "processed"},
		{"newer", cutoff.Add(time.Nanosecond), "processed"},
		{"parked-older", cutoff.Add(-time.Hour), "parked"},
		{"pending-older", cutoff.Add(-time.Hour), "pending"},
	} {
		if _, err := h.pool.Exec(ctx, `
			INSERT INTO webhook_deliveries (
			    delivery_guid, event, raw_body, headers, received_at,
			    status
			) VALUES ($1, 'push', 'body', '{"x":"y"}', $2, $3)
		`, delivery.guid, delivery.at, delivery.status); err != nil {
			t.Fatal(err)
		}
	}
	h.seedCheckHistory(t, cutoff)
	eventTx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer eventTx.Rollback(context.Background()) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, eventTx); err != nil {
		t.Fatal(err)
	}
	if _, err := eventTx.Exec(ctx, `
			INSERT INTO change_events (
			    stream, kind, entity_key, occurred_at, payload
			) VALUES ('entities', 'old', 'old', $1, '{}')
		`, cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := eventTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	payloads, history, err := h.service.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if payloads != 1 || history != 1 {
		t.Fatalf(
			"pruned payloads/history = %d/%d, want 1/1",
			payloads,
			history,
		)
	}
	var skeletons, oldPayloads, boundaryPayloads, replayablePayloads int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (
		           WHERE delivery_guid = 'older' AND raw_body IS NOT NULL
		       ),
		       count(*) FILTER (
		           WHERE delivery_guid = 'boundary' AND raw_body IS NOT NULL
		       ),
		       count(*) FILTER (
		           WHERE delivery_guid IN ('parked-older', 'pending-older')
		             AND raw_body IS NOT NULL
		             AND payload_pruned_at IS NULL
		       )
		FROM webhook_deliveries
		WHERE delivery_guid IN (
		    'older', 'boundary', 'newer', 'parked-older', 'pending-older'
		)
	`).Scan(
		&skeletons,
		&oldPayloads,
		&boundaryPayloads,
		&replayablePayloads,
	); err != nil {
		t.Fatal(err)
	}
	if skeletons != 5 ||
		oldPayloads != 0 ||
		boundaryPayloads != 1 ||
		replayablePayloads != 2 {
		t.Fatalf(
			"skeletons=%d old_payloads=%d boundary_payloads=%d replayable_payloads=%d",
			skeletons,
			oldPayloads,
			boundaryPayloads,
			replayablePayloads,
		)
	}
	var historyRows, changeEvents int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history
	`).Scan(&historyRows); err != nil {
		t.Fatal(err)
	}
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'old'
	`).Scan(&changeEvents); err != nil {
		t.Fatal(err)
	}
	if historyRows != 2 || changeEvents != 1 {
		t.Fatalf(
			"history rows/change events = %d/%d, want 2/1",
			historyRows,
			changeEvents,
		)
	}
}

func TestRequeueRefusesPayloadPrunedDeliveryWithVisibleReason(t *testing.T) {
	h := newSweepHarness(t, 100)
	ctx := context.Background()
	prunedAt := h.now.Add(-time.Hour)
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, raw_body, headers, received_at,
		    status, attempts, last_error, payload_pruned_at
		) VALUES (
		    'pruned-parked', 'push', NULL, '{}'::jsonb, $1,
		    'parked', 4, 'original poison error', $2
		)
	`, h.now.Add(-100*24*time.Hour), prunedAt); err != nil {
		t.Fatal(err)
	}
	requeued, err := dbgen.New(h.pool).RequeueParkedWebhookDeliveries(
		ctx,
		dbgen.RequeueParkedWebhookDeliveriesParams{
			DeliveryGuids: []string{"pruned-parked"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 {
		t.Fatalf("payload-pruned requeue count = %d, want 0", requeued)
	}
	delivery, err := dbgen.New(h.pool).GetWebhookDelivery(ctx, "pruned-parked")
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != "parked" ||
		delivery.Attempts != 4 ||
		!delivery.PayloadPrunedAt.Valid ||
		!delivery.PayloadPrunedAt.Time.Equal(prunedAt) ||
		!delivery.LastError.Valid ||
		!strings.Contains(delivery.LastError.String, "operator requeue refused") ||
		!strings.Contains(delivery.LastError.String, "payload unavailable") ||
		!strings.Contains(delivery.LastError.String, "pruned at") ||
		!strings.Contains(delivery.LastError.String, "original poison error") {
		t.Fatalf("refused payload-pruned delivery = %+v", delivery)
	}
}

func TestPrunerCommitsBoundedBatchesUntilBacklogIsEmpty(t *testing.T) {
	h := newSweepHarness(t, 100)
	h.service.config.RetentionBatchSize = 2
	h.seedCache(t, nil, false, false)
	ctx := context.Background()
	cutoff := h.now.Add(-h.service.config.RetentionAge)
	var repoID int64
	if err := h.pool.QueryRow(ctx, `
		SELECT id FROM repos WHERE gh_id = 1001
	`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, conclusion,
		    details_url, app_slug, head_sha, synced_at,
		    last_checked_at, etag, sync_source
		) VALUES (
		    9901, $1, 'node-batch', 'batch', 'completed', 'success',
		    '', 'github-actions', 'batch-head', $2, $2, '', 'reconcile'
		)
	`, repoID, h.now); err != nil {
		t.Fatal(err)
	}
	for index := range 5 {
		at := cutoff.Add(-time.Duration(index+1) * time.Second)
		if _, err := h.pool.Exec(ctx, `
			INSERT INTO webhook_deliveries (
			    delivery_guid, event, raw_body, headers, received_at,
			    status
			) VALUES ($1, 'push', 'body', '{"x":"y"}', $2, 'processed')
		`, fmt.Sprintf("batch-%d", index), at); err != nil {
			t.Fatal(err)
		}
		if _, err := h.pool.Exec(ctx, `
			INSERT INTO check_history (
			    check_run_gh_id, repo_id, name, status, conclusion,
			    observed, head_sha, synced_at, etag, sync_source
			) VALUES (
			    9901, $1, 'batch', $2, 'success', '{}', 'batch-head',
			    $3, '', 'reconcile'
			)
		`, repoID, fmt.Sprintf("batch-%d", index), at); err != nil {
			t.Fatal(err)
		}
	}

	payloads, history, err := h.service.pruneBatch(ctx, h.now, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if payloads != 2 || history != 2 {
		t.Fatalf(
			"first bounded batch pruned %d/%d, want 2/2",
			payloads,
			history,
		)
	}
	var remainingPayloads, remainingHistory int
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM webhook_deliveries
		WHERE delivery_guid LIKE 'batch-%'
		  AND raw_body IS NOT NULL
	`).Scan(&remainingPayloads); err != nil {
		t.Fatal(err)
	}
	if err := h.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM check_history
		WHERE name = 'batch'
	`).Scan(&remainingHistory); err != nil {
		t.Fatal(err)
	}
	if remainingPayloads != 3 || remainingHistory != 3 {
		t.Fatalf(
			"remaining after one batch = %d/%d, want 3/3",
			remainingPayloads,
			remainingHistory,
		)
	}
	payloads, history, err = h.service.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if payloads != 3 || history != 3 {
		t.Fatalf(
			"looped prune removed %d/%d, want 3/3",
			payloads,
			history,
		)
	}
}

func (h *sweepHarness) seedCache(
	t *testing.T,
	pullNumbers []int,
	includeStack bool,
	includeRules bool,
) {
	t.Helper()
	ctx := context.Background()
	writer := store.NewEntityWriter(h.pool)
	repository, repositoryResponse, err := h.rest.GetRepository(
		ctx,
		budget.Sweep,
		h.fixture.Owner,
		h.fixture.Repo,
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	repositoryRecord := store.RepositoryRecord{
		InstallationID:  1,
		OrgID:           1,
		GitHubID:        repository.ID,
		NodeID:          repository.NodeID,
		Owner:           repository.Owner,
		Name:            repository.Name,
		FullName:        repository.FullName,
		DefaultBranch:   repository.DefaultBranch,
		Archived:        repository.Archived,
		GitHubUpdatedAt: repository.UpdatedAt,
		ETag:            repositoryResponse.ETag,
		LastCheckedAt:   h.now,
	}
	if _, err := writer.ApplyRepository(
		ctx,
		repositoryRecord,
		store.SyncSourceReconcile,
		repositoryResponse.ETag,
		h.now,
	); err != nil {
		t.Fatal(err)
	}
	for _, number := range pullNumbers {
		pull, response, err := h.rest.GetPull(
			ctx,
			budget.Sweep,
			h.fixture.Owner,
			h.fixture.Repo,
			number,
			"",
		)
		if err != nil {
			t.Fatal(err)
		}
		var stackNumber, stackPosition *int
		if pull.Stack != nil {
			stack := pull.Stack.Number
			position := pull.Stack.Position
			stackNumber = &stack
			stackPosition = &position
		}
		if _, err := writer.ApplyPullRequest(
			ctx,
			store.PullRequestRecord{
				Repository:      repositoryRecord,
				GitHubID:        pull.GetID(),
				NodeID:          pull.GetNodeID(),
				Number:          pull.GetNumber(),
				Title:           pull.GetTitle(),
				State:           pull.GetState(),
				Draft:           pull.GetDraft(),
				AuthorLogin:     pull.GetUser().GetLogin(),
				HeadRef:         pull.GetHead().GetRef(),
				HeadSHA:         pull.GetHead().GetSHA(),
				BaseRef:         pull.GetBase().GetRef(),
				BaseSHA:         pull.GetBase().GetSHA(),
				ReviewDecision:  pull.ReviewDecision,
				MergeableState:  pull.GetMergeableState(),
				StackNumber:     stackNumber,
				StackPosition:   stackPosition,
				MembershipKnown: true,
				GitHubUpdatedAt: pull.GetUpdatedAt().Time,
				ETag:            response.ETag,
				SyncedAt:        h.now,
				Source:          store.SyncSourceReconcile,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	if includeStack {
		stack, response, err := h.rest.GetStack(
			ctx,
			budget.Sweep,
			h.fixture.Owner,
			h.fixture.Repo,
			142,
			"",
		)
		if err != nil {
			t.Fatal(err)
		}
		entries := make([]store.StackEntry, 0, len(stack.PullRequests))
		for _, pull := range stack.PullRequests {
			entries = append(entries, store.StackEntry{
				Number:    pull.Number,
				State:     pull.State,
				Draft:     pull.Draft,
				MergedAt:  pull.MergedAt,
				UpdatedAt: pull.UpdatedAt,
				HeadRef:   pull.Head.Ref,
				HeadSHA:   pull.Head.SHA,
			})
		}
		if _, err := writer.ApplyStack(ctx, store.StackRecord{
			Repository:      repositoryRecord,
			GitHubID:        stack.ID,
			NodeID:          stack.NodeID,
			Number:          stack.Number,
			BaseRef:         stack.Base.Ref,
			BaseSHA:         stack.Base.SHA,
			Open:            stack.Open,
			Entries:         entries,
			GitHubUpdatedAt: stack.UpdatedAt,
			ETag:            response.ETag,
			SyncedAt:        h.now,
			Source:          store.SyncSourceReconcile,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if includeRules {
		rules, response, err := h.rest.ListRepositoryRules(
			ctx,
			budget.Sweep,
			h.fixture.Owner,
			h.fixture.Repo,
			"",
		)
		if err != nil {
			t.Fatal(err)
		}
		records := make([]store.RepoRuleRecord, 0, len(rules))
		for _, rule := range rules {
			records = append(records, store.RepoRuleRecord{
				Key:             fmt.Sprint(rule.ID),
				Rule:            rule.Raw,
				GitHubUpdatedAt: rule.UpdatedAt,
			})
		}
		observation, err := writer.BeginObservation(
			ctx,
			store.RepoRulesEntityKey(1, repository.ID),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.ApplyRepoRulesObserved(
			ctx,
			observation,
			store.RepoRulesRecord{
				Repository: repositoryRecord,
				Rules:      records,
				ETag:       response.ETag,
				SyncedAt:   h.now,
				Source:     store.SyncSourceReconcile,
			},
		); err != nil {
			observation.Close() //nolint:errcheck
			t.Fatal(err)
		}
		if err := observation.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *sweepHarness) seedCheckHistory(
	t *testing.T,
	cutoff time.Time,
) {
	t.Helper()
	h.seedCache(t, nil, false, false)
	ctx := context.Background()
	var repoID int64
	if err := h.pool.QueryRow(ctx, `
		SELECT id FROM repos WHERE gh_id = 1001
	`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, conclusion,
		    details_url, app_slug, head_sha, synced_at,
		    last_checked_at, etag, sync_source
		) VALUES (
		    9001, $1, 'node', 'unit', 'completed', 'success',
		    '', 'github-actions', 'head', $2, $2, '', 'reconcile'
		)
	`, repoID, h.now); err != nil {
		t.Fatal(err)
	}
	for index, at := range []time.Time{
		cutoff.Add(-time.Nanosecond),
		cutoff,
		cutoff.Add(time.Nanosecond),
	} {
		if _, err := h.pool.Exec(ctx, `
			INSERT INTO check_history (
			    check_run_gh_id, repo_id, name, status, conclusion,
			    observed, head_sha, synced_at, etag, sync_source
			) VALUES (
			    9001, $1, 'unit', $2, 'success', '{}', 'head',
			    $3, '', 'reconcile'
			)
		`, repoID, fmt.Sprintf("state-%d", index), at); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(
	t *testing.T,
	timeout time.Duration,
	condition func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func sweepTestDatabase(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("frontier_sweep_%d", time.Now().UnixNano())
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

func TestDispatcherRulesFileIsLoadable(t *testing.T) {
	rules, err := dispatch.LoadRulesFile(
		"../../config/dispatcher-rules.yaml",
	)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"action": "opened",
		"number": 1,
		"repository": map[string]any{
			"full_name": "acme/repo",
		},
		"pull_request": map[string]any{
			"number": 1,
		},
	})
	intents, err := dispatch.NewClassifier(rules).Classify(
		"pull_request",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 2 ||
		!strings.Contains(intents[0].Key, "acme/repo") {
		t.Fatalf("file-backed rules produced %+v", intents)
	}
}
