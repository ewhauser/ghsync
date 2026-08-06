package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

const testWebhookSecret = "dispatch-test-secret"

func TestConcurrentDispatchersLockGenerationKeysInDeterministicOrder(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	poolA := cloneDispatchPool(t, pool)
	poolB := cloneDispatchPool(t, pool)
	riverA, err := queue.NewClient(poolA)
	if err != nil {
		t.Fatal(err)
	}
	riverB, err := queue.NewClient(poolB)
	if err != nil {
		t.Fatal(err)
	}

	const (
		iterations = 30
		keyCount   = 20
	)
	dispatchTime := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	newDispatcher := func(
		dispatchPool *pgxpool.Pool,
		riverClient *river.Client[pgx.Tx],
	) *Dispatcher {
		return mustNewDispatcher(t, dispatchPool, riverClient, Config{
			BatchSize:    keyCount,
			MaxAttempts:  3,
			Debounce:     5 * time.Second,
			PollInterval: time.Millisecond,
			Now:          func() time.Time { return dispatchTime },
			Classifier: NewClassifier([]Rule{{
				Event:  "pull_request",
				Action: ActionAny,
				Target: TargetPullRequest,
			}}),
		})
	}
	dispatcherA := newDispatcher(poolA, riverA)
	dispatcherB := newDispatcher(poolB, riverB)

	for iteration := range iterations {
		repo := fmt.Sprintf("acme/concurrent-%02d", iteration)
		guidPrefix := fmt.Sprintf("concurrent-%02d-", iteration)
		receivedAt := dispatchTime.Add(time.Duration(iteration) * time.Minute)
		batch := &pgx.Batch{}
		for index := range keyCount {
			for _, group := range []struct {
				name   string
				number int
				offset time.Duration
			}{
				{name: "a", number: index + 1},
				{
					name:   "b",
					number: keyCount - index,
					offset: time.Second,
				},
			} {
				payload, marshalErr := json.Marshal(map[string]any{
					"action":     "opened",
					"number":     group.number,
					"repository": map[string]any{"full_name": repo},
					"pull_request": map[string]any{
						"number": group.number,
						"stack":  nil,
					},
				})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				batch.Queue(`
					INSERT INTO webhook_deliveries (
						delivery_guid, event, raw_body, headers, received_at
					)
					VALUES ($1, 'pull_request', $2, '{}'::jsonb, $3)
				`,
					fmt.Sprintf(
						"%s%s-%02d",
						guidPrefix,
						group.name,
						index,
					),
					payload,
					receivedAt.Add(group.offset),
				)
			}
		}
		results := pool.SendBatch(context.Background(), batch)
		if err := results.Close(); err != nil {
			t.Fatalf("iteration %d seed deliveries: %v", iteration, err)
		}

		claimedA := make(chan struct{})
		claimedB := make(chan struct{})
		releaseClaims := make(chan struct{})
		dispatcherA.afterClaim = func() {
			close(claimedA)
			<-releaseClaims
		}
		dispatcherB.afterClaim = func() {
			close(claimedB)
			<-releaseClaims
		}
		outcomes := make(chan error, 2)
		runDispatcher := func(dispatcher *Dispatcher) {
			go func(dispatcher *Dispatcher) {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cancel()
				count, dispatchErr := dispatcher.DispatchBatch(ctx)
				if dispatchErr == nil && count != keyCount {
					dispatchErr = fmt.Errorf(
						"claimed %d deliveries, want %d",
						count,
						keyCount,
					)
				}
				outcomes <- dispatchErr
			}(dispatcher)
		}
		runDispatcher(dispatcherA)
		select {
		case <-claimedA:
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d dispatcher A did not claim", iteration)
		}
		runDispatcher(dispatcherB)
		select {
		case <-claimedB:
		case <-time.After(10 * time.Second):
			t.Fatalf("iteration %d dispatcher B did not claim", iteration)
		}
		close(releaseClaims)
		for range 2 {
			if err := <-outcomes; err != nil {
				t.Fatalf("iteration %d concurrent dispatch: %v", iteration, err)
			}
		}

		var deliveries, processedOnce int
		if err := pool.QueryRow(context.Background(), `
			SELECT
				count(*),
				count(*) FILTER (
					WHERE status = 'processed' AND attempts = 1
				)
			FROM webhook_deliveries
			WHERE delivery_guid LIKE $1
		`, guidPrefix+"%").Scan(&deliveries, &processedOnce); err != nil {
			t.Fatal(err)
		}
		if deliveries != 2*keyCount || processedOnce != deliveries {
			t.Fatalf(
				"iteration %d deliveries=%d processed_once=%d, want %d",
				iteration,
				deliveries,
				processedOnce,
				2*keyCount,
			)
		}

		var generationKeys, generationTwo int
		if err := pool.QueryRow(context.Background(), `
			SELECT
				count(*),
				count(*) FILTER (WHERE generation = 2)
			FROM refresh_intent_generations
			WHERE refresh_key LIKE $1
		`, "pr:"+repo+":%").Scan(&generationKeys, &generationTwo); err != nil {
			t.Fatal(err)
		}
		if generationKeys != keyCount || generationTwo != generationKeys {
			t.Fatalf(
				"iteration %d generation keys=%d at_generation_2=%d, want %d",
				iteration,
				generationKeys,
				generationTwo,
				keyCount,
			)
		}

		var jobs, duplicateJobs int
		if err := pool.QueryRow(context.Background(), `
			SELECT
				count(*),
				count(*) - count(DISTINCT (kind, args->>'key'))
			FROM river_job
			WHERE args->>'key' LIKE $1
		`, "%:"+repo+":%").Scan(&jobs, &duplicateJobs); err != nil {
			t.Fatal(err)
		}
		if jobs != keyCount || duplicateJobs != 0 {
			t.Fatalf(
				"iteration %d River jobs=%d duplicate kind/keys=%d, want %d/0",
				iteration,
				jobs,
				duplicateJobs,
				keyCount,
			)
		}
	}
}

func TestFullRecordedReplayIngressToRiver(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		),
	))
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	deliveries := loadRecordedDeliveries(t)
	golden := loadGoldenJobs(t)
	baseTime := time.Date(2026, 7, 28, 18, 0, 0, 0, time.UTC)

	for run := range 4 {
		random := rand.New(rand.NewSource(int64(run + 100))) //nolint:gosec // deterministic non-security use
		permuted := append([]recordedDelivery(nil), deliveries...)
		random.Shuffle(len(permuted), func(i, j int) {
			permuted[i], permuted[j] = permuted[j], permuted[i]
		})
		repo := fmt.Sprintf("acme/monolith-replay-%d", run)
		guidPrefix := fmt.Sprintf("replay-%d-", run)
		dispatchTime := baseTime.Add(time.Duration(run) * time.Hour)
		dispatchOnce := func(batchSize int) int {
			t.Helper()
			dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
				BatchSize:    batchSize,
				MaxAttempts:  3,
				Debounce:     5 * time.Second,
				PollInterval: time.Millisecond,
				Now:          func() time.Time { return dispatchTime },
				Classifier:   DefaultClassifier(),
			})
			count, err := dispatcher.DispatchBatch(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			dispatchTime = dispatchTime.Add(time.Millisecond)
			return count
		}

		for _, delivery := range permuted {
			payload, _ := deliveryPayloadForRepo(t, delivery.Payload, repo)
			guid := guidPrefix + delivery.GUID
			if _, err := fake.EmitWebhookWithGUID(
				context.Background(),
				server.URL+ingress.WebhookPath,
				delivery.Event,
				guid,
				payload,
			); err != nil {
				t.Fatalf("emit %s: %v", guid, err)
			}
			if random.Intn(3) == 0 {
				if _, err := fake.EmitWebhookWithGUID(
					context.Background(),
					server.URL+ingress.WebhookPath,
					delivery.Event,
					guid,
					payload,
				); err != nil {
					t.Fatalf("redeliver %s: %v", guid, err)
				}
			}
			if random.Intn(3) != 0 {
				dispatchOnce(1 + random.Intn(7))
			}
		}

		for dispatchOnce(1+random.Intn(7)) > 0 {
			// Each pass chooses a fresh batch boundary and runs until empty.
		}

		var deliveryCount, processedCount int
		if err := pool.QueryRow(context.Background(), `
			SELECT count(*), count(*) FILTER (WHERE status = 'processed')
			FROM webhook_deliveries
			WHERE delivery_guid LIKE $1
		`, guidPrefix+"%").Scan(&deliveryCount, &processedCount); err != nil {
			t.Fatal(err)
		}
		if deliveryCount != len(deliveries) || processedCount != len(deliveries) {
			t.Fatalf(
				"run %d deliveries=%d processed=%d want=%d",
				run,
				deliveryCount,
				processedCount,
				len(deliveries),
			)
		}

		expected := goldenJobsForRepo(golden, repo)
		got := riverDecisionsExact(t, pool, repo, len(expected))
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("run %d River decisions differ\n got: %#v\nwant: %#v", run, got, expected)
		}
	}
}

func TestRebaseStormSupersedesOldPagesWithoutLegacyBranchFanout(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		),
	))
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	repo := "acme/rebase-storm"
	branches := []string{"stack/layer-1", "stack/layer-2", "stack/layer-3"}
	now := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
	repository := store.RepositoryRecord{
		InstallationID: 1, OrgID: 1, GitHubID: 49001,
		NodeID: "repo-rebase-storm", Owner: "acme", Name: "rebase-storm",
		FullName: repo, DefaultBranch: "main", DefaultHeadSHA: "base",
		GitHubUpdatedAt: now,
	}
	entries := make([]store.StackEntry, 0, len(branches))
	for index, branch := range branches {
		entries = append(entries, store.StackEntry{
			Number: index + 1, State: "open", UpdatedAt: now,
			HeadRef: branch, HeadSHA: fmt.Sprintf("head-%d", index+1),
		})
	}
	if _, err := store.NewEntityWriter(pool).ApplyStack(
		t.Context(), store.StackRecord{
			Repository: repository, GitHubID: 49142,
			NodeID: "stack-rebase-storm", Number: 142,
			BaseRef: "main", BaseSHA: "base", Open: true, Entries: entries,
			GitHubUpdatedAt: now, SyncedAt: now, Source: store.SyncSourceReconcile,
		},
	); err != nil {
		t.Fatal(err)
	}

	for index := range 20 {
		branch := branches[index%len(branches)]
		if _, err := fake.EmitWebhookWithGUID(
			context.Background(),
			server.URL+ingress.WebhookPath,
			"push",
			fmt.Sprintf("storm-%02d", index),
			map[string]any{
				"ref":        "refs/heads/" + branch,
				"repository": map[string]any{"full_name": repo},
				"stack":      map[string]any{"number": 142},
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize:    1,
		MaxAttempts:  3,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Classifier:   DefaultClassifier(),
	})
	for index := range 20 {
		count, err := dispatcher.DispatchBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("batch %d claimed %d, want 1", index, count)
		}
		now = now.Add(500 * time.Millisecond)
	}
	if elapsed := now.Sub(time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)); elapsed != 10*time.Second {
		t.Fatalf("simulated storm duration = %s, want 10s", elapsed)
	}

	var pageJobs, legacyJobs, oversized, pending, superseded int
	if err := pool.QueryRow(t.Context(), `
		SELECT
		  count(*) FILTER (WHERE kind = $1),
		  count(*) FILTER (WHERE kind = $2),
		  count(*) FILTER (
		      WHERE kind = $1 AND jsonb_array_length(args->'targets') > 1
		  ),
		  (SELECT count(*) FROM branch_reconciliation_pages
		   WHERE status = 'pending'),
		  (SELECT count(*) FROM branch_reconciliation_pages
		   WHERE status = 'superseded')
		FROM river_job
	`, queue.KindReconcileBranchPage, queue.KindRefreshBranch).Scan(
		&pageJobs, &legacyJobs, &oversized, &pending, &superseded,
	); err != nil {
		t.Fatal(err)
	}
	if pageJobs != 20 || legacyJobs != 0 || oversized != 0 ||
		pending != 3 || superseded != 17 {
		t.Fatalf(
			"storm pages=%d legacy=%d oversized=%d pending=%d superseded=%d, want 20/0/0/3/17",
			pageJobs, legacyJobs, oversized, pending, superseded,
		)
	}
	for index, branch := range branches {
		wantGeneration := int64(7)
		if index == 2 {
			wantGeneration = 6
		}
		var generation int64
		if err := pool.QueryRow(t.Context(), `
			SELECT generation FROM branch_reconciliations
			WHERE repo_id = (
			  SELECT id FROM repos WHERE full_name = $1
			) AND branch = $2
		`, repo, branch).Scan(&generation); err != nil {
			t.Fatal(err)
		}
		if generation != wantGeneration {
			t.Fatalf(
				"%s generation = %d, want %d",
				branch, generation, wantGeneration,
			)
		}
	}
}

func TestDefaultBranchPushUsesBoundedReconciliationPages(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	repository := store.RepositoryRecord{
		InstallationID: 1, OrgID: 1, GitHubID: 48001,
		NodeID: "repo-branch-load", Owner: "acme", Name: "branch-load",
		FullName: "acme/branch-load", DefaultBranch: "main",
		DefaultHeadSHA: "base-old", GitHubUpdatedAt: now,
	}
	writer := store.NewEntityWriter(pool)
	if _, err := writer.ApplyRepository(
		t.Context(), repository, store.SyncSourceReconcile, "repo-etag", now,
	); err != nil {
		t.Fatal(err)
	}
	var repoID int64
	if err := pool.QueryRow(t.Context(), `
		SELECT id FROM repos WHERE gh_id = $1
	`, repository.GitHubID).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	const dependents = 250
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO pull_requests (
		    repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, gh_updated_at, synced_at,
		    etag, sync_source, last_checked_at
		)
		SELECT $1, 50000 + number, 'node-' || number::text, number,
		       'PR ' || number::text, 'open', false, 'octocat',
		       'feature/' || number::text, 'head-' || number::text,
		       'main', 'base-old', 'APPROVED', 'MERGEABLE', $2, $2,
		       'etag-' || number::text, 'reconcile', $2
		FROM generate_series(1, $3::integer) AS number
	`, repoID, now, dependents); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"ref":    "refs/heads/main",
		"before": "base-old",
		"after":  "base-new",
		"forced": true,
		"repository": map[string]any{
			"full_name":      repository.FullName,
			"default_branch": "main",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, raw_body, headers, received_at
		) VALUES ('branch-load-push', 'push', $1, '{}'::jsonb, $2)
	`, payload, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize: 1, MaxAttempts: 3, Debounce: time.Second,
		PollInterval: time.Millisecond, Now: func() time.Time { return now },
		Classifier: DefaultClassifier(), BranchPageSize: 25,
	})
	count, err := dispatcher.DispatchBatch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("dispatched = %d, want 1", count)
	}
	wantPages := (dependents + 24) / 25
	var pageJobs, directJobs, repositoryJobs, wrongQueue, oversizedPages int
	if err := pool.QueryRow(t.Context(), `
		SELECT
		  count(*) FILTER (WHERE kind = $1),
		  count(*) FILTER (WHERE kind IN ($2, $3)),
		  count(*) FILTER (WHERE kind = $4),
		  count(*) FILTER (
		      WHERE kind IN ($1, $4) AND queue <> 'bulk'
		  ),
		  count(*) FILTER (
		      WHERE kind = $1 AND jsonb_array_length(args->'targets') > 25
		  )
		FROM river_job
	`, queue.KindReconcileBranchPage, queue.KindRefreshPR, queue.KindRefreshStack,
		queue.KindRefreshRepository).Scan(
		&pageJobs, &directJobs, &repositoryJobs, &wrongQueue, &oversizedPages,
	); err != nil {
		t.Fatal(err)
	}
	if pageJobs != wantPages || directJobs != 0 || repositoryJobs != 1 ||
		wrongQueue != 0 ||
		oversizedPages != 0 {
		t.Fatalf(
			"jobs pages=%d direct=%d repo=%d wrong_queue=%d oversized=%d want pages=%d",
			pageJobs, directJobs, repositoryJobs, wrongQueue, oversizedPages,
			wantPages,
		)
	}
	var transitioned, generationRows, pendingPages, pendingTargets int
	if err := pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM pull_requests
		   WHERE repo_id = $1 AND base_sha = 'base-new'),
		  (SELECT count(*) FROM refresh_intent_generations
		   WHERE refresh_key LIKE 'pr:acme/branch-load:%'),
		  (SELECT count(*) FROM branch_reconciliation_pages
		   WHERE repo_id = $1 AND status = 'pending'),
		  (SELECT COALESCE(sum(target_count), 0)
		   FROM branch_reconciliation_pages
		   WHERE repo_id = $1 AND status = 'pending')
	`, repoID).Scan(
		&transitioned, &generationRows, &pendingPages, &pendingTargets,
	); err != nil {
		t.Fatal(err)
	}
	if transitioned != dependents || generationRows != dependents ||
		pendingPages != wantPages || pendingTargets != dependents {
		t.Fatalf(
			"bulk state transitioned=%d generation_rows=%d pages=%d targets=%d",
			transitioned, generationRows, pendingPages, pendingTargets,
		)
	}

	// Incomplete synthetic/replayed push payloads cannot prove a local SHA
	// transition, but they must still use bounded pages rather than reviving
	// refresh_branch's per-entity fanout.
	incompletePayload, err := json.Marshal(map[string]any{
		"ref": "refs/heads/main",
		"repository": map[string]any{
			"full_name":      repository.FullName,
			"default_branch": "main",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, raw_body, headers, received_at
		) VALUES ('branch-load-incomplete', 'push', $1, '{}'::jsonb, $2)
	`, incompletePayload, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if count, err := dispatcher.DispatchBatch(t.Context()); err != nil || count != 1 {
		t.Fatalf("dispatch incomplete push count=%d err=%v", count, err)
	}
	var incompletePages, legacyBranchJobs, incompleteGeneration int
	if err := pool.QueryRow(t.Context(), `
		SELECT
		  count(*) FILTER (
		      WHERE kind = $1 AND (args->>'generation')::bigint = 2
		  ),
		  count(*) FILTER (WHERE kind = $2),
		  (SELECT generation::integer
		   FROM branch_reconciliations
		   WHERE repo_id = $3 AND branch = 'main')
		FROM river_job
	`, queue.KindReconcileBranchPage, queue.KindRefreshBranch, repoID).Scan(
		&incompletePages, &legacyBranchJobs, &incompleteGeneration,
	); err != nil {
		t.Fatal(err)
	}
	if incompletePages != wantPages || legacyBranchJobs != 0 ||
		incompleteGeneration != 2 {
		t.Fatalf(
			"incomplete push pages=%d legacy=%d generation=%d, want %d/0/2",
			incompletePages, legacyBranchJobs, incompleteGeneration, wantPages,
		)
	}
}

func TestBranchBulkDoesNotInvertInFlightEntityGenerationLocks(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 12, 30, 0, 0, time.UTC)
	repository := store.RepositoryRecord{
		InstallationID: 1, OrgID: 1, GitHubID: 48002,
		NodeID: "repo-branch-lock-order", Owner: "acme", Name: "branch-lock-order",
		FullName: "acme/branch-lock-order", DefaultBranch: "main",
		DefaultHeadSHA: "base-old", GitHubUpdatedAt: now,
	}
	writer := store.NewEntityWriter(pool)
	if _, err := writer.ApplyRepository(
		t.Context(), repository, store.SyncSourceReconcile, "repo-etag", now,
	); err != nil {
		t.Fatal(err)
	}
	pull := store.PullRequestRecord{
		Repository: repository, GitHubID: 48102, NodeID: "pr-branch-lock-order",
		Number: 1, Title: "before", State: "open", AuthorLogin: "octocat",
		HeadRef: "feature", HeadSHA: "feature-head", BaseRef: "main",
		BaseSHA: "base-old", GitHubUpdatedAt: now, SyncedAt: now,
		Source: store.SyncSourceReconcile,
	}
	if _, err := writer.ApplyPullRequest(t.Context(), pull); err != nil {
		t.Fatal(err)
	}
	refreshKey := "pr:" + repository.FullName + ":1"
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO refresh_intent_generations (
		    kind, refresh_key, generation, completed_generation,
		    event_received_at, updated_at
		) VALUES ('refresh_pr', $1, 1, 0, $2, $2)
	`, refreshKey, now); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"ref": "refs/heads/main", "before": "base-old", "after": "base-new",
		"repository": map[string]any{
			"full_name": repository.FullName, "default_branch": "main",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, raw_body, headers, received_at
		) VALUES ('branch-lock-order-push', 'push', $1, '{}'::jsonb, $2)
	`, payload, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	observation, err := writer.BeginObservation(
		t.Context(), store.PullRequestEntityKey(1, repository.GitHubID, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close() //nolint:errcheck // test cleanup
	directAtFence := make(chan struct{}, 1)
	releaseDirect := make(chan struct{})
	directCtx := outbox.WithSequenceAllocationHook(
		t.Context(),
		func(string, int64) error {
			directAtFence <- struct{}{}
			select {
			case <-releaseDirect:
				return nil
			case <-t.Context().Done():
				return t.Context().Err()
			}
		},
	)
	directCtx = store.WithRefreshGenerationFence(
		directCtx,
		store.RefreshGenerationFence{
			Kind: "refresh_pr", RefreshKey: refreshKey, Generation: 1,
		},
	)
	directRecord := pull
	directRecord.Title = "in-flight direct observation"
	directRecord.GitHubUpdatedAt = now.Add(time.Minute)
	directRecord.SyncedAt = directRecord.GitHubUpdatedAt
	directDone := make(chan error, 1)
	go func() {
		_, applyErr := writer.ApplyPullRequestObserved(
			directCtx,
			observation,
			directRecord,
			func(store.ApplyPullRequestResult) store.TransactionHook {
				return func(ctx context.Context, tx pgx.Tx) error {
					return queue.InsertRefreshesTx(
						ctx,
						tx,
						riverClient,
						[]queue.RefreshSpec{{
							Kind: queue.KindRefreshPR,
							Key:  refreshKey,
						}},
						queue.QueueEvent,
					)
				}
			},
		)
		directDone <- applyErr
	}()
	select {
	case <-directAtFence:
	case <-time.After(10 * time.Second):
		t.Fatal("direct observation did not reach its transaction fence")
	}

	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize: 1, MaxAttempts: 3, Debounce: time.Second,
		PollInterval: time.Millisecond, Now: func() time.Time { return now },
		Classifier: DefaultClassifier(), BranchPageSize: 25,
	})
	dispatchDone := make(chan error, 1)
	go func() {
		_, dispatchErr := dispatcher.DispatchBatch(t.Context())
		dispatchDone <- dispatchErr
	}()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var waiting int
		if err := pool.QueryRow(t.Context(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND query LIKE '%UPDATE pull_requests AS pull%'
		`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatal("branch bulk transaction did not wait on the in-flight PR row")
		}
	}
	close(releaseDirect)
	select {
	case err := <-directDone:
		if err != nil {
			t.Fatalf("complete direct observation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("direct observation deadlocked with branch generation rows")
	}
	select {
	case err := <-dispatchDone:
		if err != nil {
			t.Fatalf("dispatch branch push: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("branch dispatch did not finish after direct observation")
	}

	var baseSHA string
	var generation, completed int64
	if err := pool.QueryRow(t.Context(), `
		SELECT pull.base_sha, intent.generation, intent.completed_generation
		FROM pull_requests AS pull
		JOIN repos ON repos.id = pull.repo_id
		JOIN refresh_intent_generations AS intent
		  ON intent.kind = 'refresh_pr' AND intent.refresh_key = $2
		WHERE repos.gh_id = $1 AND pull.number = 1
	`, repository.GitHubID, refreshKey).Scan(
		&baseSHA, &generation, &completed,
	); err != nil {
		t.Fatal(err)
	}
	if baseSHA != "base-new" || generation != 3 || completed != 3 {
		t.Fatalf(
			"final branch state base=%q generation=%d completed=%d, want base-new/3/3",
			baseSHA, generation, completed,
		)
	}
}

func TestMatchingStackSummarySkipsEagerStackRefresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		size         int
		position     int
		cachedSHA    string
		payloadSHA   any
		wantStackJob bool
	}{
		{
			name:       "matching known tuple",
			size:       6,
			position:   1,
			cachedSHA:  "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA: "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
		},
		{
			name:         "size mismatch",
			size:         7,
			position:     1,
			cachedSHA:    "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA:   "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			wantStackJob: true,
		},
		{
			name:         "historical position beyond current size",
			size:         6,
			position:     7,
			cachedSHA:    "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA:   "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			wantStackJob: true,
		},
		{
			name:         "payload SHA unknown",
			size:         6,
			position:     1,
			cachedSHA:    "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA:   nil,
			wantStackJob: true,
		},
		{
			name:         "cached SHA unknown",
			size:         6,
			position:     1,
			cachedSHA:    "",
			payloadSHA:   "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			wantStackJob: true,
		},
		{
			name:         "both SHAs unknown",
			size:         6,
			position:     1,
			cachedSHA:    "",
			payloadSHA:   nil,
			wantStackJob: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pool := dispatchTestDatabase(t)
			riverClient, err := queue.NewClient(pool)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 7, 29, 20, 0, 0, 0, time.UTC)
			repository := store.RepositoryRecord{
				InstallationID:  1,
				OrgID:           1,
				GitHubID:        1001,
				NodeID:          "R_stack_summary",
				Owner:           "acme",
				Name:            "monolith",
				FullName:        "acme/monolith",
				DefaultBranch:   "main",
				GitHubUpdatedAt: now,
			}
			entries := make([]store.StackEntry, 6)
			for index := range entries {
				entries[index] = store.StackEntry{
					Number:    72787 + index,
					State:     "open",
					UpdatedAt: now,
					HeadRef:   fmt.Sprintf("stack/layer-%d", index+1),
					HeadSHA:   fmt.Sprintf("head-%d", index+1),
				}
			}
			if _, err := store.NewEntityWriter(pool).ApplyStack(
				context.Background(),
				store.StackRecord{
					Repository:      repository,
					GitHubID:        46101,
					NodeID:          "S_stack_summary",
					Number:          72787,
					BaseRef:         "main",
					BaseSHA:         test.cachedSHA,
					Open:            true,
					Entries:         entries,
					GitHubUpdatedAt: now,
					SyncedAt:        now,
					Source:          store.SyncSourceWebhook,
				},
			); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]any{
				"action":     "synchronize",
				"number":     72787,
				"repository": map[string]any{"full_name": "acme/monolith"},
				"pull_request": map[string]any{
					"number": 72787,
					"stack": map[string]any{
						"id":       46101,
						"number":   72787,
						"size":     test.size,
						"position": test.position,
						"base": map[string]any{
							"ref": "main",
							"sha": test.payloadSHA,
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(context.Background(), `
				INSERT INTO webhook_deliveries (
					delivery_guid, event, raw_body, headers, received_at
				)
				VALUES ($1, 'pull_request', $2, '{}'::jsonb, $3)
			`, "stack-summary-"+test.name, payload, now); err != nil {
				t.Fatal(err)
			}
			dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
				BatchSize:    10,
				MaxAttempts:  3,
				Debounce:     5 * time.Second,
				PollInterval: time.Millisecond,
				Now:          func() time.Time { return now },
				Classifier:   DefaultClassifier(),
			})
			if count, err := dispatcher.DispatchBatch(
				context.Background(),
			); err != nil || count != 1 {
				t.Fatalf("dispatch count=%d err=%v", count, err)
			}
			var resolverJobs, stackJobs int
			if err := pool.QueryRow(context.Background(), `
				SELECT
					count(*) FILTER (
						WHERE kind = 'resolve_stack_membership'
					),
					count(*) FILTER (WHERE kind = 'refresh_stack')
				FROM river_job
				WHERE args->>'key' LIKE '%:acme/monolith:%'
			`).Scan(&resolverJobs, &stackJobs); err != nil {
				t.Fatal(err)
			}
			wantStackJobs := 0
			if test.wantStackJob {
				wantStackJobs = 1
			}
			if resolverJobs != 1 || stackJobs != wantStackJobs {
				t.Fatalf(
					"resolver/stack jobs = %d/%d, want 1/%d",
					resolverJobs,
					stackJobs,
					wantStackJobs,
				)
			}
		})
	}
}

func TestRunningRefreshRetryableTransitionCannotCollide(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		),
	))
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	repo := "acme/running-follow-up"
	key := "pr:" + repo + ":42"
	now := time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC)
	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize:    10,
		MaxAttempts:  3,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Classifier:   DefaultClassifier(),
	})
	emitPullRequest := func(guid string) {
		t.Helper()
		if _, err := fake.EmitWebhookWithGUID(
			context.Background(),
			server.URL+ingress.WebhookPath,
			"pull_request",
			guid,
			map[string]any{
				"action":       "synchronize",
				"number":       42,
				"pull_request": map[string]any{"number": 42},
				"repository":   map[string]any{"full_name": repo},
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	emitPullRequest("running-follow-up-1")
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("first dispatch count=%d err=%v", count, err)
	}
	tag, err := pool.Exec(context.Background(), `
		UPDATE river_job SET state = 'running'
		WHERE kind = 'refresh_pr' AND args->>'key' = $1
	`, key)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("mark running rows=%d err=%v", tag.RowsAffected(), err)
	}

	now = now.Add(time.Second)
	emitPullRequest("running-follow-up-2")
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("follow-up dispatch count=%d err=%v", count, err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM river_job
		WHERE kind = 'refresh_pr' AND args->>'key' = $1
	`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("jobs for running key = %d, want one coalesced running job", count)
	}
	tag, err = pool.Exec(context.Background(), `
		UPDATE river_job SET state = 'retryable'
		WHERE kind = 'refresh_pr' AND args->>'key' = $1 AND state = 'running'
	`, key)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("running -> retryable rows=%d err=%v", tag.RowsAffected(), err)
	}
	var state string
	var generation int64
	if err := pool.QueryRow(context.Background(), `
		SELECT job.state, generation.generation
		FROM river_job AS job
		JOIN refresh_intent_generations AS generation
		  ON generation.kind = job.kind
		 AND generation.refresh_key = job.args->>'key'
		WHERE job.kind = 'refresh_pr' AND job.args->>'key' = $1
	`, key).Scan(&state, &generation); err != nil {
		t.Fatal(err)
	}
	if state != "retryable" || generation != 2 {
		t.Fatalf("state=%s generation=%d, want retryable generation 2", state, generation)
	}
}

func TestPoisonDeliveryParksAndUnknownEventDoesNot(t *testing.T) {
	t.Parallel()
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		),
	))
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	observer := &recordingDispatchObserver{}
	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize:    10,
		MaxAttempts:  2,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          time.Now,
		Classifier:   DefaultClassifier(),
		Observer:     observer,
	})

	if _, err := fake.EmitWebhookWithGUID(
		context.Background(),
		server.URL+ingress.WebhookPath,
		"pull_request",
		"poison-known",
		map[string]any{"action": "opened"},
	); err != nil {
		t.Fatal(err)
	}
	firstAttemptStarted := time.Now().UTC()
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil ||
		count != 1 {
		t.Fatalf("first poison dispatch count=%d err=%v", count, err)
	}
	poison, err := dbgen.New(pool).GetWebhookDelivery(
		context.Background(),
		"poison-known",
	)
	if err != nil {
		t.Fatal(err)
	}
	if poison.Status != "pending" ||
		poison.Attempts != 1 ||
		!poison.NextAttemptAt.Valid ||
		poison.NextAttemptAt.Time.Before(
			firstAttemptStarted.Add(classificationRetryBaseBackoff),
		) {
		t.Fatalf("first poison attempt = %+v", poison)
	}
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil ||
		count != 0 {
		t.Fatalf("not-yet-due poison dispatch count=%d err=%v", count, err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE webhook_deliveries
		SET next_attempt_at = clock_timestamp()
		WHERE delivery_guid = 'poison-known'
	`); err != nil {
		t.Fatal(err)
	}
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil ||
		count != 1 {
		t.Fatalf("due poison dispatch count=%d err=%v", count, err)
	}
	poison, err = dbgen.New(pool).GetWebhookDelivery(
		context.Background(),
		"poison-known",
	)
	if err != nil {
		t.Fatal(err)
	}
	if poison.Status != "parked" || poison.Attempts != 2 || !poison.LastError.Valid {
		t.Fatalf("poison delivery = %+v", poison)
	}
	parked, err := dbgen.New(pool).ListParkedWebhookDeliveries(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked) != 1 || parked[0].DeliveryGuid != "poison-known" {
		t.Fatalf("parked deliveries = %+v", parked)
	}

	if _, err := fake.EmitWebhookWithGUID(
		context.Background(),
		server.URL+ingress.WebhookPath,
		"unknown_event",
		"poison-unknown",
		map[string]any{"deliberately": "unclassified"},
	); err != nil {
		t.Fatal(err)
	}
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("unknown dispatch count=%d err=%v", count, err)
	}
	unknown, err := dbgen.New(pool).GetWebhookDelivery(context.Background(), "poison-unknown")
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Status != "processed" || unknown.LastError.Valid {
		t.Fatalf("unknown delivery = %+v", unknown)
	}
	if !reflect.DeepEqual(observer.unmatchedEvents, []string{"unknown_event"}) {
		t.Fatalf(
			"unmatched event signals = %#v, want [unknown_event]",
			observer.unmatchedEvents,
		)
	}

	requeued, err := dbgen.New(pool).RequeueParkedWebhookDeliveries(
		context.Background(),
		dbgen.RequeueParkedWebhookDeliveriesParams{
			DeliveryGuids: []string{poison.DeliveryGuid},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 1 {
		t.Fatalf("requeued rows = %d, want 1", requeued)
	}
	poison, err = dbgen.New(pool).GetWebhookDelivery(
		context.Background(),
		poison.DeliveryGuid,
	)
	if err != nil {
		t.Fatal(err)
	}
	if poison.Status != "pending" ||
		poison.Attempts != 0 ||
		!poison.LastError.Valid ||
		!strings.Contains(poison.LastError.String, "operator requeue") ||
		!strings.Contains(poison.LastError.String, "attempts reset from 2") {
		t.Fatalf("requeued poison delivery = %+v", poison)
	}
	requeued, err = dbgen.New(pool).RequeueParkedWebhookDeliveries(
		context.Background(),
		dbgen.RequeueParkedWebhookDeliveriesParams{
			DeliveryGuids: []string{poison.DeliveryGuid},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 0 {
		t.Fatalf("non-parked requeue rows = %d, want 0", requeued)
	}
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("replayed poison dispatch count=%d err=%v", count, err)
	}
	poison, err = dbgen.New(pool).GetWebhookDelivery(
		context.Background(),
		poison.DeliveryGuid,
	)
	if err != nil {
		t.Fatal(err)
	}
	if poison.Status != "pending" || poison.Attempts != 1 {
		t.Fatalf("replayed poison delivery = %+v", poison)
	}

	for index := range 101 {
		guid := fmt.Sprintf("parked-family-%03d", index)
		if _, err := pool.Exec(context.Background(), `
			INSERT INTO webhook_deliveries (
				delivery_guid, event, raw_body, headers, status, attempts, last_error
			)
			VALUES ($1, 'push', '{}'::bytea, '{}'::jsonb, 'parked', 4, 'poison')
		`, guid); err != nil {
			t.Fatal(err)
		}
	}
	requeued, err = dbgen.New(pool).RequeueParkedWebhookDeliveries(
		context.Background(),
		dbgen.RequeueParkedWebhookDeliveriesParams{
			Event:         "push",
			ErrorContains: "poison",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 100 {
		t.Fatalf("bounded-family requeued rows = %d, want 100", requeued)
	}
	var resetCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM webhook_deliveries
		WHERE delivery_guid LIKE 'parked-family-%'
		  AND status = 'pending'
		  AND attempts = 0
		  AND last_error LIKE 'operator requeue%'
	`).Scan(&resetCount); err != nil {
		t.Fatal(err)
	}
	if resetCount != 100 {
		t.Fatalf("bounded-family reset rows = %d, want 100", resetCount)
	}
	var parkedRemainder int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM webhook_deliveries
		WHERE delivery_guid LIKE 'parked-family-%'
		  AND status = 'parked'
	`).Scan(&parkedRemainder); err != nil {
		t.Fatal(err)
	}
	if parkedRemainder != 1 {
		t.Fatalf("bounded-family parked remainder = %d, want 1", parkedRemainder)
	}
}

type recordingDispatchObserver struct {
	unmatchedEvents []string
}

func (*recordingDispatchObserver) DispatchBatch(context.Context, int) {}

func (observer *recordingDispatchObserver) DispatchUnmatchedEvent(
	_ context.Context,
	event string,
) {
	observer.unmatchedEvents = append(observer.unmatchedEvents, event)
}

func dispatchTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.New(t).Pool
}

func cloneDispatchPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clone, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatalf("clone dispatch pool: %v", err)
	}
	if err := clone.Ping(ctx); err != nil {
		clone.Close()
		t.Fatalf("ping cloned dispatch pool: %v", err)
	}
	t.Cleanup(clone.Close)
	return clone
}

func mustNewDispatcher(
	t *testing.T,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	config Config, //nolint:gocritic // helper mirrors the constructor's value-options API
) *Dispatcher {
	t.Helper()
	dispatcher, err := New(pool, client, config)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func deliveryPayloadForRepo(
	t *testing.T,
	payload json.RawMessage,
	repo string,
) (map[string]any, []byte) {
	t.Helper()
	decoded := make(map[string]any)
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded["repository"] = map[string]any{"full_name": repo}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	return decoded, encoded
}

func goldenJobsForRepo(golden []Intent, repo string) []Intent {
	result := make([]Intent, 0, len(golden))
	for _, intent := range golden {
		// The classifier still emits branch intent decisions (and the replay
		// unit golden pins those decisions), but dispatch consumes them into
		// bulk reconciliation state rather than inserting refresh_branch jobs.
		if intent.Kind == queue.KindRefreshBranch {
			continue
		}
		intent.Key = strings.Replace(intent.Key, "acme/monolith", repo, 1)
		result = append(result, intent)
	}
	sortIntents(result)
	return result
}

func riverDecisionsExact(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
	expectedCount int,
) []Intent {
	t.Helper()
	var totalCount, duplicateGroups int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM river_job
		WHERE args->>'key' LIKE $1
	`, "%:"+repo+":%").Scan(&totalCount); err != nil {
		t.Fatal(err)
	}
	if totalCount != expectedCount {
		t.Fatalf("River rows for %s = %d, want exactly %d", repo, totalCount, expectedCount)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM (
			SELECT kind, args->>'key'
			FROM river_job
			WHERE args->>'key' LIKE $1
			GROUP BY kind, args->>'key'
			HAVING count(*) <> 1
		) AS duplicate_or_missing_groups
	`, "%:"+repo+":%").Scan(&duplicateGroups); err != nil {
		t.Fatal(err)
	}
	if duplicateGroups != 0 {
		t.Fatalf("River has %d non-singleton kind/key groups for %s", duplicateGroups, repo)
	}

	rows, err := pool.Query(context.Background(), `
		SELECT kind, args, queue, priority, state,
		       river_job_state_in_bitmask(unique_states, 'running'),
		       river_job_state_in_bitmask(unique_states, 'completed')
		FROM river_job
		WHERE args->>'key' LIKE $1
		ORDER BY kind, args->>'key'
	`, "%:"+repo+":%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	decisions := make([]Intent, 0, expectedCount)
	for rows.Next() {
		var kind, queueName, state string
		var priority int
		var encodedArgs []byte
		var runningUnique, completedUnique bool
		if err := rows.Scan(
			&kind,
			&encodedArgs,
			&queueName,
			&priority,
			&state,
			&runningUnique,
			&completedUnique,
		); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		if err := json.Unmarshal(encodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		if len(args) != 2 || args["kind"] != kind {
			t.Fatalf("job %s args contain payload data: %s", kind, encodedArgs)
		}
		key, ok := args["key"].(string)
		if !ok || !strings.Contains(key, repo) {
			t.Fatalf("job %s key = %#v", kind, args["key"])
		}
		if queueName != queue.QueueEvent || priority != 1 || state != "scheduled" {
			t.Fatalf(
				"job %s/%s queue=%s priority=%d state=%s",
				kind,
				key,
				queueName,
				priority,
				state,
			)
		}
		if !runningUnique || completedUnique {
			t.Fatalf(
				"job %s/%s uniqueness running=%t completed=%t",
				kind,
				key,
				runningUnique,
				completedUnique,
			)
		}
		intent := Intent{Kind: kind, Key: key, Priority: queueName}
		decisions = append(decisions, intent)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sortIntents(decisions)
	return decisions
}
