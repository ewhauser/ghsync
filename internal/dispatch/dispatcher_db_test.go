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

func TestRebaseStormEscalatesStackBranchesWithoutSlidingDebounce(t *testing.T) {
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
	stackKey := "stack:" + repo + ":142"

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

	now := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
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

	var key string
	var jobCount int
	var scheduledAt time.Time
	err = pool.QueryRow(context.Background(), `
		SELECT args->>'key', count(*) OVER (), scheduled_at
		FROM river_job
		WHERE args->>'key' LIKE $1
	`, "%:"+repo+":%").Scan(&key, &jobCount, &scheduledAt)
	if err != nil {
		t.Fatal(err)
	}
	if key != stackKey || jobCount != 1 {
		t.Fatalf("storm jobs = key %q count %d, want %q count 1", key, jobCount, stackKey)
	}
	first := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
	wantScheduled := first.Add(5 * time.Second)
	if !scheduledAt.Equal(wantScheduled) {
		t.Fatalf("%s scheduled at %s, want %s", key, scheduledAt, wantScheduled)
	}
	var generation int64
	if err := pool.QueryRow(context.Background(), `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = $1 AND refresh_key = $2
	`, queue.KindRefreshStack, stackKey).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 20 {
		t.Fatalf("storm generation = %d, want 20 exact dispatch signals", generation)
	}
	var eventReceivedAt, firstReceivedAt time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT generations.event_received_at, min(deliveries.received_at)
		FROM refresh_intent_generations AS generations
		CROSS JOIN webhook_deliveries AS deliveries
		WHERE generations.kind = $1
		  AND generations.refresh_key = $2
		  AND deliveries.delivery_guid LIKE 'storm-%'
		GROUP BY generations.event_received_at
	`, queue.KindRefreshStack, stackKey).Scan(
		&eventReceivedAt,
		&firstReceivedAt,
	); err != nil {
		t.Fatal(err)
	}
	if !eventReceivedAt.Equal(firstReceivedAt) {
		t.Fatalf(
			"event SLO origin = %s, want earliest delivery %s",
			eventReceivedAt,
			firstReceivedAt,
		)
	}
}

func TestMatchingStackSummarySkipsEagerStackRefresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		size         int
		cachedSHA    string
		payloadSHA   any
		wantStackJob bool
	}{
		{
			name:       "matching known tuple",
			size:       6,
			cachedSHA:  "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA: "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
		},
		{
			name:         "size mismatch",
			size:         7,
			cachedSHA:    "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA:   "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			wantStackJob: true,
		},
		{
			name:         "payload SHA unknown",
			size:         6,
			cachedSHA:    "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			payloadSHA:   nil,
			wantStackJob: true,
		},
		{
			name:         "cached SHA unknown",
			size:         6,
			cachedSHA:    "",
			payloadSHA:   "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
			wantStackJob: true,
		},
		{
			name:         "both SHAs unknown",
			size:         6,
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
						"position": 1,
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
	key := "branch:" + repo + ":stack/layer"
	now := time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC)
	dispatcher := mustNewDispatcher(t, pool, riverClient, Config{
		BatchSize:    10,
		MaxAttempts:  3,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Classifier:   DefaultClassifier(),
	})
	emitPush := func(guid string) {
		t.Helper()
		if _, err := fake.EmitWebhookWithGUID(
			context.Background(),
			server.URL+ingress.WebhookPath,
			"push",
			guid,
			map[string]any{
				"ref":        "refs/heads/stack/layer",
				"repository": map[string]any{"full_name": repo},
			},
		); err != nil {
			t.Fatal(err)
		}
	}

	emitPush("running-follow-up-1")
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("first dispatch count=%d err=%v", count, err)
	}
	tag, err := pool.Exec(context.Background(), `
		UPDATE river_job SET state = 'running'
		WHERE args->>'key' = $1
	`, key)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("mark running rows=%d err=%v", tag.RowsAffected(), err)
	}

	now = now.Add(time.Second)
	emitPush("running-follow-up-2")
	if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
		t.Fatalf("follow-up dispatch count=%d err=%v", count, err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM river_job WHERE args->>'key' = $1
	`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("jobs for running key = %d, want one coalesced running job", count)
	}
	tag, err = pool.Exec(context.Background(), `
		UPDATE river_job SET state = 'retryable'
		WHERE args->>'key' = $1 AND state = 'running'
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
		WHERE job.args->>'key' = $1
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
