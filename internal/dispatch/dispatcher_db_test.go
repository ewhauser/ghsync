package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/ingress"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

const testWebhookSecret = "dispatch-test-secret"

func TestFullRecordedReplayIngressToRiver(t *testing.T) {
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	deliveries := loadRecordedDeliveries(t)
	golden := loadGoldenJobs(t)
	baseTime := time.Date(2026, 7, 28, 18, 0, 0, 0, time.UTC)

	for run := 0; run < 4; run++ {
		random := rand.New(rand.NewSource(int64(run + 100))) //nolint:gosec
		permuted := append([]recordedDelivery(nil), deliveries...)
		random.Shuffle(len(permuted), func(i, j int) {
			permuted[i], permuted[j] = permuted[j], permuted[i]
		})
		repo := fmt.Sprintf("acme/monolith-replay-%d", run)
		guidPrefix := fmt.Sprintf("replay-%d-", run)
		dispatchTime := baseTime.Add(time.Duration(run) * time.Hour)
		dispatchOnce := func(batchSize int) int {
			t.Helper()
			dispatcher := New(pool, riverClient, Config{
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
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	repo := "acme/rebase-storm"
	branches := []string{"stack/layer-1", "stack/layer-2", "stack/layer-3"}
	stackKey := "stack:" + repo + ":142"

	for index := 0; index < 20; index++ {
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
	dispatcher := New(pool, riverClient, Config{
		BatchSize:    1,
		MaxAttempts:  3,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          func() time.Time { return now },
		Classifier:   DefaultClassifier(),
	})
	for index := 0; index < 20; index++ {
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
}

func TestRunningRefreshRetryableTransitionCannotCollide(t *testing.T) {
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	repo := "acme/running-follow-up"
	key := "branch:" + repo + ":stack/layer"
	now := time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC)
	dispatcher := New(pool, riverClient, Config{
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
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(
			dbgen.New(pool),
			testWebhookSecret,
			1<<20,
			5*time.Second,
		).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	dispatcher := New(pool, riverClient, Config{
		BatchSize:    10,
		MaxAttempts:  2,
		Debounce:     5 * time.Second,
		PollInterval: time.Millisecond,
		Now:          time.Now,
		Classifier:   DefaultClassifier(),
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
	for range 2 {
		if count, err := dispatcher.DispatchBatch(context.Background()); err != nil || count != 1 {
			t.Fatalf("poison dispatch count=%d err=%v", count, err)
		}
	}
	poison, err := dbgen.New(pool).GetWebhookDelivery(context.Background(), "poison-known")
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

	requeued, err := dbgen.New(pool).RequeueParkedWebhookDeliveries(
		context.Background(),
		dbgen.RequeueParkedWebhookDeliveriesParams{
			AllParked:    false,
			DeliveryGuid: poison.DeliveryGuid,
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
			AllParked:    false,
			DeliveryGuid: poison.DeliveryGuid,
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

	for _, guid := range []string{"parked-all-1", "parked-all-2"} {
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
			AllParked: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 2 {
		t.Fatalf("all-parked requeued rows = %d, want 2", requeued)
	}
	var resetCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM webhook_deliveries
		WHERE delivery_guid = ANY($1::text[])
		  AND status = 'pending'
		  AND attempts = 0
		  AND last_error LIKE 'operator requeue%'
	`, []string{"parked-all-1", "parked-all-2"}).Scan(&resetCount); err != nil {
		t.Fatal(err)
	}
	if resetCount != 2 {
		t.Fatalf("all-parked reset rows = %d, want 2", resetCount)
	}
}

func dispatchTestDatabase(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("frontier_dispatch_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatalf("parse test database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatalf("open schema pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		admin.Close()
		t.Fatalf("ping schema pool: %v", err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		pool.Close()
		admin.Close()
		t.Fatalf("migrate schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(dropCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})
	return pool
}

func drainDispatcher(t *testing.T, dispatcher *Dispatcher) {
	t.Helper()
	for {
		count, err := dispatcher.DispatchBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
	}
}

func deliveryPayloadForRepo(
	t *testing.T,
	payload json.RawMessage,
	repo string,
) (map[string]any, []byte) {
	t.Helper()
	var decoded map[string]any
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
