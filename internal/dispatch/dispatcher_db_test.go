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
		ingress.NewHandler(dbgen.New(pool), testWebhookSecret, 1<<20).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	deliveries := loadRecordedDeliveries(t)
	baseTime := time.Date(2026, 7, 28, 18, 0, 0, 0, time.UTC)

	for run := 0; run < 4; run++ {
		random := rand.New(rand.NewSource(int64(run + 100))) //nolint:gosec
		permuted := append([]recordedDelivery(nil), deliveries...)
		random.Shuffle(len(permuted), func(i, j int) {
			permuted[i], permuted[j] = permuted[j], permuted[i]
		})
		repo := fmt.Sprintf("acme/monolith-replay-%d", run)
		guidPrefix := fmt.Sprintf("replay-%d-", run)
		expected := make(map[string]Intent)
		for _, delivery := range permuted {
			payload, encoded := deliveryPayloadForRepo(t, delivery.Payload, repo)
			intents, err := DefaultClassifier().Classify(delivery.Event, encoded)
			if err != nil {
				t.Fatal(err)
			}
			for _, intent := range intents {
				expected[decisionID(intent)] = intent
			}

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
		}

		dispatcher := New(pool, riverClient, Config{
			BatchSize:    100,
			MaxAttempts:  3,
			Debounce:     5 * time.Second,
			PollInterval: time.Millisecond,
			Now:          func() time.Time { return baseTime.Add(time.Duration(run) * time.Hour) },
			Classifier:   DefaultClassifier(),
		})
		drainDispatcher(t, dispatcher)

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

		got := riverDecisionSet(t, pool, repo)
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("run %d River decisions differ\n got: %#v\nwant: %#v", run, got, expected)
		}
	}
}

func TestRebaseStormCoalescesPerBranchWithoutSlidingDebounce(t *testing.T) {
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(dbgen.New(pool), testWebhookSecret, 1<<20).Mux(),
	)
	defer server.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), testWebhookSecret)
	repo := "acme/rebase-storm"
	branches := []string{"stack/layer-1", "stack/layer-2", "stack/layer-3"}

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

	rows, err := pool.Query(context.Background(), `
		SELECT args->>'key', count(*), min(scheduled_at)
		FROM river_job
		WHERE args->>'key' LIKE $1
		GROUP BY args->>'key'
	`, "branch:"+repo+":%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]time.Time)
	for rows.Next() {
		var key string
		var count int
		var scheduledAt time.Time
		if err := rows.Scan(&key, &count, &scheduledAt); err != nil {
			t.Fatal(err)
		}
		if count > 1 {
			t.Fatalf("%s has %d queued jobs, want at most 1", key, count)
		}
		got[key] = scheduledAt
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC)
	for index, branch := range branches {
		key := "branch:" + repo + ":" + branch
		wantScheduled := first.Add(time.Duration(index)*500*time.Millisecond + 5*time.Second)
		if !got[key].Equal(wantScheduled) {
			t.Fatalf("%s scheduled at %s, want first-intent time %s", key, got[key], wantScheduled)
		}
	}
}

func TestRunningRefreshAllowsQueuedFollowUp(t *testing.T) {
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(dbgen.New(pool), testWebhookSecret, 1<<20).Mux(),
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
	if count != 2 {
		t.Fatalf("jobs for running key = %d, want running + queued follow-up", count)
	}
}

func TestPoisonDeliveryParksAndUnknownEventDoesNot(t *testing.T) {
	pool := dispatchTestDatabase(t)
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(
		ingress.NewHandler(dbgen.New(pool), testWebhookSecret, 1<<20).Mux(),
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

func riverDecisionSet(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
) map[string]Intent {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT kind, args, queue, priority, state, unique_states::text
		FROM river_job
		WHERE args->>'key' LIKE $1
	`, "%:"+repo+":%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	decisions := make(map[string]Intent)
	for rows.Next() {
		var kind, queueName, state, uniqueStates string
		var priority int
		var encodedArgs []byte
		if err := rows.Scan(
			&kind,
			&encodedArgs,
			&queueName,
			&priority,
			&state,
			&uniqueStates,
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
		if uniqueStates != "10110001" {
			t.Fatalf("job %s/%s unique states = %s", kind, key, uniqueStates)
		}
		intent := Intent{Kind: kind, Key: key, Priority: queueName}
		decisions[decisionID(intent)] = intent
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return decisions
}
