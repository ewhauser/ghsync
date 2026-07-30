package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/rivercontrib/otelriver"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

type noopArgs struct{}

func (noopArgs) Kind() string { return "noop" }

type noopWorker struct {
	river.WorkerDefaults[noopArgs]
}

func (*noopWorker) Work(context.Context, *river.Job[noopArgs]) error {
	return nil
}

func registerNoopWorker(workers *river.Workers) {
	river.AddWorker(workers, &noopWorker{})
}

func TestThreeQueuesExecuteNoopJobs(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.New(t).Pool

	client, err := NewClient(pool, WithWorkerRegistrar(registerNoopWorker))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	events, unsubscribe := client.Subscribe(
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	defer func() {
		if stopped {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	jobIDs := make(map[int64]string, 3)
	for _, queueName := range []string{QueueInteractive, QueueEvent, QueueSweep} {
		result, err := client.Insert(ctx, noopArgs{}, &river.InsertOpts{Queue: queueName})
		if err != nil {
			t.Fatalf("insert %s job: %v", queueName, err)
		}
		jobIDs[result.Job.ID] = queueName
	}

	for len(jobIDs) > 0 {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("River event stream closed; pending=%v", jobIDs)
			}
			queueName, belongsToTest := jobIDs[event.Job.ID]
			if !belongsToTest {
				continue
			}
			if event.Kind == river.EventKindJobFailed {
				t.Fatalf("%s queue job %d failed", queueName, event.Job.ID)
			}
			delete(jobIDs, event.Job.ID)
		case <-ctx.Done():
			t.Fatalf("waiting for queue jobs: %v; pending=%v", ctx.Err(), jobIDs)
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := client.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped = true
}

func TestRiverOpenTelemetryPropagatesInsertLinkToWorker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database := testdb.New(t)

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	plugin := otelriver.NewMiddleware(&otelriver.MiddlewareConfig{
		EnableTracePropagation:      true,
		EnableWorkSpanJobKindSuffix: true,
		MeterProvider:               metricnoop.NewMeterProvider(),
		TracerProvider:              provider,
	})
	client, err := NewClient(
		database.Pool,
		WithWorkerRegistrar(registerNoopWorker),
		WithPlugins(plugin),
	)
	if err != nil {
		t.Fatal(err)
	}
	events, unsubscribe := client.Subscribe(
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if stopped {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	insertCtx, parent := provider.Tracer("queue-test").Start(ctx, "enqueue")
	result, err := client.Insert(
		insertCtx,
		noopArgs{},
		&river.InsertOpts{Queue: QueueEvent},
	)
	parent.End()
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-events:
			if event.Job.ID != result.Job.ID {
				continue
			}
			if event.Kind == river.EventKindJobFailed {
				t.Fatalf("traced River job %d failed", result.Job.ID)
			}
			goto completed
		case <-ctx.Done():
			t.Fatalf("waiting for traced River job: %v", ctx.Err())
		}
	}

completed:
	stopCtx, stopCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer stopCancel()
	if err := client.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	stopped = true

	var metadata []byte
	if err := database.Pool.QueryRow(
		ctx,
		`SELECT metadata FROM river_job WHERE id = $1`,
		result.Job.ID,
	).Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	var metadataObject map[string]any
	if err := json.Unmarshal(metadata, &metadataObject); err != nil {
		t.Fatal(err)
	}
	if metadataObject["traceparent"] == nil {
		t.Fatalf("River metadata has no traceparent: %s", metadata)
	}

	var insertSpan, workSpan tracetest.SpanStub
	var foundInsert, foundWork bool
	for _, ended := range recorder.Ended() {
		switch ended.Name() {
		case "river.insert_many":
			insertSpan = tracetest.SpanStubFromReadOnlySpan(ended)
			foundInsert = true
		case "river.work/noop":
			workSpan = tracetest.SpanStubFromReadOnlySpan(ended)
			foundWork = true
		}
	}
	if !foundInsert || !foundWork {
		t.Fatalf(
			"River spans found insert=%t work=%t",
			foundInsert,
			foundWork,
		)
	}
	if len(workSpan.Links) != 1 {
		t.Fatalf("worker span links = %d, want 1", len(workSpan.Links))
	}
	if workSpan.Links[0].SpanContext.SpanID() !=
		insertSpan.SpanContext.SpanID() {
		t.Fatalf(
			"worker link span ID = %s, insert span ID = %s",
			workSpan.Links[0].SpanContext.SpanID(),
			insertSpan.SpanContext.SpanID(),
		)
	}
}

func TestExplicitQueueSelectionDoesNotPollUnownedQueues(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.New(t).Pool

	client, err := NewClient(
		pool,
		WithQueues(QueueDrift),
		WithoutRefreshWorkers(),
		WithWorkerRegistrar(registerNoopWorker),
	)
	if err != nil {
		t.Fatalf("new isolated client: %v", err)
	}
	owned, err := client.Insert(
		ctx,
		noopArgs{},
		&river.InsertOpts{Queue: QueueDrift},
	)
	if err != nil {
		t.Fatalf("insert owned queue job: %v", err)
	}
	unowned, err := client.Insert(
		ctx,
		noopArgs{},
		&river.InsertOpts{Queue: QueueSweep},
	)
	if err != nil {
		t.Fatalf("insert unowned queue job: %v", err)
	}
	events, unsubscribe := client.Subscribe(
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start isolated client: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	for {
		select {
		case event := <-events:
			if event.Job.ID != owned.Job.ID {
				continue
			}
			if event.Kind == river.EventKindJobFailed {
				t.Fatalf("owned queue job %d failed", event.Job.ID)
			}
			var state string
			if err := pool.QueryRow(ctx, `
				SELECT state
				FROM river_job
				WHERE id = $1
			`, unowned.Job.ID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != "available" {
				t.Fatalf(
					"unowned queue job state = %q, want available",
					state,
				)
			}
			return
		case <-ctx.Done():
			t.Fatalf("waiting for owned queue job: %v", ctx.Err())
		}
	}
}

func TestDirtyGenerationReinsertsFollowUpAfterCompletion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.New(t).Pool

	key := fmt.Sprintf("branch:acme/dirty-follow-up:%d", time.Now().UnixNano())
	pointer, err := json.Marshal([]map[string]string{{
		"kind":        KindRefreshBranch,
		"refresh_key": key,
	}})
	if err != nil {
		t.Fatal(err)
	}
	generations, err := dbgen.New(pool).BumpRefreshIntentGenerations(ctx, pointer)
	if err != nil || len(generations) != 1 || generations[0].Generation != 1 {
		t.Fatalf("initial generations=%+v err=%v", generations, err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	workCount := 0
	workers := river.NewWorkers()
	river.AddWorker(workers, &refreshBranchWorker{
		pool: pool,
		work: func(ctx context.Context, _ RefreshArgs) error {
			workCount++
			if workCount == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:  map[string]river.QueueConfig{QueueEvent: {MaxWorkers: 1}},
		Workers: workers,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if _, err := client.Insert(
		ctx,
		NewRefreshBranchArgs(key),
		NewRefreshInsertOpts(time.Time{}),
	); err != nil {
		t.Fatalf("insert initial refresh: %v", err)
	}

	events, unsubscribe := client.Subscribe(
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
	)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	defer func() {
		if stopped {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = client.StopAndCancel(stopCtx)
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatalf("waiting for first refresh to start: %v", ctx.Err())
	}
	generations, err = dbgen.New(pool).BumpRefreshIntentGenerations(ctx, pointer)
	if err != nil || len(generations) != 1 || generations[0].Generation != 2 {
		t.Fatalf("dirty generations=%+v err=%v", generations, err)
	}
	duplicate, err := client.Insert(
		ctx,
		NewRefreshBranchArgs(key),
		NewRefreshInsertOpts(time.Time{}),
	)
	if err != nil {
		t.Fatalf("insert running duplicate: %v", err)
	}
	if !duplicate.UniqueSkippedAsDuplicate {
		t.Fatal("running duplicate was inserted instead of coalesced")
	}
	close(release)

	completed := 0
	for completed < 2 {
		select {
		case event := <-events:
			var args RefreshArgs
			if err := json.Unmarshal(event.Job.EncodedArgs, &args); err != nil {
				t.Fatal(err)
			}
			if args.Key != key {
				continue
			}
			if event.Kind == river.EventKindJobFailed {
				t.Fatalf("dirty refresh job %d failed", event.Job.ID)
			}
			completed++
		case <-ctx.Done():
			t.Fatalf("waiting for dirty follow-up: %v", ctx.Err())
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := client.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	stopped = true

	var jobCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM river_job
		WHERE kind = $1 AND args->>'key' = $2
	`, KindRefreshBranch, key).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 2 {
		t.Fatalf("refresh jobs = %d, want completed job + one follow-up", jobCount)
	}
}
