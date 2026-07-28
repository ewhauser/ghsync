package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/store"
)

func TestThreeQueuesExecuteNoopJobs(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := store.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	client, err := NewClient(pool)
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
		result, err := client.Insert(ctx, NoopArgs{}, &river.InsertOpts{Queue: queueName})
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
