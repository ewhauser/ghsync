package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestReconcileOrphanedRefreshPointersReplacesTerminalPointer(
	t *testing.T,
) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	client, err := NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("pr:acme/orphaned:%d", time.Now().UnixNano())
	insertRefresh := func(queueName string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
		if err := InsertRefreshesTx(
			ctx,
			tx,
			client,
			[]RefreshSpec{{Kind: KindRefreshPR, Key: key}},
			queueName,
		); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	countLivePointers := func() (int, string) {
		t.Helper()
		var count int
		var queueName string
		if err := pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(min(queue), '')
			FROM river_job
			WHERE kind = 'refresh_pr'
			  AND args->>'key' = $1
			  AND state IN (
			      'available', 'pending', 'retryable', 'running', 'scheduled'
			  )
		`, key).Scan(&count, &queueName); err != nil {
			t.Fatal(err)
		}
		return count, queueName
	}

	insertRefresh(QueueEvent)

	// River exhausts the pointer's retries and discards it; the outstanding
	// generation now has no live work capable of completing it (issue #61).
	discarded, err := pool.Exec(ctx, `
		UPDATE river_job
		SET state = 'discarded', finalized_at = now()
		WHERE kind = 'refresh_pr' AND args->>'key' = $1
	`, key)
	if err != nil {
		t.Fatal(err)
	}
	if discarded.RowsAffected() != 1 {
		t.Fatalf("discarded pointer jobs = %d, want 1", discarded.RowsAffected())
	}

	result, err := ReconcileOrphanedRefreshPointers(
		ctx, pool, client, time.Now(), 100, QueueSweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Orphans) != 1 || result.Delayed != 0 {
		t.Fatalf("first pass = %+v, want one orphan", result)
	}
	orphan := result.Orphans[0]
	if orphan.Kind != KindRefreshPR || orphan.Key != key ||
		orphan.Generation != 1 || orphan.TerminalState != "discarded" {
		t.Fatalf("orphan = %+v", orphan)
	}
	if count, queueName := countLivePointers(); count != 1 ||
		queueName != QueueSweep {
		t.Fatalf(
			"replacement pointers = %d in %q, want 1 in %q",
			count,
			queueName,
			QueueSweep,
		)
	}

	// A second pass must deduplicate against the replacement: the generation
	// is re-enqueued exactly once even with concurrent reconcilers.
	again, err := ReconcileOrphanedRefreshPointers(
		ctx, pool, client, time.Now(), 100, QueueSweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Orphans) != 0 || again.Delayed != 1 {
		t.Fatalf("second pass = %+v, want one delayed", again)
	}
	if count, _ := countLivePointers(); count != 1 {
		t.Fatalf("pointers after second pass = %d, want 1", count)
	}

	// Newer producer generations still coalesce onto the replacement pointer.
	insertRefresh(QueueEvent)
	var generation int64
	if err := pool.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr' AND refresh_key = $1
	`, key).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if count, _ := countLivePointers(); count != 1 || generation != 2 {
		t.Fatalf(
			"pointers/generation after coalesce = %d/%d, want 1/2",
			count,
			generation,
		)
	}

	// A completed generation is never rescanned or falsely re-enqueued.
	if _, err := pool.Exec(ctx, `
		UPDATE refresh_intent_generations
		SET completed_generation = generation
		WHERE kind = 'refresh_pr' AND refresh_key = $1
	`, key); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE river_job
		SET state = 'discarded', finalized_at = now()
		WHERE kind = 'refresh_pr' AND args->>'key' = $1
		  AND state IN (
		      'available', 'pending', 'retryable', 'running', 'scheduled'
		  )
	`, key); err != nil {
		t.Fatal(err)
	}
	final, err := ReconcileOrphanedRefreshPointers(
		ctx, pool, client, time.Now(), 100, QueueSweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	if final.Scanned != 0 || len(final.Orphans) != 0 {
		t.Fatalf("completed-generation pass = %+v, want empty", final)
	}
	if count, _ := countLivePointers(); count != 0 {
		t.Fatalf("pointers after completion = %d, want 0", count)
	}
}

func TestReconcileOrphanedRefreshPointersLeavesDelayedPointersAlone(
	t *testing.T,
) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	client, err := NewClient(pool)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("pr:acme/delayed:%d", time.Now().UnixNano())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if err := InsertRefreshesTx(
		ctx,
		tx,
		client,
		[]RefreshSpec{{
			Kind:        KindRefreshPR,
			Key:         key,
			ScheduledAt: time.Now().Add(time.Hour),
		}},
		QueueSweep,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// A staleness horizon before the row's updated_at leaves it unscanned.
	early, err := ReconcileOrphanedRefreshPointers(
		ctx, pool, client, time.Now().Add(-time.Hour), 100, QueueSweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	if early.Scanned != 0 {
		t.Fatalf("young outstanding row scanned = %+v", early)
	}

	// Once scanned, a live scheduled pointer proves the generation is merely
	// delayed: no replacement is inserted and its schedule is untouched.
	result, err := ReconcileOrphanedRefreshPointers(
		ctx, pool, client, time.Now(), 100, QueueSweep,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Delayed != 1 || len(result.Orphans) != 0 {
		t.Fatalf("delayed pass = %+v, want one delayed", result)
	}
	var count int
	var state string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), min(state::text)
		FROM river_job
		WHERE kind = 'refresh_pr' AND args->>'key' = $1
	`, key).Scan(&count, &state); err != nil {
		t.Fatal(err)
	}
	if count != 1 || state != "scheduled" {
		t.Fatalf("delayed pointer count/state = %d/%s, want 1/scheduled", count, state)
	}
}
