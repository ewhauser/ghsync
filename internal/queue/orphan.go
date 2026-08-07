package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// OrphanedRefreshPointer is one outstanding generation that had no live
// pointer job left and was re-enqueued by reconciliation (issue #61).
type OrphanedRefreshPointer struct {
	Kind       string
	Key        string
	Generation int64
	// TerminalState is the most recent terminal River job state recorded for
	// the refresh identity, or "unknown" when River already pruned it.
	TerminalState string
}

// OrphanReconcileResult reports one reconciliation pass. Delayed counts
// outstanding generations whose unique insert deduplicated against a live
// pointer; they are merely late, not orphaned.
type OrphanReconcileResult struct {
	Scanned int
	Delayed int
	Orphans []OrphanedRefreshPointer
}

// ReconcileOrphanedRefreshPointers restores the invariant that every
// outstanding refresh generation has live work capable of completing it. A
// pointer job that River moved to a terminal state (for example discarded
// after exhausting retries) leaves its generation permanently outstanding
// because generation bumps only insert jobs and completion only runs inside a
// finishing worker.
//
// For each stale outstanding generation the reconciler re-executes the
// producer's idempotent unique insert for the current generation, under the
// same single-key advisory lock the completion path takes. A live pointer in
// any queue deduplicates the insert, so a merely delayed generation is left
// untouched, concurrent reconcilers cannot create duplicate active work, and
// newer producer signals keep coalescing onto the replacement exactly as they
// would onto an original pointer. The generation itself is never completed
// here: only a successful refresh may advance completed_generation.
func ReconcileOrphanedRefreshPointers(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	staleBefore time.Time,
	batchLimit int32,
	queueName string,
) (OrphanReconcileResult, error) {
	var result OrphanReconcileResult
	if pool == nil || client == nil {
		return result, fmt.Errorf(
			"orphan reconciliation requires Postgres and River",
		)
	}
	if batchLimit <= 0 {
		return result, fmt.Errorf("orphan reconciliation batch limit is invalid")
	}
	outstanding, err := dbgen.New(pool).ListOutstandingRefreshIntentGenerations(
		ctx,
		dbgen.ListOutstandingRefreshIntentGenerationsParams{
			StaleBefore: repoutil.Timestamptz(staleBefore),
			BatchLimit:  batchLimit,
		},
	)
	if err != nil {
		return result, fmt.Errorf("list outstanding refresh generations: %w", err)
	}
	result.Scanned = len(outstanding)
	for index := range outstanding {
		row := &outstanding[index]
		replaced, generation, err := reinsertRefreshPointer(
			ctx, pool, client, row, queueName,
		)
		if err != nil {
			return result, err
		}
		switch {
		case generation == 0:
			// Completed by a live worker between the scan and the lock.
		case !replaced:
			result.Delayed++
		default:
			result.Orphans = append(result.Orphans, OrphanedRefreshPointer{
				Kind:          row.Kind,
				Key:           row.RefreshKey,
				Generation:    generation,
				TerminalState: latestTerminalPointerState(ctx, pool, row),
			})
		}
	}
	return result, nil
}

// reinsertRefreshPointer re-runs the unique pointer insert for one
// outstanding generation. It returns the generation that was still
// outstanding under the lock (zero when the row completed meanwhile) and
// whether the insert created a replacement rather than deduplicating.
func reinsertRefreshPointer(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	row *dbgen.ListOutstandingRefreshIntentGenerationsRow,
	queueName string,
) (bool, int64, error) {
	args, err := argsForSpec(&RefreshSpec{Kind: row.Kind, Key: row.RefreshKey})
	if err != nil {
		return false, 0, fmt.Errorf(
			"orphaned refresh generation %s %s: %w",
			row.Kind,
			row.RefreshKey,
			err,
		)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("begin orphan reconciliation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	// The completion path takes this same single-key lock before advancing
	// completed_generation, so the recheck below cannot race a finishing
	// worker into inserting a pointer for already-completed work.
	if err := lockRefreshIntentGenerationTx(
		ctx,
		tx,
		RefreshGenerationKey{Kind: row.Kind, Key: row.RefreshKey},
	); err != nil {
		return false, 0, err
	}
	state, err := dbgen.New(tx).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       row.Kind,
			RefreshKey: row.RefreshKey,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("recheck outstanding generation: %w", err)
	}
	if state.Generation <= state.CompletedGeneration {
		return false, 0, nil
	}
	inserted, err := client.InsertTx(
		ctx,
		tx,
		args,
		NewRefreshInsertOptsForQueue(queueName, time.Time{}),
	)
	if err != nil {
		return false, 0, fmt.Errorf("insert replacement refresh pointer: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, 0, fmt.Errorf("commit orphan reconciliation: %w", err)
	}
	return !inserted.UniqueSkippedAsDuplicate, state.Generation, nil
}

func latestTerminalPointerState(
	ctx context.Context,
	pool *pgxpool.Pool,
	row *dbgen.ListOutstandingRefreshIntentGenerationsRow,
) string {
	state, err := dbgen.New(pool).GetLatestTerminalRefreshJobState(
		ctx,
		dbgen.GetLatestTerminalRefreshJobStateParams{
			Kind:       row.Kind,
			RefreshKey: row.RefreshKey,
		},
	)
	if err != nil {
		return "unknown"
	}
	return state
}
