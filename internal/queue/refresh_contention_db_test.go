package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

type generationTestPointer struct {
	Kind            string `json:"kind"`
	RefreshKey      string `json:"refresh_key"`
	DeadlineAt      string `json:"deadline_at,omitempty"`
	EventReceivedAt string `json:"event_received_at,omitempty"`
}

func TestHotRefreshGenerationDoesNotBlockDisjointGeneration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	const (
		disjointKey = "branch:contention:a-disjoint"
		hotKey      = "branch:contention:z-hot"
	)
	late := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	early := late.Add(-time.Hour)
	seed := []generationTestPointer{
		{
			Kind:            KindRefreshBranch,
			RefreshKey:      disjointKey,
			DeadlineAt:      late.Format(time.RFC3339Nano),
			EventReceivedAt: late.Format(time.RFC3339Nano),
		},
		{
			Kind:            KindRefreshBranch,
			RefreshKey:      hotKey,
			DeadlineAt:      late.Format(time.RFC3339Nano),
			EventReceivedAt: late.Format(time.RFC3339Nano),
		},
	}
	if err := bumpRefreshGenerationBatch(ctx, pool, seed); err != nil {
		t.Fatalf("seed refresh generations: %v", err)
	}

	blocker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire blocker session: %v", err)
	}
	defer blocker.Release()
	blockerTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer blockerTx.Rollback(ctx) //nolint:errcheck // cleanup after explicit rollback
	if _, err := dbgen.New(blockerTx).GetRefreshIntentGenerationForUpdate(
		ctx,
		dbgen.GetRefreshIntentGenerationForUpdateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	); err != nil {
		t.Fatalf("hold hot generation row: %v", err)
	}

	worker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire worker session: %v", err)
	}
	defer worker.Release()
	err = bumpRefreshGenerationBatch(ctx, worker, []generationTestPointer{
		{Kind: KindRefreshBranch, RefreshKey: disjointKey},
		{Kind: KindRefreshBranch, RefreshKey: hotKey},
	})
	if !errors.Is(err, ErrRefreshGenerationContention) {
		t.Fatalf("contended generation batch error = %v", err)
	}

	earlier := []generationTestPointer{{
		Kind:            KindRefreshBranch,
		RefreshKey:      disjointKey,
		DeadlineAt:      early.Format(time.RFC3339Nano),
		EventReceivedAt: early.Format(time.RFC3339Nano),
	}}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
	)
	traceCtx, parent := provider.Tracer("refresh-contention-test").Start(
		ctx,
		"disjoint-bump",
	)
	if err := bumpRefreshGenerationBatch(traceCtx, worker, earlier); err != nil {
		t.Fatalf("bump disjoint generation while hot row is held: %v", err)
	}
	parent.End()
	lockSpanFound := false
	for _, ended := range recorder.Ended() {
		if ended.Name() != "ghsync.refresh_generation.lock" {
			continue
		}
		lockSpanFound = true
		for _, attr := range ended.Attributes() {
			if strings.Contains(string(attr.Key), disjointKey) ||
				strings.Contains(attr.Value.String(), disjointKey) {
				t.Fatalf("generation lock span exposed raw refresh key: %v", attr)
			}
		}
	}
	if !lockSpanFound {
		t.Fatal("generation lock span was not recorded")
	}

	hot, err := dbgen.New(blockerTx).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	)
	if err != nil {
		t.Fatalf("read hot generation state: %v", err)
	}
	disjoint, err := dbgen.New(blockerTx).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: disjointKey,
		},
	)
	if err != nil {
		t.Fatalf("read disjoint generation state: %v", err)
	}
	if hot.Generation != 1 || disjoint.Generation != 2 {
		t.Fatalf(
			"hot/disjoint generations = %d/%d, want 1/2",
			hot.Generation,
			disjoint.Generation,
		)
	}
	if !disjoint.DeadlineAt.Valid || !disjoint.DeadlineAt.Time.Equal(early) ||
		!disjoint.EventReceivedAt.Valid ||
		!disjoint.EventReceivedAt.Time.Equal(early) {
		t.Fatalf(
			"disjoint earliest deadline/event = %v/%v, want %s",
			disjoint.DeadlineAt,
			disjoint.EventReceivedAt,
			early,
		)
	}
	if err := blockerTx.Rollback(ctx); err != nil {
		t.Fatalf("release hot generation row: %v", err)
	}
	if err := bumpRefreshGenerationBatch(ctx, worker, []generationTestPointer{{
		Kind:       KindRefreshBranch,
		RefreshKey: hotKey,
	}}); err != nil {
		t.Fatalf("bump hot generation after release: %v", err)
	}
	hot, err = dbgen.New(pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	)
	if err != nil {
		t.Fatalf("read released hot generation state: %v", err)
	}
	if hot.Generation != 2 {
		t.Fatalf("hot generation after release = %d, want 2", hot.Generation)
	}
}

func TestRefreshGenerationLockRetryReleasesFailedAttemptLocks(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	const (
		disjointKey = "branch:retry:a-disjoint"
		hotKey      = "branch:retry:z-hot"
	)
	seed := []generationTestPointer{
		{Kind: KindRefreshBranch, RefreshKey: disjointKey},
		{Kind: KindRefreshBranch, RefreshKey: hotKey},
	}
	if err := bumpRefreshGenerationBatch(ctx, pool, seed); err != nil {
		t.Fatalf("seed retry generations: %v", err)
	}

	blockerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin retry blocker: %v", err)
	}
	defer blockerTx.Rollback(ctx) //nolint:errcheck // cleanup after explicit rollback
	if _, err := dbgen.New(blockerTx).GetRefreshIntentGenerationForUpdate(
		ctx,
		dbgen.GetRefreshIntentGenerationForUpdateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	); err != nil {
		t.Fatalf("hold retry hot generation row: %v", err)
	}

	waiterTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin retry waiter: %v", err)
	}
	defer waiterTx.Rollback(ctx) //nolint:errcheck // cleanup after goroutine commit
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
	)
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("shut down retry tracer provider: %v", err)
		}
	}()
	traceCtx, parent := provider.Tracer("refresh-retry-test").Start(
		ctx,
		"retrying-batch",
	)
	defer parent.End()
	waiterDone := make(chan error, 1)
	go func() {
		keys := []RefreshGenerationKey{
			{Kind: KindRefreshBranch, Key: disjointKey},
			{Kind: KindRefreshBranch, Key: hotKey},
		}
		if err := LockRefreshIntentGenerationsTx(
			traceCtx,
			waiterTx,
			keys,
		); err != nil {
			waiterDone <- err
			return
		}
		encoded, err := json.Marshal(seed)
		if err != nil {
			waiterDone <- fmt.Errorf("encode retry generation batch: %w", err)
			return
		}
		generations, err := dbgen.New(waiterTx).
			BumpRefreshIntentGenerations(traceCtx, encoded)
		if err != nil {
			waiterDone <- fmt.Errorf("bump retry generation batch: %w", err)
			return
		}
		if len(generations) != len(seed) {
			waiterDone <- fmt.Errorf(
				"retry generation rows = %d, want %d",
				len(generations),
				len(seed),
			)
			return
		}
		waiterDone <- waiterTx.Commit(traceCtx)
	}()

	for {
		contended := false
		for _, ended := range recorder.Ended() {
			if ended.Name() != "ghsync.refresh_generation.lock" {
				continue
			}
			for _, attr := range ended.Attributes() {
				if string(attr.Key) ==
					"ghsync.refresh_generation.contended" &&
					attr.Value.AsBool() {
					contended = true
					break
				}
			}
		}
		if contended {
			break
		}
		select {
		case err := <-waiterDone:
			t.Fatalf("retry waiter stopped before contention: %v", err)
		case <-ctx.Done():
			t.Fatalf("wait for retry contention span: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}

	if err := bumpRefreshGenerationBatchWithRetry(
		ctx,
		pool,
		[]generationTestPointer{{
			Kind:       KindRefreshBranch,
			RefreshKey: disjointKey,
		}},
	); err != nil {
		t.Fatalf("bump disjoint generation during retry: %v", err)
	}
	select {
	case err := <-waiterDone:
		t.Fatalf("retry waiter completed while hot row was held: %v", err)
	default:
	}
	if err := blockerTx.Rollback(ctx); err != nil {
		t.Fatalf("release retry hot generation row: %v", err)
	}
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("retry waiter after hot release: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for retry waiter: %v", ctx.Err())
	}

	hot, err := dbgen.New(pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	)
	if err != nil {
		t.Fatalf("read retried hot generation: %v", err)
	}
	disjoint, err := dbgen.New(pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: disjointKey,
		},
	)
	if err != nil {
		t.Fatalf("read retried disjoint generation: %v", err)
	}
	if hot.Generation != 2 || disjoint.Generation != 3 {
		t.Fatalf(
			"retried hot/disjoint generations = %d/%d, want 2/3",
			hot.Generation,
			disjoint.Generation,
		)
	}
}

func TestRefreshGenerationContentionStressPreservesIncrements(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	const (
		hotKey             = "branch:stress:z-hot"
		overlappingWorkers = 6
		disjointWorkers    = 6
		iterations         = 10
	)
	if err := bumpRefreshGenerationBatch(ctx, pool, []generationTestPointer{{
		Kind:       KindRefreshBranch,
		RefreshKey: hotKey,
	}}); err != nil {
		t.Fatalf("seed stress hot generation: %v", err)
	}

	blockerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin stress blocker: %v", err)
	}
	defer blockerTx.Rollback(ctx) //nolint:errcheck // cleanup after explicit rollback
	if _, err := dbgen.New(blockerTx).GetRefreshIntentGenerationForUpdate(
		ctx,
		dbgen.GetRefreshIntentGenerationForUpdateParams{
			Kind:       KindRefreshBranch,
			RefreshKey: hotKey,
		},
	); err != nil {
		t.Fatalf("hold stress hot row: %v", err)
	}

	start := make(chan struct{})
	overlappingDone := make(chan error, overlappingWorkers)
	disjointDone := make(chan error, disjointWorkers)
	for worker := range overlappingWorkers {
		go func(worker int) {
			<-start
			key := fmt.Sprintf("branch:stress:a-overlap-%02d", worker)
			for range iterations {
				if err := bumpRefreshGenerationBatchWithRetry(
					ctx,
					pool,
					[]generationTestPointer{
						{Kind: KindRefreshBranch, RefreshKey: key},
						{Kind: KindRefreshBranch, RefreshKey: hotKey},
					},
				); err != nil {
					overlappingDone <- err
					return
				}
			}
			overlappingDone <- nil
		}(worker)
	}
	for worker := range disjointWorkers {
		go func(worker int) {
			<-start
			key := fmt.Sprintf("branch:stress:disjoint-%02d", worker)
			for range iterations {
				if err := bumpRefreshGenerationBatchWithRetry(
					ctx,
					pool,
					[]generationTestPointer{{
						Kind:       KindRefreshBranch,
						RefreshKey: key,
					}},
				); err != nil {
					disjointDone <- err
					return
				}
			}
			disjointDone <- nil
		}(worker)
	}
	close(start)
	for range disjointWorkers {
		select {
		case err := <-disjointDone:
			if err != nil {
				t.Fatalf("disjoint stress worker: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for disjoint stress workers: %v", ctx.Err())
		}
	}
	var hotWhileHeld int64
	if err := blockerTx.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = $1 AND refresh_key = $2
	`, KindRefreshBranch, hotKey).Scan(&hotWhileHeld); err != nil {
		t.Fatalf("read held hot generation: %v", err)
	}
	if hotWhileHeld != 1 {
		t.Fatalf("hot generation while held = %d, want 1", hotWhileHeld)
	}
	if err := blockerTx.Rollback(ctx); err != nil {
		t.Fatalf("release stress hot row: %v", err)
	}
	for range overlappingWorkers {
		select {
		case err := <-overlappingDone:
			if err != nil {
				t.Fatalf("overlapping stress worker: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for overlapping stress workers: %v", ctx.Err())
		}
	}

	var hotGeneration, wrongOverlap, wrongDisjoint int64
	if err := pool.QueryRow(ctx, `
		SELECT
		    max(generation) FILTER (WHERE refresh_key = $1),
		    count(*) FILTER (
		        WHERE refresh_key LIKE 'branch:stress:a-overlap-%'
		          AND generation <> $2
		    ),
		    count(*) FILTER (
		        WHERE refresh_key LIKE 'branch:stress:disjoint-%'
		          AND generation <> $2
		    )
		FROM refresh_intent_generations
		WHERE kind = $3
	`, hotKey, iterations, KindRefreshBranch).Scan(
		&hotGeneration,
		&wrongOverlap,
		&wrongDisjoint,
	); err != nil {
		t.Fatalf("read stress generations: %v", err)
	}
	wantHot := int64(1 + overlappingWorkers*iterations)
	if hotGeneration != wantHot || wrongOverlap != 0 || wrongDisjoint != 0 {
		t.Fatalf(
			"stress hot=%d want=%d wrong overlap/disjoint=%d/%d",
			hotGeneration,
			wantHot,
			wrongOverlap,
			wrongDisjoint,
		)
	}
}

func TestRefreshGenerationConcurrentResultsAreStrictlyMonotonic(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	const (
		hotKey     = "branch:monotonic:hot"
		workers    = 8
		iterations = 20
	)

	start := make(chan struct{})
	results := make(chan int64, workers*iterations)
	done := make(chan error, workers)
	for range workers {
		go func() {
			<-start
			for range iterations {
				generations, err := bumpRefreshGenerationBatchReturningWithRetry(
					ctx,
					pool,
					[]generationTestPointer{{
						Kind:       KindRefreshBranch,
						RefreshKey: hotKey,
					}},
				)
				if err != nil {
					done <- err
					return
				}
				results <- generations[0].Generation
			}
			done <- nil
		}()
	}
	close(start)
	for range workers {
		if err := <-done; err != nil {
			t.Fatalf("concurrent generation bump: %v", err)
		}
	}
	close(results)

	got := make([]int64, 0, workers*iterations)
	for generation := range results {
		got = append(got, generation)
	}
	slices.Sort(got)
	if len(got) != workers*iterations {
		t.Fatalf("returned generations = %d, want %d", len(got), workers*iterations)
	}
	for index, generation := range got {
		want := int64(index + 1)
		if generation != want {
			t.Fatalf(
				"returned generation %d = %d, want %d",
				index,
				generation,
				want,
			)
		}
	}
}

func TestRefreshGenerationOverlappingBatchesOppositeOrdersDoNotDeadlock(
	t *testing.T,
) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	const (
		firstKey   = "branch:deadlock:first"
		secondKey  = "branch:deadlock:second"
		iterations = 25
	)
	orders := [][]generationTestPointer{
		{
			{Kind: KindRefreshBranch, RefreshKey: firstKey},
			{Kind: KindRefreshBranch, RefreshKey: secondKey},
		},
		{
			{Kind: KindRefreshBranch, RefreshKey: secondKey},
			{Kind: KindRefreshBranch, RefreshKey: firstKey},
		},
	}
	start := make(chan struct{})
	done := make(chan error, len(orders))
	for _, order := range orders {
		go func(pointers []generationTestPointer) {
			<-start
			for range iterations {
				if err := bumpRefreshGenerationBatchWithRetry(
					ctx,
					pool,
					pointers,
				); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(order)
	}
	close(start)
	for range orders {
		if err := <-done; err != nil {
			t.Fatalf("opposite-order generation batches: %v", err)
		}
	}

	rows, err := pool.Query(ctx, `
		SELECT refresh_key, generation
		FROM refresh_intent_generations
		WHERE kind = $1 AND refresh_key = ANY($2::text[])
		ORDER BY refresh_key
	`, KindRefreshBranch, []string{firstKey, secondKey})
	if err != nil {
		t.Fatalf("read opposite-order generations: %v", err)
	}
	defer rows.Close()
	wantGeneration := int64(len(orders) * iterations)
	count := 0
	for rows.Next() {
		var key string
		var generation int64
		if err := rows.Scan(&key, &generation); err != nil {
			t.Fatalf("scan opposite-order generation: %v", err)
		}
		if generation != wantGeneration {
			t.Fatalf(
				"opposite-order generation %s = %d, want %d",
				key,
				generation,
				wantGeneration,
			)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate opposite-order generations: %v", err)
	}
	if count != len(orders) {
		t.Fatalf("opposite-order generation rows = %d, want %d", count, len(orders))
	}
}

type generationBatchDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

func bumpRefreshGenerationBatch(
	ctx context.Context,
	database generationBatchDB,
	pointers []generationTestPointer,
) error {
	_, err := bumpRefreshGenerationBatchReturning(ctx, database, pointers)
	return err
}

func bumpRefreshGenerationBatchReturning(
	ctx context.Context,
	database generationBatchDB,
	pointers []generationTestPointer,
) ([]dbgen.BumpRefreshIntentGenerationsRow, error) {
	tx, err := database.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin generation batch: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	keys := make([]RefreshGenerationKey, 0, len(pointers))
	for _, pointer := range pointers {
		keys = append(keys, RefreshGenerationKey{
			Kind: pointer.Kind,
			Key:  pointer.RefreshKey,
		})
	}
	if err := TryLockRefreshIntentGenerationsTx(ctx, tx, keys); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(pointers)
	if err != nil {
		return nil, fmt.Errorf("encode generation batch: %w", err)
	}
	generations, err := dbgen.New(tx).BumpRefreshIntentGenerations(ctx, encoded)
	if err != nil {
		return nil, fmt.Errorf("bump generation batch: %w", err)
	}
	if len(generations) != len(pointers) {
		return nil, fmt.Errorf(
			"bumped %d generation rows for %d pointers",
			len(generations),
			len(pointers),
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation batch: %w", err)
	}
	return generations, nil
}

func bumpRefreshGenerationBatchWithRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	pointers []generationTestPointer,
) error {
	_, err := bumpRefreshGenerationBatchReturningWithRetry(ctx, pool, pointers)
	return err
}

func bumpRefreshGenerationBatchReturningWithRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	pointers []generationTestPointer,
) ([]dbgen.BumpRefreshIntentGenerationsRow, error) {
	for {
		generations, err := bumpRefreshGenerationBatchReturning(ctx, pool, pointers)
		if err == nil {
			return generations, nil
		}
		if !errors.Is(err, ErrRefreshGenerationContention) {
			return nil, err
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("retry generation contention: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
