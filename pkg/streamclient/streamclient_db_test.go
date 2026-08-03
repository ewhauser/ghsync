package streamclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/outbox"
	streammaint "github.com/ewhauser/ghsync/internal/stream"
	"github.com/ewhauser/ghsync/internal/testdb"
)

var streamTestID atomic.Int64

func TestIsRetryableTaxonomy(t *testing.T) {
	t.Parallel()
	contention := newCursorContention(
		"consumer",
		"entities",
		&pgconn.PgError{Code: "40001", Message: "serialization"},
	)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{
			name: "cursor contention",
			err:  fmt.Errorf("tail: %w", contention),
			want: true,
		},
		{
			name: "listener pool",
			err: fmt.Errorf(
				"%w: %w",
				ErrListenerUnavailable,
				context.DeadlineExceeded,
			),
			want: true,
		},
		{
			name: "raw serialization",
			err: &pgconn.PgError{
				Code:    "40001",
				Message: "serialization",
			},
			want: true,
		},
		{
			name: "pool capacity",
			err: &pgconn.PgError{
				Code:    "53300",
				Message: "too many connections",
			},
			want: true,
		},
		{
			name: "resync action",
			err:  &ErrResyncRequired{},
			want: false,
		},
		{name: "canceled", err: context.Canceled, want: false},
		{
			name: "deadline",
			err:  context.DeadlineExceeded,
			want: false,
		},
		{name: "terminal", err: errors.New("bad handler"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRetryable(test.err); got != test.want {
				t.Fatalf(
					"IsRetryable(%v) = %t, want %t",
					test.err,
					got,
					test.want,
				)
			}
		})
	}
}

func TestOutboxGapWatermarkDeliversDelayedSmallerSequenceInOrder(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("gap")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 10)
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	initialCursor := snapshot.SafeSeq
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	smallTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer smallTx.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	smallSeq := insertTestEvent(t, ctx, smallTx, streamName, "small", time.Now())

	largeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	largeSeq := insertTestEvent(t, ctx, largeTx, streamName, "large", time.Now())
	if smallSeq >= largeSeq {
		t.Fatalf("allocated sequences small=%d large=%d", smallSeq, largeSeq)
	}
	if err := largeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	stepDone := make(chan streammaint.WatermarkProgress, 1)
	stepErr := make(chan error, 1)
	go func() {
		progress, err := watermarker.Step(ctx)
		stepDone <- progress
		stepErr <- err
	}()

	tailCtx, stopTail := context.WithCancel(ctx)
	defer stopTail()
	delivered := make(chan int64, 4)
	tailErr := make(chan error, 1)
	pageAttempted := make(chan struct{})
	var pageAttemptOnce sync.Once
	client.testHooks.afterHorizon = func() {
		pageAttemptOnce.Do(func() { close(pageAttempted) })
	}
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(_ context.Context, _ pgx.Tx, event Event) error {
				delivered <- event.Seq
				return nil
			},
		)
	}()
	<-pageAttempted
	select {
	case seq := <-delivered:
		t.Fatalf("tailer exposed seq %d while smaller seq was in-flight", seq)
	default:
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor != initialCursor {
		t.Fatalf("cursor advanced to %d before gap closed", cursor)
	}

	if err := smallTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-stepErr; err != nil {
		t.Fatal(err)
	}
	if progress := <-stepDone; progress.SafeSeq < largeSeq {
		t.Fatalf(
			"watermark after writer fence = %d, want at least %d",
			progress.SafeSeq,
			largeSeq,
		)
	}
	waitForCursor(t, pool, consumer, streamName, largeSeq)
	stopTail()
	err = <-tailErr
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tail exit = %v", err)
	}
	var got []int64
	for len(delivered) > 0 {
		got = append(got, <-delivered)
	}
	if len(got) != 2 || got[0] != smallSeq || got[1] != largeSeq {
		t.Fatalf("delivered = %v, want [%d %d]", got, smallSeq, largeSeq)
	}
}

func TestTailRetriesConcurrentCursorFirstTouchRace(t *testing.T) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("cursor-first-touch")
	consumer := uniqueStreamName("consumer")
	first := newTestClient(t, pool, 10)
	second := newTestClient(t, pool, 10)

	firstInserted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var blockOnce sync.Once
	first.testHooks.afterEnsureCursor = func() {
		blockOnce.Do(func() {
			close(firstInserted)
			<-releaseFirst
		})
	}
	retried := make(chan struct{})
	var retryOnce sync.Once
	second.testHooks.cursorFirstTouchRetry = func() {
		retryOnce.Do(func() { close(retried) })
	}

	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- first.Tail(
			firstCtx,
			consumer,
			streamName,
			func(context.Context, pgx.Tx, Event) error { return nil },
		)
	}()
	<-firstInserted

	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- second.Tail(
			secondCtx,
			consumer,
			streamName,
			func(context.Context, pgx.Tx, Event) error { return nil },
		)
	}()
	// Wait until the second INSERT is demonstrably waiting on the first
	// transaction's unique-key decision with an older REPEATABLE READ snapshot.
	waitForCursorFirstTouchLockWait(t, pool)
	close(releaseFirst)

	select {
	case <-retried:
	case err := <-secondErr:
		t.Fatalf("concurrent first-touch ended Tail instead of retrying: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Tail did not observe and retry the cursor first-touch race")
	}

	stopSecond()
	if err := <-secondErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("second Tail exit after retry = %v", err)
	}
	stopFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Tail exit = %v", err)
	}
}

func waitForCursorFirstTouchLockWait(
	t *testing.T,
	pool *pgxpool.Pool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := pool.QueryRow(context.Background(), `
			SELECT EXISTS (
			    SELECT 1
			    FROM pg_stat_activity
			    WHERE pid <> pg_backend_pid()
			      AND datname = current_database()
			      AND wait_event_type = 'Lock'
			      AND query LIKE '%INSERT INTO consumer_cursors%'
			)
		`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("second Tail did not block on the cursor first-touch race")
}

func TestSnapshotCommitPriorSeqAndIdempotentClose(t *testing.T) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("snapshot-lifecycle")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 10)

	initial, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if initial.PriorSeq != 0 {
		t.Fatalf("initial PriorSeq = %d, want 0", initial.PriorSeq)
	}
	if err := initial.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("Close after Commit = %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("second Close after Commit = %v", err)
	}

	seq := insertCommittedEvent(
		t,
		ctx,
		pool,
		streamName,
		"snapshot-lifecycle",
		time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)

	beforeAcquire := pool.Stat().AcquiredConns()
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PriorSeq != initial.SafeSeq {
		t.Fatalf(
			"PriorSeq = %d, want prior committed cursor %d",
			snapshot.PriorSeq,
			initial.SafeSeq,
		)
	}
	if snapshot.SafeSeq < seq {
		t.Fatalf("SafeSeq = %d, want at least %d", snapshot.SafeSeq, seq)
	}
	if got := pool.Stat().AcquiredConns(); got != beforeAcquire+1 {
		t.Fatalf(
			"acquired connections during snapshot = %d, want %d",
			got,
			beforeAcquire+1,
		)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("abandon Snapshot = %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("second abandon Snapshot = %v", err)
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor !=
		snapshot.PriorSeq {
		t.Fatalf(
			"cursor after Close = %d, want PriorSeq %d",
			cursor,
			snapshot.PriorSeq,
		)
	}
	if got := pool.Stat().AcquiredConns(); got != beforeAcquire {
		t.Fatalf(
			"acquired connections after Close = %d, want %d",
			got,
			beforeAcquire,
		)
	}

	committed, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor !=
		committed.SafeSeq {
		t.Fatalf(
			"cursor after Commit = %d, want SafeSeq %d",
			cursor,
			committed.SafeSeq,
		)
	}
	if err := committed.Close(); err != nil {
		t.Fatalf("Close after committed reset = %v", err)
	}
}

func TestRetentionCannotDeleteBetweenHorizonCheckAndPageSnapshot(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("retention-snapshot")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 10)
	seq := insertCommittedEvent(
		t,
		ctx,
		pool,
		streamName,
		"old",
		time.Now().Add(-8*24*time.Hour),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)

	horizonRead := make(chan struct{})
	releasePage := make(chan struct{})
	var hookOnce sync.Once
	client.testHooks.afterHorizon = func() {
		hookOnce.Do(func() {
			close(horizonRead)
			<-releasePage
		})
	}
	delivered := make(chan int64, 1)
	pageResult := make(chan int, 1)
	pageErr := make(chan error, 1)
	go func() {
		count, err := client.deliverPage(
			ctx,
			consumer,
			streamName,
			func(_ context.Context, _ pgx.Tx, event Event) error {
				delivered <- event.Seq
				return nil
			},
		)
		pageResult <- count
		pageErr <- err
	}()
	<-horizonRead

	retention, err := streammaint.NewRetention(
		pool,
		streammaint.RetentionOptions{
			Age:       7 * 24 * time.Hour,
			Period:    time.Hour,
			BatchSize: 10,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := retention.Prune(ctx); err != nil || deleted != 1 {
		t.Fatalf("interleaved retention deleted=%d err=%v", deleted, err)
	}
	close(releasePage)
	if err := <-pageErr; err != nil {
		t.Fatal(err)
	}
	if count := <-pageResult; count != 1 {
		t.Fatalf("snapshot page delivered %d events, want 1", count)
	}
	if got := <-delivered; got != seq {
		t.Fatalf("snapshot page delivered seq %d, want %d", got, seq)
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor != seq {
		t.Fatalf("cursor = %d, want delivered seq %d", cursor, seq)
	}
}

func TestWatermarkIgnoresUnrelatedWriterAndLongBootstrapSnapshot(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := newTestClient(t, pool, 10)

	unrelated, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer unrelated.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if _, err := unrelated.Exec(ctx, `
		INSERT INTO consumer_cursors (consumer, stream, seq)
		VALUES ($1, $2, 0)
	`, uniqueStreamName("unrelated"), uniqueStreamName("unrelated")); err != nil {
		t.Fatal(err)
	}

	bootstrap, err := client.Bootstrap(
		ctx,
		uniqueStreamName("bootstrap-consumer"),
		uniqueStreamName("bootstrap-stream"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close() //nolint:errcheck // deferred cleanup cannot change the primary operation result

	streamName := uniqueStreamName("liveness")
	seq := insertCommittedEvent(
		t, ctx, pool, streamName, "committed", time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	stepCtx, stopStep := context.WithTimeout(ctx, time.Second)
	defer stopStep()
	progress, err := watermarker.Step(stepCtx)
	if err != nil {
		t.Fatalf("watermark blocked on unrelated transaction/snapshot: %v", err)
	}
	if progress.SafeSeq < seq {
		t.Fatalf("safe seq = %d, want at least %d", progress.SafeSeq, seq)
	}
}

func TestTailRecoversAfterListenerDisconnectWhilePollingContinues(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("listener")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 10)
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	listeners := make(chan uint32, 4)
	client.testHooks.listenerConnected = func(pid uint32) { listeners <- pid }
	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	delivered := make(chan int64, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(_ context.Context, _ pgx.Tx, event Event) error {
				delivered <- event.Seq
				return nil
			},
		)
	}()
	listenerPID := <-listeners
	var terminated bool
	if err := pool.QueryRow(
		ctx,
		`SELECT pg_terminate_backend($1)`,
		listenerPID,
	).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatalf("listener backend %d was not terminated", listenerPID)
	}

	seq := insertCommittedEvent(
		t, ctx, pool, streamName, "after-disconnect", time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)
	select {
	case got := <-delivered:
		if got != seq {
			t.Fatalf("delivered seq = %d, want %d", got, seq)
		}
	case err := <-tailErr:
		t.Fatalf("tail stopped after listener disconnect: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("tail exit = %v", err)
	}
}

func TestTailRepeatedListenerTerminationReachesMaxBackoffAndRecovers(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("listener-churn")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 10)
	client.minBackoff = 5 * time.Millisecond
	client.maxBackoff = 20 * time.Millisecond
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	const terminationCount = 4
	hookErr := make(chan error, terminationCount)
	retry := make(chan struct {
		err   error
		delay time.Duration
	}, terminationCount+2)
	recovered := make(chan struct{})
	var connected atomic.Int32
	var recoveredOnce sync.Once
	client.testHooks.listenerConnected = func(pid uint32) {
		if connected.Add(1) <= terminationCount {
			var terminated bool
			err := pool.QueryRow(
				context.Background(),
				`SELECT pg_terminate_backend($1)`,
				pid,
			).Scan(&terminated)
			if err == nil && !terminated {
				err = fmt.Errorf(
					"listener backend %d was not terminated",
					pid,
				)
			}
			if err != nil {
				hookErr <- err
			}
			return
		}
		recoveredOnce.Do(func() { close(recovered) })
	}
	client.testHooks.listenerRetry = func(
		err error,
		delay time.Duration,
	) {
		retry <- struct {
			err   error
			delay time.Duration
		}{err: err, delay: delay}
	}

	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	delivered := make(chan int64, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(_ context.Context, _ pgx.Tx, event Event) error {
				delivered <- event.Seq
				return nil
			},
		)
	}()

	wantBackoffs := []time.Duration{
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		20 * time.Millisecond,
	}
	for index, want := range wantBackoffs {
		select {
		case observation := <-retry:
			if !errors.Is(observation.err, ErrListenerUnavailable) {
				t.Fatalf(
					"retry %d error = %v, want ErrListenerUnavailable",
					index,
					observation.err,
				)
			}
			if !IsRetryable(observation.err) {
				t.Fatalf("retry %d error is not retryable", index)
			}
			if observation.delay != want {
				t.Fatalf(
					"retry %d delay = %s, want %s",
					index,
					observation.delay,
					want,
				)
			}
		case err := <-hookErr:
			t.Fatal(err)
		case err := <-tailErr:
			t.Fatalf("Tail stopped during listener churn: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-recovered:
	case err := <-hookErr:
		t.Fatal(err)
	case err := <-tailErr:
		t.Fatalf("Tail stopped before listener recovery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	seq := insertCommittedEvent(
		t, ctx, pool, streamName, "after-churn", time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)
	select {
	case got := <-delivered:
		if got != seq {
			t.Fatalf("delivered seq = %d, want %d", got, seq)
		}
	case err := <-tailErr:
		t.Fatalf("Tail stopped after listener recovery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Tail exit = %v", err)
	}
}

func TestTailPersistentListenerPoolExhaustionPollsThenRecovers(
	t *testing.T,
) {
	t.Parallel()
	pool, clientPool := streamTestDatabaseWithClientPool(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("listener-pool")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, clientPool, 10)
	client.minBackoff = 5 * time.Millisecond
	client.maxBackoff = 20 * time.Millisecond
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	const exhaustedAttempts = 3
	var attempts atomic.Int32
	var pollOnly atomic.Int32
	hookErr := make(chan error, exhaustedAttempts)
	retries := make(chan error, exhaustedAttempts)
	recovered := make(chan struct{})
	var recoveredOnce sync.Once
	client.testHooks.beforeListenerAcquire = func() {
		if attempts.Add(1) > exhaustedAttempts {
			return
		}
		acquireCtx, stopAcquire := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		connections := make([]*pgxpool.Conn, 0, 2)
		for range 2 {
			conn, err := clientPool.Acquire(acquireCtx)
			if err != nil {
				stopAcquire()
				for _, acquired := range connections {
					acquired.Release()
				}
				hookErr <- fmt.Errorf(
					"occupy listener pool: %w",
					err,
				)
				return
			}
			connections = append(connections, conn)
		}
		stopAcquire()
		time.AfterFunc(30*time.Millisecond, func() {
			for _, conn := range connections {
				conn.Release()
			}
		})
	}
	client.testHooks.listenerRetry = func(
		err error,
		_ time.Duration,
	) {
		retries <- err
	}
	client.testHooks.pollOnly = func() {
		pollOnly.Add(1)
	}
	client.testHooks.listenerConnected = func(uint32) {
		recoveredOnce.Do(func() { close(recovered) })
	}

	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	delivered := make(chan int64, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(_ context.Context, _ pgx.Tx, event Event) error {
				delivered <- event.Seq
				return nil
			},
		)
	}()
	for index := range exhaustedAttempts {
		select {
		case retryErr := <-retries:
			if !errors.Is(retryErr, ErrListenerUnavailable) {
				t.Fatalf(
					"pool retry %d = %v, want ErrListenerUnavailable",
					index,
					retryErr,
				)
			}
			if !IsRetryable(retryErr) {
				t.Fatalf("pool retry %d is not retryable", index)
			}
		case err := <-hookErr:
			t.Fatal(err)
		case err := <-tailErr:
			t.Fatalf("Tail stopped during pool exhaustion: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if got := pollOnly.Load(); got < exhaustedAttempts {
		t.Fatalf(
			"poll-only iterations = %d, want at least %d",
			got,
			exhaustedAttempts,
		)
	}
	select {
	case <-recovered:
	case err := <-hookErr:
		t.Fatal(err)
	case err := <-tailErr:
		t.Fatalf("Tail stopped before pool recovery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	seq := insertCommittedEvent(
		t, ctx, pool, streamName, "after-pool-recovery", time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)
	select {
	case got := <-delivered:
		if got != seq {
			t.Fatalf("delivered seq = %d, want %d", got, seq)
		}
	case err := <-tailErr:
		t.Fatalf("Tail stopped after pool recovery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Tail exit = %v", err)
	}
}

func TestExactlyOncePerCursorAcrossMidBatchCrashAndRestart(t *testing.T) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("restart")
	consumer := uniqueStreamName("consumer")
	client := newTestClient(t, pool, 3)
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	initialCursor := snapshot.SafeSeq
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	sequences := make([]int64, 0, 3)
	for index := range 3 {
		sequences = append(
			sequences,
			insertCommittedEvent(
				t, ctx, pool, streamName,
				fmt.Sprintf("event-%d", index),
				time.Now(),
			),
		)
	}
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, sequences[len(sequences)-1])

	table := testTableName("applications")
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (seq bigint PRIMARY KEY)`, table,
	)); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP TABLE "+table) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	crash := errors.New("simulated tailer crash")
	handled := 0
	err = client.Tail(
		ctx,
		consumer,
		streamName,
		func(ctx context.Context, tx pgx.Tx, event Event) error {
			handled++
			if _, err := tx.Exec(
				ctx,
				"INSERT INTO "+table+" (seq) VALUES ($1)",
				event.Seq,
			); err != nil {
				return err
			}
			if handled == 2 {
				return crash
			}
			return nil
		},
	)
	if !errors.Is(err, crash) {
		t.Fatalf("first tail = %v, want crash", err)
	}
	if count := tableCount(t, pool, table); count != 0 {
		t.Fatalf("rolled-back handler effects = %d, want 0", count)
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor != initialCursor {
		t.Fatalf("cursor after crash = %d, want %d", cursor, initialCursor)
	}

	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(ctx context.Context, tx pgx.Tx, event Event) error {
				_, err := tx.Exec(
					ctx,
					"INSERT INTO "+table+" (seq) VALUES ($1)",
					event.Seq,
				)
				return err
			},
		)
	}()
	waitForCursor(
		t, pool, consumer, streamName, sequences[len(sequences)-1],
	)
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("restart tail exit = %v", err)
	}
	if count := tableCount(t, pool, table); count != len(sequences) {
		t.Fatalf("committed applications = %d, want %d", count, len(sequences))
	}
}

func TestBootstrapTailOverlapConvergesWithoutLostOrDoubleAppliedEffect(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("bootstrap-overlap")
	consumer := uniqueStreamName("consumer")
	identity := uniqueStreamName("work-item")
	client := newTestClient(t, pool, 10)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result

	// Publish W before the state/event transaction commits. Bootstrap will
	// subsequently read the stale W while its later cache read sees the state
	// associated with seq > W: the documented at-least-once overlap.
	published, err := watermarker.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}

	projection := testTableName("overlap_projection")
	applied := testTableName("overlap_applied")
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
		    identity_key text PRIMARY KEY,
		    payload jsonb NOT NULL
		);
		CREATE TABLE %s (seq bigint PRIMARY KEY);
	`, projection, applied)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DROP TABLE "+projection+", "+applied,
		)
	}()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO work_items (
		    scope_key, identity_key, org_id, payload, updated_at
		)
		VALUES ($1, $2, 1, '{"state":"overlap"}', clock_timestamp())
	`, "pr:1:1:1", identity); err != nil {
		t.Fatal(err)
	}
	overlapSeq := insertTestEvent(
		t,
		ctx,
		tx,
		streamName,
		identity,
		time.Now(),
	)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if overlapSeq <= published.SafeSeq {
		t.Fatalf(
			"overlap seq = %d, want greater than published W %d",
			overlapSeq,
			published.SafeSeq,
		)
	}

	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close() //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if snapshot.SafeSeq != published.SafeSeq {
		t.Fatalf(
			"Bootstrap SafeSeq = %d, want published W %d",
			snapshot.SafeSeq,
			published.SafeSeq,
		)
	}
	if _, err := snapshot.Tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (identity_key, payload)
		SELECT identity_key, payload
		FROM work_items
		WHERE identity_key = $1
	`, projection), identity); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if count := tableCount(t, pool, projection); count != 1 {
		t.Fatalf("snapshot projection rows = %d, want 1", count)
	}
	if cursor := readCursor(t, pool, consumer, streamName); cursor !=
		published.SafeSeq {
		t.Fatalf(
			"cursor after overlap Bootstrap = %d, want W %d",
			cursor,
			published.SafeSeq,
		)
	}

	advanceThrough(t, ctx, watermarker, overlapSeq)
	var handled atomic.Int32
	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(ctx context.Context, tx pgx.Tx, event Event) error {
				handled.Add(1)
				if _, err := tx.Exec(
					ctx,
					"INSERT INTO "+applied+" (seq) VALUES ($1)",
					event.Seq,
				); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s (identity_key, payload)
					SELECT identity_key, payload
					FROM work_items
					WHERE identity_key = $1
					ON CONFLICT (identity_key) DO UPDATE
					SET payload = EXCLUDED.payload
				`, projection), event.EntityKey)
				return err
			},
		)
	}()
	waitForCursor(t, pool, consumer, streamName, overlapSeq)
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("overlap Tail exit = %v", err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("overlap event handler calls = %d, want 1", got)
	}
	if count := tableCount(t, pool, applied); count != 1 {
		t.Fatalf("applied overlap sequences = %d, want 1", count)
	}
	if count := tableCount(t, pool, projection); count != 1 {
		t.Fatalf(
			"stable-key projection rows = %d, want no duplicate effect",
			count,
		)
	}
	var payload string
	if err := pool.QueryRow(
		ctx,
		"SELECT payload::text FROM "+projection+" WHERE identity_key = $1",
		identity,
	).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"state": "overlap"`) {
		t.Fatalf("projection payload = %s, want overlap state", payload)
	}
}

func TestConcurrentTailersReturnTypedCursorContention(t *testing.T) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("cursor-contention")
	consumer := uniqueStreamName("consumer")
	first := newTestClient(t, pool, 10)
	second := newTestClient(t, pool, 10)
	snapshot, err := first.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	seq := insertCommittedEvent(
		t, ctx, pool, streamName, "contended", time.Now(),
	)
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, seq)

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstOnce sync.Once
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- first.Tail(
			firstCtx,
			consumer,
			streamName,
			func(context.Context, pgx.Tx, Event) error {
				firstOnce.Do(func() { close(firstEntered) })
				<-releaseFirst
				return nil
			},
		)
	}()
	<-firstEntered

	secondStarted := make(chan struct{})
	var secondOnce sync.Once
	second.testHooks.afterEnsureCursor = func() {
		secondOnce.Do(func() { close(secondStarted) })
	}
	secondErr := make(chan error, 1)
	go func() {
		secondErr <- second.Tail(
			ctx,
			consumer,
			streamName,
			func(context.Context, pgx.Tx, Event) error {
				return errors.New("second tailer handled an event")
			},
		)
	}()
	<-secondStarted
	waitForBlockedCursorLock(t, pool)
	close(releaseFirst)

	select {
	case err := <-secondErr:
		var contention *ErrCursorContention
		if !errors.As(err, &contention) {
			t.Fatalf(
				"second Tail error = %T %v, want *ErrCursorContention",
				err,
				err,
			)
		}
		if contention.Consumer != consumer ||
			contention.Stream != streamName {
			t.Fatalf(
				"contention = %q/%q, want %q/%q",
				contention.Consumer,
				contention.Stream,
				consumer,
				streamName,
			)
		}
		if !IsRetryable(err) {
			t.Fatalf("typed contention is not retryable: %v", err)
		}
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) ||
			postgresError.Code != "40001" {
			t.Fatalf(
				"contention cause = %#v, want PostgreSQL 40001",
				postgresError,
			)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForCursor(t, pool, consumer, streamName, seq)
	stopFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Tail exit = %v", err)
	}
}

func TestRetentionResyncBootstrapConvergesWithoutDuplicateSeqApplication(
	t *testing.T,
) {
	t.Parallel()
	pool := streamTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	streamName := uniqueStreamName("resync")
	consumer := uniqueStreamName("consumer")
	identity := uniqueStreamName("work-item")
	client := newTestClient(t, pool, 10)
	snapshot, err := client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO work_items (
		    scope_key, identity_key, org_id, payload, updated_at
		)
		VALUES ($1, $2, 1, '{"state":1}', clock_timestamp())
	`, "pr:1:1:1", identity); err != nil {
		t.Fatal(err)
	}
	oldSeq := insertTestEvent(
		t,
		ctx,
		tx,
		streamName,
		identity,
		time.Now().Add(-8*24*time.Hour),
	)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	watermarker := newTestWatermarker(t, pool)
	defer watermarker.Close(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	advanceThrough(t, ctx, watermarker, oldSeq)

	retention, err := streammaint.NewRetention(
		pool,
		streammaint.RetentionOptions{
			Age:       7 * 24 * time.Hour,
			Period:    time.Hour,
			BatchSize: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := retention.Prune(ctx); err != nil || deleted < 1 {
		t.Fatalf("retention deleted=%d err=%v", deleted, err)
	}
	err = client.Tail(
		ctx,
		consumer,
		streamName,
		func(context.Context, pgx.Tx, Event) error { return nil },
	)
	var resync *ErrResyncRequired
	if !errors.As(err, &resync) || resync.PrunedThrough < oldSeq {
		t.Fatalf("tail error = %#v, want RESYNC through %d", err, oldSeq)
	}
	var resyncCount int64
	if err := pool.QueryRow(ctx, `
		SELECT resync_count
		FROM consumer_cursors
		WHERE consumer = $1 AND stream = $2
	`, consumer, streamName).Scan(&resyncCount); err != nil {
		t.Fatal(err)
	}
	if resyncCount != 1 {
		t.Fatalf("durable resync count = %d, want 1", resyncCount)
	}

	projection := testTableName("projection")
	applied := testTableName("applied")
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
		    identity_key text PRIMARY KEY,
		    payload jsonb NOT NULL
		);
		CREATE TABLE %s (seq bigint PRIMARY KEY);
	`, projection, applied)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(
			context.Background(),
			"DROP TABLE "+projection+", "+applied,
		)
	}()

	snapshot, err = client.Bootstrap(ctx, consumer, streamName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (identity_key, payload)
		SELECT identity_key, payload
		FROM work_items
		WHERE identity_key = $1
	`, projection), identity); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE work_items
		SET payload = '{"state":2}', updated_at = clock_timestamp()
		WHERE identity_key = $1
	`, identity); err != nil {
		t.Fatal(err)
	}
	newSeq := insertTestEvent(t, ctx, tx, streamName, identity, time.Now())
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	advanceThrough(t, ctx, watermarker, newSeq)

	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	go func() {
		tailErr <- client.Tail(
			tailCtx,
			consumer,
			streamName,
			func(ctx context.Context, tx pgx.Tx, event Event) error {
				if _, err := tx.Exec(
					ctx,
					"INSERT INTO "+applied+" (seq) VALUES ($1)",
					event.Seq,
				); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					UPDATE %s AS projection
					SET payload = work_items.payload
					FROM work_items
					WHERE projection.identity_key = work_items.identity_key
					  AND work_items.identity_key = $1
				`, projection), event.EntityKey)
				return err
			},
		)
	}()
	waitForCursor(t, pool, consumer, streamName, newSeq)
	stopTail()
	if err := <-tailErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("post-bootstrap tail exit = %v", err)
	}
	if count := tableCount(t, pool, applied); count != 1 {
		t.Fatalf("applied event sequences = %d, want only new seq", count)
	}
	var payload string
	if err := pool.QueryRow(
		ctx,
		"SELECT payload::text FROM "+projection+" WHERE identity_key = $1",
		identity,
	).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"state": 2`) {
		t.Fatalf("projection payload = %s, want state 2", payload)
	}
}

func newTestClient(
	t *testing.T,
	pool *pgxpool.Pool,
	batchSize int,
) *Client {
	t.Helper()
	client, err := New(pool, Config{
		BatchSize:    batchSize,
		PollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newTestWatermarker(
	t *testing.T,
	pool *pgxpool.Pool,
) *streammaint.Watermarker {
	t.Helper()
	watermarker, err := streammaint.NewWatermarker(
		pool,
		streammaint.WatermarkOptions{
			RefreshInterval: 10 * time.Millisecond,
			LeaseTTL:        time.Second,
			Owner:           uniqueStreamName("test-watermarker"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return watermarker
}

func advanceThrough(
	t *testing.T,
	ctx context.Context,
	watermarker *streammaint.Watermarker,
	target int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		progress, err := watermarker.Step(ctx)
		if errors.Is(err, streammaint.ErrLeaseHeld) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if progress.SafeSeq >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("watermark did not advance through %d", target)
}

func insertCommittedEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	key string,
	occurredAt time.Time,
) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seq := insertTestEvent(t, ctx, tx, streamName, key, occurredAt)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return seq
}

func insertTestEvent(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	streamName string,
	key string,
	occurredAt time.Time,
) int64 {
	t.Helper()
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES ($1, 'test.changed', $2, $3, '{"version":1}')
		RETURNING seq
	`, streamName, key, occurredAt).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

func readCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
	streamName string,
) int64 {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(context.Background(), `
		SELECT seq FROM consumer_cursors
		WHERE consumer = $1 AND stream = $2
	`, consumer, streamName).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

func waitForCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
	streamName string,
	want int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if readCursor(t, pool, consumer, streamName) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"cursor %s/%s did not reach %d (got %d)",
		consumer,
		streamName,
		want,
		readCursor(t, pool, consumer, streamName),
	)
}

func waitForBlockedCursorLock(
	t *testing.T,
	pool *pgxpool.Pool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		if err := pool.QueryRow(context.Background(), `
			SELECT EXISTS (
			    SELECT 1
			    FROM pg_stat_activity
			    WHERE pid <> pg_backend_pid()
			      AND datname = current_database()
			      AND wait_event_type = 'Lock'
			      AND query LIKE '%FROM consumer_cursors%'
			)
		`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("second Tail did not block on the consumer cursor row")
}

func tableCount(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		context.Background(),
		"SELECT count(*) FROM "+table,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func streamTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.New(t).Pool
}

func streamTestDatabaseWithClientPool(
	t *testing.T,
	maxConns int32,
) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database := testdb.New(t)

	config, err := pgxpool.ParseConfig(database.URL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	config.MinConns = 0
	clientPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clientPool.Close)
	if err := clientPool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return database.Pool, clientPool
}

func uniqueStreamName(prefix string) string {
	return fmt.Sprintf(
		"m5-%s-%d-%d",
		prefix,
		time.Now().UnixNano(),
		streamTestID.Add(1),
	)
}

func testTableName(prefix string) string {
	return fmt.Sprintf(
		"m5_test_%s_%d_%d",
		prefix,
		time.Now().UnixNano(),
		streamTestID.Add(1),
	)
}
