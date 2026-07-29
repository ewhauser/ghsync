package stream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/testdb"
)

func TestWatermarkProgressIdleAndUnderLoad(t *testing.T) {
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner:           fmt.Sprintf("load-test-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- watermarker.Run(runCtx) }()

	idleSeq := insertStreamEvent(
		t,
		ctx,
		pool,
		fmt.Sprintf("m5-idle-%d", time.Now().UnixNano()),
	)
	waitSafeSequence(t, pool, idleSeq)

	var maximum atomic.Int64
	var writers sync.WaitGroup
	for worker := range 4 {
		writers.Add(1)
		go func(worker int) {
			defer writers.Done()
			for index := range 25 {
				seq, err := insertStreamEventWithKey(
					ctx,
					pool,
					fmt.Sprintf("m5-load-%d", worker),
					fmt.Sprintf("%d", index),
				)
				if err != nil {
					t.Errorf("insert load event: %v", err)
					return
				}
				for {
					prior := maximum.Load()
					if seq <= prior || maximum.CompareAndSwap(prior, seq) {
						break
					}
				}
			}
		}(worker)
	}
	writers.Wait()
	if target := maximum.Load(); target <= idleSeq {
		t.Fatalf("load maximum seq = %d, idle seq = %d", target, idleSeq)
	} else {
		waitSafeSequence(t, pool, target)
	}
	stop()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

func TestWatermarkWaitsForRegisteredWriterTransaction(t *testing.T) {
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background()) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, writer); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := writer.QueryRow(ctx, `
		INSERT INTO change_events (stream, kind, entity_key, payload)
		VALUES ('writer-wait', 'test.changed', 'writer', '{"version":1}')
		RETURNING seq
	`).Scan(&seq); err != nil {
		t.Fatal(err)
	}

	watermarker, err := NewWatermarker(pool, WatermarkOptions{
		RefreshInterval: 10 * time.Millisecond,
		LeaseTTL:        time.Second,
		Owner:           fmt.Sprintf("writer-wait-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck
	beforeFence := make(chan struct{})
	watermarker.testBeforeFence = func() { close(beforeFence) }
	progress := make(chan WatermarkProgress, 1)
	stepErr := make(chan error, 1)
	go func() {
		got, err := watermarker.Step(ctx)
		progress <- got
		stepErr <- err
	}()
	<-beforeFence
	select {
	case err := <-stepErr:
		t.Fatalf("watermark did not wait for writer transaction: %v", err)
	default:
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-stepErr; err != nil {
		t.Fatal(err)
	}
	if got := <-progress; got.SafeSeq < seq {
		t.Fatalf("safe seq = %d, want at least %d", got.SafeSeq, seq)
	}
}

func TestRetentionNeverPrunesAboveSafeWatermark(t *testing.T) {
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old := time.Now().Add(-8 * 24 * time.Hour)

	smaller, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer smaller.Rollback(context.Background()) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, smaller); err != nil {
		t.Fatal(err)
	}
	smallSeq, err := insertStreamEventTx(
		ctx, smaller, "retention-safe", "small", old,
	)
	if err != nil {
		t.Fatal(err)
	}
	largeSeq, err := insertStreamEventWithTime(
		ctx, pool, "retention-safe", "large", old,
	)
	if err != nil {
		t.Fatal(err)
	}
	if smallSeq >= largeSeq {
		t.Fatalf("sequences small=%d large=%d", smallSeq, largeSeq)
	}

	retention, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge,
		Period:    time.Hour,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := retention.Prune(ctx); err != nil || deleted != 0 {
		t.Fatalf("pruned above-safe committed event: deleted=%d err=%v", deleted, err)
	}
	var safe, horizon int64
	if err := pool.QueryRow(ctx, `
		SELECT watermark.safe_seq,
		       COALESCE(horizons.pruned_through_seq, 0)
		FROM stream_watermark AS watermark
		LEFT JOIN stream_horizons AS horizons
		  ON horizons.stream = 'retention-safe'
		WHERE watermark.singleton
	`).Scan(&safe, &horizon); err != nil {
		t.Fatal(err)
	}
	if horizon > safe {
		t.Fatalf("pruned horizon %d exceeds safe seq %d", horizon, safe)
	}
	var retained int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events WHERE seq = $1
	`, largeSeq).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 1 {
		t.Fatalf("above-safe event retained=%d, want 1", retained)
	}
}

func TestRetentionSevenDayFloor(t *testing.T) {
	pool := streamDatabase(t)
	if _, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge - time.Second,
		Period:    time.Hour,
		BatchSize: 10,
	}); err == nil {
		t.Fatal("retention accepted less than the C-S7 seven-day floor")
	}
	if _, err := NewRetention(pool, RetentionOptions{
		Age:       minimumRetentionAge,
		Period:    time.Hour,
		BatchSize: 10,
	}); err != nil {
		t.Fatalf("seven-day retention rejected: %v", err)
	}
}

func TestStatementLevelWakeTriggersEmitOneConstantNotification(t *testing.T) {
	pool := streamDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assertOneStatementWake(
		t,
		ctx,
		pool,
		"frontier_change_events",
		"changed",
		func(tx pgx.Tx) error {
			if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO change_events (
				    stream, kind, entity_key, payload
				)
				SELECT 'trigger-test-' || value::text,
				       'test.changed',
				       value::text,
				       '{"version":1}'
				FROM generate_series(1, 25) AS value
			`)
			return err
		},
	)
	assertOneStatementWake(
		t,
		ctx,
		pool,
		"frontier_derivation_dirty",
		"dirty",
		func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
				INSERT INTO derivation_dirty (scope_key, marked_at)
				SELECT 'pr:1:1:' || value::text, clock_timestamp()
				FROM generate_series(1, 25) AS value
			`)
			return err
		},
	)
}

func assertOneStatementWake(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	channel string,
	payload string,
	write func(pgx.Tx) error,
) {
	t.Helper()
	listener, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+channel); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	var writerPID uint32
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	if err := write(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var notification *pgconn.Notification
	for notification == nil || notification.PID != writerPID {
		notification, err = listener.Conn().WaitForNotification(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if notification.Channel != channel || notification.Payload != payload {
		t.Fatalf(
			"notification = %s/%q, want %s/%q",
			notification.Channel,
			notification.Payload,
			channel,
			payload,
		)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		extraCtx, cancel := context.WithDeadline(ctx, deadline)
		extra, err := listener.Conn().WaitForNotification(extraCtx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("wait for extra notification: %v", err)
		}
		if extra.PID == writerPID {
			t.Fatalf("extra per-row notification: %+v", extra)
		}
	}
}

func insertStreamEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
) int64 {
	t.Helper()
	seq, err := insertStreamEventWithKey(ctx, pool, streamName, "idle")
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func insertStreamEventWithKey(
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	key string,
) (int64, error) {
	return insertStreamEventWithTime(ctx, pool, streamName, key, time.Now())
}

func insertStreamEventWithTime(
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	key string,
	occurredAt time.Time,
) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		return 0, err
	}
	seq, err := insertStreamEventTx(ctx, tx, streamName, key, occurredAt)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

func insertStreamEventTx(
	ctx context.Context,
	tx pgx.Tx,
	streamName string,
	key string,
	occurredAt time.Time,
) (int64, error) {
	var seq int64
	err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES ($1, 'test.changed', $2, $3, '{"version":1}')
		RETURNING seq
	`, streamName, key, occurredAt).Scan(&seq)
	return seq, err
}

func waitSafeSequence(
	t *testing.T,
	pool *pgxpool.Pool,
	target int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var safe int64
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(context.Background(), `
			SELECT safe_seq FROM stream_watermark WHERE singleton
		`).Scan(&safe); err != nil {
			t.Fatal(err)
		}
		if safe >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("safe watermark = %d, want at least %d", safe, target)
}

func streamDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, url, "stream")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	return database.Pool
}
