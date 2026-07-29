package streamclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/store"
	streammaint "github.com/acme/frontier/internal/stream"
)

var streamTestID atomic.Int64

func TestOutboxGapWatermarkDeliversDelayedSmallerSequenceInOrder(
	t *testing.T,
) {
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
	if err := snapshot.Tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	smallTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer smallTx.Rollback(context.Background()) //nolint:errcheck
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
	defer watermarker.Close(context.Background()) //nolint:errcheck
	for range 4 {
		progress, err := watermarker.Step(ctx)
		if err != nil {
			if errors.Is(err, streammaint.ErrLeaseHeld) {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatal(err)
		}
		if progress.SafeSeq >= largeSeq {
			t.Fatalf(
				"C-S2 watermark crossed delayed seq: safe=%d delayed=%d",
				progress.SafeSeq,
				smallSeq,
			)
		}
	}

	tailCtx, stopTail := context.WithCancel(ctx)
	defer stopTail()
	delivered := make(chan int64, 4)
	tailErr := make(chan error, 1)
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
	time.Sleep(75 * time.Millisecond)
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
	advanceThrough(t, ctx, watermarker, largeSeq)
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

func TestExactlyOncePerCursorAcrossMidBatchCrashAndRestart(t *testing.T) {
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
	if err := snapshot.Tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var sequences []int64
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
	defer watermarker.Close(context.Background()) //nolint:errcheck
	advanceThrough(t, ctx, watermarker, sequences[len(sequences)-1])

	table := testTableName("applications")
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (seq bigint PRIMARY KEY)`, table,
	)); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP TABLE "+table) //nolint:errcheck
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

func TestRetentionResyncBootstrapConvergesWithoutDuplicateSeqApplication(
	t *testing.T,
) {
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
	if err := snapshot.Tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO work_items (identity_key, org_id, payload, updated_at)
		VALUES ($1, 1, '{"state":1}', clock_timestamp())
	`, identity); err != nil {
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
	defer watermarker.Close(context.Background()) //nolint:errcheck
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
	defer pool.Exec(
		context.Background(),
		"DROP TABLE "+projection+", "+applied,
	) //nolint:errcheck

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
	if err := snapshot.Tx.Commit(ctx); err != nil {
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
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
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
