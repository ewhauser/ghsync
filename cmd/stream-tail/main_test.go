package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/outbox"
	streammaint "github.com/acme/frontier/internal/stream"
	"github.com/acme/frontier/internal/testdb"
)

func TestStreamTailSmoke(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, url, "streamtail")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pool := database.Pool
	url = database.URL

	suffix := time.Now().UnixNano()
	consumer := fmt.Sprintf("m5-stream-tail-%d", suffix)
	streamName := fmt.Sprintf("m5-stream-tail-events-%d", suffix)
	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	go func() {
		tailErr <- runContext(
			tailCtx,
			[]string{
				"--consumer=" + consumer,
				"--stream=" + streamName,
				"--poll-interval=10ms",
			},
			url,
		)
	}()
	waitCommandCursor(t, pool, consumer, streamName, -1)

	seq := insertCommandEvent(t, ctx, pool, streamName, "smoke")
	watermarker := newCommandWatermarker(t, pool, consumer)
	defer watermarker.Close(context.Background()) //nolint:errcheck
	advanceCommandWatermark(t, ctx, watermarker, seq)
	waitCommandCursor(t, pool, consumer, streamName, seq)
	stopTail()
	if err := <-tailErr; err != nil {
		t.Fatalf("stream-tail exit = %v", err)
	}
}

func TestStreamTailResyncSmoke(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, url, "streamtailresync")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pool := database.Pool

	suffix := time.Now().UnixNano()
	consumer := fmt.Sprintf("m5-stream-tail-resync-%d", suffix)
	streamName := fmt.Sprintf("m5-stream-tail-resync-events-%d", suffix)
	oldSeq := insertCommandEvent(t, ctx, pool, streamName, "before-horizon")
	watermarker := newCommandWatermarker(t, pool, consumer)
	defer watermarker.Close(context.Background()) //nolint:errcheck
	advanceCommandWatermark(t, ctx, watermarker, oldSeq)
	if _, err := pool.Exec(ctx, `
		INSERT INTO stream_horizons (
		    stream, pruned_through_seq, updated_at
		)
		VALUES ($1, $2, clock_timestamp())
		ON CONFLICT (stream) DO UPDATE
		SET pruned_through_seq = EXCLUDED.pruned_through_seq,
		    updated_at = EXCLUDED.updated_at
	`, streamName, oldSeq); err != nil {
		t.Fatal(err)
	}

	tailCtx, stopTail := context.WithCancel(ctx)
	tailErr := make(chan error, 1)
	go func() {
		tailErr <- runContext(
			tailCtx,
			[]string{
				"--consumer=" + consumer,
				"--stream=" + streamName,
				"--poll-interval=10ms",
			},
			database.URL,
		)
	}()
	waitCommandResync(t, pool, consumer, streamName, oldSeq, 1)

	newSeq := insertCommandEvent(t, ctx, pool, streamName, "after-resync")
	advanceCommandWatermark(t, ctx, watermarker, newSeq)
	waitCommandCursor(t, pool, consumer, streamName, newSeq)
	stopTail()
	if err := <-tailErr; err != nil {
		t.Fatalf("stream-tail resync exit = %v", err)
	}
}

func insertCommandEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
	entityKey string,
) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, payload
		)
		VALUES ($1, 'smoke.changed', $2, '{"version":1}')
		RETURNING seq
	`, streamName, entityKey).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return seq
}

func newCommandWatermarker(
	t *testing.T,
	pool *pgxpool.Pool,
	owner string,
) *streammaint.Watermarker {
	t.Helper()
	watermarker, err := streammaint.NewWatermarker(
		pool,
		streammaint.WatermarkOptions{
			RefreshInterval: 10 * time.Millisecond,
			LeaseTTL:        time.Second,
			Owner:           owner,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return watermarker
}

func advanceCommandWatermark(
	t *testing.T,
	ctx context.Context,
	watermarker *streammaint.Watermarker,
	want int64,
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
		if progress.SafeSeq >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("watermark did not advance through %d", want)
}

func waitCommandCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
	streamName string,
	want int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var seq int64
		err := pool.QueryRow(context.Background(), `
			SELECT seq FROM consumer_cursors
			WHERE consumer = $1 AND stream = $2
		`, consumer, streamName).Scan(&seq)
		if err == nil && (want < 0 || seq == want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cursor %s/%s did not reach %d", consumer, streamName, want)
}

func waitCommandResync(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
	streamName string,
	wantSeq int64,
	wantCount int64,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var seq, count int64
		err := pool.QueryRow(context.Background(), `
			SELECT seq, resync_count
			FROM consumer_cursors
			WHERE consumer = $1 AND stream = $2
		`, consumer, streamName).Scan(&seq, &count)
		if err == nil && seq == wantSeq && count == wantCount {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"cursor %s/%s did not resync to %d with count %d",
		consumer,
		streamName,
		wantSeq,
		wantCount,
	)
}
