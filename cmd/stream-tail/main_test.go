package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/store"
	streammaint "github.com/acme/frontier/internal/stream"
)

func TestStreamTailSmoke(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

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

	var seq int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, payload
		)
		VALUES ($1, 'smoke.changed', 'smoke', '{"version":1}')
		RETURNING seq
	`, streamName).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	watermarker, err := streammaint.NewWatermarker(
		pool,
		streammaint.WatermarkOptions{
			RefreshInterval: 10 * time.Millisecond,
			LeaseTTL:        time.Second,
			Owner:           consumer,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck
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
		if progress.SafeSeq >= seq {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitCommandCursor(t, pool, consumer, streamName, seq)
	stopTail()
	if err := <-tailErr; err != nil {
		t.Fatalf("stream-tail exit = %v", err)
	}
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
