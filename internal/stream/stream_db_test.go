package stream

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/store"
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
				var seq int64
				err := pool.QueryRow(ctx, `
					INSERT INTO change_events (
					    stream, kind, entity_key, payload
					)
					VALUES ($1, 'load.changed', $2, '{"version":1}')
					RETURNING seq
				`,
					fmt.Sprintf("m5-load-%d", worker),
					fmt.Sprintf("%d", index),
				).Scan(&seq)
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

func insertStreamEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	streamName string,
) int64 {
	t.Helper()
	var seq int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, payload
		)
		VALUES ($1, 'idle.changed', 'idle', '{"version":1}')
		RETURNING seq
	`, streamName).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
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
