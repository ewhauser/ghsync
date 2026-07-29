package dispatch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/queue"
)

func TestNewEnforcesDebounceHardCap(t *testing.T) {
	pool := new(pgxpool.Pool)
	riverClient := new(river.Client[pgx.Tx])
	base := Config{
		BatchSize:    1,
		MaxAttempts:  1,
		Debounce:     MaxDebounce,
		PollInterval: time.Millisecond,
	}

	assertDoesNotPanic(t, func() {
		New(pool, riverClient, base)
	})
	base.Debounce = MaxDebounce + time.Nanosecond
	assertPanics(t, func() {
		New(pool, riverClient, base)
	})
}

func TestRunReturnsNilOnContextCancellation(t *testing.T) {
	dispatcher := newRunTestDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher.dispatchBatch = func(context.Context) (int, error) {
		cancel()
		return 0, context.Canceled
	}

	if err := dispatcher.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
}

func TestRunRetriesTransientError(t *testing.T) {
	dispatcher := newRunTestDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	dispatcher.dispatchBatch = func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, fmt.Errorf(
				"dispatch probe: %w",
				&pgconn.PgError{Code: "40P01", Message: "deadlock"},
			)
		}
		cancel()
		return 0, nil
	}
	dispatcher.retryDelay = func(interval time.Duration) time.Duration {
		if interval != dispatcher.config.PollInterval {
			t.Fatalf("retry base = %s, want %s", interval, dispatcher.config.PollInterval)
		}
		return 0
	}

	if err := dispatcher.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if calls != 2 {
		t.Fatalf("dispatch calls = %d, want 2", calls)
	}
}

func TestRunReturnsFatalError(t *testing.T) {
	dispatcher := newRunTestDispatcher()
	fatal := errors.New("invalid dispatcher state")
	dispatcher.dispatchBatch = func(context.Context) (int, error) {
		return 0, fatal
	}

	err := dispatcher.Run(context.Background())
	if !errors.Is(err, fatal) {
		t.Fatalf("Run() error = %v, want %v", err, fatal)
	}
}

func TestRetryableDispatchErrorClassification(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "08006"} {
		t.Run(code, func(t *testing.T) {
			err := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code})
			if !retryableDispatchError(err) {
				t.Fatalf("SQLSTATE %s was not retryable", code)
			}
		})
	}
	if retryableDispatchError(errors.New("fatal")) {
		t.Fatal("ordinary error classified as retryable")
	}
}

func TestDedupeIntentsSortsGenerationKeys(t *testing.T) {
	got := dedupeIntents([]Intent{
		{Kind: queue.KindRefreshStack, Key: "stack:z:2", Priority: PriorityEvent},
		{Kind: queue.KindRefreshPR, Key: "pr:z:9", Priority: PriorityEvent},
		{Kind: queue.KindRefreshPR, Key: "pr:a:1", Priority: PriorityEvent},
		{Kind: queue.KindRefreshStack, Key: "stack:z:2", Priority: PriorityEvent},
	})
	want := []Intent{
		{Kind: queue.KindRefreshPR, Key: "pr:a:1", Priority: PriorityEvent},
		{Kind: queue.KindRefreshPR, Key: "pr:z:9", Priority: PriorityEvent},
		{Kind: queue.KindRefreshStack, Key: "stack:z:2", Priority: PriorityEvent},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deduped intents = %#v, want %#v", got, want)
	}
}

func newRunTestDispatcher() *Dispatcher {
	return New(
		new(pgxpool.Pool),
		new(river.Client[pgx.Tx]),
		Config{
			BatchSize:    1,
			MaxAttempts:  1,
			Debounce:     time.Millisecond,
			PollInterval: time.Millisecond,
		},
	)
}

func assertDoesNotPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("unexpected panic: %v", recovered)
		}
	}()
	fn()
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
