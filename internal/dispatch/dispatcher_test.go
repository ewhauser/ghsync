package dispatch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/queue"
)

func TestNewEnforcesDebounceHardCap(t *testing.T) {
	pool := new(pgxpool.Pool)
	riverClient := new(river.Client[pgx.Tx])
	base := Config{
		BatchSize:    1,
		MaxAttempts:  1,
		Debounce:     MaxDebounce,
		PollInterval: time.Millisecond,
		Classifier:   DefaultClassifier(),
	}

	if _, err := New(pool, riverClient, base); err != nil {
		t.Fatalf("maximum debounce rejected: %v", err)
	}
	base.Debounce = MaxDebounce + time.Nanosecond
	if _, err := New(pool, riverClient, base); err == nil {
		t.Fatal("debounce above hard cap accepted")
	}
}

func TestNewRejectsEmptyClassifier(t *testing.T) {
	_, err := New(
		new(pgxpool.Pool),
		new(river.Client[pgx.Tx]),
		Config{
			BatchSize:    1,
			MaxAttempts:  1,
			Debounce:     time.Millisecond,
			PollInterval: time.Millisecond,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "classifier") {
		t.Fatalf("empty classifier error = %v", err)
	}
}

func TestRunReturnsNilOnContextCancellation(t *testing.T) {
	dispatcher := newRunTestDispatcher(t)
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
	dispatcher := newRunTestDispatcher(t)
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
	dispatcher := newRunTestDispatcher(t)
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

func newRunTestDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	dispatcher, err := New(
		new(pgxpool.Pool),
		new(river.Client[pgx.Tx]),
		Config{
			BatchSize:    1,
			MaxAttempts:  1,
			Debounce:     time.Millisecond,
			PollInterval: time.Millisecond,
			Classifier:   DefaultClassifier(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}
