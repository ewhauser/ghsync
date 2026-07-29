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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func TestNewEnforcesDebounceHardCap(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	for _, code := range []string{"40001", "40P01", "08006"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
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

func TestAddDeliveryTraceLinksDeduplicatesValidContexts(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
	)
	_, span := provider.Tracer("dispatch-test").Start(
		context.Background(),
		"dispatch",
	)
	traceparent := "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	addDeliveryTraceLinks(context.Background(), span, []dbgen.WebhookDelivery{
		{
			Event:       "push",
			Traceparent: pgtype.Text{String: traceparent, Valid: true},
		},
		{
			Event:       "push",
			Traceparent: pgtype.Text{String: traceparent, Valid: true},
		},
		{
			Event:       "issues",
			Traceparent: pgtype.Text{String: "invalid", Valid: true},
		},
	})
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	links := ended[0].Links()
	if len(links) != 1 {
		t.Fatalf("trace links = %d, want 1", len(links))
	}
	wantTraceID, err := trace.TraceIDFromHex(
		"0102030405060708090a0b0c0d0e0f10",
	)
	if err != nil {
		t.Fatal(err)
	}
	if links[0].SpanContext.TraceID() != wantTraceID {
		t.Fatalf(
			"linked trace ID = %s, want %s",
			links[0].SpanContext.TraceID(),
			wantTraceID,
		)
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
