package dispatch

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
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
