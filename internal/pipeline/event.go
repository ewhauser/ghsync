// Package pipeline carries one event-origin timestamp and the latest real
// cache-transaction commit through refresh fan-out.
package pipeline

import (
	"context"
	"sync"
	"time"
)

type eventState struct {
	receivedAt time.Time

	mu             sync.Mutex
	cacheCommitted time.Time
}

type eventKey struct{}

// WithEvent returns a context that preserves the PostgreSQL webhook receipt
// timestamp and tracks the latest authoritative cache commit in this refresh.
func WithEvent(ctx context.Context, receivedAt time.Time) context.Context {
	if receivedAt.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, eventKey{}, &eventState{receivedAt: receivedAt})
}

// EventReceivedAt returns the PostgreSQL receipt timestamp carried by ctx.
func EventReceivedAt(ctx context.Context) time.Time {
	state, _ := ctx.Value(eventKey{}).(*eventState)
	if state == nil {
		return time.Time{}
	}
	return state.receivedAt
}

// MarkCacheCommitted records a PostgreSQL timestamp obtained from the real
// entity-writer transaction immediately before its successful commit.
func MarkCacheCommitted(ctx context.Context, committedAt time.Time) {
	state, _ := ctx.Value(eventKey{}).(*eventState)
	if state == nil || committedAt.IsZero() {
		return
	}
	state.mu.Lock()
	if committedAt.After(state.cacheCommitted) {
		state.cacheCommitted = committedAt
	}
	state.mu.Unlock()
}

// CacheCommittedAt returns the latest real entity-transaction commit observed
// while processing this refresh. Fan-out-only jobs therefore return zero.
func CacheCommittedAt(ctx context.Context) time.Time {
	state, _ := ctx.Value(eventKey{}).(*eventState)
	if state == nil {
		return time.Time{}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.cacheCommitted
}
