package fetch

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// discoveryGate serializes cold repository discovery by full name without
// reserving a database connection while the elected caller performs GitHub
// RPCs. Entries are reference-counted so the key map does not grow forever.
type discoveryGate struct {
	mu      sync.Mutex
	entries map[string]*discoveryGateEntry
}

type discoveryGateEntry struct {
	semaphore *semaphore.Weighted
	refs      int
}

func (g *discoveryGate) acquire(
	ctx context.Context,
	key string,
) (func(), error) {
	g.mu.Lock()
	if g.entries == nil {
		g.entries = make(map[string]*discoveryGateEntry)
	}
	entry := g.entries[key]
	if entry == nil {
		entry = &discoveryGateEntry{semaphore: semaphore.NewWeighted(1)}
		g.entries[key] = entry
	}
	entry.refs++
	g.mu.Unlock()

	if err := entry.semaphore.Acquire(ctx, 1); err != nil {
		g.releaseReference(key, entry)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.semaphore.Release(1)
			g.releaseReference(key, entry)
		})
	}, nil
}

func (g *discoveryGate) releaseReference(
	key string,
	entry *discoveryGateEntry,
) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && g.entries[key] == entry {
		delete(g.entries, key)
	}
}
