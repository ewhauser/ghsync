// Package clocktest provides deterministic clocks for tests that coordinate
// concurrent work around absolute deadlines.
package clocktest

import (
	"sync"
	"time"
)

// Manual is a concurrency-safe clock advanced explicitly by tests. Timer
// registration and the current time are checked under one lock, so a deadline
// cannot move from the future to the past between those operations.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	when   time.Time
	ch     chan time.Time
	active bool
}

// New returns a manual clock starting at now.
func New(now time.Time) *Manual {
	return &Manual{
		now:    now,
		timers: make(map[*manualTimer]struct{}),
	}
}

// Now returns the clock's current time.
func (c *Manual) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimerAt registers a one-shot timer for deadline. The returned stop
// function reports whether it stopped an active timer.
func (c *Manual) NewTimerAt(
	deadline time.Time,
) (<-chan time.Time, func() bool) {
	c.mu.Lock()
	timer := &manualTimer{
		when:   deadline,
		ch:     make(chan time.Time, 1),
		active: deadline.After(c.now),
	}
	if timer.active {
		c.timers[timer] = struct{}{}
	} else {
		timer.ch <- c.now
	}
	c.mu.Unlock()

	stop := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		wasActive := timer.active
		timer.active = false
		delete(c.timers, timer)
		return wasActive
	}
	return timer.ch, stop
}

// Advance moves the clock forward and fires every timer whose deadline has
// elapsed.
func (c *Manual) Advance(by time.Duration) {
	if by < 0 {
		panic("clocktest: cannot advance clock backwards")
	}
	c.mu.Lock()
	c.now = c.now.Add(by)
	now := c.now
	ready := make([]*manualTimer, 0)
	for timer := range c.timers {
		if timer.active && !timer.when.After(now) {
			timer.active = false
			delete(c.timers, timer)
			ready = append(ready, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range ready {
		timer.ch <- now
	}
}

// TimerCount reports the number of active timers.
func (c *Manual) TimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
