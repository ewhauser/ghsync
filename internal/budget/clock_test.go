package budget

import (
	"sync"
	"time"
)

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	clock  *manualClock
	when   time.Time
	ch     chan time.Time
	active bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{
		now:    now,
		timers: make(map[*manualTimer]struct{}),
	}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(delay time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualTimer{
		clock:  c,
		when:   c.now.Add(delay),
		ch:     make(chan time.Time, 1),
		active: true,
	}
	c.timers[timer] = struct{}{}
	return timer
}

func (c *manualClock) Advance(by time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(by)
	now := c.now
	var ready []*manualTimer
	for timer := range c.timers {
		if timer.active && !timer.when.After(now) {
			timer.active = false
			ready = append(ready, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range ready {
		timer.ch <- now
	}
}

func (c *manualClock) TimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for timer := range c.timers {
		if timer.active {
			count++
		}
	}
	return count
}

func (t *manualTimer) C() <-chan time.Time {
	return t.ch
}

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	delete(t.clock.timers, t)
	return wasActive
}
