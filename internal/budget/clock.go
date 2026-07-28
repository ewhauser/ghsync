package budget

import "time"

// Clock is the time source for admission, backoff, and lease-expiry decisions.
// Tests inject a manual implementation so C-B2/C-B3 timing is deterministic.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Timer is the subset of time.Timer used by the budget gate.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTimer(delay time.Duration) Timer {
	return realTimer{Timer: time.NewTimer(delay)}
}

type realTimer struct {
	*time.Timer
}

func (t realTimer) C() <-chan time.Time {
	return t.Timer.C
}
