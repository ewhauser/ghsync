package budget

import "time"

// Clock is the time source for admission, backoff, and lease-expiry decisions.
// Tests inject a manual implementation so C-B2/C-B3 timing is deterministic.
type Clock interface {
	Now() time.Time
	NewTimerAt(time.Time) (<-chan time.Time, func() bool)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTimerAt(
	deadline time.Time,
) (<-chan time.Time, func() bool) {
	timer := time.NewTimer(time.Until(deadline))
	return timer.C, timer.Stop
}
