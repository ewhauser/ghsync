package budget

import (
	"time"

	"github.com/ewhauser/ghsync/internal/clocktest"
)

type manualClock = clocktest.Manual

func newManualClock(now time.Time) *manualClock {
	return clocktest.New(now)
}
