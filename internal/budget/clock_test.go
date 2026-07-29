package budget

import (
	"time"

	"github.com/acme/frontier/internal/clocktest"
)

type manualClock = clocktest.Manual

func newManualClock(now time.Time) *manualClock {
	return clocktest.New(now)
}
