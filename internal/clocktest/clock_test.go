package clocktest

import (
	"testing"
	"time"
)

func TestTimerRegisteredAfterDeadlineFiresImmediately(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := New(now)
	deadline := now.Add(time.Second)
	clock.Advance(2 * time.Second)
	channel, stop := clock.NewTimerAt(deadline)
	defer stop()
	select {
	case firedAt := <-channel:
		if !firedAt.Equal(now.Add(2 * time.Second)) {
			t.Fatalf("timer fired at %v", firedAt)
		}
	default:
		t.Fatal("timer registered after its deadline did not fire immediately")
	}
}

func TestAdvanceFiresOnlyElapsedDeadlines(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := New(now)
	first, stopFirst := clock.NewTimerAt(now.Add(time.Second))
	defer stopFirst()
	second, stopSecond := clock.NewTimerAt(now.Add(2 * time.Second))
	defer stopSecond()
	if got := clock.TimerCount(); got != 2 {
		t.Fatalf("active timers = %d, want 2", got)
	}
	clock.Advance(time.Second)
	select {
	case <-first:
	default:
		t.Fatal("elapsed timer did not fire")
	}
	select {
	case <-second:
		t.Fatal("future timer fired early")
	default:
	}
	if got := clock.TimerCount(); got != 1 {
		t.Fatalf("active timers after first deadline = %d, want 1", got)
	}
}
