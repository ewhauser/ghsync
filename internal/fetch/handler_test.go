package fetch

import (
	"errors"
	"testing"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
)

func TestInteractiveQueueHasInteractiveSyncSource(t *testing.T) {
	t.Parallel()
	class, source, err := classAndSource(queue.QueueInteractive)
	if err != nil {
		t.Fatal(err)
	}
	if class != budget.Interactive || source != store.SyncSourceInteractive {
		t.Fatalf("interactive queue class=%q source=%q", class, source)
	}
}

func TestRetrySupersededObservationIsBounded(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := retrySupersededObservation(t.Context(), func() error {
		attempts++
		return store.ErrObservationSuperseded
	})
	if !errors.Is(err, store.ErrObservationSuperseded) {
		t.Fatalf("retry error = %v, want %v", err, store.ErrObservationSuperseded)
	}
	if attempts != observationRetryLimit {
		t.Fatalf("observation attempts = %d, want %d", attempts, observationRetryLimit)
	}
}

func TestRetrySupersededObservationStopsAfterSuccess(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := retrySupersededObservation(t.Context(), func() error {
		attempts++
		if attempts < observationRetryLimit {
			return store.ErrObservationSuperseded
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != observationRetryLimit {
		t.Fatalf("observation attempts = %d, want %d", attempts, observationRetryLimit)
	}
}
