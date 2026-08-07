package fetch

import (
	"context"
	"errors"
	"testing"
)

func TestDiscoveryGateWaitIsCancellableAndCleansUp(t *testing.T) {
	t.Parallel()
	var gate discoveryGate
	release, err := gate.acquire(t.Context(), "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	if _, err := gate.acquire(waitCtx, "acme/monolith"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled discovery wait error = %v, want %v", err, context.Canceled)
	}
	release()

	release, err = gate.acquire(t.Context(), "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	release()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.entries) != 0 {
		t.Fatalf("discovery gate retained %d idle entries", len(gate.entries))
	}
}
