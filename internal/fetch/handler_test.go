package fetch

import (
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
