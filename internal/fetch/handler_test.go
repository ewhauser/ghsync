package fetch

import (
	"testing"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
)

func TestInteractiveQueueHasInteractiveSyncSource(t *testing.T) {
	class, source, err := classAndSource(queue.QueueInteractive)
	if err != nil {
		t.Fatal(err)
	}
	if class != budget.Interactive || source != store.SyncSourceInteractive {
		t.Fatalf("interactive queue class=%q source=%q", class, source)
	}
}
