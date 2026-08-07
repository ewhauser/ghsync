package fetch

import (
	"context"
	"log/slog"

	"github.com/ewhauser/ghsync/internal/store"
)

// closeObservation retains the fetch API's symmetric observation lifetime.
// Optimistic observations own no resources, so close is currently a no-op.
func closeObservation(ctx context.Context, observation *store.Observation) {
	if observation == nil {
		return
	}
	if err := observation.CloseContext(ctx); err != nil {
		slog.WarnContext(
			context.WithoutCancel(ctx),
			"observation cleanup failed",
			"entity_key", observation.Key(),
			"error", err,
		)
	}
}
