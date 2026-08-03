package fetch

import (
	"context"
	"log/slog"

	"github.com/ewhauser/ghsync/internal/store"
)

// closeObservation makes C-C6 cleanup failures operationally visible. The
// store owns the bounded unlock and destroys an ambiguous physical session;
// fetch callers cannot change an already-completed primary operation here.
func closeObservation(ctx context.Context, observation *store.Observation) {
	if observation == nil {
		return
	}
	if err := observation.CloseContext(ctx); err != nil {
		slog.WarnContext(
			context.WithoutCancel(ctx),
			"observation cleanup failed; connection destroyed",
			"entity_key", observation.Key(),
			"error", err,
		)
	}
}
