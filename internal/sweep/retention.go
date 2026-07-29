package sweep

import (
	"context"
	"fmt"

	"github.com/acme/frontier/internal/store/dbgen"
)

// Prune enforces the decided 90-day bulky-data policy. Delivery skeleton rows
// remain for C-R4; M5 alone owns change_events retention.
func (s *Service) Prune(ctx context.Context) (int64, int64, error) {
	now := s.config.Now().UTC()
	cutoff := now.Add(-s.config.RetentionAge)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin retention prune: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	payloads, err := queries.PruneWebhookDeliveryPayloads(
		ctx,
		dbgen.PruneWebhookDeliveryPayloadsParams{
			PrunedAt: timestamptz(now),
			Cutoff:   timestamptz(cutoff),
		},
	)
	if err != nil {
		return 0, 0, fmt.Errorf("prune webhook payloads: %w", err)
	}
	history, err := queries.DeleteCheckHistoryBefore(
		ctx,
		timestamptz(cutoff),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("prune check history: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit retention prune: %w", err)
	}
	return payloads, history, nil
}
