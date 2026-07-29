package sweep

import (
	"context"
	"fmt"
	"time"

	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Prune enforces the decided 90-day bulky-data policy. Delivery skeleton rows
// remain for C-R4; M5 alone owns change_events retention.
func (s *Service) Prune(ctx context.Context) (int64, int64, error) {
	now := s.config.Now().UTC()
	cutoff := now.Add(-s.config.RetentionAge)
	var totalPayloads, totalHistory int64
	for {
		payloads, history, err := s.pruneBatch(ctx, now, cutoff)
		if err != nil {
			return totalPayloads, totalHistory, err
		}
		totalPayloads += payloads
		totalHistory += history
		if payloads < int64(s.config.RetentionBatchSize) &&
			history < int64(s.config.RetentionBatchSize) {
			if s.config.OnPrune != nil {
				s.config.OnPrune(ctx, "webhook_payload", totalPayloads)
				s.config.OnPrune(ctx, "check_history", totalHistory)
			}
			return totalPayloads, totalHistory, nil
		}
	}
}

func (s *Service) pruneBatch(
	ctx context.Context,
	now time.Time,
	cutoff time.Time,
) (int64, int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin retention prune: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	queries := dbgen.New(tx)
	payloads, err := queries.PruneWebhookDeliveryPayloadBatch(
		ctx,
		dbgen.PruneWebhookDeliveryPayloadBatchParams{
			Cutoff:    repoutil.Timestamptz(cutoff),
			BatchSize: int32(s.config.RetentionBatchSize),
			PrunedAt:  repoutil.Timestamptz(now),
		},
	)
	if err != nil {
		return 0, 0, fmt.Errorf("prune webhook payloads: %w", err)
	}
	history, err := queries.DeleteCheckHistoryBatch(
		ctx,
		dbgen.DeleteCheckHistoryBatchParams{
			Cutoff:    repoutil.Timestamptz(cutoff),
			BatchSize: int32(s.config.RetentionBatchSize),
		},
	)
	if err != nil {
		return 0, 0, fmt.Errorf("prune check history: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit retention prune: %w", err)
	}
	return payloads, history, nil
}
