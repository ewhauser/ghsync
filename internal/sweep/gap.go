package sweep

import (
	"context"
	"fmt"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store/dbgen"
)

func (s *Service) HealDeliveryGaps(
	ctx context.Context,
	args GapHealArgs,
) error {
	if args.Installation != s.config.InstallationID {
		return nil
	}
	if s.deliveries == nil {
		return fmt.Errorf("deliveries client is not configured")
	}
	state, err := s.loadOrStartGapWindow(ctx)
	if err != nil {
		return err
	}
	if args.Cursor != "" && args.Cursor != state.Cursor {
		return nil
	}
	cursor := state.Cursor
	seen := make(map[string]struct{})
	for pageNumber := 1; pageNumber <= s.config.GapMaxPages; pageNumber++ {
		deliveries, response, err := s.deliveries.ListAppHookDeliveries(
			ctx,
			gh.ListAppHookDeliveriesOptions{
				PerPage: s.config.GapPageSize,
				Cursor:  cursor,
			},
			"",
		)
		if err != nil {
			return fmt.Errorf("list App webhook deliveries: %w", err)
		}
		candidates := make([]gh.AppHookDelivery, 0, len(deliveries))
		for _, delivery := range deliveries {
			// GitHub does not contractually promise delivery-list ordering.
			// Scan to the terminal cursor and only filter membership in the
			// fixed time window; an old row never terminates the scan.
			if delivery.DeliveredAt.Before(state.Cutoff.Time) ||
				delivery.GUID == "" || delivery.ID <= 0 {
				continue
			}
			if _, duplicate := seen[delivery.GUID]; duplicate {
				continue
			}
			seen[delivery.GUID] = struct{}{}
			candidates = append(candidates, delivery)
		}
		if err := s.redeliverMissing(ctx, candidates); err != nil {
			return err
		}
		if response.NextCursor == "" {
			return s.completeGapWindow(ctx, cursor)
		}
		next := response.NextCursor
		capped := pageNumber == s.config.GapMaxPages
		if err := s.advanceGapWindow(
			ctx,
			cursor,
			next,
			capped,
		); err != nil {
			return err
		}
		cursor = next
		if capped {
			s.config.Observer.GapWindowIncomplete(
				ctx,
				cursor,
				pageNumber,
			)
			return nil
		}
	}
	return nil
}

func (s *Service) loadOrStartGapWindow(
	ctx context.Context,
) (dbgen.GapHealCursor, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbgen.GapHealCursor{}, fmt.Errorf(
			"begin delivery-gap cursor: %w",
			err,
		)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	if _, err := queries.EnsureGapHealCursor(
		ctx,
		s.config.InstallationID,
	); err != nil {
		return dbgen.GapHealCursor{}, fmt.Errorf(
			"ensure delivery-gap cursor: %w",
			err,
		)
	}
	state, err := queries.GetGapHealCursorForUpdate(
		ctx,
		s.config.InstallationID,
	)
	if err != nil {
		return dbgen.GapHealCursor{}, fmt.Errorf(
			"lock delivery-gap cursor: %w",
			err,
		)
	}
	if !state.StartedAt.Valid || state.CompletedAt.Valid {
		now := s.config.Now().UTC()
		state, err = queries.StartGapHealCursor(
			ctx,
			dbgen.StartGapHealCursorParams{
				Cutoff:         timestamptz(now.Add(-s.config.GapWindow)),
				StartedAt:      timestamptz(now),
				InstallationID: s.config.InstallationID,
			},
		)
		if err != nil {
			return dbgen.GapHealCursor{}, fmt.Errorf(
				"start delivery-gap cursor: %w",
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.GapHealCursor{}, fmt.Errorf(
			"commit delivery-gap cursor: %w",
			err,
		)
	}
	return state, nil
}

func (s *Service) advanceGapWindow(
	ctx context.Context,
	expected string,
	next string,
	scheduleContinuation bool,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery-gap advance: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := dbgen.New(tx).AdvanceGapHealCursor(
		ctx,
		dbgen.AdvanceGapHealCursorParams{
			NextCursor:     next,
			UpdatedAt:      timestamptz(s.config.Now()),
			InstallationID: s.config.InstallationID,
			ExpectedCursor: expected,
		},
	); err != nil {
		return fmt.Errorf("advance delivery-gap cursor: %w", err)
	}
	if scheduleContinuation {
		client := s.riverClient()
		if client == nil {
			return fmt.Errorf("sweep River client is not configured")
		}
		if _, err := client.InsertTx(
			ctx,
			tx,
			GapHealArgs{
				Installation: s.config.InstallationID,
				Cursor:       next,
			},
			gapContinuationInsertOpts(),
		); err != nil {
			return fmt.Errorf(
				"schedule delivery-gap continuation: %w",
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery-gap advance: %w", err)
	}
	return nil
}

func (s *Service) completeGapWindow(
	ctx context.Context,
	expected string,
) error {
	_, err := dbgen.New(s.pool).CompleteGapHealCursor(
		ctx,
		dbgen.CompleteGapHealCursorParams{
			CompletedAt:    timestamptz(s.config.Now()),
			InstallationID: s.config.InstallationID,
			ExpectedCursor: expected,
		},
	)
	if err != nil {
		return fmt.Errorf("complete delivery-gap cursor: %w", err)
	}
	return nil
}

func gapContinuationInsertOpts() *river.InsertOpts {
	return &river.InsertOpts{
		Queue:    queue.QueueReconcile,
		Priority: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

func (s *Service) redeliverMissing(
	ctx context.Context,
	deliveries []gh.AppHookDelivery,
) error {
	if len(deliveries) == 0 {
		return nil
	}
	guids := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		guids = append(guids, delivery.GUID)
	}
	existing, err := dbgen.New(s.pool).ListExistingWebhookDeliveryGUIDs(
		ctx,
		guids,
	)
	if err != nil {
		return fmt.Errorf("compare webhook delivery GUIDs: %w", err)
	}
	present := make(map[string]struct{}, len(existing))
	for _, guid := range existing {
		present[guid] = struct{}{}
	}
	for _, delivery := range deliveries {
		if _, ok := present[delivery.GUID]; ok {
			continue
		}
		if err := s.deliveries.RedeliverAppHookDelivery(
			ctx,
			delivery.ID,
		); err != nil {
			return fmt.Errorf(
				"redeliver webhook delivery %d: %w",
				delivery.ID,
				err,
			)
		}
		s.config.Observer.GapRedelivery(
			ctx,
			delivery.ID,
			delivery.GUID,
		)
	}
	return nil
}
