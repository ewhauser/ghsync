package sweep

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

var errGapHealLeaseLost = errors.New("delivery-gap lease lost")

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
	if args.LeaseToken == "" {
		args.LeaseToken = newGapLeaseToken()
	}
	state, acquired, err := s.loadOrStartGapWindow(ctx, args)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	workCtx, cancelWork := context.WithCancelCause(ctx)
	stopLease := make(chan struct{})
	go s.maintainGapLease(workCtx, cancelWork, stopLease, args.LeaseToken)
	defer func() {
		close(stopLease)
		cancelWork(nil)
	}()
	cursor := state.Cursor
	seen := make(map[string]struct{})
	for pageNumber := 1; pageNumber <= s.config.GapMaxPages; pageNumber++ {
		deliveries, response, err := s.deliveries.ListAppHookDeliveries(
			workCtx,
			gh.ListAppHookDeliveriesOptions{
				PerPage: s.config.GapPageSize,
				Cursor:  cursor,
			},
			"",
		)
		if err != nil {
			if cause := context.Cause(workCtx); cause != nil &&
				!errors.Is(cause, context.Canceled) {
				return cause
			}
			return fmt.Errorf("list App webhook deliveries: %w", err)
		}
		candidates := make([]gh.AppHookDelivery, 0, len(deliveries))
		for index := range deliveries {
			delivery := &deliveries[index]
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
			candidates = append(candidates, *delivery)
		}
		if err := s.redeliverMissing(workCtx, candidates); err != nil {
			return err
		}
		if response.NextCursor == "" {
			return s.completeGapWindow(ctx, cursor, args.LeaseToken)
		}
		next := response.NextCursor
		capped := pageNumber == s.config.GapMaxPages
		if err := s.advanceGapWindow(
			ctx,
			cursor,
			next,
			capped,
			args.LeaseToken,
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
	args GapHealArgs,
) (dbgen.GapHealCursor, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbgen.GapHealCursor{}, false, fmt.Errorf(
			"begin delivery-gap cursor: %w",
			err,
		)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	queries := dbgen.New(tx)
	if _, err := queries.EnsureGapHealCursor(
		ctx,
		s.config.InstallationID,
	); err != nil {
		return dbgen.GapHealCursor{}, false, fmt.Errorf(
			"ensure delivery-gap cursor: %w",
			err,
		)
	}
	state, err := queries.GetGapHealCursorForUpdate(
		ctx,
		s.config.InstallationID,
	)
	if err != nil {
		return dbgen.GapHealCursor{}, false, fmt.Errorf(
			"lock delivery-gap cursor: %w",
			err,
		)
	}
	now := s.config.Now().UTC()
	leaseUntil := now.Add(s.config.GapLeaseTTL)
	if !state.StartedAt.Valid || state.CompletedAt.Valid {
		// Only a fresh periodic kickoff may open a new comparison window. A
		// delayed continuation from an already-completed window must be inert.
		if args.Cursor != "" {
			return dbgen.GapHealCursor{}, false, nil
		}
		state, err = queries.StartGapHealCursor(
			ctx,
			dbgen.StartGapHealCursorParams{
				Cutoff:         repoutil.Timestamptz(now.Add(-s.config.GapWindow)),
				StartedAt:      repoutil.Timestamptz(now),
				InstallationID: s.config.InstallationID,
				LeaseToken:     pgtype.Text{String: args.LeaseToken, Valid: true},
				LeaseUntil:     repoutil.Timestamptz(leaseUntil),
			},
		)
		if err != nil {
			return dbgen.GapHealCursor{}, false, fmt.Errorf(
				"start delivery-gap cursor: %w",
				err,
			)
		}
	} else {
		if args.Cursor != "" && args.Cursor != state.Cursor {
			return dbgen.GapHealCursor{}, false, nil
		}
		leaseHeld := state.LeaseToken.Valid &&
			state.LeaseToken.String != args.LeaseToken &&
			state.LeaseUntil.Valid && now.Before(state.LeaseUntil.Time)
		if leaseHeld {
			return dbgen.GapHealCursor{}, false, nil
		}
		state, err = queries.ClaimGapHealCursor(
			ctx,
			dbgen.ClaimGapHealCursorParams{
				LeaseToken:     pgtype.Text{String: args.LeaseToken, Valid: true},
				LeaseUntil:     repoutil.Timestamptz(leaseUntil),
				ClaimedAt:      repoutil.Timestamptz(now),
				InstallationID: s.config.InstallationID,
			},
		)
		if err != nil {
			return dbgen.GapHealCursor{}, false, fmt.Errorf(
				"claim delivery-gap lease: %w",
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.GapHealCursor{}, false, fmt.Errorf(
			"commit delivery-gap cursor: %w",
			err,
		)
	}
	return state, true, nil
}

func (s *Service) maintainGapLease(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	stop <-chan struct{},
	leaseToken string,
) {
	ticker := time.NewTicker(s.config.GapLeaseTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			rows, err := dbgen.New(s.pool).RenewGapHealLease(
				ctx,
				dbgen.RenewGapHealLeaseParams{
					LeaseUntil: repoutil.Timestamptz(
						s.config.Now().UTC().Add(s.config.GapLeaseTTL),
					),
					InstallationID: s.config.InstallationID,
					LeaseToken: pgtype.Text{
						String: leaseToken,
						Valid:  true,
					},
				},
			)
			if err != nil {
				cancel(fmt.Errorf("renew delivery-gap lease: %w", err))
				return
			}
			if rows == 0 {
				cancel(errGapHealLeaseLost)
				return
			}
		}
	}
}

func (s *Service) advanceGapWindow(
	ctx context.Context,
	expected string,
	next string,
	scheduleContinuation bool,
	leaseToken string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery-gap advance: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if _, err := dbgen.New(tx).AdvanceGapHealCursor(
		ctx,
		dbgen.AdvanceGapHealCursorParams{
			NextCursor:     next,
			UpdatedAt:      repoutil.Timestamptz(s.config.Now()),
			InstallationID: s.config.InstallationID,
			ExpectedCursor: expected,
			LeaseToken: pgtype.Text{
				String: leaseToken,
				Valid:  true,
			},
			LeaseUntil: repoutil.Timestamptz(
				s.config.Now().UTC().Add(s.config.GapLeaseTTL),
			),
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
				LeaseToken:   leaseToken,
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
	leaseToken string,
) error {
	_, err := dbgen.New(s.pool).CompleteGapHealCursor(
		ctx,
		dbgen.CompleteGapHealCursorParams{
			CompletedAt:    repoutil.Timestamptz(s.config.Now()),
			InstallationID: s.config.InstallationID,
			ExpectedCursor: expected,
			LeaseToken: pgtype.Text{
				String: leaseToken,
				Valid:  true,
			},
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
	for index := range deliveries {
		delivery := &deliveries[index]
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
	for index := range deliveries {
		delivery := &deliveries[index]
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
