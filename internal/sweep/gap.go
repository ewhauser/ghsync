package sweep

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

var errGapHealLeaseLost = errors.New("delivery-gap lease lost")

const (
	gapCursorVersion = 1
	gapScanDeep      = "deep"
	gapScanIncrement = "incremental"
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
	highWatermark := state.HighWatermarkAt
	observedHighWatermark := state.PassHighWatermarkAt
	boundaryDeliveryID := state.BoundaryDeliveryID
	observedBoundaryDeliveryID := state.PassBoundaryDeliveryID
	seen := make(map[string]struct{})
	pageNumber := 0
	restartedCursor := false
	for pageNumber < s.config.GapMaxPages {
		deliveries, response, err := s.deliveries.ListAppHookDeliveries(
			workCtx,
			gh.ListAppHookDeliveriesOptions{
				PerPage: s.config.GapPageSize,
				Cursor:  cursor,
			},
			"",
		)
		if err != nil {
			if cursor != "" && !restartedCursor &&
				isInvalidDeliveryCursor(err) {
				state, err = s.restartGapWindow(
					ctx,
					&state,
					args.LeaseToken,
				)
				if err != nil {
					return err
				}
				cursor = state.Cursor
				highWatermark = state.HighWatermarkAt
				observedHighWatermark = state.PassHighWatermarkAt
				boundaryDeliveryID = state.BoundaryDeliveryID
				observedBoundaryDeliveryID = state.PassBoundaryDeliveryID
				restartedCursor = true
				continue
			}
			if cause := context.Cause(workCtx); cause != nil &&
				!errors.Is(cause, context.Canceled) {
				return cause
			}
			return fmt.Errorf("list App webhook deliveries: %w", err)
		}
		pageNumber++
		candidates := make([]gh.AppHookDelivery, 0, len(deliveries))
		reachedHighWatermark := false
		pageWithinLookback := false
		for index := range deliveries {
			delivery := &deliveries[index]
			if cursor == "" && observedBoundaryDeliveryID == 0 &&
				delivery.ID > 0 {
				observedBoundaryDeliveryID = delivery.ID
			}
			if !observedHighWatermark.Valid ||
				delivery.DeliveredAt.After(observedHighWatermark.Time) {
				observedHighWatermark = repoutil.Timestamptz(
					delivery.DeliveredAt,
				)
			}
			// Delivery IDs are not an ordering key across redeliveries. The
			// cheap pass uses the prior root delivery ID only as an exact
			// boundary identity and treats the remainder of that page as
			// overlap. The delivered-at watermark is durable evidence, not an
			// ID comparison. The scheduled deep pass covers deliveries listed
			// late behind this boundary.
			if state.ScanMode == gapScanIncrement && highWatermark.Valid &&
				boundaryDeliveryID > 0 && delivery.ID == boundaryDeliveryID {
				reachedHighWatermark = true
			}
			if delivery.DeliveredAt.Before(state.Cutoff.Time) {
				continue
			}
			pageWithinLookback = true
			if delivery.GUID == "" || delivery.ID <= 0 {
				continue
			}
			if _, duplicate := seen[delivery.GUID]; duplicate {
				continue
			}
			seen[delivery.GUID] = struct{}{}
			candidates = append(candidates, *delivery)
		}
		if err := s.redeliverMissing(
			workCtx,
			candidates,
			args.LeaseToken,
		); err != nil {
			if cause := context.Cause(workCtx); cause != nil &&
				!errors.Is(cause, context.Canceled) {
				return cause
			}
			return err
		}
		reachedLookbackEnd := len(deliveries) > 0 && !pageWithinLookback
		if response.NextCursor == "" || reachedHighWatermark ||
			reachedLookbackEnd {
			return s.completeGapWindow(
				ctx,
				cursor,
				observedHighWatermark,
				observedBoundaryDeliveryID,
				args.LeaseToken,
			)
		}
		next := response.NextCursor
		capped := pageNumber == s.config.GapMaxPages
		if err := s.advanceGapWindow(
			ctx,
			cursor,
			next,
			observedHighWatermark,
			observedBoundaryDeliveryID,
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
	lookback := s.gapLookback()
	shapeCompatible := s.gapCursorShapeCompatible(&state)
	if !state.StartedAt.Valid || state.CompletedAt.Valid {
		// Only a fresh periodic kickoff may open a new comparison window. A
		// delayed continuation from an already-completed window must be inert.
		if args.Cursor != "" {
			return dbgen.GapHealCursor{}, false, nil
		}
		scanMode := gapScanIncrement
		highWatermark := state.HighWatermarkAt
		boundaryDeliveryID := state.BoundaryDeliveryID
		deepDue := !shapeCompatible || !state.LastDeepStartedAt.Valid ||
			!state.LastDeepStartedAt.Time.After(
				now.Add(-s.config.GapDeepScanPeriod),
			)
		if deepDue || !highWatermark.Valid || boundaryDeliveryID == 0 {
			scanMode = gapScanDeep
		}
		if !shapeCompatible {
			highWatermark = pgtype.Timestamptz{}
			boundaryDeliveryID = 0
		}
		cutoff := now.Add(-lookback)
		if scanMode == gapScanDeep {
			cutoff = s.gapDeepCutoff(now, &state)
		}
		state, err = queries.StartGapHealCursor(
			ctx,
			dbgen.StartGapHealCursorParams{
				Cutoff:             repoutil.Timestamptz(cutoff),
				StartedAt:          repoutil.Timestamptz(now),
				HighWatermarkAt:    highWatermark,
				BoundaryDeliveryID: boundaryDeliveryID,
				ScanMode:           scanMode,
				CursorVersion:      gapCursorVersion,
				LookbackDurationNs: lookback.Nanoseconds(),
				PageSize:           int32(s.config.GapPageSize),
				InstallationID:     s.config.InstallationID,
				LeaseToken: pgtype.Text{
					String: args.LeaseToken,
					Valid:  true,
				},
				LeaseUntil: repoutil.Timestamptz(leaseUntil),
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
		passStale := state.StartedAt.Time.Before(now.Add(-lookback))
		if !shapeCompatible || passStale {
			highWatermark := state.HighWatermarkAt
			boundaryDeliveryID := state.BoundaryDeliveryID
			if !shapeCompatible {
				highWatermark = pgtype.Timestamptz{}
				boundaryDeliveryID = 0
			}
			cutoff := s.gapDeepCutoff(now, &state)
			state, err = queries.StartGapHealCursor(
				ctx,
				dbgen.StartGapHealCursorParams{
					Cutoff:             repoutil.Timestamptz(cutoff),
					StartedAt:          repoutil.Timestamptz(now),
					HighWatermarkAt:    highWatermark,
					BoundaryDeliveryID: boundaryDeliveryID,
					ScanMode:           gapScanDeep,
					CursorVersion:      gapCursorVersion,
					LookbackDurationNs: lookback.Nanoseconds(),
					PageSize:           int32(s.config.GapPageSize),
					LeaseToken: pgtype.Text{
						String: args.LeaseToken,
						Valid:  true,
					},
					LeaseUntil:     repoutil.Timestamptz(leaseUntil),
					InstallationID: s.config.InstallationID,
				},
			)
			if err != nil {
				return dbgen.GapHealCursor{}, false, fmt.Errorf(
					"restart stale delivery-gap cursor: %w",
					err,
				)
			}
		} else {
			state, err = queries.ClaimGapHealCursor(
				ctx,
				dbgen.ClaimGapHealCursorParams{
					LeaseToken: pgtype.Text{
						String: args.LeaseToken,
						Valid:  true,
					},
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
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.GapHealCursor{}, false, fmt.Errorf(
			"commit delivery-gap cursor: %w",
			err,
		)
	}
	return state, true, nil
}

func (s *Service) gapLookback() time.Duration {
	return s.config.GapWindow + s.config.GapDeepScanPeriod
}

func (s *Service) gapDeepCutoff(
	now time.Time,
	state *dbgen.GapHealCursor,
) time.Time {
	cutoff := now.Add(-s.gapLookback())
	if state.LastDeepStartedAt.Valid {
		priorBoundary := state.LastDeepStartedAt.Time.Add(-s.config.GapWindow)
		if priorBoundary.Before(cutoff) {
			cutoff = priorBoundary
		}
	}
	oldestRetained := now.Add(-githubDeliveryRetention)
	if cutoff.Before(oldestRetained) {
		return oldestRetained
	}
	return cutoff
}

func (s *Service) gapCursorShapeCompatible(state *dbgen.GapHealCursor) bool {
	return state.CursorVersion == gapCursorVersion &&
		state.LookbackDurationNs == s.gapLookback().Nanoseconds() &&
		state.PageSize == int32(s.config.GapPageSize)
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

func isInvalidDeliveryCursor(err error) bool {
	var httpErr *gh.HTTPError
	if !errors.As(err, &httpErr) || httpErr == nil {
		return false
	}
	return httpErr.StatusCode == http.StatusUnprocessableEntity
}

func (s *Service) restartGapWindow(
	ctx context.Context,
	state *dbgen.GapHealCursor,
	leaseToken string,
) (dbgen.GapHealCursor, error) {
	now := s.config.Now().UTC()
	restarted, err := dbgen.New(s.pool).RestartGapHealCursor(
		ctx,
		dbgen.RestartGapHealCursorParams{
			RestartedAt:    repoutil.Timestamptz(now),
			LeaseUntil:     repoutil.Timestamptz(now.Add(s.config.GapLeaseTTL)),
			InstallationID: s.config.InstallationID,
			ExpectedCursor: state.Cursor,
			LeaseToken: pgtype.Text{
				String: leaseToken,
				Valid:  true,
			},
		},
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return dbgen.GapHealCursor{}, fmt.Errorf(
				"restart invalid delivery-gap cursor: %w",
				errGapHealLeaseLost,
			)
		}
		return dbgen.GapHealCursor{}, fmt.Errorf(
			"restart invalid delivery-gap cursor: %w",
			err,
		)
	}
	return restarted, nil
}

func (s *Service) advanceGapWindow(
	ctx context.Context,
	expected string,
	next string,
	observedHighWatermark pgtype.Timestamptz,
	observedBoundaryDeliveryID int64,
	scheduleContinuation bool,
	leaseToken string,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery-gap advance: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	now := s.config.Now().UTC()
	rows, err := dbgen.New(tx).AdvanceGapHealCursor(
		ctx,
		dbgen.AdvanceGapHealCursorParams{
			NextCursor:                 next,
			ObservedHighWatermarkAt:    observedHighWatermark,
			ObservedBoundaryDeliveryID: observedBoundaryDeliveryID,
			UpdatedAt:                  repoutil.Timestamptz(now),
			InstallationID:             s.config.InstallationID,
			ExpectedCursor:             expected,
			LeaseToken: pgtype.Text{
				String: leaseToken,
				Valid:  true,
			},
			LeaseUntil: repoutil.Timestamptz(
				now.Add(s.config.GapLeaseTTL),
			),
		},
	)
	if err != nil {
		return fmt.Errorf("advance delivery-gap cursor: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf(
			"advance delivery-gap cursor: %w",
			errGapHealLeaseLost,
		)
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
			gapContinuationInsertOpts(
				now.Add(s.config.GapContinuationDelay),
			),
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
	observedHighWatermark pgtype.Timestamptz,
	observedBoundaryDeliveryID int64,
	leaseToken string,
) error {
	rows, err := dbgen.New(s.pool).CompleteGapHealCursor(
		ctx,
		dbgen.CompleteGapHealCursorParams{
			ObservedHighWatermarkAt:    observedHighWatermark,
			ObservedBoundaryDeliveryID: observedBoundaryDeliveryID,
			CompletedAt:                repoutil.Timestamptz(s.config.Now()),
			InstallationID:             s.config.InstallationID,
			ExpectedCursor:             expected,
			LeaseToken: pgtype.Text{
				String: leaseToken,
				Valid:  true,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("complete delivery-gap cursor: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf(
			"complete delivery-gap cursor: %w",
			errGapHealLeaseLost,
		)
	}
	return nil
}

func gapContinuationInsertOpts(scheduledAt time.Time) *river.InsertOpts {
	return &river.InsertOpts{
		Queue:       queue.QueueReconcile,
		Priority:    1,
		ScheduledAt: scheduledAt,
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
	claimToken string,
) error {
	if len(deliveries) == 0 {
		return nil
	}
	guids := make([]string, 0, len(deliveries))
	for index := range deliveries {
		delivery := &deliveries[index]
		guids = append(guids, delivery.GUID)
	}
	queries := dbgen.New(s.pool)
	existing, err := queries.ListExistingWebhookDeliveryGUIDs(
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
	if len(existing) > 0 {
		if err := queries.DeleteWebhookDeliveryRetries(
			ctx,
			dbgen.DeleteWebhookDeliveryRetriesParams{
				InstallationID: s.config.InstallationID,
				DeliveryGuids:  existing,
			},
		); err != nil {
			return fmt.Errorf("clear healed webhook delivery retries: %w", err)
		}
	}
	now := s.config.Now().UTC()
	for index := range deliveries {
		delivery := &deliveries[index]
		if _, ok := present[delivery.GUID]; ok {
			continue
		}
		retry, newlyObserved, err := s.observeWebhookDeliveryRetry(
			ctx,
			queries,
			delivery,
			now,
		)
		if err != nil {
			return err
		}
		if newlyObserved && delivery.Redelivery {
			s.config.Observer.GapUnresolved(
				ctx,
				delivery.ID,
				delivery.GUID,
				retry.Outcome,
				deliveryStatusClass(delivery.StatusCode),
			)
		}
		nextAttempt := now.Add(
			s.gapRedeliveryBackoff(retry.AttemptCount + 1),
		)
		if _, err := queries.ClaimWebhookDeliveryRetry(
			ctx,
			dbgen.ClaimWebhookDeliveryRetryParams{
				NextAttemptAt: repoutil.Timestamptz(nextAttempt),
				ClaimToken: pgtype.Text{
					String: claimToken,
					Valid:  true,
				},
				AttemptedAt:        repoutil.Timestamptz(now),
				InstallationID:     s.config.InstallationID,
				DeliveryGuid:       delivery.GUID,
				ExpectedDeliveryID: retry.LatestDeliveryID,
			},
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return fmt.Errorf("claim webhook delivery retry: %w", err)
		}
		if err := s.deliveries.RedeliverAppHookDelivery(
			ctx,
			delivery.ID,
		); err != nil {
			releaseErr := s.releaseWebhookDeliveryRetryClaim(
				ctx,
				queries,
				delivery,
				claimToken,
				now,
			)
			if releaseErr != nil {
				return fmt.Errorf(
					"redeliver webhook delivery %d: %w",
					delivery.ID,
					errors.Join(
						err,
						fmt.Errorf(
							"release retry claim: %w",
							releaseErr,
						),
					),
				)
			}
			return fmt.Errorf(
				"redeliver webhook delivery %d: %w",
				delivery.ID,
				err,
			)
		}
		accepted, err := queries.AcceptWebhookDeliveryRetry(
			ctx,
			dbgen.AcceptWebhookDeliveryRetryParams{
				AcceptedAt:         repoutil.Timestamptz(now),
				InstallationID:     s.config.InstallationID,
				DeliveryGuid:       delivery.GUID,
				ExpectedDeliveryID: delivery.ID,
				ClaimToken: pgtype.Text{
					String: claimToken,
					Valid:  true,
				},
			},
		)
		if err != nil {
			return fmt.Errorf("accept webhook delivery retry: %w", err)
		}
		if accepted == 0 {
			continue
		}
		s.config.Observer.GapRedelivery(
			ctx,
			delivery.ID,
			delivery.GUID,
		)
	}
	return nil
}

func (s *Service) releaseWebhookDeliveryRetryClaim(
	ctx context.Context,
	queries *dbgen.Queries,
	delivery *gh.AppHookDelivery,
	claimToken string,
	failedAt time.Time,
) error {
	_, err := queries.ReleaseWebhookDeliveryRetryClaim(
		ctx,
		dbgen.ReleaseWebhookDeliveryRetryClaimParams{
			FailedAt:           repoutil.Timestamptz(failedAt),
			InstallationID:     s.config.InstallationID,
			DeliveryGuid:       delivery.GUID,
			ExpectedDeliveryID: delivery.ID,
			ClaimToken: pgtype.Text{
				String: claimToken,
				Valid:  true,
			},
		},
	)
	return err
}

func (s *Service) observeWebhookDeliveryRetry(
	ctx context.Context,
	queries *dbgen.Queries,
	delivery *gh.AppHookDelivery,
	now time.Time,
) (dbgen.WebhookDeliveryRetry, bool, error) {
	lookup := dbgen.GetWebhookDeliveryRetryParams{
		InstallationID: s.config.InstallationID,
		DeliveryGuid:   delivery.GUID,
	}
	prior, err := queries.GetWebhookDeliveryRetry(ctx, lookup)
	if err == nil && prior.LatestDeliveryID == delivery.ID {
		return prior, false, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return dbgen.WebhookDeliveryRetry{}, false, fmt.Errorf(
			"load webhook delivery retry: %w",
			err,
		)
	}

	previousDeliveryID := int64(0)
	attemptCount := int32(0)
	lastAttemptedAt := pgtype.Timestamptz{}
	if err == nil {
		previousDeliveryID = prior.LatestDeliveryID
		attemptCount = prior.AttemptCount
		lastAttemptedAt = prior.LastAttemptedAt
	} else if delivery.Redelivery {
		// A retry row may not exist yet during rollout. Seed one prior attempt
		// from GitHub's durable redelivery outcome so existing failures do not
		// get one more immediate retry after migration.
		attemptCount = 1
		lastAttemptedAt = repoutil.Timestamptz(delivery.DeliveredAt)
	}

	outcome := "ready"
	statusCode := boundedDeliveryStatusCode(delivery.StatusCode)
	nextAttemptAt := now
	if delivery.Redelivery {
		outcome = "transient"
		deliveredAt := delivery.DeliveredAt.UTC()
		if deliveredAt.IsZero() {
			deliveredAt = now
		}
		if statusCode >= http.StatusBadRequest &&
			statusCode < http.StatusInternalServerError {
			outcome = "terminal"
			nextAttemptAt = deliveredAt.Add(
				s.config.GapRedeliveryBackoffMax,
			)
		} else {
			nextAttemptAt = deliveredAt.Add(
				s.gapRedeliveryBackoff(max(attemptCount, 1)),
			)
		}
	}

	retry, err := queries.UpsertWebhookDeliveryRetryObservation(
		ctx,
		dbgen.UpsertWebhookDeliveryRetryObservationParams{
			InstallationID:     s.config.InstallationID,
			DeliveryGuid:       delivery.GUID,
			LatestDeliveryID:   delivery.ID,
			AttemptCount:       attemptCount,
			Outcome:            outcome,
			StatusCode:         int32(statusCode), //nolint:gosec // bounded to 0..599
			NextAttemptAt:      repoutil.Timestamptz(nextAttemptAt),
			LastAttemptedAt:    lastAttemptedAt,
			UpdatedAt:          repoutil.Timestamptz(now),
			PreviousDeliveryID: previousDeliveryID,
		},
	)
	if err == nil {
		return retry, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return dbgen.WebhookDeliveryRetry{}, false, fmt.Errorf(
			"record webhook delivery retry observation: %w",
			err,
		)
	}
	// A successor may have observed a newer attempt after this worker lost its
	// lease. Do not overwrite that state with this page's older observation.
	retry, err = queries.GetWebhookDeliveryRetry(ctx, lookup)
	if err != nil {
		return dbgen.WebhookDeliveryRetry{}, false, fmt.Errorf(
			"reload concurrent webhook delivery retry: %w",
			err,
		)
	}
	return retry, false, nil
}

func (s *Service) gapRedeliveryBackoff(attempt int32) time.Duration {
	delay := s.config.GapRedeliveryBackoff
	for step := int32(1); step < attempt; step++ {
		if delay >= s.config.GapRedeliveryBackoffMax/2 {
			return s.config.GapRedeliveryBackoffMax
		}
		delay *= 2
	}
	return min(delay, s.config.GapRedeliveryBackoffMax)
}

func boundedDeliveryStatusCode(statusCode int) int {
	if statusCode < 0 || statusCode > 599 {
		return 0
	}
	return statusCode
}

func deliveryStatusClass(statusCode int) string {
	statusCode = boundedDeliveryStatusCode(statusCode)
	switch {
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 300 && statusCode < 400:
		return "3xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500:
		return "5xx"
	default:
		return "other"
	}
}
