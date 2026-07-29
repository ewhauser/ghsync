package sweep

import (
	"context"
	"fmt"

	"github.com/acme/frontier/internal/gh"
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
	cutoff := s.config.Now().Add(-s.config.GapWindow)
	cursor := ""
	seen := make(map[string]struct{})
	for pageNumber := 0; pageNumber < s.config.GapMaxPages; pageNumber++ {
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
		windowComplete := false
		for _, delivery := range deliveries {
			if delivery.DeliveredAt.Before(cutoff) {
				windowComplete = true
				continue
			}
			if delivery.GUID == "" || delivery.ID <= 0 {
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
		if windowComplete || response.NextCursor == "" {
			return nil
		}
		cursor = response.NextCursor
	}
	// C-R4's comparison is intentionally bounded; the next scheduled run
	// repeats the newest configured window rather than escalating to full sync.
	return nil
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
