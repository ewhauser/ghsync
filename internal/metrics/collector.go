package metrics

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func (r *Runtime) registerObservables(meter metric.Meter) error {
	var err error
	if r.budgetRemaining, err = meter.Int64ObservableGauge(
		"ghsync_c_b3_budget_remaining",
		metric.WithDescription("Server-authoritative remaining budget by priority, resource, and auth context (C-B3)."),
	); err != nil {
		return err
	}
	if r.budgetLimit, err = meter.Int64ObservableGauge(
		"ghsync_c_b3_budget_limit",
		metric.WithDescription("Server-authoritative budget denominator by resource and auth context (C-B3)."),
	); err != nil {
		return err
	}
	if r.gateClosed, err = meter.Int64ObservableGauge(
		"ghsync_c_b2_gate_closed",
		metric.WithDescription("Whether an auth context's secondary-limit gate is closed (C-B2)."),
	); err != nil {
		return err
	}
	if r.queueDepth, err = meter.Int64ObservableGauge(
		"ghsync_c_p2_queue_depth",
		metric.WithDescription("Runnable or scheduled River jobs by queue (C-P2)."),
	); err != nil {
		return err
	}
	if r.oldestDeliveryAge, err = meter.Float64ObservableGauge(
		"ghsync_c_q2_oldest_unprocessed_delivery_age_seconds",
		metric.WithDescription("Age of the oldest pending or processing webhook delivery (C-Q2)."),
	); err != nil {
		return err
	}
	if r.outstandingGenCount, err = meter.Int64ObservableGauge(
		"ghsync_c_q2_outstanding_generations",
		metric.WithDescription("Refresh generations not yet completed (C-Q2)."),
	); err != nil {
		return err
	}
	if r.outstandingGenAge, err = meter.Float64ObservableGauge(
		"ghsync_c_q2_oldest_outstanding_generation_age_seconds",
		metric.WithDescription("Age of the oldest event-backed incomplete generation (C-Q2)."),
	); err != nil {
		return err
	}
	if r.parkedCount, err = meter.Int64ObservableGauge(
		"ghsync_c_i5_parked_deliveries",
		metric.WithDescription("Durably parked poison webhook deliveries (C-I5)."),
	); err != nil {
		return err
	}
	if r.parkedAge, err = meter.Float64ObservableGauge(
		"ghsync_c_i5_oldest_parked_delivery_age_seconds",
		metric.WithDescription("Age of the oldest parked poison delivery (C-I5)."),
	); err != nil {
		return err
	}
	if r.cacheStaleness, err = meter.Float64ObservableGauge(
		"ghsync_c_r1_cache_staleness_seconds",
		metric.WithDescription("Worst live cache validation age by C-R1 entity class."),
	); err != nil {
		return err
	}
	if r.stalenessBound, err = meter.Float64ObservableGauge(
		"ghsync_c_r1_staleness_bound_seconds",
		metric.WithDescription("Configured maximum cache age by C-R1 entity class."),
	); err != nil {
		return err
	}
	if r.casRejectRatio, err = meter.Float64ObservableGauge(
		"ghsync_c_c2_cache_cas_reject_ratio",
		metric.WithDescription("Process-lifetime compare-and-swap reject ratio by entity class (C-C2)."),
	); err != nil {
		return err
	}
	if r.tombstoneCount, err = meter.Int64ObservableGauge(
		"ghsync_c_c4_tombstones",
		metric.WithDescription("Retained tombstones by mirror entity class (C-C4)."),
	); err != nil {
		return err
	}
	if r.sweepDuration, err = meter.Float64ObservableGauge(
		"ghsync_c_r2_sweep_duration_seconds",
		metric.WithDescription("Latest or in-progress sweep duration by kind (C-R2)."),
	); err != nil {
		return err
	}
	if r.sweepPeriod, err = meter.Float64ObservableGauge(
		"ghsync_c_r2_sweep_period_seconds",
		metric.WithDescription("Configured sweep period by kind (C-R2)."),
	); err != nil {
		return err
	}
	if r.driftFindings, err = meter.Int64ObservableGauge(
		"ghsync_c_o3_drift_findings",
		metric.WithDescription("Durable semantic drift findings by entity kind and state (C-O3)."),
	); err != nil {
		return err
	}
	if r.watermarkLag, err = meter.Int64ObservableGauge(
		"ghsync_c_s2_watermark_lag_sequences",
		metric.WithDescription("Committed outbox max sequence minus safe sequence (C-S2)."),
	); err != nil {
		return err
	}
	if r.watermarkAge, err = meter.Float64ObservableGauge(
		"ghsync_c_s2_watermark_age_seconds",
		metric.WithDescription("Age of the last visibility watermark publication (C-S2)."),
	); err != nil {
		return err
	}
	if r.prunableOutboxDepth, err = meter.Int64ObservableGauge(
		"ghsync_c_s7_prunable_outbox_depth",
		metric.WithDescription("Retention-eligible change events at or below the safe consumer horizon (C-S7)."),
	); err != nil {
		return err
	}
	if r.consumerOutstanding, err = meter.Int64ObservableGauge(
		"ghsync_c_s4_consumer_outstanding_events",
		metric.WithDescription("Visible unconsumed events in the consumer's own stream (C-S4)."),
	); err != nil {
		return err
	}
	if r.consumerOutstandingAge, err = meter.Float64ObservableGauge(
		"ghsync_c_s4_oldest_outstanding_event_age_seconds",
		metric.WithDescription("Age of the oldest visible unconsumed event in the consumer's stream (C-S4)."),
	); err != nil {
		return err
	}
	if r.resyncCount, err = meter.Int64ObservableCounter(
		"ghsync_c_s4_resyncs",
		metric.WithDescription("Durable RESYNC_REQUIRED count by consumer (C-S4)."),
	); err != nil {
		return err
	}
	if r.deriverDirtyBacklog, err = meter.Int64ObservableGauge(
		"ghsync_c_p5_deriver_dirty_backlog",
		metric.WithDescription("Dirty derivation scopes awaiting a set-drain pass (C-P5)."),
	); err != nil {
		return err
	}
	if r.operationSuccesses, err = meter.Int64ObservableGauge(
		"ghsync_c_o4_operation_successes",
		metric.WithDescription("Durable completed trust-operation passes (C-O4)."),
	); err != nil {
		return err
	}
	if r.operationSamples, err = meter.Int64ObservableGauge(
		"ghsync_c_o4_operation_samples",
		metric.WithDescription("Durable trust-operation samples inspected (C-O4)."),
	); err != nil {
		return err
	}
	if r.operationSuccessAge, err = meter.Float64ObservableGauge(
		"ghsync_c_o4_last_operation_success_age_seconds",
		metric.WithDescription("Age of the last durable trust-operation completion; -1 means never (C-O4)."),
	); err != nil {
		return err
	}
	if r.operationSampleAge, err = meter.Float64ObservableGauge(
		"ghsync_c_o4_last_operation_sample_age_seconds",
		metric.WithDescription("Age of the last durable trust-operation sample; -1 means never (C-O4)."),
	); err != nil {
		return err
	}
	if r.roleEnabled, err = meter.Int64ObservableGauge(
		"ghsync_c_o4_role_enabled",
		metric.WithDescription("Roles enabled in this ghsyncd process (C-O4)."),
	); err != nil {
		return err
	}

	r.callbackRegistration, err = meter.RegisterCallback(
		r.observe,
		r.budgetRemaining,
		r.budgetLimit,
		r.gateClosed,
		r.queueDepth,
		r.oldestDeliveryAge,
		r.outstandingGenCount,
		r.outstandingGenAge,
		r.parkedCount,
		r.parkedAge,
		r.cacheStaleness,
		r.stalenessBound,
		r.casRejectRatio,
		r.tombstoneCount,
		r.sweepDuration,
		r.sweepPeriod,
		r.driftFindings,
		r.watermarkLag,
		r.watermarkAge,
		r.prunableOutboxDepth,
		r.consumerOutstanding,
		r.consumerOutstandingAge,
		r.resyncCount,
		r.deriverDirtyBacklog,
		r.operationSuccesses,
		r.operationSamples,
		r.operationSuccessAge,
		r.operationSampleAge,
		r.roleEnabled,
	)
	return err
}

func (r *Runtime) observe(ctx context.Context, observer metric.Observer) error {
	for _, role := range r.options.Roles {
		observer.ObserveInt64(
			r.roleEnabled,
			1,
			metric.WithAttributes(attribute.String("role", role)),
		)
	}
	r.observeRatios(observer)
	if !r.options.CollectDatabase {
		return nil
	}
	r.observeConfiguredBounds(observer)
	for _, observe := range []func(context.Context, metric.Observer) error{
		r.observeBudget,
		r.observeQueues,
		r.observeDeliveries,
		r.observeStaleness,
		r.observeTombstones,
		r.observeSweeps,
		r.observeDrift,
		r.observeStream,
		r.observeDeriver,
		r.observeOperationHeartbeats,
	} {
		if err := observe(ctx, observer); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) observeConfiguredBounds(observer metric.Observer) {
	bounds := map[string]time.Duration{
		"open_stack":       r.options.OpenStackStaleness,
		"open_pr":          r.options.OpenPRStaleness,
		"repo_rules":       r.options.RepoRulesStaleness,
		"closed_displayed": r.options.ClosedStaleness,
		"repository":       r.options.RepositoryPeriod,
	}
	for entityClass, bound := range bounds {
		observer.ObserveFloat64(
			r.stalenessBound,
			bound.Seconds(),
			metric.WithAttributes(
				attribute.String("entity_class", entityClass),
			),
		)
	}
	periods := map[string]time.Duration{
		// C-R2: compare duration with the scheduler's real cadence, which
		// reserves 25% of every C-R1 bound for completion and safety.
		"stacks":         sweepCadence(r.options.OpenStackStaleness),
		"pull_requests":  sweepCadence(r.options.OpenPRStaleness),
		"repo_rules":     sweepCadence(r.options.RepoRulesStaleness),
		"closed_tracked": sweepCadence(r.options.ClosedStaleness),
		"repositories":   sweepCadence(r.options.RepositoryPeriod),
	}
	for kind, period := range periods {
		observer.ObserveFloat64(
			r.sweepPeriod,
			period.Seconds(),
			metric.WithAttributes(attribute.String("sweep_kind", kind)),
		)
	}
}

func sweepCadence(bound time.Duration) time.Duration {
	return bound * 3 / 4
}

func (r *Runtime) observeRatios(observer metric.Observer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for kind, counts := range r.cas {
		if counts.total == 0 {
			continue
		}
		observer.ObserveFloat64(
			r.casRejectRatio,
			float64(counts.hits)/float64(counts.total),
			metric.WithAttributes(
				attribute.String("entity_kind", kind),
			),
		)
	}
}

func (r *Runtime) observeBudget(
	ctx context.Context,
	observer metric.Observer,
) error {
	type state struct {
		remaining int64
		limit     int64
		closed    bool
	}
	type budgetIdentity struct {
		resource    string
		authContext string
	}
	states := map[budgetIdentity]state{
		{resource: "rest", authContext: "installation"}: {
			remaining: -1,
			limit:     -1,
		},
		{resource: "rest", authContext: "app_jwt"}: {
			remaining: -1,
			limit:     -1,
		},
		{resource: "graphql", authContext: "installation"}: {
			remaining: -1,
			limit:     -1,
		},
	}
	rows, err := dbgen.New(r.options.Pool).CollectBudgetMetrics(
		ctx,
		r.options.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("collect C-B budget metrics: %w", err)
	}
	for _, row := range rows {
		identity := budgetIdentity{
			resource:    row.Class,
			authContext: "installation",
		}
		if row.Class == "app_jwt_rest" {
			identity = budgetIdentity{
				resource:    "rest",
				authContext: "app_jwt",
			}
		}
		states[identity] = state{
			remaining: row.Remaining,
			limit:     row.RateLimit,
			closed:    row.GateClosed,
		}
	}
	for identity, state := range states {
		closed := int64(0)
		if state.closed {
			closed = 1
		}
		observer.ObserveInt64(
			r.gateClosed,
			closed,
			metric.WithAttributes(
				attribute.String("installation_id", strconv.FormatInt(
					r.options.InstallationID, 10,
				)),
				attribute.String("resource", identity.resource),
				attribute.String("auth_context", identity.authContext),
			),
		)
		if state.remaining < 0 || state.limit <= 0 {
			continue
		}
		for _, class := range []string{"interactive", "event", "sweep"} {
			attrs := metric.WithAttributes(
				attribute.String("installation_id", strconv.FormatInt(
					r.options.InstallationID, 10,
				)),
				attribute.String("class", class),
				attribute.String("resource", identity.resource),
				attribute.String("auth_context", identity.authContext),
			)
			observer.ObserveInt64(
				r.budgetRemaining,
				state.remaining,
				attrs,
			)
			observer.ObserveInt64(r.budgetLimit, state.limit, attrs)
		}
	}
	return nil
}

func (r *Runtime) observeQueues(
	ctx context.Context,
	observer metric.Observer,
) error {
	depths := map[string]int64{
		"interactive": 0,
		"event":       0,
		"sweep":       0,
		"reconcile":   0,
		"drift":       0,
		"pruner":      0,
	}
	rows, err := dbgen.New(r.options.Pool).CollectRiverQueueDepthMetrics(ctx)
	if err != nil {
		return fmt.Errorf("collect C-P2 queue metrics: %w", err)
	}
	for _, row := range rows {
		depths[row.Queue] = row.Depth
	}
	names := make([]string, 0, len(depths))
	for name := range depths {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		observer.ObserveInt64(
			r.queueDepth,
			depths[name],
			metric.WithAttributes(attribute.String("queue", name)),
		)
	}
	return nil
}

func (r *Runtime) observeDeliveries(
	ctx context.Context,
	observer metric.Observer,
) error {
	row, err := dbgen.New(r.options.Pool).CollectDeliveryMetrics(ctx)
	if err != nil {
		return fmt.Errorf("collect C-Q2/C-I5 delivery metrics: %w", err)
	}
	observer.ObserveFloat64(r.oldestDeliveryAge, row.OldestUnprocessed)
	observer.ObserveInt64(
		r.outstandingGenCount,
		row.OutstandingGenerations,
	)
	observer.ObserveFloat64(r.outstandingGenAge, row.OldestGeneration)
	observer.ObserveInt64(r.parkedCount, row.Parked)
	observer.ObserveFloat64(r.parkedAge, row.OldestParked)
	return nil
}

func (r *Runtime) observeStaleness(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := dbgen.New(r.options.Pool).CollectCacheStalenessMetrics(
		ctx,
		r.options.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("collect C-R1 staleness metrics: %w", err)
	}
	for _, row := range rows {
		observer.ObserveFloat64(
			r.cacheStaleness,
			row.AgeSeconds,
			metric.WithAttributes(
				attribute.String("entity_class", row.EntityClass),
			),
		)
	}
	return nil
}

func (r *Runtime) observeTombstones(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := dbgen.New(r.options.Pool).CollectTombstoneMetrics(ctx)
	if err != nil {
		return fmt.Errorf("collect C-C4 tombstone metrics: %w", err)
	}
	for _, row := range rows {
		observer.ObserveInt64(
			r.tombstoneCount,
			row.TombstoneCount,
			metric.WithAttributes(
				attribute.String("entity_kind", row.EntityKind),
			),
		)
	}
	return nil
}

func (r *Runtime) observeSweeps(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := dbgen.New(r.options.Pool).CollectSweepMetrics(
		ctx,
		r.options.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("collect C-R2 sweep metrics: %w", err)
	}
	for _, row := range rows {
		observer.ObserveFloat64(
			r.sweepDuration,
			row.DurationSeconds,
			metric.WithAttributes(
				attribute.String("sweep_kind", row.SweepKind),
			),
		)
	}
	return nil
}

func (r *Runtime) observeDrift(
	ctx context.Context,
	observer metric.Observer,
) error {
	counts := map[string]int64{"open\x00all": 0, "resolved\x00all": 0}
	rows, err := dbgen.New(r.options.Pool).CollectDriftMetrics(
		ctx,
		r.options.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("collect C-O3 drift metrics: %w", err)
	}
	for _, row := range rows {
		counts[row.FindingState+"\x00"+row.EntityKind] = row.FindingCount
		counts[row.FindingState+"\x00all"] += row.FindingCount
	}
	for key, count := range counts {
		parts := strings.SplitN(key, "\x00", 2)
		observer.ObserveInt64(
			r.driftFindings,
			count,
			metric.WithAttributes(
				attribute.String("state", parts[0]),
				attribute.String("entity_kind", parts[1]),
			),
		)
	}
	return nil
}

func (r *Runtime) observeStream(
	ctx context.Context,
	observer metric.Observer,
) error {
	queries := dbgen.New(r.options.Pool)
	watermark, err := queries.CollectStreamWatermarkMetrics(ctx)
	if err != nil {
		return fmt.Errorf("collect C-S2 watermark metrics: %w", err)
	}
	observer.ObserveInt64(
		r.watermarkLag,
		watermark.MaxSeq-watermark.SafeSeq,
	)
	observer.ObserveFloat64(r.watermarkAge, watermark.AgeSeconds)

	depths := map[string]int64{"all": 0}
	rows, err := queries.CollectPrunableOutboxMetrics(
		ctx,
		r.options.StreamRetentionAge.String(),
	)
	if err != nil {
		return fmt.Errorf("collect C-S7 prunable outbox metrics: %w", err)
	}
	for _, row := range rows {
		depths[row.Stream] = row.PrunableCount
		depths["all"] += row.PrunableCount
	}
	for stream, count := range depths {
		observer.ObserveInt64(
			r.prunableOutboxDepth,
			count,
			metric.WithAttributes(attribute.String("stream", stream)),
		)
	}

	consumerRows, err := queries.CollectConsumerStreamMetrics(ctx)
	if err != nil {
		return fmt.Errorf("collect C-S4 consumer metrics: %w", err)
	}
	for _, row := range consumerRows {
		attrs := metric.WithAttributes(
			attribute.String("consumer", row.Consumer),
			attribute.String("stream", row.Stream),
		)
		observer.ObserveInt64(
			r.consumerOutstanding,
			row.OutstandingCount,
			attrs,
		)
		observer.ObserveFloat64(
			r.consumerOutstandingAge,
			row.OldestOutstandingAge,
			attrs,
		)
		observer.ObserveInt64(r.resyncCount, row.ResyncCount, attrs)
	}
	return nil
}

func (r *Runtime) observeDeriver(
	ctx context.Context,
	observer metric.Observer,
) error {
	count, err := dbgen.New(r.options.Pool).CountDerivationDirty(ctx)
	if err != nil {
		return fmt.Errorf("collect C-P5 deriver metrics: %w", err)
	}
	observer.ObserveInt64(r.deriverDirtyBacklog, count)
	return nil
}

func (r *Runtime) observeOperationHeartbeats(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := dbgen.New(r.options.Pool).CollectOperationHeartbeatMetrics(
		ctx,
		r.options.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("collect C-O4 operation heartbeats: %w", err)
	}
	for _, row := range rows {
		attrs := metric.WithAttributes(
			attribute.String("component", row.Component),
			attribute.String("operation", row.Operation),
		)
		observer.ObserveInt64(
			r.operationSuccesses,
			row.SuccessCount,
			attrs,
		)
		observer.ObserveInt64(r.operationSamples, row.SampleCount, attrs)
		observer.ObserveFloat64(
			r.operationSuccessAge,
			row.SuccessAgeSeconds,
			attrs,
		)
		observer.ObserveFloat64(
			r.operationSampleAge,
			row.SampleAgeSeconds,
			attrs,
		)
	}
	return nil
}
