package metrics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func (r *Runtime) registerObservables(meter metric.Meter) error {
	var err error
	if r.budgetRemaining, err = meter.Int64ObservableGauge(
		"ghsync_c_b3_budget_remaining",
		metric.WithDescription("Server-authoritative remaining budget by priority and resource (C-B3)."),
	); err != nil {
		return err
	}
	if r.budgetLimit, err = meter.Int64ObservableGauge(
		"ghsync_c_b3_budget_limit",
		metric.WithDescription("Server-authoritative budget denominator by resource (C-B3)."),
	); err != nil {
		return err
	}
	if r.gateClosed, err = meter.Int64ObservableGauge(
		"ghsync_c_b2_gate_closed",
		metric.WithDescription("Whether the installation-wide secondary-limit gate is closed (C-B2)."),
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
	states := map[string]state{
		"rest":    {remaining: -1, limit: -1},
		"graphql": {remaining: -1, limit: -1},
	}
	rows, err := r.options.Pool.Query(ctx, `
		SELECT class, COALESCE(remaining, -1), COALESCE(rate_limit, -1),
		       COALESCE(backoff_until > clock_timestamp(), false)
		FROM installation_budgets
		WHERE installation_id = $1
		ORDER BY class
	`, r.options.InstallationID)
	if err != nil {
		return fmt.Errorf("collect C-B budget metrics: %w", err)
	}
	for rows.Next() {
		var resource string
		var remaining, limit int64
		var closed bool
		if err := rows.Scan(
			&resource, &remaining, &limit, &closed,
		); err != nil {
			rows.Close()
			return err
		}
		states[resource] = state{
			remaining: remaining,
			limit:     limit,
			closed:    closed,
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for resource, state := range states {
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
				attribute.String("resource", resource),
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
				attribute.String("resource", resource),
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
	rows, err := r.options.Pool.Query(ctx, `
		SELECT queue, count(*)
		FROM river_job
		WHERE state IN (
		    'available', 'pending', 'retryable', 'running', 'scheduled'
		)
		GROUP BY queue
	`)
	if err != nil {
		return fmt.Errorf("collect C-P2 queue metrics: %w", err)
	}
	for rows.Next() {
		var queueName string
		var depth int64
		if err := rows.Scan(&queueName, &depth); err != nil {
			rows.Close()
			return err
		}
		depths[queueName] = depth
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
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
	var oldestUnprocessed, oldestParked, oldestGeneration float64
	var parked, outstandingGenerations int64
	err := r.options.Pool.QueryRow(ctx, `
		SELECT
		    COALESCE(EXTRACT(EPOCH FROM (
		        clock_timestamp() - min(received_at)
		            FILTER (WHERE status IN ('pending', 'processing'))
		    )), 0)::double precision,
		    count(*) FILTER (WHERE status = 'parked'),
		    COALESCE(EXTRACT(EPOCH FROM (
		        clock_timestamp() - min(received_at)
		            FILTER (WHERE status = 'parked')
		    )), 0)::double precision,
		    (
		        SELECT count(*)
		        FROM refresh_intent_generations
		        WHERE completed_generation < generation
		    ),
		    (
		        SELECT COALESCE(EXTRACT(EPOCH FROM (
		            clock_timestamp() - min(event_received_at)
		        )), 0)::double precision
		        FROM refresh_intent_generations
		        WHERE completed_generation < generation
		          AND event_received_at IS NOT NULL
		    )
		FROM webhook_deliveries
	`).Scan(
		&oldestUnprocessed,
		&parked,
		&oldestParked,
		&outstandingGenerations,
		&oldestGeneration,
	)
	if err != nil {
		return fmt.Errorf("collect C-Q2/C-I5 delivery metrics: %w", err)
	}
	observer.ObserveFloat64(r.oldestDeliveryAge, oldestUnprocessed)
	observer.ObserveInt64(r.outstandingGenCount, outstandingGenerations)
	observer.ObserveFloat64(r.outstandingGenAge, oldestGeneration)
	observer.ObserveInt64(r.parkedCount, parked)
	observer.ObserveFloat64(r.parkedAge, oldestParked)
	return nil
}

func (r *Runtime) observeStaleness(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := r.options.Pool.Query(ctx, `
		WITH ages(entity_class, age_seconds) AS (
		    SELECT 'repository',
		           max(EXTRACT(EPOCH FROM (
		               clock_timestamp() - repos.last_checked_at
		           )))
		    FROM repos
		    WHERE installation_id = $1 AND tombstoned_at IS NULL
		    UNION ALL
		    SELECT 'open_stack',
		           max(EXTRACT(EPOCH FROM (
		               clock_timestamp() - stacks.last_checked_at
		           )))
		    FROM stacks
		    JOIN repos ON repos.id = stacks.repo_id
		    WHERE repos.installation_id = $1
		      AND repos.tombstoned_at IS NULL
		      AND stacks.tombstoned_at IS NULL
		      AND stacks.open
		    UNION ALL
		    SELECT 'open_pr',
		           max(EXTRACT(EPOCH FROM (
		               clock_timestamp() - pull_requests.last_checked_at
		           )))
		    FROM pull_requests
		    JOIN repos ON repos.id = pull_requests.repo_id
		    WHERE repos.installation_id = $1
		      AND repos.tombstoned_at IS NULL
		      AND pull_requests.tombstoned_at IS NULL
		      AND pull_requests.state = 'open'
		    UNION ALL
		    SELECT 'repo_rules',
		           max(EXTRACT(EPOCH FROM (
		               clock_timestamp() - state.last_checked_at
		           )))
		    FROM repo_rule_sync_state AS state
		    JOIN repos ON repos.id = state.repo_id
		    WHERE repos.installation_id = $1
		      AND repos.tombstoned_at IS NULL
		    UNION ALL
		    SELECT 'closed_displayed',
		           max(age_seconds)
		    FROM (
		        SELECT EXTRACT(EPOCH FROM (
		                   clock_timestamp() - stacks.last_checked_at
		               )) AS age_seconds
		        FROM stacks
		        JOIN repos ON repos.id = stacks.repo_id
		        WHERE repos.installation_id = $1
		          AND repos.tombstoned_at IS NULL
			          AND stacks.tombstoned_at IS NULL
			          AND NOT stacks.open
			          AND stacks.display_until > clock_timestamp()
		        UNION ALL
		        SELECT EXTRACT(EPOCH FROM (
		                   clock_timestamp() - pull_requests.last_checked_at
		               ))
		        FROM pull_requests
		        JOIN repos ON repos.id = pull_requests.repo_id
		        WHERE repos.installation_id = $1
		          AND repos.tombstoned_at IS NULL
			          AND pull_requests.tombstoned_at IS NULL
			          AND pull_requests.state <> 'open'
			          AND pull_requests.display_until > clock_timestamp()
		    ) AS closed
		)
		SELECT entity_class, COALESCE(age_seconds, 0)
		FROM ages
		ORDER BY entity_class
	`, r.options.InstallationID)
	if err != nil {
		return fmt.Errorf("collect C-R1 staleness metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entityClass string
		var age float64
		if err := rows.Scan(&entityClass, &age); err != nil {
			return err
		}
		observer.ObserveFloat64(
			r.cacheStaleness,
			age,
			metric.WithAttributes(
				attribute.String("entity_class", entityClass),
			),
		)
	}
	return rows.Err()
}

func (r *Runtime) observeTombstones(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := r.options.Pool.Query(ctx, `
		SELECT entity_kind, count(*)
		FROM (
		    SELECT 'repository' AS entity_kind FROM repos
		    WHERE tombstoned_at IS NOT NULL
		    UNION ALL
		    SELECT 'repo_rules' FROM repo_rules
		    WHERE tombstoned_at IS NOT NULL
		    UNION ALL
		    SELECT 'stack' FROM stacks
		    WHERE tombstoned_at IS NOT NULL
		    UNION ALL
		    SELECT 'pull_request' FROM pull_requests
		    WHERE tombstoned_at IS NOT NULL
		    UNION ALL
		    SELECT 'review_thread' FROM review_threads
		    WHERE tombstoned_at IS NOT NULL
		    UNION ALL
		    SELECT 'check_run' FROM check_runs
		    WHERE tombstoned_at IS NOT NULL
		) AS tombstones
		GROUP BY entity_kind
	`)
	if err != nil {
		return fmt.Errorf("collect C-C4 tombstone metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var count int64
		if err := rows.Scan(&kind, &count); err != nil {
			return err
		}
		observer.ObserveInt64(
			r.tombstoneCount,
			count,
			metric.WithAttributes(attribute.String("entity_kind", kind)),
		)
	}
	return rows.Err()
}

func (r *Runtime) observeSweeps(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := r.options.Pool.Query(ctx, `
		SELECT sweep_kind,
		       max(GREATEST(EXTRACT(EPOCH FROM (
		           COALESCE(completed_at, clock_timestamp()) - started_at
		       )), 0))::double precision
		FROM sweep_cursors
		WHERE installation_id = $1 AND started_at IS NOT NULL
		GROUP BY sweep_kind
	`, r.options.InstallationID)
	if err != nil {
		return fmt.Errorf("collect C-R2 sweep metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var duration float64
		if err := rows.Scan(&kind, &duration); err != nil {
			return err
		}
		observer.ObserveFloat64(
			r.sweepDuration,
			duration,
			metric.WithAttributes(attribute.String("sweep_kind", kind)),
		)
	}
	return rows.Err()
}

func (r *Runtime) observeDrift(
	ctx context.Context,
	observer metric.Observer,
) error {
	counts := map[string]int64{"open\x00all": 0, "resolved\x00all": 0}
	rows, err := r.options.Pool.Query(ctx, `
		SELECT CASE WHEN resolved_at IS NULL THEN 'open' ELSE 'resolved' END,
		       entity_kind,
		       count(*)
		FROM drift_findings
		WHERE installation_id = $1
		GROUP BY 1, entity_kind
	`, r.options.InstallationID)
	if err != nil {
		return fmt.Errorf("collect C-O3 drift metrics: %w", err)
	}
	for rows.Next() {
		var state, kind string
		var count int64
		if err := rows.Scan(&state, &kind, &count); err != nil {
			rows.Close()
			return err
		}
		counts[state+"\x00"+kind] = count
		counts[state+"\x00all"] += count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
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
	var safeSeq, maxSeq int64
	var age float64
	err := r.options.Pool.QueryRow(ctx, `
		SELECT watermark.safe_seq,
		       COALESCE(max(events.seq), 0),
		       EXTRACT(EPOCH FROM (
		           clock_timestamp() - watermark.updated_at
		       ))::double precision
		FROM stream_watermark AS watermark
		LEFT JOIN change_events AS events ON true
		WHERE watermark.singleton
		GROUP BY watermark.safe_seq, watermark.updated_at
	`).Scan(&safeSeq, &maxSeq, &age)
	if err != nil {
		return fmt.Errorf("collect C-S2 watermark metrics: %w", err)
	}
	observer.ObserveInt64(r.watermarkLag, maxSeq-safeSeq)
	observer.ObserveFloat64(r.watermarkAge, age)

	depths := map[string]int64{"all": 0}
	rows, err := r.options.Pool.Query(ctx, `
		SELECT events.stream, count(*)
		FROM change_events AS events
		CROSS JOIN stream_watermark AS watermark
		WHERE events.occurred_at <
		      clock_timestamp() - $1::interval
		  AND events.seq <= watermark.safe_seq
		GROUP BY events.stream
	`, r.options.StreamRetentionAge.String())
	if err != nil {
		return fmt.Errorf("collect C-S7 prunable outbox metrics: %w", err)
	}
	for rows.Next() {
		var stream string
		var count int64
		if err := rows.Scan(&stream, &count); err != nil {
			rows.Close()
			return err
		}
		depths[stream] = count
		depths["all"] += count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for stream, count := range depths {
		observer.ObserveInt64(
			r.prunableOutboxDepth,
			count,
			metric.WithAttributes(attribute.String("stream", stream)),
		)
	}

	rows, err = r.options.Pool.Query(ctx, `
		SELECT cursor.consumer, cursor.stream,
		       count(events.seq),
		       COALESCE(EXTRACT(EPOCH FROM (
		           clock_timestamp() - min(events.occurred_at)
		       )), 0)::double precision,
		       cursor.resync_count
		FROM consumer_cursors AS cursor
		CROSS JOIN stream_watermark AS watermark
		LEFT JOIN change_events AS events
		  ON events.stream = cursor.stream
		 AND events.seq > cursor.seq
		 AND events.seq <= watermark.safe_seq
		GROUP BY cursor.consumer, cursor.stream, cursor.resync_count
		ORDER BY consumer, stream
	`)
	if err != nil {
		return fmt.Errorf("collect C-S4 consumer metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var consumer, stream string
		var outstanding, resyncs int64
		var oldestOutstandingAge float64
		if err := rows.Scan(
			&consumer,
			&stream,
			&outstanding,
			&oldestOutstandingAge,
			&resyncs,
		); err != nil {
			return err
		}
		attrs := metric.WithAttributes(
			attribute.String("consumer", consumer),
			attribute.String("stream", stream),
		)
		observer.ObserveInt64(r.consumerOutstanding, outstanding, attrs)
		observer.ObserveFloat64(
			r.consumerOutstandingAge,
			oldestOutstandingAge,
			attrs,
		)
		observer.ObserveInt64(r.resyncCount, resyncs, attrs)
	}
	return rows.Err()
}

func (r *Runtime) observeDeriver(
	ctx context.Context,
	observer metric.Observer,
) error {
	var count int64
	if err := r.options.Pool.QueryRow(
		ctx,
		`SELECT count(*) FROM derivation_dirty`,
	).Scan(&count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			count = 0
		} else {
			return fmt.Errorf("collect C-P5 deriver metrics: %w", err)
		}
	}
	observer.ObserveInt64(r.deriverDirtyBacklog, count)
	return nil
}

func (r *Runtime) observeOperationHeartbeats(
	ctx context.Context,
	observer metric.Observer,
) error {
	rows, err := r.options.Pool.Query(ctx, `
		WITH expected(component, operation) AS (
		    VALUES
		        ('drift', 'detector'),
		        ('sweep', 'repositories'),
		        ('sweep', 'stacks'),
		        ('sweep', 'pull_requests'),
		        ('sweep', 'repo_rules'),
		        ('sweep', 'closed_tracked'),
		        ('watermarker', 'entities')
		)
		SELECT expected.component,
		       expected.operation,
		       COALESCE(heartbeat.success_count, 0),
		       COALESCE(heartbeat.sample_count, 0),
		       CASE
		           WHEN heartbeat.last_success_at IS NULL THEN -1
		           ELSE EXTRACT(EPOCH FROM (
		               clock_timestamp() - heartbeat.last_success_at
		           ))::double precision
		       END,
		       CASE
		           WHEN heartbeat.last_sample_at IS NULL THEN -1
		           ELSE EXTRACT(EPOCH FROM (
		               clock_timestamp() - heartbeat.last_sample_at
		           ))::double precision
		       END
		FROM expected
		LEFT JOIN operation_heartbeats AS heartbeat
		  ON heartbeat.installation_id = $1
		 AND heartbeat.component = expected.component
		 AND heartbeat.operation = expected.operation
		ORDER BY expected.component, expected.operation
	`, r.options.InstallationID)
	if err != nil {
		return fmt.Errorf("collect C-O4 operation heartbeats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var component, operation string
		var successes, samples int64
		var age, sampleAge float64
		if err := rows.Scan(
			&component,
			&operation,
			&successes,
			&samples,
			&age,
			&sampleAge,
		); err != nil {
			return err
		}
		attrs := metric.WithAttributes(
			attribute.String("component", component),
			attribute.String("operation", operation),
		)
		observer.ObserveInt64(r.operationSuccesses, successes, attrs)
		observer.ObserveInt64(r.operationSamples, samples, attrs)
		observer.ObserveFloat64(r.operationSuccessAge, age, attrs)
		observer.ObserveFloat64(r.operationSampleAge, sampleAge, attrs)
	}
	return rows.Err()
}
