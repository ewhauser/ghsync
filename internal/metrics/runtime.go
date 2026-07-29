package metrics

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store/dbgen"
	streammaint "github.com/acme/frontier/internal/stream"
)

type RuntimeOptions struct {
	Pool           *pgxpool.Pool
	InstallationID int64
	Roles          []string

	OpenStackStaleness time.Duration
	OpenPRStaleness    time.Duration
	RepoRulesStaleness time.Duration
	ClosedStaleness    time.Duration
	RepositoryPeriod   time.Duration
}

type ratioCounts struct {
	hits  int64
	total int64
}

// Runtime implements the engine's observer hooks and DB-backed asynchronous
// instruments. Every metric name includes the constraint it verifies (C-O4).
type Runtime struct {
	options RuntimeOptions

	mu          sync.Mutex
	conditional map[string]ratioCounts
	cas         map[string]ratioCounts

	githubRequests       metric.Int64Counter
	starvations          metric.Int64Counter
	dispatchBatches      metric.Int64Histogram
	fetches              metric.Int64Counter
	eventToCache         metric.Float64Histogram
	cacheWrites          metric.Int64Counter
	sweepOverruns        metric.Int64Counter
	gapHealRequests      metric.Int64Counter
	gapWindowIncomplete  metric.Int64Counter
	driftTransitions     metric.Int64Counter
	prunerDeletes        metric.Int64Counter
	watermarkAdvances    metric.Int64Counter
	deriverPassDuration  metric.Float64Histogram
	deriverPasses        metric.Int64Counter
	stalenessMisses      metric.Int64Counter
	budgetRemaining      metric.Int64ObservableGauge
	budgetLimit          metric.Int64ObservableGauge
	budgetFloor          metric.Int64ObservableGauge
	gateClosed           metric.Float64ObservableGauge
	conditionalRatio     metric.Float64ObservableGauge
	queueDepth           metric.Int64ObservableGauge
	oldestDeliveryAge    metric.Float64ObservableGauge
	parkedCount          metric.Int64ObservableGauge
	parkedAge            metric.Float64ObservableGauge
	cacheStaleness       metric.Float64ObservableGauge
	stalenessBound       metric.Float64ObservableGauge
	casRejectRatio       metric.Float64ObservableGauge
	tombstoneCount       metric.Int64ObservableGauge
	sweepDuration        metric.Float64ObservableGauge
	sweepPeriod          metric.Float64ObservableGauge
	driftFindings        metric.Int64ObservableGauge
	watermarkLag         metric.Int64ObservableGauge
	watermarkAge         metric.Float64ObservableGauge
	outboxDepth          metric.Int64ObservableGauge
	consumerCursorLag    metric.Int64ObservableGauge
	consumerCursorAge    metric.Float64ObservableGauge
	resyncCount          metric.Int64ObservableCounter
	deriverDirtyBacklog  metric.Int64ObservableGauge
	roleEnabled          metric.Int64ObservableGauge
	callbackRegistration metric.Registration
}

func NewRuntime(options RuntimeOptions) (*Runtime, error) {
	if options.Pool == nil {
		return nil, fmt.Errorf("runtime metrics require Postgres")
	}
	for name, bound := range map[string]time.Duration{
		"open stack": options.OpenStackStaleness,
		"open PR":    options.OpenPRStaleness,
		"repo rules": options.RepoRulesStaleness,
		"closed":     options.ClosedStaleness,
		"repository": options.RepositoryPeriod,
	} {
		if bound <= 0 {
			return nil, fmt.Errorf("%s metrics bound must be positive", name)
		}
	}
	return &Runtime{
		options:     options,
		conditional: make(map[string]ratioCounts),
		cas:         make(map[string]ratioCounts),
	}, nil
}

func (r *Runtime) RegisterMetrics(meter metric.Meter) error {
	var err error
	if r.githubRequests, err = meter.Int64Counter(
		"frontier_c_b1_github_requests",
		metric.WithDescription("Admitted GitHub requests by class and resource (C-B1)."),
	); err != nil {
		return err
	}
	if r.starvations, err = meter.Int64Counter(
		"frontier_c_b3_starvations",
		metric.WithDescription("Requests queued behind a reserved budget floor (C-B3)."),
	); err != nil {
		return err
	}
	if r.dispatchBatches, err = meter.Int64Histogram(
		"frontier_c_p2_dispatch_batch_size",
		metric.WithDescription("Webhook deliveries committed per dispatch transaction (C-P2)."),
	); err != nil {
		return err
	}
	if r.fetches, err = meter.Int64Counter(
		"frontier_c_c1_fetches",
		metric.WithDescription("Authoritative fetch jobs by pointer kind and outcome (C-C1)."),
	); err != nil {
		return err
	}
	if r.eventToCache, err = meter.Float64Histogram(
		"frontier_c_q2_event_to_cache_latency_seconds",
		metric.WithDescription("Webhook received to authoritative cache completion latency (C-Q2)."),
	); err != nil {
		return err
	}
	if r.cacheWrites, err = meter.Int64Counter(
		"frontier_c_c2_cache_cas_writes",
		metric.WithDescription("Cache compare-and-swap outcomes by entity kind (C-C2)."),
	); err != nil {
		return err
	}
	if r.sweepOverruns, err = meter.Int64Counter(
		"frontier_c_r2_sweep_overruns",
		metric.WithDescription("Sweep passes observed beyond their configured period (C-R2)."),
	); err != nil {
		return err
	}
	if r.gapHealRequests, err = meter.Int64Counter(
		"frontier_c_r4_gap_heal_requests",
		metric.WithDescription("GitHub delivery redelivery requests (C-R4)."),
	); err != nil {
		return err
	}
	if r.gapWindowIncomplete, err = meter.Int64Counter(
		"frontier_c_r4_gap_windows_incomplete",
		metric.WithDescription("Gap scans that reached their page cap (C-R4)."),
	); err != nil {
		return err
	}
	if r.driftTransitions, err = meter.Int64Counter(
		"frontier_c_o3_drift_transitions",
		metric.WithDescription("New and persistent semantic drift observations (C-O3)."),
	); err != nil {
		return err
	}
	if r.prunerDeletes, err = meter.Int64Counter(
		"frontier_c_r2_pruner_deletes",
		metric.WithDescription("Rows or payloads removed by bounded retention passes (C-R2/C-S7)."),
	); err != nil {
		return err
	}
	if r.watermarkAdvances, err = meter.Int64Counter(
		"frontier_c_s2_watermark_advances",
		metric.WithDescription("Visibility watermark advances (C-S2)."),
	); err != nil {
		return err
	}
	if r.deriverPassDuration, err = meter.Float64Histogram(
		"frontier_c_p5_deriver_pass_duration_seconds",
		metric.WithDescription("Dirty-set derivation pass duration (C-P5)."),
	); err != nil {
		return err
	}
	if r.deriverPasses, err = meter.Int64Counter(
		"frontier_c_p5_deriver_passes",
		metric.WithDescription("Dirty-set derivation passes and outcomes (C-P5)."),
	); err != nil {
		return err
	}
	if r.stalenessMisses, err = meter.Int64Counter(
		"frontier_c_r1_refresh_deadline_misses",
		metric.WithDescription("Reconciliation fetches completed beyond C-R1 deadlines."),
	); err != nil {
		return err
	}
	if err := r.registerObservables(meter); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) BudgetRequest(observation budget.RequestObservation) {
	outcome := "success"
	if observation.Err != nil {
		outcome = "error"
	}
	status := strconv.Itoa(observation.StatusCode)
	r.githubRequests.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("class", string(observation.Class)),
		attribute.String("resource", string(observation.Resource)),
		attribute.String("status", status),
		attribute.String("outcome", outcome),
	))
	if !observation.Conditional {
		return
	}
	key := string(observation.Class) + "\x00" + string(observation.Resource)
	r.mu.Lock()
	counts := r.conditional[key]
	counts.total++
	if observation.NotModified {
		counts.hits++
	}
	r.conditional[key] = counts
	r.mu.Unlock()
}

func (r *Runtime) BudgetStarvation(starvation budget.Starvation) {
	r.starvations.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("class", string(starvation.Class)),
		attribute.String("resource", string(starvation.Resource)),
	))
}

func (r *Runtime) DispatchBatch(
	ctx context.Context,
	count int,
	_ int,
) {
	r.dispatchBatches.Record(ctx, int64(count))
}

func (r *Runtime) RefreshFinished(
	ctx context.Context,
	observation queue.RefreshObservation,
) {
	outcome := "success"
	if observation.Err != nil {
		outcome = "error"
	}
	r.fetches.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", observation.Kind),
		attribute.String("queue", observation.Queue),
		attribute.String("outcome", outcome),
	))
	if observation.Err == nil && !observation.EventReceivedAt.IsZero() {
		latency := observation.CompletedAt.Sub(
			observation.EventReceivedAt,
		).Seconds()
		if latency < 0 {
			latency = 0
		}
		r.eventToCache.Record(ctx, latency, metric.WithAttributes(
			attribute.String("kind", observation.Kind),
		))
	}
}

func (r *Runtime) CacheWrite(
	ctx context.Context,
	kind string,
	accepted bool,
	tombstone bool,
) {
	result := "rejected"
	if accepted {
		result = "accepted"
	}
	r.cacheWrites.Add(ctx, 1, metric.WithAttributes(
		attribute.String("entity_kind", kind),
		attribute.String("result", result),
		attribute.Bool("tombstone", tombstone),
	))
	r.mu.Lock()
	counts := r.cas[kind]
	counts.total++
	if !accepted {
		counts.hits++
	}
	r.cas[kind] = counts
	r.mu.Unlock()
}

func (r *Runtime) RefreshDeadlineMissed(
	ctx context.Context,
	kind string,
	_ string,
	_ time.Time,
	_ time.Time,
) {
	r.stalenessMisses.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", kind),
	))
}

func (r *Runtime) SweepOverrun(
	ctx context.Context,
	kind string,
	_ string,
	_ time.Duration,
) {
	r.sweepOverruns.Add(ctx, 1, metric.WithAttributes(
		attribute.String("sweep_kind", kind),
	))
}

func (r *Runtime) GapRedelivery(
	ctx context.Context,
	_ int64,
	_ string,
) {
	r.gapHealRequests.Add(ctx, 1)
}

func (r *Runtime) GapWindowIncomplete(
	ctx context.Context,
	_ string,
	_ int,
) {
	r.gapWindowIncomplete.Add(ctx, 1)
}

func (r *Runtime) Divergence(
	ctx context.Context,
	finding dbgen.DriftFinding,
) {
	r.driftTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("entity_kind", finding.EntityKind),
		attribute.String("transition", "opened"),
	))
}

func (r *Runtime) PersistentDivergence(
	ctx context.Context,
	finding dbgen.DriftFinding,
) {
	r.driftTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("entity_kind", finding.EntityKind),
		attribute.String("transition", "persistent"),
	))
}

func (r *Runtime) PrunerDelete(
	ctx context.Context,
	kind string,
	count int64,
) {
	if count == 0 {
		return
	}
	r.prunerDeletes.Add(ctx, count, metric.WithAttributes(
		attribute.String("kind", kind),
	))
}

func (r *Runtime) WatermarkStep(
	ctx context.Context,
	progress streammaint.WatermarkProgress,
) {
	if progress.Advanced {
		r.watermarkAdvances.Add(ctx, 1)
	}
}

func (r *Runtime) DeriverPass(
	ctx context.Context,
	count int,
	duration time.Duration,
	err error,
) {
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	r.deriverPassDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("outcome", outcome),
	))
	r.deriverPasses.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("batch", batchLabel(count, r.options)),
	))
}

func batchLabel(count int, _ RuntimeOptions) string {
	switch {
	case count == 0:
		return "empty"
	case count < 100:
		return "partial"
	default:
		return "large"
	}
}
