// Package sweep implements M4's bounded-staleness reconciliation, resumable
// authoritative listings, disappearance verification, delivery-gap healing,
// and retention work on River's sweep queue.
package sweep

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/observer"
	"github.com/ewhauser/ghsync/internal/opsstate"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	KindRepositories = "repositories"
	KindStacks       = "stacks"
	KindPullRequests = "pull_requests"
	KindRepoRules    = "repo_rules"
	KindClosed       = "closed_tracked"

	jobKindKickoff = "sweep_kickoff"
	jobKindPage    = "sweep_list_page"
	jobKindGapHeal = "sweep_gap_heal"
	jobKindPrune   = "retention_prune"

	defaultGapLeaseTTL          = 5 * time.Minute
	defaultGapContinuationDelay = 30 * time.Second
	defaultGapDeepScanPeriod    = 24 * time.Hour
	githubDeliveryRetention     = 72 * time.Hour
)

var gapLeaseFallbackCounter atomic.Uint64

type Config struct {
	// InstallationID selects the GitHub installation reconciled by this service.
	InstallationID int64

	// OpenStackMaxStaleness bounds how long an open stack may go unchecked.
	OpenStackMaxStaleness time.Duration
	// OpenPRMaxStaleness bounds how long an open pull request may go unchecked.
	OpenPRMaxStaleness time.Duration
	// RepoRulesMaxStaleness bounds repository-rules cache staleness.
	RepoRulesMaxStaleness time.Duration
	// ClosedMaxStaleness bounds checks of tracked closed entities.
	ClosedMaxStaleness time.Duration
	// RepositoryListPeriod controls authoritative installation listings.
	RepositoryListPeriod time.Duration

	// PageSize bounds authoritative GitHub list pages.
	PageSize int

	// GapHealPeriod controls delivery-gap scan scheduling.
	GapHealPeriod time.Duration
	// GapWindow is the delivery-history interval inspected by each scan.
	GapWindow time.Duration
	// GapPageSize bounds one deliveries API page.
	GapPageSize int
	// GapMaxPages bounds pages inspected before scheduling a continuation.
	GapMaxPages int
	// GapLeaseTTL bounds failover after a gap-heal worker stops making progress.
	GapLeaseTTL time.Duration
	// GapContinuationDelay paces page-cap continuations.
	GapContinuationDelay time.Duration
	// GapDeepScanPeriod bounds detection of deliveries behind the cheap cursor.
	GapDeepScanPeriod time.Duration

	// RetentionPeriod controls payload-pruner scheduling.
	RetentionPeriod time.Duration
	// RetentionAge determines when bulky retained data becomes eligible.
	RetentionAge time.Duration
	// RetentionBatchSize bounds deletes per transaction.
	RetentionBatchSize int

	// Now supplies service time; it defaults to time.Now.
	Now func() time.Time
	// Observer receives sweep-overrun and gap-healing signals.
	Observer Observer
	// OnPrune receives per-kind deletion totals after a prune pass.
	OnPrune PruneHook
}

// PruneHook is M6's C-R retention-deletion accounting seam.
type PruneHook func(context.Context, string, int64)

func (c *Config) validate() error {
	for name, value := range map[string]time.Duration{
		"open stack staleness":   c.OpenStackMaxStaleness,
		"open PR staleness":      c.OpenPRMaxStaleness,
		"repo rules staleness":   c.RepoRulesMaxStaleness,
		"closed staleness":       c.ClosedMaxStaleness,
		"repository period":      c.RepositoryListPeriod,
		"gap-heal period":        c.GapHealPeriod,
		"gap-heal lease TTL":     c.GapLeaseTTL,
		"gap continuation delay": c.GapContinuationDelay,
		"gap deep-scan period":   c.GapDeepScanPeriod,
		"gap window":             c.GapWindow,
		"retention period":       c.RetentionPeriod,
		"retention age":          c.RetentionAge,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.PageSize <= 0 || c.PageSize > 100 ||
		c.GapPageSize <= 0 || c.GapPageSize > 100 ||
		c.GapMaxPages <= 0 || c.RetentionBatchSize <= 0 {
		return fmt.Errorf("sweep page sizes/max pages are invalid")
	}
	if c.GapWindow > githubDeliveryRetention ||
		c.GapDeepScanPeriod > githubDeliveryRetention-c.GapWindow {
		return fmt.Errorf(
			"gap window plus deep-scan period must not exceed %s",
			githubDeliveryRetention,
		)
	}
	for _, bound := range []time.Duration{
		c.OpenStackMaxStaleness,
		c.OpenPRMaxStaleness,
		c.RepoRulesMaxStaleness,
		c.ClosedMaxStaleness,
		c.RepositoryListPeriod,
	} {
		plan := scheduleForBound(bound)
		if plan.Cadence <= 0 || plan.CompletionHeadroom <= 0 ||
			plan.Cadence+plan.CompletionHeadroom >= bound {
			return fmt.Errorf(
				"staleness bound %s is too small for schedule headroom",
				bound,
			)
		}
	}
	return nil
}

type SchedulePlan struct {
	Bound              time.Duration
	Cadence            time.Duration
	CompletionHeadroom time.Duration
}

// scheduleForBound reserves 20% of C-R1 for queueing plus the authoritative
// fetch and leaves a further 5% safety margin. The scheduler therefore runs
// strictly before the public staleness bound.
func scheduleForBound(bound time.Duration) SchedulePlan {
	return SchedulePlan{
		Bound:              bound,
		Cadence:            bound * 3 / 4,
		CompletionHeadroom: bound / 5,
	}
}

func (p SchedulePlan) dueBefore(now time.Time) time.Time {
	return now.Add(p.Cadence + p.CompletionHeadroom - p.Bound)
}

// schedulePlanForSweepKind returns the cadence and completion headroom for a
// list-based sweep. Page fan-out and disappearance verification use the same
// plan as the stale-row kickoff so their refreshes share one C-R1 budget.
func (s *Service) schedulePlanForSweepKind(kind string) (SchedulePlan, error) {
	var bound time.Duration
	switch kind {
	case KindRepositories:
		bound = s.config.RepositoryListPeriod
	case KindStacks:
		bound = s.config.OpenStackMaxStaleness
	case KindPullRequests:
		bound = s.config.OpenPRMaxStaleness
	default:
		return SchedulePlan{}, fmt.Errorf("unsupported sweep kind %q", kind)
	}
	return scheduleForBound(bound), nil
}

// spreadRefreshSchedule distributes one sweep fan-out across the cadence
// window. Every job remains scheduled no later than its hard deadline minus
// the plan's completion headroom, so smoothing cannot consume C-R1's fetch
// allowance. The stable stale-query order makes the schedule deterministic.
func spreadRefreshSchedule(
	specs []queue.RefreshSpec,
	now time.Time,
	plan SchedulePlan,
) {
	if len(specs) < 2 || plan.Cadence <= 0 ||
		plan.CompletionHeadroom <= 0 {
		return
	}
	last := max(len(specs)-1, 1)
	for index := range specs {
		latestStart := specs[index].Deadline.Add(-plan.CompletionHeadroom)
		scheduledAt := now.Add(time.Duration(
			int64(plan.Cadence) * int64(index) / int64(last),
		))
		if scheduledAt.After(latestStart) {
			scheduledAt = latestStart
		}
		if scheduledAt.Before(now) {
			scheduledAt = now
		}
		specs[index].ScheduledAt = scheduledAt
	}
}

type Observer interface {
	SweepOverrun(
		context.Context,
		string,
		string,
		time.Duration,
	)
	GapRedelivery(context.Context, int64, string)
	GapWindowIncomplete(context.Context, string, int)
}

type LogObserver struct{}

func (LogObserver) SweepOverrun(
	_ context.Context,
	kind string,
	scope string,
	elapsed time.Duration,
) {
	slog.Warn(
		"C-R2 sweep overran its configured period",
		"sweep_kind", kind,
		"scope", scope,
		"elapsed", elapsed,
	)
}

func (LogObserver) GapRedelivery(
	_ context.Context,
	deliveryID int64,
	guid string,
) {
	slog.Warn(
		"C-R4 webhook delivery gap requested for redelivery",
		"delivery_id", deliveryID,
		"delivery_guid", guid,
	)
}

func (LogObserver) GapWindowIncomplete(
	_ context.Context,
	cursor string,
	pages int,
) {
	slog.Debug(
		"C-R4 webhook delivery gap window hit its page cap; paced continuation scheduled",
		"cursor", cursor,
		"pages", pages,
	)
}

type Observers []Observer

func (observers Observers) SweepOverrun(
	ctx context.Context,
	kind string,
	scope string,
	elapsed time.Duration,
) {
	observer.FanOut(observers, func(item Observer) {
		item.SweepOverrun(ctx, kind, scope, elapsed)
	})
}

func (observers Observers) GapRedelivery(
	ctx context.Context,
	deliveryID int64,
	guid string,
) {
	observer.FanOut(observers, func(item Observer) {
		item.GapRedelivery(ctx, deliveryID, guid)
	})
}

func (observers Observers) GapWindowIncomplete(
	ctx context.Context,
	cursor string,
	pages int,
) {
	observer.FanOut(observers, func(item Observer) {
		item.GapWindowIncomplete(ctx, cursor, pages)
	})
}

type Options struct {
	Pool       *pgxpool.Pool
	REST       *gh.RESTClient
	Deliveries *gh.DeliveriesClient
	Config     Config
}

type Service struct {
	pool       *pgxpool.Pool
	rest       *gh.RESTClient
	deliveries *gh.DeliveriesClient
	config     Config

	riverMu sync.RWMutex
	river   *river.Client[pgx.Tx]
}

func New(options Options) (*Service, error) { //nolint:gocritic // constructor normalizes a private options copy
	if options.Pool == nil {
		return nil, fmt.Errorf("sweep service requires Postgres")
	}
	if options.Config.Now == nil {
		options.Config.Now = time.Now
	}
	if options.Config.Observer == nil {
		options.Config.Observer = Observers{}
	}
	if options.Config.GapLeaseTTL <= 0 {
		options.Config.GapLeaseTTL = defaultGapLeaseTTL
	}
	if options.Config.GapContinuationDelay <= 0 {
		options.Config.GapContinuationDelay = defaultGapContinuationDelay
	}
	if options.Config.GapDeepScanPeriod <= 0 {
		options.Config.GapDeepScanPeriod = defaultGapDeepScanPeriod
	}
	if err := options.Config.validate(); err != nil {
		return nil, err
	}
	return &Service{
		pool:       options.Pool,
		rest:       options.REST,
		deliveries: options.Deliveries,
		config:     options.Config,
	}, nil
}

func (s *Service) SetRiverClient(client *river.Client[pgx.Tx]) {
	s.riverMu.Lock()
	s.river = client
	s.riverMu.Unlock()
}

func (s *Service) riverClient() *river.Client[pgx.Tx] {
	s.riverMu.RLock()
	defer s.riverMu.RUnlock()
	return s.river
}

type KickoffArgs struct {
	SweepKind    string `json:"sweep_kind"`
	Installation int64  `json:"installation_id"`
}

func (KickoffArgs) Kind() string { return jobKindKickoff }

type ListPageArgs struct {
	SweepKind    string `json:"sweep_kind"`
	Installation int64  `json:"installation_id"`
	ScopeKey     string `json:"scope_key"`
	Cursor       string `json:"cursor"`
}

func (ListPageArgs) Kind() string { return jobKindPage }

type GapHealArgs struct {
	Installation int64  `json:"installation_id"`
	Cursor       string `json:"cursor,omitempty"`
	LeaseToken   string `json:"lease_token,omitempty"`
}

func (GapHealArgs) Kind() string { return jobKindGapHeal }

type PruneArgs struct{}

func (PruneArgs) Kind() string { return jobKindPrune }

type kickoffWorker struct {
	river.WorkerDefaults[KickoffArgs]
	service *Service
}

func (w *kickoffWorker) Work(
	ctx context.Context,
	job *river.Job[KickoffArgs],
) error {
	return w.service.Kickoff(ctx, job.Args)
}

type listPageWorker struct {
	river.WorkerDefaults[ListPageArgs]
	service *Service
}

func (w *listPageWorker) Work(
	ctx context.Context,
	job *river.Job[ListPageArgs],
) error {
	return w.service.ReconcilePage(ctx, job.Args)
}

type gapHealWorker struct {
	river.WorkerDefaults[GapHealArgs]
	service *Service
}

func (w *gapHealWorker) Work(
	ctx context.Context,
	job *river.Job[GapHealArgs],
) error {
	err := w.service.HealDeliveryGaps(ctx, job.Args)
	if !errors.Is(err, errGapHealLeaseLost) {
		return err
	}
	slog.DebugContext(
		ctx,
		"C-R4 delivery-gap lease lost; stale job completed without retry",
		"installation_id", job.Args.Installation,
		"cursor", job.Args.Cursor,
	)
	return nil
}

type pruneWorker struct {
	river.WorkerDefaults[PruneArgs]
	service *Service
}

func (w *pruneWorker) Work(
	ctx context.Context,
	_ *river.Job[PruneArgs],
) error {
	_, _, err := w.service.Prune(ctx)
	return err
}

func (s *Service) RegisterReconciliationWorkers(
	workers *river.Workers,
) {
	river.AddWorker(workers, &kickoffWorker{service: s})
	river.AddWorker(workers, &listPageWorker{service: s})
	river.AddWorker(workers, &gapHealWorker{service: s})
}

func (s *Service) RegisterPrunerWorker(workers *river.Workers) {
	river.AddWorker(workers, &pruneWorker{service: s})
}

func (s *Service) ReconciliationPeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		s.kickoffPeriodic(KindStacks, s.config.OpenStackMaxStaleness),
		s.kickoffPeriodic(KindPullRequests, s.config.OpenPRMaxStaleness),
		s.kickoffPeriodic(KindRepoRules, s.config.RepoRulesMaxStaleness),
		s.kickoffPeriodic(KindClosed, s.config.ClosedMaxStaleness),
		s.kickoffPeriodic(KindRepositories, s.config.RepositoryListPeriod),
		river.NewPeriodicJob(
			river.PeriodicInterval(s.config.GapHealPeriod),
			func() (river.JobArgs, *river.InsertOpts) {
				return GapHealArgs{
						Installation: s.config.InstallationID,
						LeaseToken:   newGapLeaseToken(),
					},
					periodicInsertOpts(queue.QueueReconcile)
			},
			&river.PeriodicJobOpts{
				ID:         "ghsync_gap_heal",
				RunOnStart: true,
			},
		),
	}
}

func newGapLeaseToken() string {
	var token [32]byte
	if _, err := rand.Read(token[:]); err == nil {
		return hex.EncodeToString(token[:])
	}
	return fmt.Sprintf(
		"fallback-%d-%d-%d",
		os.Getpid(),
		time.Now().UnixNano(),
		gapLeaseFallbackCounter.Add(1),
	)
}

func ReconciliationPeriodicJobs(config *Config) []*river.PeriodicJob {
	return (&Service{config: *config}).ReconciliationPeriodicJobs()
}

func (s *Service) PrunerPeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(s.config.RetentionPeriod),
			func() (river.JobArgs, *river.InsertOpts) {
				return PruneArgs{}, periodicInsertOpts(queue.QueuePruner)
			},
			&river.PeriodicJobOpts{
				ID:         "ghsync_retention_prune",
				RunOnStart: true,
			},
		),
	}
}

func PrunerPeriodicJobs(config *Config) []*river.PeriodicJob {
	return (&Service{config: *config}).PrunerPeriodicJobs()
}

func (s *Service) kickoffPeriodic(
	kind string,
	bound time.Duration,
) *river.PeriodicJob {
	plan := scheduleForBound(bound)
	return river.NewPeriodicJob(
		river.PeriodicInterval(plan.Cadence),
		func() (river.JobArgs, *river.InsertOpts) {
			return KickoffArgs{
					SweepKind:    kind,
					Installation: s.config.InstallationID,
				},
				periodicInsertOpts(queue.QueueReconcile)
		},
		&river.PeriodicJobOpts{
			ID:         "ghsync_sweep_" + kind,
			RunOnStart: true,
		},
	)
}

func periodicInsertOpts(queueName string) *river.InsertOpts {
	return &river.InsertOpts{
		Queue:    queueName,
		Priority: 1,
	}
}

func listPageInsertOpts() *river.InsertOpts {
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

func (s *Service) enqueueStaleStacks(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	plan := scheduleForBound(s.config.OpenStackMaxStaleness)
	open, err := queries.ListStaleOpenStacks(
		ctx,
		dbgen.ListStaleOpenStacksParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    repoutil.Timestamptz(plan.dueBefore(s.config.Now())),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale open stacks: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(open))
	for _, row := range open {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key: fmt.Sprintf(
				"stack:%s:%d",
				row.RepoFullName,
				row.Number,
			),
			Deadline: row.LastCheckedAt.Time.Add(plan.Bound),
		})
	}
	spreadRefreshSchedule(specs, s.config.Now(), plan)
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueStalePullRequests(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	plan := scheduleForBound(s.config.OpenPRMaxStaleness)
	open, err := queries.ListStaleOpenPullRequests(
		ctx,
		dbgen.ListStaleOpenPullRequestsParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    repoutil.Timestamptz(plan.dueBefore(s.config.Now())),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale open pull requests: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(open))
	for _, row := range open {
		specs = append(specs, queue.RefreshSpec{
			Kind:     queue.KindRefreshPR,
			Key:      fmt.Sprintf("pr:%s:%d", row.RepoFullName, row.Number),
			Deadline: row.LastCheckedAt.Time.Add(plan.Bound),
		})
	}
	spreadRefreshSchedule(specs, s.config.Now(), plan)
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueStaleRepoRules(ctx context.Context) error {
	plan := scheduleForBound(s.config.RepoRulesMaxStaleness)
	names, err := dbgen.New(s.pool).ListStaleRepoRules(
		ctx,
		dbgen.ListStaleRepoRulesParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    repoutil.Timestamptz(plan.dueBefore(s.config.Now())),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale repository rules: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(names))
	for _, row := range names {
		specs = append(specs, queue.RefreshSpec{
			Kind:     queue.KindRefreshRepoRules,
			Key:      "repo_rules:" + row.FullName + ":rules",
			Deadline: row.LastCheckedAt.Time.Add(plan.Bound),
		})
	}
	return s.enqueueRefreshesWithHeartbeat(ctx, KindRepoRules, specs)
}

func (s *Service) enqueueClosedTracked(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	plan := scheduleForBound(s.config.ClosedMaxStaleness)
	stacks, err := queries.ListStaleClosedStacks(
		ctx,
		dbgen.ListStaleClosedStacksParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    repoutil.Timestamptz(plan.dueBefore(s.config.Now())),
		},
	)
	if err != nil {
		return fmt.Errorf("list closed tracked stacks: %w", err)
	}
	pulls, err := queries.ListStaleClosedPullRequests(
		ctx,
		dbgen.ListStaleClosedPullRequestsParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    repoutil.Timestamptz(plan.dueBefore(s.config.Now())),
		},
	)
	if err != nil {
		return fmt.Errorf("list closed tracked pull requests: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(stacks)+len(pulls))
	for _, row := range stacks {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key: fmt.Sprintf(
				"stack:%s:%d",
				row.RepoFullName,
				row.Number,
			),
			Deadline: row.LastCheckedAt.Time.Add(plan.Bound),
		})
	}
	for _, row := range pulls {
		specs = append(specs, queue.RefreshSpec{
			Kind:     queue.KindRefreshPR,
			Key:      fmt.Sprintf("pr:%s:%d", row.RepoFullName, row.Number),
			Deadline: row.LastCheckedAt.Time.Add(plan.Bound),
		})
	}
	return s.enqueueRefreshesWithHeartbeat(ctx, KindClosed, specs)
}

func (s *Service) enqueueRefreshes(
	ctx context.Context,
	specs []queue.RefreshSpec,
) error {
	if len(specs) == 0 {
		return nil
	}
	client := s.riverClient()
	if client == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin stale refresh enqueue: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		specs,
		queue.QueueSweep,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit stale refresh enqueue: %w", err)
	}
	return nil
}

// enqueueRefreshesWithHeartbeat inserts the stale refreshes and the sweep
// pass's durable C-O4 operation heartbeat in one transaction. The
// transaction is opened even when nothing is stale: a pass that inspected
// the cache and found no due work still completed, and the aggregate
// metrics contract expects its heartbeat (issue #21). The sample count is
// the number of refreshes the pass enqueued.
func (s *Service) enqueueRefreshesWithHeartbeat(
	ctx context.Context,
	sweepKind string,
	specs []queue.RefreshSpec,
) error {
	client := s.riverClient()
	if client == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin stale refresh enqueue: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if len(specs) > 0 {
		if err := queue.InsertRefreshesTx(
			ctx,
			tx,
			client,
			specs,
			queue.QueueSweep,
		); err != nil {
			return err
		}
	}
	if err := opsstate.RecordSuccessN(
		ctx,
		tx,
		s.config.InstallationID,
		"sweep",
		sweepKind,
		int64(len(specs)),
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit stale refresh enqueue: %w", err)
	}
	return nil
}
