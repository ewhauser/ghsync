package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	// KindRefreshPR refreshes one pull request.
	KindRefreshPR = "refresh_pr"
	// KindRefreshRepository refreshes one repository.
	KindRefreshRepository = "refresh_repository"
	// KindRefreshRepoRules refreshes repository rules.
	KindRefreshRepoRules = "refresh_repo_rules"
	// KindRefreshStack refreshes one stack.
	KindRefreshStack = "refresh_stack"
	// KindRefreshChecks refreshes check runs for one head SHA.
	KindRefreshChecks = "refresh_checks"
	// KindRefreshBranch refreshes branch-dependent entities.
	KindRefreshBranch = "refresh_branch"
	// KindResolveStackMembership resolves a pull request's stack ownership.
	KindResolveStackMembership = "resolve_stack_membership"
	// KindBackfillRepoPage continues one repository backfill.
	KindBackfillRepoPage = "backfill_repo_page"
	// KindBackfillInstallation continues installation repository discovery.
	KindBackfillInstallation = "backfill_installation_page"
)

// RefreshArgs is the complete durable job pointer. It intentionally contains
// no webhook or entity payload (SYNC_ENGINE §8 and C-I4).
type RefreshArgs struct {
	PointerKind string `json:"kind"`
	Key         string `json:"key"`
}

// RefreshPRArgs points at one pull request.
type RefreshPRArgs struct{ RefreshArgs }

// RefreshRepositoryArgs points at one repository.
type RefreshRepositoryArgs struct{ RefreshArgs }

// RefreshRepoRulesArgs points at one repository ruleset.
type RefreshRepoRulesArgs struct{ RefreshArgs }

// RefreshStackArgs points at one stack.
type RefreshStackArgs struct{ RefreshArgs }

// RefreshChecksArgs points at one repository head SHA.
type RefreshChecksArgs struct{ RefreshArgs }

// RefreshBranchArgs points at one repository branch.
type RefreshBranchArgs struct{ RefreshArgs }

// ResolveStackMembershipArgs points at a pull request requiring resolution.
type ResolveStackMembershipArgs struct{ RefreshArgs }

// BackfillRepoPageArgs identifies one resumable repository backfill page.
type BackfillRepoPageArgs struct {
	InstallationID int64  `json:"installation_id"`
	RepoFullName   string `json:"repo"`
	Phase          string `json:"phase"`
	Page           int    `json:"page"`
}

// BackfillInstallationPageArgs identifies one repository-discovery page.
type BackfillInstallationPageArgs struct {
	InstallationID int64  `json:"installation_id"`
	Phase          string `json:"phase"`
	Page           int    `json:"page"`
}

// NewRefreshPRArgs constructs a pull-request refresh pointer.
func NewRefreshPRArgs(key string) RefreshPRArgs {
	return RefreshPRArgs{RefreshArgs{PointerKind: KindRefreshPR, Key: key}}
}

// NewRefreshRepositoryArgs constructs a repository refresh pointer.
func NewRefreshRepositoryArgs(key string) RefreshRepositoryArgs {
	return RefreshRepositoryArgs{
		RefreshArgs{PointerKind: KindRefreshRepository, Key: key},
	}
}

// NewRefreshRepoRulesArgs constructs a repository-rules refresh pointer.
func NewRefreshRepoRulesArgs(key string) RefreshRepoRulesArgs {
	return RefreshRepoRulesArgs{
		RefreshArgs{PointerKind: KindRefreshRepoRules, Key: key},
	}
}

// NewRefreshStackArgs constructs a stack refresh pointer.
func NewRefreshStackArgs(key string) RefreshStackArgs {
	return RefreshStackArgs{RefreshArgs{PointerKind: KindRefreshStack, Key: key}}
}

// NewRefreshChecksArgs constructs a checks refresh pointer.
func NewRefreshChecksArgs(key string) RefreshChecksArgs {
	return RefreshChecksArgs{RefreshArgs{PointerKind: KindRefreshChecks, Key: key}}
}

// NewRefreshBranchArgs constructs a branch refresh pointer.
func NewRefreshBranchArgs(key string) RefreshBranchArgs {
	return RefreshBranchArgs{RefreshArgs{PointerKind: KindRefreshBranch, Key: key}}
}

// NewResolveStackMembershipArgs constructs a membership-resolution pointer.
func NewResolveStackMembershipArgs(key string) ResolveStackMembershipArgs {
	return ResolveStackMembershipArgs{
		RefreshArgs{PointerKind: KindResolveStackMembership, Key: key},
	}
}

// Kind returns the River job kind.
func (RefreshPRArgs) Kind() string { return KindRefreshPR }

// Kind returns the River job kind.
func (RefreshRepositoryArgs) Kind() string { return KindRefreshRepository }

// Kind returns the River job kind.
func (RefreshRepoRulesArgs) Kind() string { return KindRefreshRepoRules }

// Kind returns the River job kind.
func (RefreshStackArgs) Kind() string { return KindRefreshStack }

// Kind returns the River job kind.
func (RefreshChecksArgs) Kind() string { return KindRefreshChecks }

// Kind returns the River job kind.
func (RefreshBranchArgs) Kind() string { return KindRefreshBranch }

// Kind returns the River job kind.
func (ResolveStackMembershipArgs) Kind() string {
	return KindResolveStackMembership
}

// Kind returns the River job kind.
func (BackfillRepoPageArgs) Kind() string { return KindBackfillRepoPage }

// Kind returns the River job kind.
func (BackfillInstallationPageArgs) Kind() string {
	return KindBackfillInstallation
}

// NewBackfillRepoPageArgs constructs a resumable repository backfill pointer.
func NewBackfillRepoPageArgs(
	installationID int64,
	repoFullName string,
	phase string,
	page int,
) BackfillRepoPageArgs {
	return BackfillRepoPageArgs{
		InstallationID: installationID,
		RepoFullName:   repoFullName,
		Phase:          phase,
		Page:           page,
	}
}

// NewBackfillInstallationPageArgs constructs a discovery backfill pointer.
func NewBackfillInstallationPageArgs(
	installationID int64,
	phase string,
	page int,
) BackfillInstallationPageArgs {
	return BackfillInstallationPageArgs{
		InstallationID: installationID,
		Phase:          phase,
		Page:           page,
	}
}

// NewRefreshInsertOpts is the one supported uniqueness definition for refresh
// work. River requires running in this mask; a durable generation handles
// signals that coalesce while a job runs.
func NewRefreshInsertOpts(scheduledAt time.Time) *river.InsertOpts {
	return NewRefreshInsertOptsForQueue(QueueEvent, scheduledAt)
}

// NewRefreshInsertOptsForQueue returns refresh uniqueness for queueName.
func NewRefreshInsertOptsForQueue(
	queueName string,
	scheduledAt time.Time,
) *river.InsertOpts {
	return &river.InsertOpts{
		Queue:       queueName,
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

// NewBackfillInsertOpts returns interactive-queue uniqueness for backfills.
func NewBackfillInsertOpts() *river.InsertOpts {
	return NewBackfillInsertOptsForQueue(QueueInteractive)
}

// NewBackfillInsertOptsForQueue returns backfill uniqueness for queueName.
func NewBackfillInsertOptsForQueue(queueName string) *river.InsertOpts {
	return &river.InsertOpts{
		Queue:    queueName,
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

// RefreshRequest gives a handler its durable pointer and owning queue.
type RefreshRequest struct {
	Args  RefreshArgs
	Queue string
}

// RefreshSpec describes one desired refresh generation and optional timing
// metadata.
type RefreshSpec struct {
	Kind            string
	Key             string
	ScheduledAt     time.Time
	Deadline        time.Time
	EventReceivedAt time.Time
}

// RefreshHandler is implemented by internal/fetch. Keeping this interface in
// queue avoids coupling durable job definitions to GitHub/cache internals.
type RefreshHandler interface {
	RefreshPR(context.Context, RefreshRequest) error
	RefreshRepository(context.Context, RefreshRequest) error
	RefreshRepoRules(context.Context, RefreshRequest) error
	RefreshStack(context.Context, RefreshRequest) error
	RefreshChecks(context.Context, RefreshRequest) error
	RefreshBranch(context.Context, RefreshRequest) error
	ResolveStackMembership(context.Context, RefreshRequest) error
	BackfillRepoPage(context.Context, BackfillRepoPageArgs) error
	BackfillInstallationPage(
		context.Context,
		BackfillInstallationPageArgs,
	) error
}

// Each durable M2 job kind delegates to the M3 fetch handler without changing
// its pointer-only argument contract.
type refreshPRWorker struct {
	river.WorkerDefaults[RefreshPRArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshRepositoryWorker struct {
	river.WorkerDefaults[RefreshRepositoryArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshRepoRulesWorker struct {
	river.WorkerDefaults[RefreshRepoRulesArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshStackWorker struct {
	river.WorkerDefaults[RefreshStackArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshChecksWorker struct {
	river.WorkerDefaults[RefreshChecksArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshBranchWorker struct {
	river.WorkerDefaults[RefreshBranchArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type resolveStackMembershipWorker struct {
	river.WorkerDefaults[ResolveStackMembershipArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type backfillRepoPageWorker struct {
	river.WorkerDefaults[BackfillRepoPageArgs]
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type backfillInstallationPageWorker struct {
	river.WorkerDefaults[BackfillInstallationPageArgs]
	handler RefreshHandler
	monitor refreshDeadlineMonitor
}

type refreshWork func(context.Context, RefreshArgs) error

type refreshDeadlineMonitor struct {
	deadlineObserver DeadlineObserver
	refreshObserver  RefreshObserver
	now              func() time.Time
}

func (w *refreshPRWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshPRArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshPR(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *refreshRepositoryWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshRepositoryArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshRepository(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *refreshRepoRulesWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshRepoRulesArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshRepoRules(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *refreshStackWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshStackArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshStack(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *refreshChecksWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshChecksArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshChecks(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *refreshBranchWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshBranchArgs],
) error {
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.RefreshBranch(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *resolveStackMembershipWorker) Work(
	ctx context.Context,
	job *river.Job[ResolveStackMembershipArgs],
) error {
	// M3 must read the PR's cached old membership, fetch current membership,
	// then refresh both the old and new stacks. The durable args deliberately
	// carry only the PR key; payload stack state is never treated as truth.
	work := w.work
	if work == nil && w.handler != nil {
		work = func(ctx context.Context, args RefreshArgs) error {
			return w.handler.ResolveStackMembership(
				ctx,
				RefreshRequest{Args: args, Queue: job.Queue},
			)
		}
	}
	return runRefresh(
		ctx, w.pool, work, job, job.Args.RefreshArgs, w.monitor,
	)
}

func (w *backfillRepoPageWorker) Work(
	ctx context.Context,
	job *river.Job[BackfillRepoPageArgs],
) error {
	if w.handler == nil {
		return fmt.Errorf("backfill worker is not configured")
	}
	startedAt := w.monitor.now()
	err := w.handler.BackfillRepoPage(ctx, job.Args)
	w.monitor.observeRefresh(
		ctx,
		KindBackfillRepoPage,
		job.Queue,
		time.Time{},
		time.Time{},
		startedAt,
		w.monitor.now(),
		err,
	)
	return err
}

func (w *backfillInstallationPageWorker) Work(
	ctx context.Context,
	job *river.Job[BackfillInstallationPageArgs],
) error {
	if w.handler == nil {
		return fmt.Errorf("installation backfill worker is not configured")
	}
	startedAt := w.monitor.now()
	err := w.handler.BackfillInstallationPage(ctx, job.Args)
	w.monitor.observeRefresh(
		ctx,
		KindBackfillInstallation,
		job.Queue,
		time.Time{},
		time.Time{},
		startedAt,
		w.monitor.now(),
		err,
	)
	return err
}

func registerRefreshWorkers(
	workers *river.Workers,
	pool *pgxpool.Pool,
	handler RefreshHandler,
	deadlineObserver DeadlineObserver,
	refreshObserver RefreshObserver,
	now func() time.Time,
) {
	if now == nil {
		now = time.Now
	}
	monitor := refreshDeadlineMonitor{
		deadlineObserver: deadlineObserver,
		refreshObserver:  refreshObserver,
		now:              now,
	}
	river.AddWorker(workers, &refreshPRWorker{
		pool: pool, handler: handler, monitor: monitor,
	})
	river.AddWorker(
		workers,
		&refreshRepositoryWorker{
			pool: pool, handler: handler, monitor: monitor,
		},
	)
	river.AddWorker(
		workers,
		&refreshRepoRulesWorker{
			pool: pool, handler: handler, monitor: monitor,
		},
	)
	river.AddWorker(workers, &refreshStackWorker{
		pool: pool, handler: handler, monitor: monitor,
	})
	river.AddWorker(workers, &refreshChecksWorker{
		pool: pool, handler: handler, monitor: monitor,
	})
	river.AddWorker(workers, &refreshBranchWorker{
		pool: pool, handler: handler, monitor: monitor,
	})
	river.AddWorker(
		workers,
		&resolveStackMembershipWorker{
			pool: pool, handler: handler, monitor: monitor,
		},
	)
	river.AddWorker(workers, &backfillRepoPageWorker{
		handler: handler, monitor: monitor,
	})
	river.AddWorker(
		workers,
		&backfillInstallationPageWorker{
			handler: handler, monitor: monitor,
		},
	)
}

func runRefresh[T river.JobArgs](
	ctx context.Context,
	pool *pgxpool.Pool,
	work refreshWork,
	job *river.Job[T],
	args RefreshArgs,
	monitor refreshDeadlineMonitor,
) error {
	started, err := dbgen.New(pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       args.PointerKind,
			RefreshKey: args.Key,
		},
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("snapshot refresh generation: %w", err)
	}

	now := monitor.now
	if now == nil {
		now = time.Now
	}
	startedAt := now()
	if work == nil {
		return fmt.Errorf(
			"refresh worker %s is not configured",
			args.PointerKind,
		)
	}
	workCtx := pipeline.WithEvent(ctx, started.EventReceivedAt.Time)
	if workErr := work(workCtx, args); workErr != nil {
		monitor.observeRefresh(
			ctx,
			args.PointerKind,
			job.Queue,
			started.EventReceivedAt.Time,
			pipeline.CacheCommittedAt(workCtx),
			startedAt,
			now(),
			workErr,
		)
		return workErr
	}
	completedAt := now()
	if completeErr := completeRefresh(
		ctx, pool, job, args, started.Generation,
	); completeErr != nil {
		monitor.observeRefresh(
			ctx,
			args.PointerKind,
			job.Queue,
			started.EventReceivedAt.Time,
			pipeline.CacheCommittedAt(workCtx),
			startedAt,
			completedAt,
			completeErr,
		)
		return completeErr
	}
	monitor.observeRefresh(
		ctx,
		args.PointerKind,
		job.Queue,
		started.EventReceivedAt.Time,
		pipeline.CacheCommittedAt(workCtx),
		startedAt,
		completedAt,
		nil,
	)
	if monitor.deadlineObserver != nil && started.DeadlineAt.Valid &&
		completedAt.After(started.DeadlineAt.Time) {
		monitor.deadlineObserver.RefreshDeadlineMissed(
			ctx,
			args.PointerKind,
			args.Key,
			started.DeadlineAt.Time,
			completedAt,
		)
	}
	return nil
}

func (m refreshDeadlineMonitor) observeRefresh(
	ctx context.Context,
	kind string,
	queueName string,
	eventReceivedAt time.Time,
	cacheCommittedAt time.Time,
	startedAt time.Time,
	completedAt time.Time,
	err error,
) {
	if m.refreshObserver == nil {
		return
	}
	m.refreshObserver.RefreshFinished(ctx, &RefreshObservation{
		Kind:             kind,
		Queue:            queueName,
		EventReceivedAt:  eventReceivedAt,
		CacheCommittedAt: cacheCommittedAt,
		StartedAt:        startedAt,
		CompletedAt:      completedAt,
		Err:              err,
	})
}

// RefreshGeneration pairs a deduplicated refresh spec with its durable
// generation after insertion.
type RefreshGeneration struct {
	Spec       RefreshSpec
	Generation int64
}

// InsertRefreshesTx atomically advances durable generations and inserts
// follow-up pointers. Stack diffs, branch fan-out, and backfill all use this
// path so running-state coalescing keeps the same meaning everywhere.
func InsertRefreshesTx(
	ctx context.Context,
	tx pgx.Tx,
	client *river.Client[pgx.Tx],
	specs []RefreshSpec,
	queueName string,
) error {
	_, err := InsertRefreshesTxReturning(
		ctx, tx, client, specs, queueName,
	)
	return err
}

// InsertRefreshesTxReturning performs InsertRefreshesTx and returns each
// deduplicated spec's new durable generation.
func InsertRefreshesTxReturning(
	ctx context.Context,
	tx pgx.Tx,
	client *river.Client[pgx.Tx],
	specs []RefreshSpec,
	queueName string,
) ([]RefreshGeneration, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	if client == nil {
		return nil, fmt.Errorf("river client is required")
	}
	type generationPointer struct {
		Kind            string `json:"kind"`
		RefreshKey      string `json:"refresh_key"`
		DeadlineAt      string `json:"deadline_at,omitempty"`
		EventReceivedAt string `json:"event_received_at,omitempty"`
	}
	type refreshKey struct {
		Kind string
		Key  string
	}
	seen := make(map[refreshKey]int, len(specs))
	deduped := make([]RefreshSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.EventReceivedAt.IsZero() {
			spec.EventReceivedAt = pipeline.EventReceivedAt(ctx)
		}
		if spec.Key == "" {
			return nil, fmt.Errorf("refresh key is required")
		}
		key := refreshKey{Kind: spec.Kind, Key: spec.Key}
		if index, duplicate := seen[key]; duplicate {
			// A zero schedule means available immediately, so it is earlier
			// than every explicit schedule. This matters when an event and a
			// staggered sweep pointer meet in the same insertion batch.
			if !deduped[index].ScheduledAt.IsZero() &&
				(spec.ScheduledAt.IsZero() ||
					spec.ScheduledAt.Before(deduped[index].ScheduledAt)) {
				deduped[index].ScheduledAt = spec.ScheduledAt
			}
			if !spec.Deadline.IsZero() &&
				(deduped[index].Deadline.IsZero() ||
					spec.Deadline.Before(deduped[index].Deadline)) {
				deduped[index].Deadline = spec.Deadline
			}
			if !spec.EventReceivedAt.IsZero() &&
				(deduped[index].EventReceivedAt.IsZero() ||
					spec.EventReceivedAt.Before(
						deduped[index].EventReceivedAt,
					)) {
				deduped[index].EventReceivedAt = spec.EventReceivedAt
			}
			continue
		}
		seen[key] = len(deduped)
		deduped = append(deduped, spec)
	}
	// Match the SQL upsert's conflict-row lock order across every fan-out path.
	sort.Slice(deduped, func(i, j int) bool {
		if deduped[i].Kind != deduped[j].Kind {
			return deduped[i].Kind < deduped[j].Kind
		}
		return deduped[i].Key < deduped[j].Key
	})
	pointers := make([]generationPointer, 0, len(deduped))
	for _, spec := range deduped {
		pointer := generationPointer{
			Kind:       spec.Kind,
			RefreshKey: spec.Key,
		}
		if !spec.Deadline.IsZero() {
			pointer.DeadlineAt = spec.Deadline.
				UTC().Format(time.RFC3339Nano)
		}
		if !spec.EventReceivedAt.IsZero() {
			pointer.EventReceivedAt = spec.EventReceivedAt.
				UTC().Format(time.RFC3339Nano)
		}
		pointers = append(pointers, pointer)
	}
	encoded, err := json.Marshal(pointers)
	if err != nil {
		return nil, fmt.Errorf("encode refresh generations: %w", err)
	}
	generations, err := dbgen.New(tx).BumpRefreshIntentGenerations(ctx, encoded)
	if err != nil {
		return nil, fmt.Errorf("bump refresh generations: %w", err)
	}
	if len(generations) != len(deduped) {
		return nil, fmt.Errorf(
			"bumped %d refresh generations for %d specs",
			len(generations),
			len(deduped),
		)
	}
	params := make([]river.InsertManyParams, 0, len(deduped))
	for index := range deduped {
		args, err := argsForSpec(&deduped[index])
		if err != nil {
			return nil, err
		}
		params = append(params, river.InsertManyParams{
			Args: args,
			InsertOpts: NewRefreshInsertOptsForQueue(
				queueName,
				deduped[index].ScheduledAt,
			),
		})
	}
	inserted, err := client.InsertManyTx(ctx, tx, params)
	if err != nil {
		return nil, fmt.Errorf("insert refresh jobs: %w", err)
	}
	if len(inserted) != len(deduped) {
		return nil, fmt.Errorf(
			"inserted %d refresh jobs for %d specs",
			len(inserted),
			len(deduped),
		)
	}
	// River retains the original scheduled_at when a unique insert coalesces.
	// If a later signal is more urgent (notably an event racing a staggered
	// sweep), make the existing pointer available now. Its durable generation
	// still guarantees one follow-up when the pointer is already running.
	scheduledByKey := make(map[refreshKey]time.Time, len(deduped))
	for _, spec := range deduped {
		scheduledByKey[refreshKey{Kind: spec.Kind, Key: spec.Key}] =
			spec.ScheduledAt
	}
	for _, result := range inserted {
		if result == nil || result.Job == nil ||
			!result.UniqueSkippedAsDuplicate {
			continue
		}
		var args RefreshArgs
		if err := json.Unmarshal(result.Job.EncodedArgs, &args); err != nil {
			return nil, fmt.Errorf(
				"decode coalesced refresh job %d: %w",
				result.Job.ID,
				err,
			)
		}
		desired, ok := scheduledByKey[refreshKey{
			Kind: args.PointerKind,
			Key:  args.Key,
		}]
		if !ok {
			return nil, fmt.Errorf(
				"coalesced refresh job %d was not in insertion batch",
				result.Job.ID,
			)
		}
		if !desired.IsZero() && !desired.Before(result.Job.ScheduledAt) {
			continue
		}
		if _, err := client.JobRetryTx(ctx, tx, result.Job.ID); err != nil {
			return nil, fmt.Errorf(
				"expedite coalesced refresh job %d: %w",
				result.Job.ID,
				err,
			)
		}
	}
	bySpec := make(map[refreshKey]int64, len(generations))
	for _, generation := range generations {
		bySpec[refreshKey{
			Kind: generation.Kind,
			Key:  generation.RefreshKey,
		}] = generation.Generation
	}
	result := make([]RefreshGeneration, 0, len(deduped))
	for _, spec := range deduped {
		result = append(result, RefreshGeneration{
			Spec: spec,
			Generation: bySpec[refreshKey{
				Kind: spec.Kind,
				Key:  spec.Key,
			}],
		})
	}
	return result, nil
}

func argsForSpec(spec *RefreshSpec) (rivertype.JobArgs, error) {
	switch spec.Kind {
	case KindRefreshPR:
		return NewRefreshPRArgs(spec.Key), nil
	case KindRefreshRepository:
		return NewRefreshRepositoryArgs(spec.Key), nil
	case KindRefreshRepoRules:
		return NewRefreshRepoRulesArgs(spec.Key), nil
	case KindRefreshStack:
		return NewRefreshStackArgs(spec.Key), nil
	case KindRefreshChecks:
		return NewRefreshChecksArgs(spec.Key), nil
	case KindRefreshBranch:
		return NewRefreshBranchArgs(spec.Key), nil
	case KindResolveStackMembership:
		return NewResolveStackMembershipArgs(spec.Key), nil
	default:
		return nil, fmt.Errorf("unsupported refresh kind %q", spec.Kind)
	}
}

func completeRefresh[T river.JobArgs](
	ctx context.Context,
	pool *pgxpool.Pool,
	job *river.Job[T],
	args RefreshArgs,
	startedGeneration int64,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh completion: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	generation, err := dbgen.New(tx).GetRefreshIntentGenerationForUpdate(
		ctx,
		dbgen.GetRefreshIntentGenerationForUpdateParams{
			Kind:       args.PointerKind,
			RefreshKey: args.Key,
		},
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock refresh generation: %w", err)
	}

	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return fmt.Errorf("complete refresh job: %w", err)
	}
	if startedGeneration > 0 {
		if err := dbgen.New(tx).CompleteRefreshIntentGeneration(
			ctx,
			dbgen.CompleteRefreshIntentGenerationParams{
				CompletedGeneration: startedGeneration,
				Kind:                args.PointerKind,
				RefreshKey:          args.Key,
			},
		); err != nil {
			return fmt.Errorf("complete refresh generation: %w", err)
		}
	}

	if generation > startedGeneration {
		client := river.ClientFromContext[pgx.Tx](ctx)
		if client == nil {
			return fmt.Errorf("river client missing from refresh worker context")
		}
		if _, err := client.InsertTx(
			ctx,
			tx,
			job.Args,
			NewRefreshInsertOptsForQueue(job.Queue, time.Time{}),
		); err != nil {
			return fmt.Errorf("insert dirty refresh follow-up: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit refresh completion: %w", err)
	}
	return nil
}
