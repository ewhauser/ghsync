package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	"github.com/acme/frontier/internal/store/dbgen"
)

const (
	KindRefreshPR              = "refresh_pr"
	KindRefreshRepository      = "refresh_repository"
	KindRefreshRepoRules       = "refresh_repo_rules"
	KindRefreshStack           = "refresh_stack"
	KindRefreshChecks          = "refresh_checks"
	KindRefreshBranch          = "refresh_branch"
	KindResolveStackMembership = "resolve_stack_membership"
	KindBackfillRepoPage       = "backfill_repo_page"
	KindBackfillInstallation   = "backfill_installation_page"
)

// RefreshArgs is the complete durable job pointer. It intentionally contains
// no webhook or entity payload (SYNC_ENGINE §8 and C-I4).
type RefreshArgs struct {
	PointerKind string `json:"kind"`
	Key         string `json:"key"`
}

type RefreshPRArgs struct{ RefreshArgs }
type RefreshRepositoryArgs struct{ RefreshArgs }
type RefreshRepoRulesArgs struct{ RefreshArgs }
type RefreshStackArgs struct{ RefreshArgs }
type RefreshChecksArgs struct{ RefreshArgs }
type RefreshBranchArgs struct{ RefreshArgs }
type ResolveStackMembershipArgs struct{ RefreshArgs }

type BackfillRepoPageArgs struct {
	InstallationID int64  `json:"installation_id"`
	RepoFullName   string `json:"repo"`
	Phase          string `json:"phase"`
	Page           int    `json:"page"`
}

type BackfillInstallationPageArgs struct {
	InstallationID int64  `json:"installation_id"`
	Phase          string `json:"phase"`
	Page           int    `json:"page"`
}

func NewRefreshPRArgs(key string) RefreshPRArgs {
	return RefreshPRArgs{RefreshArgs{PointerKind: KindRefreshPR, Key: key}}
}

func NewRefreshRepositoryArgs(key string) RefreshRepositoryArgs {
	return RefreshRepositoryArgs{
		RefreshArgs{PointerKind: KindRefreshRepository, Key: key},
	}
}

func NewRefreshRepoRulesArgs(key string) RefreshRepoRulesArgs {
	return RefreshRepoRulesArgs{
		RefreshArgs{PointerKind: KindRefreshRepoRules, Key: key},
	}
}

func NewRefreshStackArgs(key string) RefreshStackArgs {
	return RefreshStackArgs{RefreshArgs{PointerKind: KindRefreshStack, Key: key}}
}

func NewRefreshChecksArgs(key string) RefreshChecksArgs {
	return RefreshChecksArgs{RefreshArgs{PointerKind: KindRefreshChecks, Key: key}}
}

func NewRefreshBranchArgs(key string) RefreshBranchArgs {
	return RefreshBranchArgs{RefreshArgs{PointerKind: KindRefreshBranch, Key: key}}
}

func NewResolveStackMembershipArgs(key string) ResolveStackMembershipArgs {
	return ResolveStackMembershipArgs{
		RefreshArgs{PointerKind: KindResolveStackMembership, Key: key},
	}
}

func (RefreshPRArgs) Kind() string              { return KindRefreshPR }
func (RefreshRepositoryArgs) Kind() string      { return KindRefreshRepository }
func (RefreshRepoRulesArgs) Kind() string       { return KindRefreshRepoRules }
func (RefreshStackArgs) Kind() string           { return KindRefreshStack }
func (RefreshChecksArgs) Kind() string          { return KindRefreshChecks }
func (RefreshBranchArgs) Kind() string          { return KindRefreshBranch }
func (ResolveStackMembershipArgs) Kind() string { return KindResolveStackMembership }
func (BackfillRepoPageArgs) Kind() string       { return KindBackfillRepoPage }
func (BackfillInstallationPageArgs) Kind() string {
	return KindBackfillInstallation
}

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

func NewBackfillInsertOpts() *river.InsertOpts {
	return NewBackfillInsertOptsForQueue(QueueInteractive)
}

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

type RefreshRequest struct {
	Args  RefreshArgs
	Queue string
}

type RefreshSpec struct {
	Kind string
	Key  string
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
}

type refreshRepositoryWorker struct {
	river.WorkerDefaults[RefreshRepositoryArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type refreshRepoRulesWorker struct {
	river.WorkerDefaults[RefreshRepoRulesArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type refreshStackWorker struct {
	river.WorkerDefaults[RefreshStackArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type refreshChecksWorker struct {
	river.WorkerDefaults[RefreshChecksArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type refreshBranchWorker struct {
	river.WorkerDefaults[RefreshBranchArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type resolveStackMembershipWorker struct {
	river.WorkerDefaults[ResolveStackMembershipArgs]
	pool    *pgxpool.Pool
	work    refreshWork
	handler RefreshHandler
}

type backfillRepoPageWorker struct {
	river.WorkerDefaults[BackfillRepoPageArgs]
	handler RefreshHandler
}

type backfillInstallationPageWorker struct {
	river.WorkerDefaults[BackfillInstallationPageArgs]
	handler RefreshHandler
}

type refreshWork func(context.Context, RefreshArgs) error

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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
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
	return runRefresh(ctx, w.pool, work, job, job.Args.RefreshArgs)
}

func (w *backfillRepoPageWorker) Work(
	ctx context.Context,
	job *river.Job[BackfillRepoPageArgs],
) error {
	if w.handler == nil {
		return fmt.Errorf("backfill worker is not configured")
	}
	return w.handler.BackfillRepoPage(ctx, job.Args)
}

func (w *backfillInstallationPageWorker) Work(
	ctx context.Context,
	job *river.Job[BackfillInstallationPageArgs],
) error {
	if w.handler == nil {
		return fmt.Errorf("installation backfill worker is not configured")
	}
	return w.handler.BackfillInstallationPage(ctx, job.Args)
}

func registerRefreshWorkers(
	workers *river.Workers,
	pool *pgxpool.Pool,
	handler RefreshHandler,
) {
	river.AddWorker(workers, &refreshPRWorker{pool: pool, handler: handler})
	river.AddWorker(
		workers,
		&refreshRepositoryWorker{pool: pool, handler: handler},
	)
	river.AddWorker(
		workers,
		&refreshRepoRulesWorker{pool: pool, handler: handler},
	)
	river.AddWorker(workers, &refreshStackWorker{pool: pool, handler: handler})
	river.AddWorker(workers, &refreshChecksWorker{pool: pool, handler: handler})
	river.AddWorker(workers, &refreshBranchWorker{pool: pool, handler: handler})
	river.AddWorker(
		workers,
		&resolveStackMembershipWorker{pool: pool, handler: handler},
	)
	river.AddWorker(workers, &backfillRepoPageWorker{handler: handler})
	river.AddWorker(
		workers,
		&backfillInstallationPageWorker{handler: handler},
	)
}

func runRefresh[T river.JobArgs](
	ctx context.Context,
	pool *pgxpool.Pool,
	work refreshWork,
	job *river.Job[T],
	args RefreshArgs,
) error {
	startedGeneration, err := dbgen.New(pool).GetRefreshIntentGeneration(
		ctx,
		dbgen.GetRefreshIntentGenerationParams{
			Kind:       args.PointerKind,
			RefreshKey: args.Key,
		},
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("snapshot refresh generation: %w", err)
	}

	if work == nil {
		return fmt.Errorf(
			"refresh worker %s is not configured",
			args.PointerKind,
		)
	} else if err := work(ctx, args); err != nil {
		return err
	}
	return completeRefresh(ctx, pool, job, args, startedGeneration)
}

// InsertRefreshesTx atomically advances M2's durable generations and inserts
// follow-up pointers. Stack diffs, branch fan-out, and backfill all use this
// path so running-state coalescing keeps the same meaning everywhere.
type RefreshGeneration struct {
	Spec       RefreshSpec
	Generation int64
}

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
		return nil, fmt.Errorf("River client is required")
	}
	type generationPointer struct {
		Kind       string `json:"kind"`
		RefreshKey string `json:"refresh_key"`
	}
	seen := make(map[RefreshSpec]struct{}, len(specs))
	deduped := make([]RefreshSpec, 0, len(specs))
	pointers := make([]generationPointer, 0, len(specs))
	for _, spec := range specs {
		if spec.Key == "" {
			return nil, fmt.Errorf("refresh key is required")
		}
		if _, duplicate := seen[spec]; duplicate {
			continue
		}
		seen[spec] = struct{}{}
		deduped = append(deduped, spec)
		pointers = append(pointers, generationPointer{
			Kind:       spec.Kind,
			RefreshKey: spec.Key,
		})
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
	for _, spec := range deduped {
		args, err := argsForSpec(spec)
		if err != nil {
			return nil, err
		}
		params = append(params, river.InsertManyParams{
			Args:       args,
			InsertOpts: NewRefreshInsertOptsForQueue(queueName, time.Time{}),
		})
	}
	if _, err := client.InsertManyTx(ctx, tx, params); err != nil {
		return nil, fmt.Errorf("insert refresh jobs: %w", err)
	}
	bySpec := make(map[RefreshSpec]int64, len(generations))
	for _, generation := range generations {
		bySpec[RefreshSpec{
			Kind: generation.Kind,
			Key:  generation.RefreshKey,
		}] = generation.Generation
	}
	result := make([]RefreshGeneration, 0, len(deduped))
	for _, spec := range deduped {
		result = append(result, RefreshGeneration{
			Spec:       spec,
			Generation: bySpec[spec],
		})
	}
	return result, nil
}

func argsForSpec(spec RefreshSpec) (rivertype.JobArgs, error) {
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
			return fmt.Errorf("River client missing from refresh worker context")
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
