package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	KindRefreshStack           = "refresh_stack"
	KindRefreshChecks          = "refresh_checks"
	KindRefreshBranch          = "refresh_branch"
	KindResolveStackMembership = "resolve_stack_membership"
)

// RefreshArgs is the complete durable job pointer. It intentionally contains
// no webhook or entity payload (SYNC_ENGINE §8 and C-I4).
type RefreshArgs struct {
	PointerKind string `json:"kind"`
	Key         string `json:"key"`
}

type RefreshPRArgs struct{ RefreshArgs }
type RefreshStackArgs struct{ RefreshArgs }
type RefreshChecksArgs struct{ RefreshArgs }
type RefreshBranchArgs struct{ RefreshArgs }
type ResolveStackMembershipArgs struct{ RefreshArgs }

func NewRefreshPRArgs(key string) RefreshPRArgs {
	return RefreshPRArgs{RefreshArgs{PointerKind: KindRefreshPR, Key: key}}
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
func (RefreshStackArgs) Kind() string           { return KindRefreshStack }
func (RefreshChecksArgs) Kind() string          { return KindRefreshChecks }
func (RefreshBranchArgs) Kind() string          { return KindRefreshBranch }
func (ResolveStackMembershipArgs) Kind() string { return KindResolveStackMembership }

// NewRefreshInsertOpts is the one supported uniqueness definition for refresh
// work. River requires running in this mask; a durable generation handles
// signals that coalesce while a job runs.
func NewRefreshInsertOpts(scheduledAt time.Time) *river.InsertOpts {
	return &river.InsertOpts{
		Queue:       QueueEvent,
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

// M2 registers distinct placeholder worker types so M3 can fill each
// fetch path independently without changing durable job kinds.
type refreshPRWorker struct {
	river.WorkerDefaults[RefreshPRArgs]
	pool *pgxpool.Pool
	work refreshWork
}

type refreshStackWorker struct {
	river.WorkerDefaults[RefreshStackArgs]
	pool *pgxpool.Pool
	work refreshWork
}

type refreshChecksWorker struct {
	river.WorkerDefaults[RefreshChecksArgs]
	pool *pgxpool.Pool
	work refreshWork
}

type refreshBranchWorker struct {
	river.WorkerDefaults[RefreshBranchArgs]
	pool *pgxpool.Pool
	work refreshWork
}

type resolveStackMembershipWorker struct {
	river.WorkerDefaults[ResolveStackMembershipArgs]
	pool *pgxpool.Pool
	work refreshWork
}

type refreshWork func(context.Context, RefreshArgs) error

func (w *refreshPRWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshPRArgs],
) error {
	return runRefresh(ctx, w.pool, w.work, job, job.Args.RefreshArgs)
}

func (w *refreshStackWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshStackArgs],
) error {
	return runRefresh(ctx, w.pool, w.work, job, job.Args.RefreshArgs)
}

func (w *refreshChecksWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshChecksArgs],
) error {
	return runRefresh(ctx, w.pool, w.work, job, job.Args.RefreshArgs)
}

func (w *refreshBranchWorker) Work(
	ctx context.Context,
	job *river.Job[RefreshBranchArgs],
) error {
	return runRefresh(ctx, w.pool, w.work, job, job.Args.RefreshArgs)
}

func (w *resolveStackMembershipWorker) Work(
	ctx context.Context,
	job *river.Job[ResolveStackMembershipArgs],
) error {
	// M3 must read the PR's cached old membership, fetch current membership,
	// then refresh both the old and new stacks. The durable args deliberately
	// carry only the PR key; payload stack state is never treated as truth.
	return runRefresh(ctx, w.pool, w.work, job, job.Args.RefreshArgs)
}

func logPlaceholder(args RefreshArgs) {
	slog.Info("refresh worker placeholder", "kind", args.PointerKind, "key", args.Key)
}

func registerRefreshWorkers(workers *river.Workers, pool *pgxpool.Pool) {
	river.AddWorker(workers, &refreshPRWorker{pool: pool})
	river.AddWorker(workers, &refreshStackWorker{pool: pool})
	river.AddWorker(workers, &refreshChecksWorker{pool: pool})
	river.AddWorker(workers, &refreshBranchWorker{pool: pool})
	river.AddWorker(workers, &resolveStackMembershipWorker{pool: pool})
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
		logPlaceholder(args)
	} else if err := work(ctx, args); err != nil {
		return err
	}
	return completeRefresh(ctx, pool, job, args, startedGeneration)
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

	if generation > startedGeneration {
		client := river.ClientFromContext[pgx.Tx](ctx)
		if client == nil {
			return fmt.Errorf("River client missing from refresh worker context")
		}
		if _, err := client.InsertTx(
			ctx,
			tx,
			job.Args,
			NewRefreshInsertOpts(time.Time{}),
		); err != nil {
			return fmt.Errorf("insert dirty refresh follow-up: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit refresh completion: %w", err)
	}
	return nil
}
