package queue

import (
	"context"
	"log/slog"

	"github.com/riverqueue/river"
)

const (
	KindRefreshPR     = "refresh_pr"
	KindRefreshStack  = "refresh_stack"
	KindRefreshChecks = "refresh_checks"
	KindRefreshBranch = "refresh_branch"
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

func (RefreshPRArgs) Kind() string     { return KindRefreshPR }
func (RefreshStackArgs) Kind() string  { return KindRefreshStack }
func (RefreshChecksArgs) Kind() string { return KindRefreshChecks }
func (RefreshBranchArgs) Kind() string { return KindRefreshBranch }

// M2 registers four distinct placeholder worker types so M3 can fill each
// fetch path independently without changing durable job kinds.
type refreshPRWorker struct {
	river.WorkerDefaults[RefreshPRArgs]
}

type refreshStackWorker struct {
	river.WorkerDefaults[RefreshStackArgs]
}

type refreshChecksWorker struct {
	river.WorkerDefaults[RefreshChecksArgs]
}

type refreshBranchWorker struct {
	river.WorkerDefaults[RefreshBranchArgs]
}

func (*refreshPRWorker) Work(_ context.Context, job *river.Job[RefreshPRArgs]) error {
	logPlaceholder(job.Args.RefreshArgs)
	return nil
}

func (*refreshStackWorker) Work(_ context.Context, job *river.Job[RefreshStackArgs]) error {
	logPlaceholder(job.Args.RefreshArgs)
	return nil
}

func (*refreshChecksWorker) Work(_ context.Context, job *river.Job[RefreshChecksArgs]) error {
	logPlaceholder(job.Args.RefreshArgs)
	return nil
}

func (*refreshBranchWorker) Work(_ context.Context, job *river.Job[RefreshBranchArgs]) error {
	logPlaceholder(job.Args.RefreshArgs)
	return nil
}

func logPlaceholder(args RefreshArgs) {
	slog.Info("refresh worker placeholder", "kind", args.PointerKind, "key", args.Key)
}

func registerRefreshWorkers(workers *river.Workers) {
	river.AddWorker(workers, &refreshPRWorker{})
	river.AddWorker(workers, &refreshStackWorker{})
	river.AddWorker(workers, &refreshChecksWorker{})
	river.AddWorker(workers, &refreshBranchWorker{})
}
