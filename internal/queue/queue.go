// Package queue wires River with the sync engine's three priority-class
// queues (SYNC_ENGINE C-B3): interactive > event > sweep.
package queue

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	QueueInteractive = "interactive"
	QueueEvent       = "event"
	QueueSweep       = "sweep"
)

// NoopArgs is a placeholder job used to verify queue plumbing end to end
// before real workers exist (M2+ replaces it).
type NoopArgs struct{}

func (NoopArgs) Kind() string { return "noop" }

type noopWorker struct {
	river.WorkerDefaults[NoopArgs]
}

func (*noopWorker) Work(ctx context.Context, job *river.Job[NoopArgs]) error {
	return nil
}

// NewClient builds the River client with the three queues configured.
type clientOptions struct {
	refreshHandler        RefreshHandler
	registerRefreshWorker bool
	registrars            []func(*river.Workers)
	periodicJobs          []*river.PeriodicJob
}

type ClientOption func(*clientOptions)

func WithRefreshHandler(handler RefreshHandler) ClientOption {
	return func(options *clientOptions) {
		options.refreshHandler = handler
	}
}

// WithoutRefreshWorkers is for a worker-only role such as the retention
// pruner. Producer clients keep the default registrations so River can
// validate refresh args at insertion time.
func WithoutRefreshWorkers() ClientOption {
	return func(options *clientOptions) {
		options.registerRefreshWorker = false
	}
}

// WithWorkerRegistrar lets milestone packages own their typed River args and
// workers without making queue import sweep/drift and creating a package
// cycle.
func WithWorkerRegistrar(registrar func(*river.Workers)) ClientOption {
	return func(options *clientOptions) {
		if registrar != nil {
			options.registrars = append(options.registrars, registrar)
		}
	}
}

// WithPeriodicJobs installs leader-elected River schedules supplied by the
// enabled serve roles.
func WithPeriodicJobs(jobs ...*river.PeriodicJob) ClientOption {
	return func(options *clientOptions) {
		options.periodicJobs = append(options.periodicJobs, jobs...)
	}
}

func NewClient(
	pool *pgxpool.Pool,
	options ...ClientOption,
) (*river.Client[pgx.Tx], error) {
	configured := clientOptions{registerRefreshWorker: true}
	for _, option := range options {
		option(&configured)
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &noopWorker{})
	if configured.registerRefreshWorker {
		registerRefreshWorkers(workers, pool, configured.refreshHandler)
	}
	for _, register := range configured.registrars {
		register(workers)
	}
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		PeriodicJobs: configured.periodicJobs,
		Queues: map[string]river.QueueConfig{
			QueueInteractive: {MaxWorkers: 4},
			QueueEvent:       {MaxWorkers: 8},
			QueueSweep:       {MaxWorkers: 4},
		},
		Workers: workers,
	})
}
