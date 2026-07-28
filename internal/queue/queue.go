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
func NewClient(pool *pgxpool.Pool) (*river.Client[pgx.Tx], error) {
	workers := river.NewWorkers()
	river.AddWorker(workers, &noopWorker{})
	registerRefreshWorkers(workers)
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			QueueInteractive: {MaxWorkers: 4},
			QueueEvent:       {MaxWorkers: 8},
			QueueSweep:       {MaxWorkers: 2},
		},
		Workers: workers,
	})
}
