// Package queue wires River with the sync engine's three priority-class
// queues (SYNC_ENGINE C-B3): interactive > event > sweep.
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

const (
	QueueInteractive = "interactive"
	QueueEvent       = "event"
	QueueSweep       = "sweep"
	QueueReconcile   = "reconcile"
	QueueDrift       = "drift"
	QueuePruner      = "pruner"
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
	queueNames            []string
	queuesExplicit        bool
	maxWorkers            map[string]int
	deadlineObserver      DeadlineObserver
	refreshObserver       RefreshObserver
	now                   func() time.Time
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

// WithQueues limits a started client to the component queues owned by its
// enabled roles. Producer-only clients may pass no names and are never
// started, while combined roles pass the union of their queue families.
func WithQueues(names ...string) ClientOption {
	return func(options *clientOptions) {
		options.queuesExplicit = true
		options.queueNames = append([]string(nil), names...)
	}
}

// WithQueueMaxWorkers is primarily a deterministic test seam. Production
// keeps the reserved defaults below.
func WithQueueMaxWorkers(queueName string, maxWorkers int) ClientOption {
	return func(options *clientOptions) {
		if options.maxWorkers == nil {
			options.maxWorkers = make(map[string]int)
		}
		options.maxWorkers[queueName] = maxWorkers
	}
}

type DeadlineObserver interface {
	RefreshDeadlineMissed(
		context.Context,
		string,
		string,
		time.Time,
		time.Time,
	)
}

type LogDeadlineObserver struct{}

func (LogDeadlineObserver) RefreshDeadlineMissed(
	_ context.Context,
	kind string,
	key string,
	deadline time.Time,
	completedAt time.Time,
) {
	slog.Error(
		"C-R1 reconciliation refresh missed its completion deadline",
		"kind", kind,
		"key", key,
		"deadline", deadline,
		"completed_at", completedAt,
		"lateness", completedAt.Sub(deadline),
	)
}

type DeadlineObservers []DeadlineObserver

func (observers DeadlineObservers) RefreshDeadlineMissed(
	ctx context.Context,
	kind string,
	key string,
	deadline time.Time,
	completedAt time.Time,
) {
	for _, observer := range observers {
		if observer != nil {
			observer.RefreshDeadlineMissed(
				ctx, kind, key, deadline, completedAt,
			)
		}
	}
}

func WithDeadlineObserver(observer DeadlineObserver) ClientOption {
	return func(options *clientOptions) {
		options.deadlineObserver = observer
	}
}

// RefreshObservation describes one authoritative fetch attempt. C-Q2's
// event-to-cache latency is populated only for event-originated generations.
type RefreshObservation struct {
	Kind             string
	Queue            string
	EventReceivedAt  time.Time
	CacheCommittedAt time.Time
	StartedAt        time.Time
	CompletedAt      time.Time
	Err              error
}

type RefreshObserver interface {
	RefreshFinished(context.Context, RefreshObservation)
}

func WithRefreshObserver(observer RefreshObserver) ClientOption {
	return func(options *clientOptions) {
		options.refreshObserver = observer
	}
}

func WithNow(now func() time.Time) ClientOption {
	return func(options *clientOptions) {
		options.now = now
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
		registerRefreshWorkers(
			workers,
			pool,
			configured.refreshHandler,
			configured.deadlineObserver,
			configured.refreshObserver,
			configured.now,
		)
	}
	for _, register := range configured.registrars {
		register(workers)
	}
	queueNames := configured.queueNames
	if !configured.queuesExplicit {
		queueNames = []string{QueueInteractive, QueueEvent, QueueSweep}
	}
	queues := make(map[string]river.QueueConfig, len(queueNames))
	defaults := map[string]int{
		QueueInteractive: 4,
		QueueEvent:       8,
		QueueSweep:       4,
		QueueReconcile:   2,
		QueueDrift:       1,
		QueuePruner:      1,
	}
	for _, name := range queueNames {
		maxWorkers, ok := defaults[name]
		if !ok {
			return nil, fmt.Errorf("unsupported River queue %q", name)
		}
		if override := configured.maxWorkers[name]; override != 0 {
			maxWorkers = override
		}
		if maxWorkers <= 0 {
			return nil, fmt.Errorf(
				"River queue %q max workers must be positive",
				name,
			)
		}
		queues[name] = river.QueueConfig{MaxWorkers: maxWorkers}
	}
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		PeriodicJobs: configured.periodicJobs,
		Queues:       queues,
		Workers:      workers,
	})
}
