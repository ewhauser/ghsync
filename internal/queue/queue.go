// Package queue wires River with isolated interactive, event, bulk, and sweep
// work lanes (SYNC_ENGINE C-B3).
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
	"github.com/riverqueue/river/rivertype"
)

const (
	// QueueInteractive serves latency-sensitive user-requested refreshes.
	QueueInteractive = "interactive"
	// QueueEvent serves webhook-originated refreshes.
	QueueEvent = "event"
	// QueueBulk serves bounded branch-reconciliation pages without consuming
	// direct-event or C-R1 sweep workers.
	QueueBulk = "bulk"
	// QueueSweep serves bounded-staleness refreshes.
	QueueSweep = "sweep"
	// QueueReconcile serves sweep state-machine and gap-healing work.
	QueueReconcile = "reconcile"
	// QueueDrift serves sampled semantic-drift detection.
	QueueDrift = "drift"
	// QueuePruner serves retention deletion work.
	QueuePruner = "pruner"
)

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
	plugins               []rivertype.Plugin
	now                   func() time.Time
}

// ClientOption customizes River workers, queues, schedules, and observers.
type ClientOption func(*clientOptions)

// WithRefreshHandler installs the authoritative refresh implementation.
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

// DeadlineObserver receives completed refreshes that missed reconciliation
// deadlines.
type DeadlineObserver interface {
	RefreshDeadlineMissed(
		context.Context,
		string,
		string,
		time.Time,
		time.Time,
	)
}

// LogDeadlineObserver logs reconciliation deadline misses.
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

// DeadlineObservers fans deadline misses out in declaration order.
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

// WithDeadlineObserver installs refresh-deadline instrumentation.
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
	Superseded       bool
	Err              error
}

// RefreshObserver receives completed authoritative-fetch observations.
type RefreshObserver interface {
	RefreshFinished(context.Context, *RefreshObservation)
}

// WithRefreshObserver installs authoritative-fetch instrumentation.
func WithRefreshObserver(observer RefreshObserver) ClientOption {
	return func(options *clientOptions) {
		options.refreshObserver = observer
	}
}

// WithNow supplies worker time for deterministic testing.
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

// WithPlugins installs River plugins on inserts, workers, and internal hooks.
func WithPlugins(plugins ...rivertype.Plugin) ClientOption {
	return func(options *clientOptions) {
		options.plugins = append(options.plugins, plugins...)
	}
}

// NewClient builds a River client for the selected component queues and
// worker registrars. Without WithQueues it owns the four fetch queues:
// interactive, event, bulk, and sweep.
func NewClient(
	pool *pgxpool.Pool,
	options ...ClientOption,
) (*river.Client[pgx.Tx], error) {
	configured := clientOptions{registerRefreshWorker: true}
	for _, option := range options {
		option(&configured)
	}
	workers := river.NewWorkers()
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
		queueNames = []string{
			QueueInteractive, QueueEvent, QueueBulk, QueueSweep,
		}
	}
	queues := make(map[string]river.QueueConfig, len(queueNames))
	defaults := map[string]int{
		QueueInteractive: 4,
		QueueEvent:       8,
		QueueBulk:        4,
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
				"river queue %q max workers must be positive",
				name,
			)
		}
		queues[name] = river.QueueConfig{MaxWorkers: maxWorkers}
	}
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		PeriodicJobs: configured.periodicJobs,
		Plugins:      configured.plugins,
		Queues:       queues,
		Workers:      workers,
	})
}
