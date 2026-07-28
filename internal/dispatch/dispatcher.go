package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store/dbgen"
)

const refreshUniqueStatesSQL = `
UPDATE river_job
-- River v0.41 bit order: scheduled, retryable, pending, available.
SET unique_states = B'10110001'
WHERE id = ANY($1::bigint[])`

// Config controls dispatcher batching, poison tolerance, and bounded debounce.
type Config struct {
	BatchSize    int
	MaxAttempts  int
	Debounce     time.Duration
	PollInterval time.Duration
	Now          func() time.Time
	Classifier   Classifier
}

// Dispatcher owns the delivery → River transaction boundary (C-P2).
type Dispatcher struct {
	pool   *pgxpool.Pool
	river  *river.Client[pgx.Tx]
	config Config
}

func New(pool *pgxpool.Pool, riverClient *river.Client[pgx.Tx], config Config) *Dispatcher {
	if pool == nil || riverClient == nil {
		panic("dispatcher requires Postgres and River clients")
	}
	if config.BatchSize <= 0 || config.MaxAttempts <= 0 ||
		config.Debounce <= 0 || config.PollInterval <= 0 {
		panic("dispatcher sizes and durations must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if len(config.Classifier.rules) == 0 {
		config.Classifier = DefaultClassifier()
	}
	return &Dispatcher{pool: pool, river: riverClient, config: config}
}

// Run continuously drains available batches and polls when idle.
func (d *Dispatcher) Run(ctx context.Context) error {
	timer := time.NewTimer(d.config.PollInterval)
	defer timer.Stop()
	for {
		count, err := d.DispatchBatch(ctx)
		if err != nil {
			return err
		}
		if count > 0 {
			continue
		}
		timer.Reset(d.config.PollInterval)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
	}
}

// DispatchBatch claims, classifies, enqueues, and finishes one batch in one
// pgx transaction shared by sqlc and River.
func (d *Dispatcher) DispatchBatch(ctx context.Context) (int, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin dispatch batch: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	queries := dbgen.New(tx)
	deliveries, err := queries.ClaimWebhookDeliveries(ctx, int32(d.config.BatchSize))
	if err != nil {
		return 0, fmt.Errorf("claim webhook deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty dispatch batch: %w", err)
		}
		return 0, nil
	}

	type deliveryResult struct {
		DeliveryGUID string `json:"delivery_guid"`
		Status       string `json:"status"`
		LastError    string `json:"last_error"`
	}
	results := make([]deliveryResult, 0, len(deliveries))
	intents := make([]Intent, 0, len(deliveries))
	for _, delivery := range deliveries {
		classified, classifyErr := d.config.Classifier.Classify(
			delivery.Event,
			delivery.RawBody,
		)
		if classifyErr != nil {
			status := "pending"
			if int(delivery.Attempts) >= d.config.MaxAttempts {
				status = "parked"
			}
			results = append(results, deliveryResult{
				DeliveryGUID: delivery.DeliveryGuid,
				Status:       status,
				LastError:    classifyErr.Error(),
			})
			continue
		}
		intents = append(intents, classified...)
		results = append(results, deliveryResult{
			DeliveryGUID: delivery.DeliveryGuid,
			Status:       "processed",
		})
	}

	if len(intents) > 0 {
		params, err := d.insertParams(intents)
		if err != nil {
			return 0, err
		}
		inserted, err := d.river.InsertManyTx(ctx, tx, params)
		if err != nil {
			return 0, fmt.Errorf("insert refresh intents: %w", err)
		}
		if err := normalizeRefreshUniqueStates(ctx, tx, inserted); err != nil {
			return 0, err
		}
	}

	encodedResults, err := json.Marshal(results)
	if err != nil {
		return 0, fmt.Errorf("encode delivery results: %w", err)
	}
	updated, err := queries.SetWebhookDeliveryResults(ctx, encodedResults)
	if err != nil {
		return 0, fmt.Errorf("finish webhook delivery batch: %w", err)
	}
	if updated != int64(len(deliveries)) {
		return 0, fmt.Errorf(
			"finish webhook delivery batch: updated %d of %d rows",
			updated,
			len(deliveries),
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit dispatch batch: %w", err)
	}
	return len(deliveries), nil
}

func (d *Dispatcher) insertParams(intents []Intent) ([]river.InsertManyParams, error) {
	scheduledAt := d.config.Now().Add(d.config.Debounce)
	// C-Q1 intra-batch coalescing: River's unique insert dedupes against
	// EXISTING rows, but two identical intents inside one InsertManyTx
	// statement conflict with each other ("ON CONFLICT DO UPDATE command
	// cannot affect row a second time"). Dedupe by {kind, key} first.
	seen := make(map[Intent]struct{}, len(intents))
	deduped := intents[:0]
	for _, intent := range intents {
		if _, dup := seen[intent]; dup {
			continue
		}
		seen[intent] = struct{}{}
		deduped = append(deduped, intent)
	}
	intents = deduped
	params := make([]river.InsertManyParams, 0, len(intents))
	for _, intent := range intents {
		args, err := refreshArgs(intent)
		if err != nil {
			return nil, err
		}
		params = append(params, river.InsertManyParams{
			Args: args,
			InsertOpts: &river.InsertOpts{
				Queue:       queue.QueueEvent,
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
			},
		})
	}
	return params, nil
}

func refreshArgs(intent Intent) (rivertype.JobArgs, error) {
	if intent.Priority != PriorityEvent {
		return nil, fmt.Errorf("unsupported refresh priority %q", intent.Priority)
	}
	switch intent.Kind {
	case queue.KindRefreshPR:
		return queue.NewRefreshPRArgs(intent.Key), nil
	case queue.KindRefreshStack:
		return queue.NewRefreshStackArgs(intent.Key), nil
	case queue.KindRefreshChecks:
		return queue.NewRefreshChecksArgs(intent.Key), nil
	case queue.KindRefreshBranch:
		return queue.NewRefreshBranchArgs(intent.Key), nil
	default:
		return nil, fmt.Errorf("unsupported refresh kind %q", intent.Kind)
	}
}

func normalizeRefreshUniqueStates(
	ctx context.Context,
	tx pgx.Tx,
	results []*rivertype.JobInsertResult,
) error {
	jobIDs := make([]int64, 0, len(results))
	for _, result := range results {
		if !result.UniqueSkippedAsDuplicate {
			jobIDs = append(jobIDs, result.Job.ID)
		}
	}
	if len(jobIDs) == 0 {
		return nil
	}

	// C-Q1 requires pending/scheduled/available/retryable only. River v0.41's
	// public validator requires running as well, so InsertManyTx receives its
	// compatible superset and this same transaction stores the exact C-Q1 mask
	// for the rows it inserted. Completed is excluded at both layers.
	tag, err := tx.Exec(ctx, refreshUniqueStatesSQL, jobIDs)
	if err != nil {
		return fmt.Errorf("set refresh uniqueness states: %w", err)
	}
	if tag.RowsAffected() != int64(len(jobIDs)) {
		return fmt.Errorf(
			"set refresh uniqueness states: updated %d of %d jobs",
			tag.RowsAffected(),
			len(jobIDs),
		)
	}
	return nil
}
