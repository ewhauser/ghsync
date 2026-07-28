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

const MaxDebounce = 15 * time.Second

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
	if config.Debounce > MaxDebounce {
		panic("dispatcher debounce must not exceed 15s")
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
		intents = dedupeIntents(intents)
		encodedIntents, err := json.Marshal(intentGenerationPointers(intents))
		if err != nil {
			return 0, fmt.Errorf("encode refresh generation pointers: %w", err)
		}
		generations, err := queries.BumpRefreshIntentGenerations(ctx, encodedIntents)
		if err != nil {
			return 0, fmt.Errorf("bump refresh intent generations: %w", err)
		}
		if len(generations) != len(intents) {
			return 0, fmt.Errorf(
				"bump refresh intent generations: got %d of %d rows",
				len(generations),
				len(intents),
			)
		}
		params, err := d.insertParams(intents)
		if err != nil {
			return 0, err
		}
		_, err = d.river.InsertManyTx(ctx, tx, params)
		if err != nil {
			return 0, fmt.Errorf("insert refresh intents: %w", err)
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

func (d *Dispatcher) insertParams(
	intents []Intent,
) ([]river.InsertManyParams, error) {
	scheduledAt := d.config.Now().Add(d.config.Debounce)
	params := make([]river.InsertManyParams, 0, len(intents))
	for _, intent := range intents {
		args, err := refreshArgs(intent)
		if err != nil {
			return nil, err
		}
		params = append(params, river.InsertManyParams{
			Args:       args,
			InsertOpts: queue.NewRefreshInsertOpts(scheduledAt),
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
	case queue.KindResolveStackMembership:
		return queue.NewResolveStackMembershipArgs(intent.Key), nil
	default:
		return nil, fmt.Errorf("unsupported refresh kind %q", intent.Kind)
	}
}

func dedupeIntents(intents []Intent) []Intent {
	// River dedupes against existing rows, but duplicate unique keys within one
	// bulk INSERT would conflict with each other. Coalesce each batch first.
	seen := make(map[Intent]struct{}, len(intents))
	deduped := make([]Intent, 0, len(intents))
	for _, intent := range intents {
		if _, duplicate := seen[intent]; duplicate {
			continue
		}
		seen[intent] = struct{}{}
		deduped = append(deduped, intent)
	}
	return deduped
}

type intentGenerationPointer struct {
	Kind       string `json:"kind"`
	RefreshKey string `json:"refresh_key"`
}

func intentGenerationPointers(intents []Intent) []intentGenerationPointer {
	pointers := make([]intentGenerationPointer, 0, len(intents))
	for _, intent := range intents {
		pointers = append(pointers, intentGenerationPointer{
			Kind:       intent.Kind,
			RefreshKey: intent.Key,
		})
	}
	return pointers
}
