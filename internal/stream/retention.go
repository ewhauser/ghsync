package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const minimumRetentionAge = 7 * 24 * time.Hour

// RetentionOptions configures independent C-S7 change-event pruning.
type RetentionOptions struct {
	Age       time.Duration
	Period    time.Duration
	BatchSize int
	Now       func() time.Time
	OnPrune   func(context.Context, string, int64)
}

// Retention prunes change events without consulting consumer cursors and
// advances per-stream horizons in the same transaction as each delete batch.
type Retention struct {
	pool      *pgxpool.Pool
	age       time.Duration
	period    time.Duration
	batchSize int
	now       func() time.Time
	onPrune   func(context.Context, string, int64)
}

// NewRetention validates the locked seven-day C-S7 floor.
func NewRetention(
	pool *pgxpool.Pool,
	options RetentionOptions,
) (*Retention, error) {
	if pool == nil {
		return nil, fmt.Errorf("stream retention requires Postgres")
	}
	if options.Age < minimumRetentionAge {
		return nil, fmt.Errorf(
			"stream retention age must be at least %s (C-S7)",
			minimumRetentionAge,
		)
	}
	if options.Period <= 0 {
		return nil, fmt.Errorf("stream retention period must be positive")
	}
	if options.BatchSize <= 0 {
		return nil, fmt.Errorf("stream retention batch size must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Retention{
		pool:      pool,
		age:       options.Age,
		period:    options.Period,
		batchSize: options.BatchSize,
		now:       options.Now,
		onPrune:   options.OnPrune,
	}, nil
}

// Prune deletes every expired event in bounded C-S7 batches and returns the
// number removed. Cursor positions never participate in eligibility.
func (r *Retention) Prune(ctx context.Context) (int64, error) {
	cutoff := r.now().UTC().Add(-r.age)
	var total int64
	for {
		deleted, err := dbgen.New(r.pool).PruneChangeEvents(
			ctx,
			dbgen.PruneChangeEventsParams{
				Cutoff: pgtype.Timestamptz{
					Time:  cutoff,
					Valid: true,
				},
				BatchSize: int32(r.batchSize),
			},
		)
		if err != nil {
			return total, fmt.Errorf("prune change events: %w", err)
		}
		total += deleted
		if deleted < int64(r.batchSize) {
			if r.onPrune != nil {
				r.onPrune(ctx, "change_events", total)
			}
			return total, nil
		}
	}
}

// Run prunes on startup and periodically thereafter. It is intentionally
// independent of River granularity and consumer cursor state (C-S7).
func (r *Retention) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if _, err := r.Prune(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			timer.Reset(r.period)
		}
	}
}
