package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const minimumRetentionAge = 7 * 24 * time.Hour

// RetentionOptions configures independent C-S7 change-event pruning.
type RetentionOptions struct {
	Age       time.Duration
	Period    time.Duration
	BatchSize int
	Now       func() time.Time
}

// Retention prunes change events without consulting consumer cursors and
// advances per-stream horizons in the same transaction as each delete batch.
type Retention struct {
	pool      *pgxpool.Pool
	age       time.Duration
	period    time.Duration
	batchSize int
	now       func() time.Time
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
	}, nil
}

// Prune deletes every expired event in bounded C-S7 batches and returns the
// number removed. Cursor positions never participate in eligibility.
func (r *Retention) Prune(ctx context.Context) (int64, error) {
	cutoff := r.now().UTC().Add(-r.age)
	var total int64
	for {
		tag, err := r.pool.Exec(ctx, `
			WITH doomed AS MATERIALIZED (
			    SELECT seq, stream
			    FROM change_events
			    WHERE occurred_at < $1
			    ORDER BY occurred_at, seq
			    LIMIT $2
			    FOR UPDATE SKIP LOCKED
			),
			advanced AS (
			    INSERT INTO stream_horizons (
			        stream, pruned_through_seq, updated_at
			    )
			    SELECT stream, max(seq), clock_timestamp()
			    FROM doomed
			    GROUP BY stream
			    ON CONFLICT (stream) DO UPDATE
			    SET pruned_through_seq = GREATEST(
			            stream_horizons.pruned_through_seq,
			            EXCLUDED.pruned_through_seq
			        ),
			        updated_at = EXCLUDED.updated_at
			    RETURNING stream
			)
			DELETE FROM change_events AS events
			USING doomed
			WHERE events.seq = doomed.seq
			  -- Reference the data-changing CTE explicitly: horizon and delete
			  -- are one atomic RESYNC boundary (C-S4/C-S7).
			  AND (SELECT count(*) FROM advanced) >= 0
		`, cutoff, r.batchSize)
		if err != nil {
			return total, fmt.Errorf("prune change events: %w", err)
		}
		deleted := tag.RowsAffected()
		total += deleted
		if deleted < int64(r.batchSize) {
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
