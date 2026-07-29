// Package stream owns the C-S2 visibility watermark and C-S7 retention
// machinery for the transactional change-event outbox.
package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWatermarkRefresh = 100 * time.Millisecond
	defaultWatermarkLease   = 3 * time.Second
)

// ErrLeaseHeld means another watermarker owns the unexpired singleton lease.
var ErrLeaseHeld = errors.New("stream watermark lease is held")

// WatermarkOptions configures the leased C-S2 maintenance loop.
type WatermarkOptions struct {
	RefreshInterval time.Duration
	LeaseTTL        time.Duration
	Owner           string
}

// WatermarkProgress is one observed state of the public stream watermark.
type WatermarkProgress struct {
	SafeSeq      int64
	CandidateSeq *int64
	Advanced     bool
}

// Watermarker advances stream_watermark.safe_seq only after PostgreSQL proves
// that every transaction old enough to own a captured sequence has finished.
type Watermarker struct {
	pool            *pgxpool.Pool
	refreshInterval time.Duration
	leaseTTL        time.Duration
	token           string
}

// NewWatermarker constructs a leader-coordinated C-S2 watermarker.
func NewWatermarker(
	pool *pgxpool.Pool,
	options WatermarkOptions,
) (*Watermarker, error) {
	if pool == nil {
		return nil, fmt.Errorf("watermarker requires Postgres")
	}
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = defaultWatermarkRefresh
	}
	if options.LeaseTTL <= 0 {
		options.LeaseTTL = defaultWatermarkLease
	}
	if options.RefreshInterval >= options.LeaseTTL/2 {
		return nil, fmt.Errorf(
			"watermark refresh interval must be less than half the lease TTL",
		)
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("watermark lease token: %w", err)
	}
	owner := options.Owner
	if owner == "" {
		owner = "frontier-watermarker"
	}
	return &Watermarker{
		pool:            pool,
		refreshInterval: options.RefreshInterval,
		leaseTTL:        options.LeaseTTL,
		token:           owner + ":" + hex.EncodeToString(tokenBytes),
	}, nil
}

// Step renews or acquires the singleton lease, promotes a proven candidate,
// and captures the next candidate. It is exposed for deterministic C-S2 tests
// and operational probes; normal callers use Run.
func (w *Watermarker) Step(
	ctx context.Context,
) (WatermarkProgress, error) {
	if err := w.acquireOrRenew(ctx); err != nil {
		return WatermarkProgress{}, err
	}

	var advancedSeq int64
	advanced := false
	err := w.pool.QueryRow(ctx, `
		UPDATE stream_watermark
		SET safe_seq = candidate_seq,
		    candidate_seq = NULL,
		    candidate_xid = NULL,
		    updated_at = clock_timestamp()
		WHERE singleton
		  AND lease_token = $1
		  AND lease_until > clock_timestamp()
		  AND candidate_xid IS NOT NULL
		  -- C-S2: pg_snapshot_xmin is the lowest still-in-flight XID.
		  -- 0013 guarantees every seq in candidate_seq was allocated only
		  -- after its writer acquired an older XID.
		  AND candidate_xid < pg_snapshot_xmin(pg_current_snapshot())
		RETURNING safe_seq
	`, w.token).Scan(&advancedSeq)
	switch {
	case err == nil:
		advanced = true
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return WatermarkProgress{}, fmt.Errorf(
			"promote stream watermark: %w", err,
		)
	}

	// Capture target sequence before assigning the candidate transaction XID.
	// frontier_next_change_event_seq does the inverse in writers (XID first,
	// then sequence), which creates a sound happens-before barrier (C-S2).
	var target int64
	if err := w.pool.QueryRow(ctx, `
		SELECT CASE WHEN is_called THEN last_value ELSE 0 END
		FROM change_events_seq_seq
	`).Scan(&target); err != nil {
		return WatermarkProgress{}, fmt.Errorf(
			"read change-event sequence: %w", err,
		)
	}
	if _, err := w.pool.Exec(ctx, `
		UPDATE stream_watermark
		SET candidate_seq = $2,
		    candidate_xid = pg_current_xact_id()
		WHERE singleton
		  AND lease_token = $1
		  AND lease_until > clock_timestamp()
		  AND candidate_xid IS NULL
	`, w.token, target); err != nil {
		return WatermarkProgress{}, fmt.Errorf(
			"capture stream watermark candidate: %w", err,
		)
	}

	var progress WatermarkProgress
	var candidate *int64
	if err := w.pool.QueryRow(ctx, `
		SELECT safe_seq, candidate_seq
		FROM stream_watermark
		WHERE singleton
		  AND lease_token = $1
		  AND lease_until > clock_timestamp()
	`, w.token).Scan(&progress.SafeSeq, &candidate); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return WatermarkProgress{}, ErrLeaseHeld
		}
		return WatermarkProgress{}, fmt.Errorf(
			"read stream watermark: %w", err,
		)
	}
	progress.CandidateSeq = candidate
	progress.Advanced = advanced && progress.SafeSeq == advancedSeq
	return progress, nil
}

// Run maintains the watermark at approximately RefreshInterval. Standby
// processes keep attempting the expiring lease, so failover needs no external
// coordinator (C-O2).
func (w *Watermarker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	defer w.release(context.Background()) //nolint:errcheck
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			_, err := w.Step(ctx)
			if err != nil && !errors.Is(err, ErrLeaseHeld) &&
				!errors.Is(err, context.Canceled) {
				return err
			}
			timer.Reset(w.refreshInterval)
		}
	}
}

// Close releases this runtime's singleton lease. It does not alter the safe
// watermark or a pending candidate.
func (w *Watermarker) Close(ctx context.Context) error {
	return w.release(ctx)
}

func (w *Watermarker) acquireOrRenew(ctx context.Context) error {
	var token string
	err := w.pool.QueryRow(ctx, `
		UPDATE stream_watermark
		SET lease_token = $1,
		    lease_until = clock_timestamp() + $2::interval
		WHERE singleton
		  AND (
		      lease_token IS NULL
		      OR lease_until <= clock_timestamp()
		      OR lease_token = $1
		  )
		RETURNING lease_token
	`, w.token, w.leaseTTL.String()).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseHeld
	}
	if err != nil {
		return fmt.Errorf("acquire stream watermark lease: %w", err)
	}
	return nil
}

func (w *Watermarker) release(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	_, err := w.pool.Exec(ctx, `
		UPDATE stream_watermark
		SET lease_token = NULL, lease_until = NULL
		WHERE singleton AND lease_token = $1
	`, w.token)
	if err != nil {
		return fmt.Errorf("release stream watermark lease: %w", err)
	}
	return nil
}
