// Package stream owns the C-S2 visibility watermark and C-S7 retention
// machinery for the transactional change-event outbox.
package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/opsstate"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	defaultWatermarkRefresh          = 100 * time.Millisecond
	defaultWatermarkLease            = 3 * time.Second
	defaultWatermarkFenceLockTimeout = time.Second
	watermarkHeartbeatInterval       = time.Second
)

// ErrLeaseHeld means another watermarker owns the unexpired singleton lease.
var ErrLeaseHeld = errors.New("stream watermark lease is held")

// WatermarkOptions configures the leased C-S2 maintenance loop.
type WatermarkOptions struct {
	RefreshInterval  time.Duration
	LeaseTTL         time.Duration
	FenceLockTimeout time.Duration
	Owner            string
	Observer         WatermarkObserver
	InstallationID   int64
}

// WatermarkObserver is M6's C-S2 progress seam.
type WatermarkObserver interface {
	WatermarkStep(context.Context, WatermarkProgress)
}

type noopWatermarkObserver struct{}

func (noopWatermarkObserver) WatermarkStep(
	context.Context,
	WatermarkProgress,
) {
}

// WatermarkProgress is one observed state of the public stream watermark.
type WatermarkProgress struct {
	SafeSeq      int64
	MaxSeq       int64
	CandidateSeq *int64
	Advanced     bool
	// FenceTimedOut is a bounded, retryable step outcome rather than a
	// watermarker failure. Observers meter it separately from advances.
	FenceTimedOut bool
}

// Watermarker advances stream_watermark.safe_seq while holding the exclusive
// side of the outbox writer fence.
type Watermarker struct {
	pool             *pgxpool.Pool
	refreshInterval  time.Duration
	leaseTTL         time.Duration
	fenceLockTimeout time.Duration
	token            string
	observer         WatermarkObserver
	installationID   int64
	stepMu           sync.Mutex
	lastHeartbeatAt  time.Time
	testBeforeFence  func()
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
	if options.FenceLockTimeout == 0 {
		options.FenceLockTimeout = defaultWatermarkFenceLockTimeout
	}
	if options.FenceLockTimeout < 0 {
		return nil, fmt.Errorf("watermark fence lock timeout must be positive")
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
		owner = "ghsync-watermarker"
	}
	if options.Observer == nil {
		options.Observer = noopWatermarkObserver{}
	}
	return &Watermarker{
		pool:             pool,
		refreshInterval:  options.RefreshInterval,
		leaseTTL:         options.LeaseTTL,
		fenceLockTimeout: options.FenceLockTimeout,
		token:            owner + ":" + hex.EncodeToString(tokenBytes),
		observer:         options.Observer,
		installationID:   options.InstallationID,
	}, nil
}

// Step renews or acquires the singleton lease, waits for all registered outbox
// writers, and publishes the greatest committed sequence. Read-only snapshots
// and unrelated transactions do not participate in this fence.
func (w *Watermarker) Step(
	ctx context.Context,
) (WatermarkProgress, error) {
	w.stepMu.Lock()
	defer w.stepMu.Unlock()

	if err := w.acquireOrRenew(ctx); err != nil {
		return WatermarkProgress{}, err
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return WatermarkProgress{}, fmt.Errorf("begin stream watermark step: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if w.testBeforeFence != nil {
		w.testBeforeFence()
	}
	queries := dbgen.New(tx)
	if err := queries.SetLocalLockTimeout(
		ctx,
		w.fenceLockTimeout.String(),
	); err != nil {
		return WatermarkProgress{}, fmt.Errorf(
			"set outbox watermark fence timeout: %w", err,
		)
	}
	if err := outbox.AcquireWatermarkFence(ctx, tx); err != nil {
		if isLockTimeout(err) {
			progress := WatermarkProgress{FenceTimedOut: true}
			w.observer.WatermarkStep(ctx, progress)
			return progress, nil
		}
		return WatermarkProgress{}, err
	}

	targetState, err := queries.ReadStreamWatermarkTarget(ctx, w.token)
	if err != nil {
		return WatermarkProgress{}, fmt.Errorf(
			"read committed change-event maximum: %w", err,
		)
	}
	prior := targetState.SafeSeq
	target := targetState.TargetSeq
	leaseValid := targetState.LeaseValid
	if !leaseValid {
		return WatermarkProgress{}, ErrLeaseHeld
	}

	safe := prior
	if target > prior {
		safe, err = queries.PublishStreamWatermark(
			ctx,
			dbgen.PublishStreamWatermarkParams{
				SafeSeq:    target,
				LeaseToken: w.token,
			},
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return WatermarkProgress{}, ErrLeaseHeld
			}
			return WatermarkProgress{}, fmt.Errorf(
				"publish stream watermark: %w", err,
			)
		}
	}

	heartbeatAt := time.Now()
	recordHeartbeat := w.installationID > 0 &&
		(w.lastHeartbeatAt.IsZero() ||
			heartbeatAt.Sub(w.lastHeartbeatAt) >= watermarkHeartbeatInterval)
	if recordHeartbeat {
		if err := opsstate.RecordSuccessN(
			ctx,
			tx,
			w.installationID,
			"watermarker",
			outbox.EntitiesStream,
			1,
		); err != nil {
			return WatermarkProgress{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return WatermarkProgress{}, fmt.Errorf(
			"commit stream watermark: %w", err,
		)
	}
	if recordHeartbeat {
		w.lastHeartbeatAt = heartbeatAt
	}
	progress := WatermarkProgress{
		SafeSeq:  safe,
		MaxSeq:   target,
		Advanced: safe > prior,
	}
	w.observer.WatermarkStep(ctx, progress)
	return progress, nil
}

func isLockTimeout(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		postgresError != nil &&
		postgresError.Code == "55P03"
}

// Run maintains the watermark at approximately RefreshInterval. Standby
// processes keep attempting the expiring lease, so failover needs no external
// coordinator (C-O2).
func (w *Watermarker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	defer w.release(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
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
// watermark.
func (w *Watermarker) Close(ctx context.Context) error {
	return w.release(ctx)
}

func (w *Watermarker) acquireOrRenew(ctx context.Context) error {
	_, err := dbgen.New(w.pool).AcquireOrRenewStreamWatermarkLease(
		ctx,
		dbgen.AcquireOrRenewStreamWatermarkLeaseParams{
			LeaseToken: w.token,
			LeaseTtl:   w.leaseTTL.String(),
		},
	)
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
	err := dbgen.New(w.pool).ReleaseStreamWatermarkLease(ctx, w.token)
	if err != nil {
		return fmt.Errorf("release stream watermark lease: %w", err)
	}
	return nil
}
