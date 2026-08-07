package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Observation is an optimistic snapshot of one entity's committed write
// version. It never owns a transaction, connection, or advisory lock, so it is
// safe to retain across remote I/O. The writer verifies the version under the
// short transaction-scoped entity lock and advances it only on commit.
type Observation struct {
	key     string
	version int64
}

// Key returns the entity key protected by the observation.
func (o *Observation) Key() string {
	if o == nil {
		return ""
	}
	return o.key
}

// Close is retained for API compatibility. Observations own no resources.
func (o *Observation) Close() error {
	return nil
}

// CloseContext is retained for API compatibility. Observations own no
// resources, including after cancellation.
func (o *Observation) CloseContext(ctx context.Context) error {
	return nil
}

// EntityWriter owns C-C1..C-C6. Network fetches never enter this package.
type EntityWriter struct {
	pool     *pgxpool.Pool
	now      func() time.Time
	observer CacheObserver
}

// CacheObserver is M6's C-C2 compare-and-swap accounting seam.
type CacheObserver interface {
	CacheWrite(context.Context, string, bool, bool)
}

type noopCacheObserver struct{}

func (noopCacheObserver) CacheWrite(context.Context, string, bool, bool) {}

// NewEntityWriter constructs a cache writer backed by pool.
func NewEntityWriter(
	pool *pgxpool.Pool,
	observers ...CacheObserver,
) *EntityWriter {
	if pool == nil {
		panic("entity writer requires Postgres")
	}
	var observer CacheObserver = noopCacheObserver{}
	if len(observers) > 0 && observers[0] != nil {
		observer = observers[0]
	}
	return &EntityWriter{
		pool: pool, now: time.Now, observer: observer,
	}
}

// BeginObservation snapshots the entity's last committed write version. The
// query checks out a connection only for its own duration; the returned token
// owns no database resource while callers perform remote I/O.
func (w *EntityWriter) BeginObservation(
	ctx context.Context,
	entityKey string,
) (*Observation, error) {
	if entityKey == "" {
		return nil, fmt.Errorf("observation entity key is required")
	}
	version, err := dbgen.New(w.pool).GetEntityObservationVersion(ctx, entityKey)
	if errors.Is(err, pgx.ErrNoRows) {
		version = 0
	} else if err != nil {
		return nil, fmt.Errorf("read observation version %s: %w", entityKey, err)
	}
	return &Observation{key: entityKey, version: version}, nil
}

func (w *EntityWriter) beginEntityTx(
	ctx context.Context,
	observation *Observation,
	key string,
) (pgx.Tx, error) {
	if observation != nil {
		if err := requireObservation(observation, key); err != nil {
			return nil, err
		}
	}
	tx, err := w.pool.Begin(ctx)
	if err == nil {
		err = dbgen.New(tx).AcquireEntityAdvisoryLock(ctx, key)
		if err != nil {
			err = fmt.Errorf("lock %s: %w", key, err)
		}
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback(ctx)
		}
		return nil, err
	}
	if tx == nil {
		return nil, fmt.Errorf("begin transaction returned nil transaction")
	}
	fenceObserver, _ := w.observer.(outbox.FenceObserver)
	observedTx, err := outbox.AcquireObservedWriterFence(
		ctx,
		tx,
		fenceObserver,
	)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	tx = observedTx
	return tx, nil
}

type entityTx struct {
	ctx          context.Context //nolint:containedctx // transaction callbacks share this exact transaction-scoped context
	tx           pgx.Tx
	queries      *dbgen.Queries
	databaseTime time.Time
}

type entityTxFunc func(entityTx) error

// ErrRefreshGenerationSuperseded means a branch bulk transaction claimed a
// later direct-entity generation while this authoritative fetch was in flight.
// The response is discarded before any cache row can be committed.
var ErrRefreshGenerationSuperseded = errors.New(
	"refresh generation superseded",
)

// ErrObservationSuperseded means another write committed after the remote
// observation began. The stale response is discarded before any cache row or
// outbox event can commit; ordinary direct refreshes may safely retry it.
var ErrObservationSuperseded = errors.New("observation superseded")

// RefreshGenerationFence is the generation a direct River worker observed
// immediately before it began remote I/O.
type RefreshGenerationFence struct {
	Kind       string
	RefreshKey string
	Generation int64
}

type refreshGenerationFenceContextKey struct{}

// WithRefreshGenerationFence attaches a direct worker's start generation to
// the short post-fetch entity transaction.
func WithRefreshGenerationFence(
	ctx context.Context,
	fence RefreshGenerationFence,
) context.Context {
	return context.WithValue(ctx, refreshGenerationFenceContextKey{}, fence)
}

func refreshGenerationFenceFromContext(
	ctx context.Context,
) (RefreshGenerationFence, bool) {
	fence, ok := ctx.Value(refreshGenerationFenceContextKey{}).(RefreshGenerationFence)
	return fence, ok
}

// ErrBranchReconciliationSuperseded means a newer direct entity generation or
// branch generation won before a branch page's post-fetch write transaction.
// The remote result is deliberately discarded without retrying that target.
var ErrBranchReconciliationSuperseded = errors.New(
	"branch reconciliation superseded",
)

// BranchReconciliationFence is the local generation proof attached to one
// bounded page target. It is checked only after all external I/O, inside the
// short entity write transaction (C-C6).
type BranchReconciliationFence struct {
	RepoID            int64
	Branch            string
	BranchGeneration  int64
	RefreshKind       string
	RefreshKey        string
	RefreshGeneration int64
	EntityKey         string
}

type branchReconciliationFenceContextKey struct{}

// WithBranchReconciliationFence attaches a page target's generation proof to
// the context consumed by the entity writer.
func WithBranchReconciliationFence(
	ctx context.Context,
	fence *BranchReconciliationFence,
) context.Context {
	return context.WithValue(
		ctx,
		branchReconciliationFenceContextKey{},
		fence,
	)
}

func branchReconciliationFenceFromContext(
	ctx context.Context,
) (*BranchReconciliationFence, bool) {
	fence, ok := ctx.Value(branchReconciliationFenceContextKey{}).(*BranchReconciliationFence)
	return fence, ok
}

func (w *EntityWriter) withEntityTx(
	ctx context.Context,
	observation *Observation,
	key string,
	fn entityTxFunc,
) error {
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return fmt.Errorf("begin entity transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if observation != nil {
		currentVersion, versionErr := dbgen.New(tx).
			GetEntityObservationVersionForUpdate(ctx, key)
		if errors.Is(versionErr, pgx.ErrNoRows) {
			currentVersion = 0
		} else if versionErr != nil {
			return fmt.Errorf("read locked observation version: %w", versionErr)
		}
		if currentVersion != observation.version {
			if _, ok := branchReconciliationFenceFromContext(ctx); ok {
				return ErrBranchReconciliationSuperseded
			}
			return ErrObservationSuperseded
		}
	}

	databaseTime, err := databaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := fn(entityTx{
		ctx:          ctx,
		tx:           tx,
		queries:      dbgen.New(tx),
		databaseTime: databaseTime,
	}); err != nil {
		return err
	}
	// Generation checks are intentionally last. They still hold shared rows
	// through commit, making the cache write atomic with the fence, but they do
	// not add a lock-order edge while transactional follow-up hooks run.
	if fence, ok := refreshGenerationFenceFromContext(ctx); ok {
		if err := checkRefreshGenerationFence(ctx, tx, fence); err != nil {
			return err
		}
	}
	if fence, ok := branchReconciliationFenceFromContext(ctx); ok {
		if err := checkBranchReconciliationFence(ctx, tx, key, fence); err != nil {
			return err
		}
	}
	if _, err := dbgen.New(tx).AdvanceEntityObservationVersion(ctx, key); err != nil {
		return fmt.Errorf("advance observation version: %w", err)
	}
	if err := w.commitEntityTx(ctx, tx); err != nil {
		return fmt.Errorf("commit entity transaction: %w", err)
	}
	return nil
}

func checkRefreshGenerationFence(
	ctx context.Context,
	tx pgx.Tx,
	fence RefreshGenerationFence,
) error {
	if fence.Kind == "" || fence.RefreshKey == "" || fence.Generation < 0 {
		return fmt.Errorf("invalid refresh generation fence")
	}
	queries := dbgen.New(tx)
	state, err := queries.GetRefreshIntentStateForShare(
		ctx,
		dbgen.GetRefreshIntentStateForShareParams{
			Kind:       fence.Kind,
			RefreshKey: fence.RefreshKey,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if fence.Generation == 0 {
			return nil
		}
		return ErrRefreshGenerationSuperseded
	}
	if err == nil && (fence.Generation == 0 ||
		(state.Generation != fence.Generation &&
			state.CompletedGeneration > fence.Generation)) {
		return ErrRefreshGenerationSuperseded
	}
	if err != nil {
		return fmt.Errorf("read refresh generation fence: %w", err)
	}
	return nil
}

// RefreshGeneration returns the current generation for a pointer without
// creating it. A zero generation is a meaningful snapshot proving that no row
// existed when an incidental parent observation began.
func (w *EntityWriter) RefreshGeneration(
	ctx context.Context,
	kind string,
	refreshKey string,
) (int64, error) {
	if kind == "" || refreshKey == "" {
		return 0, fmt.Errorf("invalid refresh generation identity")
	}
	state, err := dbgen.New(w.pool).GetRefreshIntentState(
		ctx,
		dbgen.GetRefreshIntentStateParams{
			Kind:       kind,
			RefreshKey: refreshKey,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read refresh generation: %w", err)
	}
	return state.Generation, nil
}

func checkBranchReconciliationFence(
	ctx context.Context,
	tx pgx.Tx,
	entityKey string,
	fence *BranchReconciliationFence,
) error {
	if fence.RepoID <= 0 || fence.Branch == "" ||
		fence.BranchGeneration <= 0 || fence.RefreshKind == "" ||
		fence.RefreshKey == "" || fence.RefreshGeneration <= 0 ||
		fence.EntityKey == "" || fence.EntityKey != entityKey {
		return fmt.Errorf("invalid branch reconciliation fence")
	}
	queries := dbgen.New(tx)
	// Bulk apply materializes every page target generation. A shared row lock
	// makes the check atomic with this short entity transaction without adding
	// an advisory-lock ordering edge to the writer's transactional hooks.
	currentRefreshGeneration, err := queries.GetRefreshIntentGenerationForShare(
		ctx,
		dbgen.GetRefreshIntentGenerationForShareParams{
			Kind:       fence.RefreshKind,
			RefreshKey: fence.RefreshKey,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrBranchReconciliationSuperseded
	}
	if err != nil {
		return fmt.Errorf("read branch target generation: %w", err)
	}
	currentBranchGeneration, err :=
		queries.GetBranchReconciliationGenerationForShare(
			ctx,
			dbgen.GetBranchReconciliationGenerationForShareParams{
				RepoID: fence.RepoID,
				Branch: fence.Branch,
			},
		)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrBranchReconciliationSuperseded
		}
		return fmt.Errorf("read branch generation: %w", err)
	}
	if currentRefreshGeneration != fence.RefreshGeneration ||
		currentBranchGeneration != fence.BranchGeneration {
		return ErrBranchReconciliationSuperseded
	}
	return nil
}

func requireObservation(observation *Observation, wantKey string) error {
	if observation == nil {
		return fmt.Errorf("observation for %s is required", wantKey)
	}
	if observation.Key() != wantKey {
		return fmt.Errorf(
			"observation key %q does not match %q",
			observation.Key(),
			wantKey,
		)
	}
	return nil
}

func (w *EntityWriter) markAndEmit(
	ctx context.Context,
	queries *dbgen.Queries,
	scopes []string,
	kind string,
	entityKey string,
	at time.Time,
) error {
	if len(scopes) > 0 {
		if err := queries.MarkDerivationDirty(
			ctx,
			dbgen.MarkDerivationDirtyParams{
				MarkedAt:  timestamp(at),
				ScopeKeys: scopes,
			},
		); err != nil {
			return fmt.Errorf("mark derivation dirty: %w", err)
		}
	}
	payload := []byte(`{"version":1}`)
	seq, err := queries.InsertChangeEvent(
		ctx,
		dbgen.InsertChangeEventParams{
			Stream:     outbox.EntitiesStream,
			Kind:       kind,
			EntityKey:  entityKey,
			OccurredAt: timestamp(at),
			Payload:    payload,
		},
	)
	if err != nil {
		return fmt.Errorf("insert entity change event: %w", err)
	}
	if err := outbox.AfterSequenceAllocated(
		ctx,
		outbox.EntityWriterOrigin,
		seq,
	); err != nil {
		return fmt.Errorf("after entity change sequence allocation: %w", err)
	}
	return nil
}

func databaseClock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	now, err := dbgen.New(tx).GetDatabaseClock(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read PostgreSQL clock: %w", err)
	}
	return now.Time, nil
}

func (w *EntityWriter) commitEntityTx(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cache transaction: %w", err)
	}
	if pipeline.EventReceivedAt(ctx).IsZero() {
		return nil
	}
	committedAt, err := dbgen.New(w.pool).GetDatabaseClock(ctx)
	if err != nil {
		return fmt.Errorf(
			"read PostgreSQL clock after cache commit: %w",
			err,
		)
	}
	pipeline.MarkCacheCommitted(ctx, committedAt.Time)
	return nil
}
