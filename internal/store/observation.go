package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Observation owns C-C1's narrowly allowed C-C6 exception: a session-level
// advisory lock on one dedicated connection, held across one entity's GitHub
// fetch and write. No transaction is open during the network call, and shared
// repository metadata locks must use a shorter post-fetch scope.
type Observation struct {
	conn *pgxpool.Conn
	key  string

	mu     sync.Mutex
	closed bool
}

// Key returns the entity key protected by the observation.
func (o *Observation) Key() string {
	if o == nil {
		return ""
	}
	return o.key
}

// Close releases the observation's advisory lock and dedicated connection.
func (o *Observation) Close() error {
	return o.CloseContext(context.Background())
}

// CloseContext releases the observation using a bounded cleanup context
// derived from ctx while remaining usable after caller cancellation.
func (o *Observation) CloseContext(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	conn := o.conn
	o.conn = nil
	o.mu.Unlock()

	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()
	unlocked, err := dbgen.New(conn).ReleaseEntitySessionLock(cleanupCtx, o.key)
	if err == nil && unlocked {
		conn.Release()
		return nil
	}
	if err == nil {
		err = errors.New("pg_advisory_unlock reported lock not held")
	}

	// C-C6: an unlock error leaves the session-lock state unknown. Hijack the
	// physical connection so pgxpool can never lend that session to another
	// borrower, then close its socket; PostgreSQL releases session locks when
	// it observes backend teardown. Close always closes the underlying socket,
	// even if the graceful Terminate exchange itself reports an error.
	destroyErr := destroyObservationConnection(cleanupCtx, conn)
	releaseErr := fmt.Errorf("release observation lock %s: %w", o.key, err)
	if destroyErr != nil {
		return errors.Join(
			releaseErr,
			fmt.Errorf("destroy observation connection %s: %w", o.key, destroyErr),
		)
	}
	return releaseErr
}

func destroyObservationConnection(
	ctx context.Context,
	conn *pgxpool.Conn,
) error {
	physical := conn.Hijack()
	destroyCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		time.Second,
	)
	defer cancel()
	// pgconn.Close defers the underlying net.Conn.Close, so the physical socket
	// is closed even if its bounded graceful Terminate exchange reports an
	// error. PostgreSQL then tears down the backend and its session locks.
	return physical.Close(destroyCtx)
}

func (o *Observation) begin(ctx context.Context) (pgx.Tx, error) {
	if o == nil {
		return nil, fmt.Errorf("observation is required")
	}
	o.mu.Lock()
	closed := o.closed
	conn := o.conn
	o.mu.Unlock()
	if closed || conn == nil {
		return nil, fmt.Errorf("observation %s is closed", o.key)
	}
	return conn.Begin(ctx)
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

// BeginObservation acquires C-C1's dedicated session-level advisory lock for
// entityKey. See C-C6 before extending its lifetime or nesting observations.
func (w *EntityWriter) BeginObservation(
	ctx context.Context,
	entityKey string,
) (*Observation, error) {
	if entityKey == "" {
		return nil, fmt.Errorf("observation entity key is required")
	}
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire observation connection: %w", err)
	}
	if err := dbgen.New(conn).AcquireEntitySessionLock(ctx, entityKey); err != nil {
		lockErr := fmt.Errorf("lock observation %s: %w", entityKey, err)
		// A failed or canceled lock statement has ambiguous server-side state.
		// Never return that physical session to pgxpool.
		if destroyErr := destroyObservationConnection(ctx, conn); destroyErr != nil {
			return nil, errors.Join(
				lockErr,
				fmt.Errorf(
					"destroy failed observation connection %s: %w",
					entityKey,
					destroyErr,
				),
			)
		}
		return nil, lockErr
	}
	return &Observation{conn: conn, key: entityKey}, nil
}

func (w *EntityWriter) beginEntityTx(
	ctx context.Context,
	observation *Observation,
	key string,
) (pgx.Tx, error) {
	var tx pgx.Tx
	var err error
	if observation != nil {
		if err := requireObservation(observation, key); err != nil {
			return nil, err
		}
		tx, err = observation.begin(ctx)
	} else {
		tx, err = w.pool.Begin(ctx)
		if err == nil {
			err = dbgen.New(tx).AcquireEntityAdvisoryLock(ctx, key)
			if err != nil {
				err = fmt.Errorf("lock %s: %w", key, err)
			}
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
