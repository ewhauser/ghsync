package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Observation owns a session-level advisory lock on one dedicated connection.
// It is held from before the GitHub call until the writer transaction commits.
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
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := dbgen.New(o.conn).ReleaseEntitySessionLock(ctx, o.key)
	o.conn.Release()
	if err != nil {
		return fmt.Errorf("release observation lock %s: %w", o.key, err)
	}
	return nil
}

func (o *Observation) begin(ctx context.Context) (pgx.Tx, error) {
	if o == nil || o.conn == nil {
		return nil, fmt.Errorf("observation is required")
	}
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("observation %s is closed", o.key)
	}
	return o.conn.Begin(ctx)
}

// EntityWriter owns C-C1..C-C5. Network fetches never enter this package.
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

// BeginObservation acquires a session-level advisory lock for entityKey.
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
		conn.Release()
		return nil, fmt.Errorf("lock observation %s: %w", entityKey, err)
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
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

type entityTx struct {
	ctx          context.Context
	tx           pgx.Tx
	queries      *dbgen.Queries
	databaseTime time.Time
}

type entityTxFunc func(entityTx) error

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
	defer tx.Rollback(ctx) //nolint:errcheck

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
	if err := w.commitEntityTx(ctx, tx); err != nil {
		return fmt.Errorf("commit entity transaction: %w", err)
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
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read PostgreSQL clock: %w", err)
	}
	return now, nil
}

func (w *EntityWriter) commitEntityTx(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cache transaction: %w", err)
	}
	if pipeline.EventReceivedAt(ctx).IsZero() {
		return nil
	}
	var committedAt time.Time
	if err := w.pool.QueryRow(
		ctx,
		`SELECT clock_timestamp()`,
	).Scan(&committedAt); err != nil {
		return fmt.Errorf(
			"read PostgreSQL clock after cache commit: %w",
			err,
		)
	}
	pipeline.MarkCacheCommitted(ctx, committedAt)
	return nil
}
