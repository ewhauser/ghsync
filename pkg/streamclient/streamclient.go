// Package streamclient is the reference implementation of ghsync's public
// Postgres change-stream contract. It owns watermark-bounded paging, durable
// cursors, transactional handler delivery, bootstrap, and RESYNC_REQUIRED.
//
// ghsync v1 deliberately binds this library to pgx v5 and database
// co-location. External services consume the stream through this library and
// a shared Postgres database connection; v1 does not provide a wire API.
package streamclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	defaultBatchSize    = 256
	defaultPollInterval = 500 * time.Millisecond
	minListenerBackoff  = 100 * time.Millisecond
	maxListenerBackoff  = 5 * time.Second
	notificationChannel = "ghsync_change_events"
)

var errCursorFirstTouchRace = errors.New("consumer cursor first-touch race")

// ErrListenerUnavailable classifies a transient failure to acquire or use the
// dedicated LISTEN connection, including connection-pool exhaustion. Tail
// handles this condition internally by polling and reconnecting, so callers
// normally observe it only through an error wrapped by their own
// instrumentation.
var ErrListenerUnavailable = errors.New("stream listener unavailable")

// Config controls paging and the correctness-preserving poll fallback.
type Config struct {
	// BatchSize is the maximum number of events handled in one cursor
	// transaction (C-P6).
	BatchSize int
	// PollInterval bounds wake latency when LISTEN/NOTIFY is delayed or lost.
	PollInterval time.Duration
}

// Event is the stable C-S6 change-event envelope. Payload is a versioned
// reference, never an internal database row image.
type Event struct {
	// Seq is the global monotonic outbox sequence.
	Seq int64
	// Stream identifies the event tier, such as entities or work_items.
	Stream string
	// Kind identifies the additive event variant.
	Kind string
	// EntityKey is the immutable reference consumers use to fetch current state.
	EntityKey string
	// OccurredAt is the source transaction's event time.
	OccurredAt time.Time
	// Payload contains the versioned, additive reference metadata.
	Payload json.RawMessage
}

// Handler applies one event using tx. Database effects written through tx
// commit atomically with the durable cursor advance. Returning an error rolls
// the entire page back, so a restart receives the page again. External I/O
// cannot share this exactly-once transaction and must use its own idempotency.
type Handler func(context.Context, pgx.Tx, Event) error

// ErrResyncRequired is returned when a cursor is behind its stream's pruned
// horizon. Call Bootstrap and replace the local snapshot before tailing again.
type ErrResyncRequired struct {
	// Consumer is the durable consumer name.
	Consumer string
	// Stream is the expired stream.
	Stream string
	// Cursor is the consumer's last committed sequence.
	Cursor int64
	// PrunedThrough is the greatest sequence known to have been pruned.
	PrunedThrough int64
}

// Error implements error.
func (e *ErrResyncRequired) Error() string {
	return fmt.Sprintf(
		"RESYNC_REQUIRED: consumer %q stream %q cursor %d is below pruned horizon %d",
		e.Consumer,
		e.Stream,
		e.Cursor,
		e.PrunedThrough,
	)
}

// ErrCursorContention reports that multiple transactions tried to own one
// durable (consumer, stream) cursor. It is retryable, but the durable fix is to
// obey Tail's one-tailer rule rather than run competing retry loops.
type ErrCursorContention struct {
	// Consumer is the contended durable consumer name.
	Consumer string
	// Stream is the contended stream.
	Stream string
	cause  error
}

// Error implements error.
func (e *ErrCursorContention) Error() string {
	return fmt.Sprintf(
		"stream cursor contention for consumer %q stream %q: %v",
		e.Consumer,
		e.Stream,
		e.cause,
	)
}

// Unwrap returns the underlying PostgreSQL serialization error.
func (e *ErrCursorContention) Unwrap() error {
	return e.cause
}

// IsRetryable reports whether an operation may be attempted again without a
// Bootstrap. Cursor contention, transient listener/pool unavailability,
// PostgreSQL serialization/deadlock/connection-capacity failures, and pgx
// failures known to occur before a request was sent are retryable.
//
// Context cancellation and deadline expiry are not retryable with the same
// context. ErrResyncRequired is also false: it requires Bootstrap and a
// projection replacement before Tail resumes.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var contention *ErrCursorContention
	if errors.As(err, &contention) ||
		errors.Is(err, ErrListenerUnavailable) {
		return true
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case
			"08000", // connection_exception
			"08001", // sqlclient_unable_to_establish_sqlconnection
			"08003", // connection_does_not_exist
			"08004", // sqlserver_rejected_establishment_of_sqlconnection
			"08006", // connection_failure
			"08007", // transaction_resolution_unknown
			"08P01", // protocol_violation
			"40001", // serialization_failure
			"40P01", // deadlock_detected
			"53300", // too_many_connections
			"55P03", // lock_not_available
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03": // cannot_connect_now
			return true
		}
	}
	return pgconn.SafeToRetry(err)
}

// Snapshot is an open, repeatable-read cache snapshot paired with SafeSeq and
// PriorSeq. The caller reads public cache tables through Tx and replaces its
// projection in that same transaction, calls Commit to atomically reset the
// cursor, and defers Close immediately after Bootstrap.
//
// Bootstrap DISCARDS every undelivered event at or below SafeSeq for this
// consumer when Commit succeeds. Ignoring both Commit and Close silently skips
// that cursor reset and leaks a pooled connection while retaining the cursor
// row lock, which can block the consumer and eventually exhaust the pool. Tx
// remains exported for compatibility; prefer Commit and Close for lifecycle
// management.
type Snapshot struct {
	// SafeSeq is the sequence after which Tail resumes.
	SafeSeq int64
	// PriorSeq is the durable cursor value Bootstrap replaces on Commit.
	PriorSeq int64
	// Tx is the snapshot-consistent Postgres transaction over the cache.
	Tx pgx.Tx

	mu     sync.Mutex
	closed bool
}

// Commit commits the snapshot transaction, its projection replacement, and
// the cursor reset to SafeSeq. A failed Commit closes the Snapshot; callers
// must start a new Bootstrap rather than reuse it.
func (s *Snapshot) Commit(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("commit nil stream snapshot")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pgx.ErrTxClosed
	}
	s.closed = true
	tx := s.Tx
	s.mu.Unlock()
	if tx == nil {
		return fmt.Errorf("commit stream snapshot without transaction")
	}
	return tx.Commit(ctx)
}

// Close abandons an uncommitted Snapshot by rolling it back and releasing its
// pooled connection. Close is idempotent and is safe to defer immediately
// after Bootstrap; after Commit or a direct Tx finalization it returns nil.
func (s *Snapshot) Close() error {
	return s.CloseContext(context.Background())
}

// CloseContext abandons an uncommitted Snapshot using a bounded cleanup
// context derived from ctx while remaining usable after caller cancellation.
func (s *Snapshot) CloseContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	tx := s.Tx
	s.mu.Unlock()
	if tx == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	err := tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}

// Client consumes ghsync change streams from the same Postgres database as
// the cache read model.
type Client struct {
	pool         *pgxpool.Pool
	batchSize    int
	pollInterval time.Duration
	minBackoff   time.Duration
	maxBackoff   time.Duration
	testHooks    clientTestHooks
}

type clientTestHooks struct {
	afterHorizon          func()
	afterEnsureCursor     func()
	cursorFirstTouchRetry func()
	beforeListenerAcquire func()
	listenerConnected     func(uint32)
	listenerRetry         func(error, time.Duration)
	pollOnly              func()
}

// New validates configuration and constructs a reference stream client.
func New(pool *pgxpool.Pool, config Config) (*Client, error) {
	if pool == nil {
		return nil, fmt.Errorf("stream client requires Postgres")
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.BatchSize < 0 {
		return nil, fmt.Errorf("stream batch size must be positive")
	}
	if config.PollInterval < 0 {
		return nil, fmt.Errorf("stream poll interval must be positive")
	}
	return &Client{
		pool:         pool,
		batchSize:    config.BatchSize,
		pollInterval: config.PollInterval,
		minBackoff:   minListenerBackoff,
		maxBackoff:   maxListenerBackoff,
	}, nil
}

// Bootstrap starts a snapshot-then-stream cycle. It returns the current safe
// watermark, prior cursor, and a repeatable-read transaction over the public
// cache tables. Bootstrap DISCARDS every undelivered event at or below the
// returned SafeSeq when Snapshot.Commit succeeds; the caller must replace its
// projection in Snapshot.Tx before that commit. Snapshot.Close rolls back and
// preserves PriorSeq.
//
// Callers must defer Snapshot.Close immediately. Ignoring the lifecycle leaks
// a pooled connection and retains the cursor row lock, potentially blocking
// Tail and exhausting the pool (C-S3/C-S4).
func (c *Client) Bootstrap(
	ctx context.Context,
	consumer string,
	stream string,
) (*Snapshot, error) {
	if err := validateIdentity(consumer, stream); err != nil {
		return nil, err
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return nil, fmt.Errorf("begin stream bootstrap: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	queries := dbgen.New(tx)

	// Establish and lock the cursor before selecting the watermark. One
	// (consumer, stream) has only one active delivery transaction (C-D4).
	if err := queries.EnsureConsumerCursor(
		ctx,
		dbgen.EnsureConsumerCursorParams{
			Consumer: consumer,
			Stream:   stream,
		},
	); err != nil {
		if isSerializationFailure(err) {
			return nil, newCursorContention(consumer, stream, err)
		}
		return nil, fmt.Errorf("ensure bootstrap cursor: %w", err)
	}
	prior, err := queries.GetConsumerCursorForUpdate(
		ctx,
		dbgen.GetConsumerCursorForUpdateParams{
			Consumer: consumer,
			Stream:   stream,
		},
	)
	if err != nil {
		if isSerializationFailure(err) {
			return nil, newCursorContention(consumer, stream, err)
		}
		return nil, fmt.Errorf("lock bootstrap cursor: %w", err)
	}

	safeSeq, err := queries.GetStreamSafeSequence(ctx)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap watermark: %w", err)
	}
	if err := queries.UpdateConsumerCursor(
		ctx,
		dbgen.UpdateConsumerCursorParams{
			Seq:      safeSeq,
			Consumer: consumer,
			Stream:   stream,
		},
	); err != nil {
		return nil, fmt.Errorf("reset bootstrap cursor: %w", err)
	}
	ok = true
	return &Snapshot{
		SafeSeq:  safeSeq,
		PriorSeq: prior,
		Tx:       tx,
	}, nil
}

// Tail continuously pages events with seq > cursor AND seq <= safe_seq,
// invokes handler inside the cursor transaction, and waits using
// LISTEN/NOTIFY plus a polling fallback. Migration 0014's after-insert trigger
// emits ghsync_change_events notifications at commit; correctness never
// depends on receiving one (C-S2/C-S5/C-P6).
//
// Run exactly one Tail call for each (consumer, stream). A competing tailer
// returns *ErrCursorContention rather than an opaque PostgreSQL serialization
// failure. ErrCursorContention and other errors for which IsRetryable is true
// may be retried with backoff. ErrResyncRequired is terminal for this Tail
// call and requires Bootstrap plus projection replacement before resuming.
// Invalid arguments, handler failures, and context cancellation are terminal
// unless their wrapped cause is independently classified as retryable.
//
// Tail internally retries concurrent cursor first-touch races and all LISTEN
// failures, including listener connection-pool exhaustion. While LISTEN is
// unavailable it continues correctness-path polling with bounded reconnect
// backoff; listener errors are not returned to the caller.
func (c *Client) Tail(
	ctx context.Context,
	consumer string,
	stream string,
	handler Handler,
) error {
	if err := validateIdentity(consumer, stream); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("stream handler is required")
	}
	var listener *pgxpool.Conn
	defer func() { releaseListener(ctx, listener) }()
	listenerBackoff := c.minBackoff
	var nextListenerAttempt time.Time

	for {
		delivered, err := c.deliverPage(
			ctx, consumer, stream, handler,
		)
		if errors.Is(err, errCursorFirstTouchRace) {
			if hook := c.testHooks.cursorFirstTouchRetry; hook != nil {
				hook()
			}
			continue
		}
		if err != nil {
			return err
		}
		if delivered == c.batchSize {
			continue
		}

		if listener == nil &&
			!time.Now().Before(nextListenerAttempt) {
			if hook := c.testHooks.beforeListenerAcquire; hook != nil {
				hook()
			}
			listener, err = c.acquireListener(ctx)
			if err == nil {
				if hook := c.testHooks.listenerConnected; hook != nil {
					hook(listener.Conn().PgConn().PID())
				}
			} else {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				err = fmt.Errorf(
					"%w while acquiring LISTEN connection: %w",
					ErrListenerUnavailable,
					err,
				)
				nextListenerAttempt = time.Now().Add(listenerBackoff)
				if hook := c.testHooks.listenerRetry; hook != nil {
					hook(err, listenerBackoff)
				}
				listenerBackoff = growBackoff(
					listenerBackoff,
					c.maxBackoff,
				)
			}
		}

		if listener == nil {
			if hook := c.testHooks.pollOnly; hook != nil {
				hook()
			}
			timer := time.NewTimer(c.pollInterval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
				// Poll fallback remains active while LISTEN reconnects.
			}
			continue
		}

		waitCtx, cancel := context.WithTimeout(ctx, c.pollInterval)
		_, waitErr := listener.Conn().WaitForNotification(waitCtx)
		cancel()
		switch {
		case waitErr == nil:
			listenerBackoff = c.minBackoff
		case errors.Is(waitErr, context.DeadlineExceeded):
			// Poll fallback is the correctness path (C-S5).
			// One healthy wait interval proves the listener is stable enough
			// to reset churn backoff.
			listenerBackoff = c.minBackoff
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			releaseListener(ctx, listener)
			listener = nil
			waitErr = fmt.Errorf(
				"%w while waiting for notification: %w",
				ErrListenerUnavailable,
				waitErr,
			)
			nextListenerAttempt = time.Now().Add(listenerBackoff)
			if hook := c.testHooks.listenerRetry; hook != nil {
				hook(waitErr, listenerBackoff)
			}
			listenerBackoff = growBackoff(
				listenerBackoff,
				c.maxBackoff,
			)
			// A failed LISTEN connection only loses the latency hint. The next
			// loop performs another correctness-path page poll immediately.
		}
	}
}

func (c *Client) acquireListener(ctx context.Context) (*pgxpool.Conn, error) {
	timeout := min(c.pollInterval, 250*time.Millisecond)
	acquireCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	listener, err := c.pool.Acquire(acquireCtx)
	if err != nil {
		return nil, err
	}
	// Raw SQL exception: PostgreSQL does not parameterize LISTEN channel
	// identifiers, so this fixed protocol channel cannot be expressed in sqlc.
	if _, err := listener.Exec(
		acquireCtx,
		"LISTEN "+notificationChannel,
	); err != nil {
		listener.Release()
		return nil, err
	}
	return listener, nil
}

func releaseListener(ctx context.Context, listener *pgxpool.Conn) {
	if listener == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	// Raw SQL exception: PostgreSQL does not parameterize UNLISTEN channel
	// identifiers, so this fixed protocol channel cannot be expressed in sqlc.
	_, _ = listener.Exec(ctx, "UNLISTEN "+notificationChannel)
	listener.Release()
}

func growBackoff(
	current time.Duration,
	maximum time.Duration,
) time.Duration {
	current *= 2
	if current > maximum {
		return maximum
	}
	return current
}

func (c *Client) deliverPage(
	ctx context.Context,
	consumer string,
	stream string,
	handler Handler,
) (int, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return 0, fmt.Errorf("begin stream page: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	queries := dbgen.New(tx)

	if err := queries.EnsureConsumerCursor(
		ctx,
		dbgen.EnsureConsumerCursorParams{
			Consumer: consumer,
			Stream:   stream,
		},
	); err != nil {
		if isSerializationFailure(err) {
			return 0, errCursorFirstTouchRace
		}
		return 0, fmt.Errorf("ensure consumer cursor: %w", err)
	}
	if hook := c.testHooks.afterEnsureCursor; hook != nil {
		hook()
	}
	cursor, err := queries.GetConsumerCursorForUpdate(
		ctx,
		dbgen.GetConsumerCursorForUpdateParams{
			Consumer: consumer,
			Stream:   stream,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// REPEATABLE READ can observe a concurrent INSERT's uniqueness conflict
		// without seeing the newly committed row in this transaction snapshot.
		// Tail retries in a new snapshot; no handler or cursor effects exist yet.
		return 0, errCursorFirstTouchRace
	}
	if isSerializationFailure(err) {
		return 0, newCursorContention(consumer, stream, err)
	}
	if err != nil {
		return 0, fmt.Errorf("lock consumer cursor: %w", err)
	}
	prunedThrough, err := queries.GetStreamPrunedThroughSequence(ctx, stream)
	if err != nil {
		return 0, fmt.Errorf("read stream horizon: %w", err)
	}
	if cursor < prunedThrough {
		resyncErr := &ErrResyncRequired{
			Consumer:      consumer,
			Stream:        stream,
			Cursor:        cursor,
			PrunedThrough: prunedThrough,
		}
		// C-S4/C-O4: record the protocol-level resync before returning it.
		// There are no event-handler effects in this transaction, so committing
		// the monotonic counter leaves the consumer cursor unchanged.
		if err := queries.RecordConsumerResync(
			ctx,
			dbgen.RecordConsumerResyncParams{
				Consumer: consumer,
				Stream:   stream,
			},
		); err != nil {
			return 0, fmt.Errorf("record stream resync: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit stream resync: %w", err)
		}
		return 0, resyncErr
	}
	if hook := c.testHooks.afterHorizon; hook != nil {
		hook()
	}

	safeSeq, err := queries.GetStreamSafeSequence(ctx)
	if err != nil {
		return 0, fmt.Errorf("read stream watermark: %w", err)
	}
	eventRows, err := queries.PageChangeEvents(
		ctx,
		dbgen.PageChangeEventsParams{
			Stream:     stream,
			AfterSeq:   cursor,
			ThroughSeq: safeSeq,
			PageSize:   int32(c.batchSize),
		},
	)
	if err != nil {
		return 0, fmt.Errorf("page change events: %w", err)
	}
	events := make([]Event, 0, len(eventRows))
	for _, row := range eventRows {
		events = append(events, Event{
			Seq:        row.Seq,
			Stream:     row.Stream,
			Kind:       row.Kind,
			EntityKey:  row.EntityKey,
			OccurredAt: row.OccurredAt.Time,
			Payload:    append(json.RawMessage(nil), row.Payload...),
		})
	}
	for _, event := range events {
		if err := handler(ctx, tx, event); err != nil {
			return 0, fmt.Errorf(
				"handle stream event %d: %w", event.Seq, err,
			)
		}
		cursor = event.Seq
	}
	if len(events) > 0 {
		if err := queries.UpdateConsumerCursor(
			ctx,
			dbgen.UpdateConsumerCursorParams{
				Seq:      cursor,
				Consumer: consumer,
				Stream:   stream,
			},
		); err != nil {
			return 0, fmt.Errorf("advance consumer cursor: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit stream page: %w", err)
	}
	return len(events), nil
}

func isSerializationFailure(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		postgresError != nil &&
		postgresError.Code == "40001"
}

func newCursorContention(
	consumer string,
	stream string,
	cause error,
) *ErrCursorContention {
	return &ErrCursorContention{
		Consumer: consumer,
		Stream:   stream,
		cause:    cause,
	}
}

func validateIdentity(consumer, stream string) error {
	if strings.TrimSpace(consumer) == "" {
		return fmt.Errorf("consumer name is required")
	}
	if strings.TrimSpace(stream) == "" {
		return fmt.Errorf("stream name is required")
	}
	if strings.IndexByte(consumer, 0) >= 0 ||
		strings.IndexByte(stream, 0) >= 0 {
		return fmt.Errorf("consumer and stream names must not contain NUL")
	}
	return nil
}
