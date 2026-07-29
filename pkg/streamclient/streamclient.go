// Package streamclient is the reference implementation of Frontier's public
// Postgres change-stream contract. It owns watermark-bounded paging, durable
// cursors, transactional handler delivery, bootstrap, and RESYNC_REQUIRED.
package streamclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultBatchSize    = 256
	defaultPollInterval = 500 * time.Millisecond
	notificationChannel = "frontier_change_events"
)

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

// Snapshot is an open, repeatable-read cache snapshot paired with SafeSeq.
// The caller reads public cache tables through Tx, updates its local
// projection in that same transaction when applicable, then commits Tx. The
// consumer cursor is reset to SafeSeq only by that commit.
type Snapshot struct {
	// SafeSeq is the sequence after which Tail resumes.
	SafeSeq int64
	// Tx is the snapshot-consistent Postgres transaction over the cache.
	Tx pgx.Tx
}

// Client consumes Frontier change streams from the same Postgres database as
// the cache read model.
type Client struct {
	pool         *pgxpool.Pool
	batchSize    int
	pollInterval time.Duration
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
	}, nil
}

// Bootstrap starts a snapshot-then-stream cycle. It returns the current safe
// watermark and a repeatable-read transaction over the public cache tables.
// Committing the returned transaction atomically resets the durable cursor to
// SafeSeq; rolling it back leaves the prior cursor unchanged (C-S3/C-S4).
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

	// Establish and lock the cursor before selecting the watermark. One
	// (consumer, stream) has only one active delivery transaction (C-D4).
	if _, err := tx.Exec(ctx, `
		INSERT INTO consumer_cursors (consumer, stream, seq, updated_at)
		VALUES ($1, $2, 0, clock_timestamp())
		ON CONFLICT (consumer, stream) DO NOTHING
	`, consumer, stream); err != nil {
		return nil, fmt.Errorf("ensure bootstrap cursor: %w", err)
	}
	var prior int64
	if err := tx.QueryRow(ctx, `
		SELECT seq
		FROM consumer_cursors
		WHERE consumer = $1 AND stream = $2
		FOR UPDATE
	`, consumer, stream).Scan(&prior); err != nil {
		return nil, fmt.Errorf("lock bootstrap cursor: %w", err)
	}

	var safeSeq int64
	if err := tx.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&safeSeq); err != nil {
		return nil, fmt.Errorf("read bootstrap watermark: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE consumer_cursors
		SET seq = $3, updated_at = clock_timestamp()
		WHERE consumer = $1 AND stream = $2
	`, consumer, stream, safeSeq); err != nil {
		return nil, fmt.Errorf("reset bootstrap cursor: %w", err)
	}
	ok = true
	return &Snapshot{SafeSeq: safeSeq, Tx: tx}, nil
}

// Tail continuously pages events with seq > cursor AND seq <= safe_seq,
// invokes handler inside the cursor transaction, and waits using
// LISTEN/NOTIFY plus a polling fallback. Migration 0013's after-insert trigger
// emits frontier_change_events notifications at commit; correctness never
// depends on receiving one (C-S2/C-S5/C-P6).
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
	listener, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire stream listener: %w", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(
		ctx,
		"LISTEN "+notificationChannel,
	); err != nil {
		return fmt.Errorf("listen for change events: %w", err)
	}

	for {
		delivered, err := c.deliverPage(
			ctx, consumer, stream, handler,
		)
		if err != nil {
			return err
		}
		if delivered == c.batchSize {
			continue
		}

		waitCtx, cancel := context.WithTimeout(ctx, c.pollInterval)
		_, waitErr := listener.Conn().WaitForNotification(waitCtx)
		cancel()
		switch {
		case waitErr == nil:
		case errors.Is(waitErr, context.DeadlineExceeded):
			// Poll fallback is the correctness path (C-S5).
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			return fmt.Errorf("wait for change-event notification: %w", waitErr)
		}
	}
}

func (c *Client) deliverPage(
	ctx context.Context,
	consumer string,
	stream string,
	handler Handler,
) (int, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin stream page: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
		INSERT INTO consumer_cursors (consumer, stream, seq, updated_at)
		VALUES ($1, $2, 0, clock_timestamp())
		ON CONFLICT (consumer, stream) DO NOTHING
	`, consumer, stream); err != nil {
		return 0, fmt.Errorf("ensure consumer cursor: %w", err)
	}
	var cursor int64
	if err := tx.QueryRow(ctx, `
		SELECT seq
		FROM consumer_cursors
		WHERE consumer = $1 AND stream = $2
		FOR UPDATE
	`, consumer, stream).Scan(&cursor); err != nil {
		return 0, fmt.Errorf("lock consumer cursor: %w", err)
	}
	var prunedThrough int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE((
		    SELECT pruned_through_seq
		    FROM stream_horizons
		    WHERE stream = $1
		), 0)
	`, stream).Scan(&prunedThrough); err != nil {
		return 0, fmt.Errorf("read stream horizon: %w", err)
	}
	if cursor < prunedThrough {
		return 0, &ErrResyncRequired{
			Consumer:      consumer,
			Stream:        stream,
			Cursor:        cursor,
			PrunedThrough: prunedThrough,
		}
	}

	var safeSeq int64
	if err := tx.QueryRow(ctx, `
		SELECT safe_seq FROM stream_watermark WHERE singleton
	`).Scan(&safeSeq); err != nil {
		return 0, fmt.Errorf("read stream watermark: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT seq, stream, kind, entity_key, occurred_at, payload
		FROM change_events
		WHERE stream = $1
		  AND seq > $2
		  AND seq <= $3
		ORDER BY seq
		LIMIT $4
	`, stream, cursor, safeSeq, c.batchSize)
	if err != nil {
		return 0, fmt.Errorf("page change events: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Event])
	if err != nil {
		return 0, fmt.Errorf("scan change events: %w", err)
	}
	for _, event := range events {
		event.Payload = append(json.RawMessage(nil), event.Payload...)
		if err := handler(ctx, tx, event); err != nil {
			return 0, fmt.Errorf(
				"handle stream event %d: %w", event.Seq, err,
			)
		}
		cursor = event.Seq
	}
	if len(events) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE consumer_cursors
			SET seq = $3, updated_at = clock_timestamp()
			WHERE consumer = $1 AND stream = $2
		`, consumer, stream, cursor); err != nil {
			return 0, fmt.Errorf("advance consumer cursor: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit stream page: %w", err)
	}
	return len(events), nil
}

func validateIdentity(consumer string, stream string) error {
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
