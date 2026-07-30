package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/pkg/streamclient"
)

type loadStreamConsumer struct {
	pool        *pgxpool.Pool
	consumer    string
	tableSQL    string
	initialSeq  int64
	restartSeq  int64
	restartRows int64
	restarted   bool
	cancel      context.CancelFunc
	done        chan error
	terminalErr error
}

type streamCounts struct {
	total    int64
	distinct int64
	expected int64
	cursor   int64
	maxSeq   int64
}

func newLoadStreamConsumer(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
) (*loadStreamConsumer, error) {
	if _, err := strconv.ParseInt(runID, 36, 64); err != nil {
		return nil, fmt.Errorf("load stream run ID is invalid: %w", err)
	}
	table := "ghsync_loadgen_applied_" + runID
	tableSQL := pgx.Identifier{table}.Sanitize()
	// Raw SQL exception: this run-scoped scratch-table identifier is dynamic,
	// sanitized, and cannot be parameterized by PostgreSQL or modeled by sqlc.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE `+tableSQL+` (
		    seq BIGINT PRIMARY KEY,
		    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)
	`); err != nil {
		return nil, fmt.Errorf("create load stream applied set: %w", err)
	}
	consumer := &loadStreamConsumer{
		pool:     pool,
		consumer: "ghsync-loadgen-" + runID,
		tableSQL: tableSQL,
	}
	client, err := streamclient.New(pool, streamclient.Config{
		BatchSize:    256,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		consumer.cleanup(ctx)
		return nil, fmt.Errorf("create load stream client: %w", err)
	}
	snapshot, err := client.Bootstrap(ctx, consumer.consumer, loadStream)
	if err != nil {
		consumer.cleanup(ctx)
		return nil, fmt.Errorf("bootstrap load stream consumer: %w", err)
	}
	consumer.initialSeq = snapshot.SafeSeq
	if err := snapshot.Tx.Commit(ctx); err != nil {
		_ = snapshot.Tx.Rollback(context.WithoutCancel(ctx))
		consumer.cleanup(ctx)
		return nil, fmt.Errorf("commit load stream bootstrap: %w", err)
	}
	return consumer, nil
}

func (c *loadStreamConsumer) start(ctx context.Context) error {
	if c.cancel != nil || c.done != nil {
		return fmt.Errorf("load stream consumer is already running")
	}
	client, err := streamclient.New(c.pool, streamclient.Config{
		BatchSize:    256,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("restart load stream client: %w", err)
	}
	tailCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan error, 1)
	go func(done chan<- error) {
		done <- client.Tail(
			tailCtx,
			c.consumer,
			loadStream,
			func(
				ctx context.Context,
				tx pgx.Tx,
				event streamclient.Event,
			) error {
				// Raw SQL exception: this INSERT targets the dynamic,
				// sanitized run-scoped scratch table created above.
				if _, err := tx.Exec(
					ctx,
					"INSERT INTO "+c.tableSQL+" (seq) VALUES ($1)",
					event.Seq,
				); err != nil {
					return fmt.Errorf(
						"apply stream seq %d exactly once: %w",
						event.Seq,
						err,
					)
				}
				return nil
			},
		)
	}(c.done)
	return nil
}

func (c *loadStreamConsumer) check() error {
	if c.terminalErr != nil {
		return c.terminalErr
	}
	if c.done == nil {
		return nil
	}
	select {
	case err := <-c.done:
		c.done = nil
		c.cancel = nil
		c.terminalErr = fmt.Errorf("load stream consumer stopped: %w", err)
		return c.terminalErr
	default:
		return nil
	}
}

func (c *loadStreamConsumer) restart(ctx context.Context) error {
	if c.restarted {
		return fmt.Errorf("load stream consumer restarted more than once")
	}
	if err := c.stop(ctx); err != nil {
		return fmt.Errorf("stop load stream consumer for restart: %w", err)
	}
	counts, err := c.counts(ctx)
	if err != nil {
		return err
	}
	if counts.cursor <= c.initialSeq || counts.total == 0 {
		return fmt.Errorf(
			"mid-load stream restart observed no committed progress "+
				"(initial_seq=%d cursor=%d applied=%d)",
			c.initialSeq,
			counts.cursor,
			counts.total,
		)
	}
	c.restartSeq = counts.cursor
	c.restartRows = counts.total
	c.restarted = true
	if err := c.start(ctx); err != nil {
		return err
	}
	return nil
}

func (c *loadStreamConsumer) restartAfterProgress(
	ctx context.Context,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := c.check(); err != nil {
			return err
		}
		counts, err := c.counts(ctx)
		if err != nil {
			return err
		}
		if counts.cursor > c.initialSeq && counts.total > 0 {
			return c.restart(ctx)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"load stream made no progress before restart deadline "+
					"(initial_seq=%d cursor=%d applied=%d)",
				c.initialSeq,
				counts.cursor,
				counts.total,
			)
		}
		if err := wait(ctx, 25*time.Millisecond); err != nil {
			return err
		}
	}
}

func (c *loadStreamConsumer) stop(ctx context.Context) error {
	if c.cancel == nil || c.done == nil {
		return c.check()
	}
	cancel := c.cancel
	done := c.done
	c.cancel = nil
	c.done = nil
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("stop load stream consumer: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *loadStreamConsumer) caughtUp(ctx context.Context) (bool, error) {
	counts, err := c.counts(ctx)
	if err != nil {
		return false, err
	}
	exactlyOnce := counts.total == counts.distinct
	fullyApplied := counts.total == counts.expected
	return exactlyOnce && fullyApplied &&
		counts.cursor == counts.maxSeq, nil
}

func (c *loadStreamConsumer) assertFinal(
	ctx context.Context,
) (streamCounts, error) {
	if !c.restarted {
		return streamCounts{}, fmt.Errorf(
			"load stream consumer never completed its mid-run restart",
		)
	}
	counts, err := c.counts(ctx)
	if err != nil {
		return streamCounts{}, err
	}
	if counts.total != counts.distinct ||
		counts.total != counts.expected {
		return streamCounts{}, fmt.Errorf(
			"stream exactly-once assertion failed: "+
				"count(*)=%d count(distinct seq)=%d expected=%d",
			counts.total,
			counts.distinct,
			counts.expected,
		)
	}
	if counts.cursor != counts.maxSeq {
		return streamCounts{}, fmt.Errorf(
			"stream cursor did not reach final entity sequence: "+
				"cursor=%d max_seq=%d",
			counts.cursor,
			counts.maxSeq,
		)
	}
	if c.restartSeq <= c.initialSeq ||
		counts.cursor <= c.restartSeq ||
		counts.total <= c.restartRows {
		return streamCounts{}, fmt.Errorf(
			"stream cursor did not prove durability across restart: "+
				"initial_seq=%d restart_seq=%d final_seq=%d "+
				"restart_rows=%d final_rows=%d",
			c.initialSeq,
			c.restartSeq,
			counts.cursor,
			c.restartRows,
			counts.total,
		)
	}
	return counts, nil
}

func (c *loadStreamConsumer) counts(ctx context.Context) (streamCounts, error) {
	var result streamCounts
	// Raw SQL exception: the aggregate reads the dynamic, sanitized run-scoped
	// scratch table; PostgreSQL cannot parameterize that table identifier.
	err := c.pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM `+c.tableSQL+`),
		    (SELECT count(DISTINCT seq) FROM `+c.tableSQL+`),
		    count(events.seq),
		    COALESCE(cursor.seq, $2),
		    COALESCE(max(events.seq), $2)
		FROM stream_watermark AS watermark
		LEFT JOIN change_events AS events
		  ON events.stream = $1
		 AND events.seq > $2
		 AND events.seq <= watermark.safe_seq
		LEFT JOIN consumer_cursors AS cursor
		  ON cursor.consumer = $3
		 AND cursor.stream = $1
		WHERE watermark.singleton
		GROUP BY cursor.seq
	`, loadStream, c.initialSeq, c.consumer).Scan(
		&result.total,
		&result.distinct,
		&result.expected,
		&result.cursor,
		&result.maxSeq,
	)
	if err != nil {
		return streamCounts{}, fmt.Errorf(
			"read load stream applied set: %w",
			err,
		)
	}
	return result, nil
}

func (c *loadStreamConsumer) cleanup(parent context.Context) {
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.WithoutCancel(parent),
		5*time.Second,
	)
	defer cancel()
	_ = c.stop(ctx)
	if c.tableSQL != "" {
		// Raw SQL exception: this DROP targets the dynamic, sanitized
		// run-scoped scratch-table identifier.
		_, _ = c.pool.Exec(ctx, "DROP TABLE IF EXISTS "+c.tableSQL)
	}
	if c.consumer != "" {
		_ = dbgen.New(c.pool).DeleteLoadgenConsumerCursor(
			ctx,
			dbgen.DeleteLoadgenConsumerCursorParams{
				Consumer: c.consumer,
				Stream:   loadStream,
			},
		)
	}
}
