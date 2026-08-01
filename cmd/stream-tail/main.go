// stream-tail is the reference example consumer for pkg/streamclient.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/pkg/streamclient"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "stream-tail:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()
	databaseAuth, err := config.ParseDatabaseAuth(os.Getenv("DATABASE_AUTH"))
	if err != nil {
		return err
	}
	return runContext(ctx, args, os.Getenv("DATABASE_URL"), databaseAuth)
}

func runContext(
	ctx context.Context,
	args []string,
	databaseURL string,
	databaseAuth config.DatabaseAuth,
) error {
	fs := flag.NewFlagSet("stream-tail", flag.ContinueOnError)
	consumer := fs.String(
		"consumer", "stream-tail-example", "durable consumer name",
	)
	stream := fs.String(
		"stream", "entities", "change-event stream name",
	)
	bootstrap := fs.Bool(
		"bootstrap",
		false,
		"replace this logging stub's snapshot (throwaway consumer names only)",
	)
	batchSize := fs.Int("batch-size", 256, "events per cursor transaction")
	poll := fs.Duration(
		"poll-interval",
		500*time.Millisecond,
		"correctness fallback poll interval",
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("stream-tail does not accept positional arguments")
	}
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	var connectOptions []store.ConnectOption
	if databaseAuth == config.DatabaseAuthRDSIAM {
		connectOptions = append(connectOptions, store.WithRDSIAMAuthentication())
	}
	pool, err := store.Connect(ctx, databaseURL, connectOptions...)
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := streamclient.New(pool, streamclient.Config{
		BatchSize:    *batchSize,
		PollInterval: *poll,
	})
	if err != nil {
		return err
	}
	if *bootstrap {
		if err := bootstrapAndRebuild(
			ctx, client, *consumer, *stream,
		); err != nil {
			return err
		}
	}
	err = tailWithResync(ctx, client, *consumer, *stream)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// tailWithResync is THE reference implementation of ghsync's consumer
// resync protocol: detect ErrResyncRequired, Bootstrap and rebuild the
// projection transactionally, then resume Tail from the newly committed
// cursor. Production consumers should preserve this loop and replace only the
// projection stub and event handler with their own transactional writes.
func tailWithResync(
	ctx context.Context,
	client *streamclient.Client,
	consumer string,
	stream string,
) error {
	for {
		err := client.Tail(
			ctx,
			consumer,
			stream,
			func(
				_ context.Context,
				_ pgx.Tx,
				event streamclient.Event,
			) error {
				slog.Info(
					"change event",
					"seq", event.Seq,
					"stream", event.Stream,
					"kind", event.Kind,
					"entity_key", event.EntityKey,
					"occurred_at", event.OccurredAt,
					"payload", string(event.Payload),
				)
				return nil
			},
		)
		var resync *streamclient.ErrResyncRequired
		if errors.As(err, &resync) {
			slog.Warn(
				"stream resync required; rebuilding projection",
				"consumer", resync.Consumer,
				"stream", resync.Stream,
				"cursor", resync.Cursor,
				"pruned_through", resync.PrunedThrough,
			)
			if err := bootstrapAndRebuild(
				ctx, client, consumer, stream,
			); err != nil {
				return fmt.Errorf("resync stream: %w", err)
			}
			continue
		}
		if !streamclient.IsRetryable(err) {
			return err
		}
		slog.Warn(
			"retryable stream error; restarting tail",
			"consumer", consumer,
			"stream", stream,
			"error", err,
		)
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func bootstrapAndRebuild(
	ctx context.Context,
	client *streamclient.Client,
	consumer string,
	stream string,
) (resultErr error) {
	snapshot, err := client.Bootstrap(ctx, consumer, stream)
	if err != nil {
		return fmt.Errorf("bootstrap stream snapshot: %w", err)
	}
	defer func() {
		if closeErr := snapshot.CloseContext(ctx); resultErr == nil &&
			closeErr != nil {
			resultErr = fmt.Errorf(
				"close bootstrap snapshot: %w",
				closeErr,
			)
		}
	}()
	// The projection replacement and cursor reset share snapshot.Tx. A real
	// consumer deletes/replaces its projection from ghsync's public cache
	// tables here. This logging reference has no materialized projection, so
	// its rebuild is intentionally a stub. Never move a real rebuild after
	// Commit: doing so would acknowledge events without applying their state.
	if err := rebuildProjection(ctx, snapshot.Tx); err != nil {
		return fmt.Errorf("rebuild projection: %w", err)
	}
	if err := snapshot.Commit(ctx); err != nil {
		return fmt.Errorf("commit bootstrap snapshot: %w", err)
	}
	slog.Info(
		"stream snapshot established",
		"consumer", consumer,
		"stream", stream,
		"safe_seq", snapshot.SafeSeq,
		"prior_seq", snapshot.PriorSeq,
	)
	return nil
}

func rebuildProjection(context.Context, pgx.Tx) error {
	// Projection stub for the logging CLI. Production reference consumers
	// replace their complete projection here using only the supplied tx.
	return nil
}
