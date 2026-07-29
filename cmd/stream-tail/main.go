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

	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/pkg/streamclient"
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
	return runContext(ctx, args, os.Getenv("DATABASE_URL"))
}

func runContext(
	ctx context.Context,
	args []string,
	databaseURL string,
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
		"reset the cursor after taking a cache snapshot transaction",
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

	pool, err := store.Connect(ctx, databaseURL)
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
		snapshot, err := client.Bootstrap(ctx, *consumer, *stream)
		if err != nil {
			return err
		}
		// A real consumer reads the public cache tables through snapshot.Tx and
		// replaces its projection before commit. This logger has no projection.
		if err := snapshot.Tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit bootstrap snapshot: %w", err)
		}
		slog.Info(
			"stream snapshot established",
			"consumer", *consumer,
			"stream", *stream,
			"safe_seq", snapshot.SafeSeq,
		)
	}
	err = client.Tail(
		ctx,
		*consumer,
		*stream,
		func(_ context.Context, _ pgx.Tx, event streamclient.Event) error {
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
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
