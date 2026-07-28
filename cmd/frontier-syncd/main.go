// frontier-syncd is the Frontier sync engine daemon.
//
// Usage:
//
//	frontier-syncd serve [--roles=all]   run the engine (default command)
//	frontier-syncd migrate               apply River + schema migrations
//	frontier-syncd version               print the build version
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/acme/frontier/internal/config"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "frontier-syncd:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return serve(args)
	case "migrate":
		return migrate()
	case "version":
		fmt.Println(version)
		return nil
	default:
		return fmt.Errorf("unknown command %q (want serve, migrate, or version)", cmd)
	}
}

func migrate() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.RequireDatabase(); err != nil {
		return err
	}
	ctx := context.Background()
	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return err
	}
	slog.Info("migrations applied")
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	roles := fs.String("roles", "all",
		"comma-separated roles to run: ingress,dispatch,fetch,sweep,derive,stream (or all)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.RequireDatabase(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	client, err := queue.NewClient(pool)
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}
	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("river start: %w", err)
	}
	slog.Info("frontier-syncd running", "version", version, "roles", *roles)

	<-ctx.Done()
	slog.Info("shutting down")
	return client.Stop(context.Background())
}
