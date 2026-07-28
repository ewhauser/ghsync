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
	"time"

	"github.com/acme/frontier/internal/config"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
)

var version = "dev"

const (
	gracefulShutdownTimeout = 10 * time.Second
	forcedShutdownTimeout   = 5 * time.Second
)

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
	roles := fs.String("roles", "all", "role set to run (M0 supports only all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := validateRoles(*roles); err != nil {
		return err
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.RequireDatabase(); err != nil {
		return err
	}

	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stopSignals()

	pool, err := store.Connect(signalCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	client, err := queue.NewClient(pool)
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}
	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	if err := client.Start(startCtx); err != nil {
		return fmt.Errorf("river start: %w", err)
	}
	slog.Info("frontier-syncd running", "version", version, "roles", *roles)

	<-signalCtx.Done()
	slog.Info("shutting down")

	gracefulCtx, cancelGraceful := context.WithTimeout(
		context.Background(),
		gracefulShutdownTimeout,
	)
	err = client.Stop(gracefulCtx)
	cancelGraceful()
	if err == nil {
		return nil
	}

	slog.Warn("graceful shutdown timed out; cancelling active work", "error", err)
	forcedCtx, cancelForced := context.WithTimeout(
		context.Background(),
		forcedShutdownTimeout,
	)
	defer cancelForced()
	if forcedErr := client.StopAndCancel(forcedCtx); forcedErr != nil {
		return fmt.Errorf("river forced shutdown after graceful stop failed: %w", forcedErr)
	}
	return nil
}

func validateRoles(roles string) error {
	if roles != "all" {
		return fmt.Errorf("--roles=%q is unsupported in M0; only --roles=all is available", roles)
	}
	return nil
}
