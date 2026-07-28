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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/config"
	"github.com/acme/frontier/internal/dispatch"
	"github.com/acme/frontier/internal/ingress"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

var version = "dev"

const (
	gracefulShutdownTimeout = 10 * time.Second
	forcedShutdownTimeout   = 5 * time.Second
	roleIngress             = "ingress"
	roleDispatch            = "dispatch"
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
	rolesFlag := fs.String(
		"roles",
		"all",
		"comma-separated roles: ingress,dispatch, or all",
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	roles, err := parseRoles(*rolesFlag)
	if err != nil {
		return err
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.RequireDatabase(); err != nil {
		return err
	}
	if roles[roleIngress] {
		if err := cfg.RequireWebhookSecret(); err != nil {
			return err
		}
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

	serviceCtx, cancelServices := context.WithCancel(context.Background())
	defer cancelServices()
	serviceErrors := make(chan error, 2)

	var riverClient *river.Client[pgx.Tx]
	if roles[roleDispatch] {
		riverClient, err = queue.NewClient(pool)
		if err != nil {
			return fmt.Errorf("river client: %w", err)
		}
		if err := riverClient.Start(serviceCtx); err != nil {
			return fmt.Errorf("river start: %w", err)
		}
		dispatcher := dispatch.New(pool, riverClient, dispatch.Config{
			BatchSize:    cfg.DispatchBatchSize,
			MaxAttempts:  cfg.DispatchMaxAttempts,
			Debounce:     cfg.DispatchDebounce,
			PollInterval: cfg.DispatchPollInterval,
			Classifier:   dispatch.DefaultClassifier(),
		})
		go func() {
			if err := dispatcher.Run(serviceCtx); err != nil {
				serviceErrors <- fmt.Errorf("dispatcher: %w", err)
			}
		}()
	}

	var httpServer *http.Server
	if roles[roleIngress] {
		handler := ingress.NewHandler(
			dbgen.New(pool),
			cfg.GitHubWebhookSecret,
			cfg.WebhookMaxBodyBytes,
		)
		httpServer = &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           handler.Mux(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		listener, err := net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
		}
		go func() {
			err := httpServer.Serve(listener)
			if err != nil && err != http.ErrServerClosed {
				serviceErrors <- fmt.Errorf("ingress HTTP server: %w", err)
			}
		}()
	}

	slog.Info("frontier-syncd running", "version", version, "roles", *rolesFlag)
	var serviceErr error
	select {
	case <-signalCtx.Done():
	case serviceErr = <-serviceErrors:
	}
	slog.Info("shutting down")
	cancelServices()

	gracefulCtx, cancelGraceful := context.WithTimeout(
		context.Background(),
		gracefulShutdownTimeout,
	)
	if httpServer != nil {
		if err := httpServer.Shutdown(gracefulCtx); err != nil && serviceErr == nil {
			serviceErr = fmt.Errorf("ingress graceful shutdown: %w", err)
		}
	}
	if riverClient != nil {
		err = riverClient.Stop(gracefulCtx)
	}
	cancelGraceful()
	if riverClient == nil || err == nil {
		return serviceErr
	}

	slog.Warn("graceful shutdown timed out; cancelling active work", "error", err)
	forcedCtx, cancelForced := context.WithTimeout(
		context.Background(),
		forcedShutdownTimeout,
	)
	defer cancelForced()
	if forcedErr := riverClient.StopAndCancel(forcedCtx); forcedErr != nil {
		return fmt.Errorf("river forced shutdown after graceful stop failed: %w", forcedErr)
	}
	return serviceErr
}

func validateRoles(roles string) error {
	_, err := parseRoles(roles)
	return err
}

func parseRoles(raw string) (map[string]bool, error) {
	if raw == "all" {
		return map[string]bool{roleIngress: true, roleDispatch: true}, nil
	}
	roles := make(map[string]bool, 2)
	for _, role := range strings.Split(raw, ",") {
		switch role {
		case roleIngress, roleDispatch:
			roles[role] = true
		default:
			return nil, fmt.Errorf(
				"--roles=%q contains unsupported role %q (want ingress, dispatch, or all)",
				raw,
				role,
			)
		}
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("--roles must not be empty")
	}
	return roles, nil
}
