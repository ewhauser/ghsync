// frontier-syncd is the Frontier sync engine daemon.
//
// Usage:
//
//	frontier-syncd serve [--roles=all]   run the engine (default command)
//	frontier-syncd migrate               apply River + schema migrations
//	frontier-syncd requeue --guid=...    replay parked deliveries
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
	httpReadTimeout        = 10 * time.Second
	webhookRequestTimeout  = 9 * time.Second
	httpShutdownTimeout    = 10 * time.Second
	riverShutdownTimeout   = 10 * time.Second
	riverForcedStopTimeout = 5 * time.Second
	roleIngress            = "ingress"
	roleDispatch           = "dispatch"
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
	case "requeue":
		return requeue(args)
	case "version":
		fmt.Println(version)
		return nil
	default:
		return fmt.Errorf(
			"unknown command %q (want serve, migrate, requeue, or version)",
			cmd,
		)
	}
}

type requeueOptions struct {
	guid      string
	allParked bool
}

func parseRequeueOptions(args []string) (requeueOptions, error) {
	fs := flag.NewFlagSet("requeue", flag.ContinueOnError)
	guid := fs.String("guid", "", "parked delivery GUID to requeue")
	allParked := fs.Bool("all-parked", false, "requeue every parked delivery")
	if err := fs.Parse(args); err != nil {
		return requeueOptions{}, err
	}
	if fs.NArg() != 0 {
		return requeueOptions{}, fmt.Errorf("requeue does not accept positional arguments")
	}
	if (*guid == "") == !*allParked {
		return requeueOptions{}, fmt.Errorf(
			"requeue requires exactly one of --guid=... or --all-parked",
		)
	}
	return requeueOptions{guid: *guid, allParked: *allParked}, nil
}

func requeue(args []string) error {
	options, err := parseRequeueOptions(args)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	count, err := dbgen.New(pool).RequeueParkedWebhookDeliveries(
		ctx,
		dbgen.RequeueParkedWebhookDeliveriesParams{
			AllParked:    options.allParked,
			DeliveryGuid: options.guid,
		},
	)
	if err != nil {
		return fmt.Errorf("requeue parked deliveries: %w", err)
	}
	if !options.allParked && count != 1 {
		return fmt.Errorf(
			"parked delivery %q was not requeued (not found or not parked)",
			options.guid,
		)
	}
	slog.Info("parked deliveries requeued", "count", count)
	return nil
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
			webhookRequestTimeout,
		)
		httpServer = &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           handler.Mux(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       httpReadTimeout,
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

	if httpServer != nil {
		httpCtx, cancelHTTP := context.WithTimeout(
			context.Background(),
			httpShutdownTimeout,
		)
		httpErr := httpServer.Shutdown(httpCtx)
		cancelHTTP()
		if httpErr != nil && serviceErr == nil {
			serviceErr = fmt.Errorf("ingress graceful shutdown: %w", httpErr)
		}
	}
	if riverClient != nil {
		riverCtx, cancelRiver := context.WithTimeout(
			context.Background(),
			riverShutdownTimeout,
		)
		riverErr := riverClient.Stop(riverCtx)
		cancelRiver()
		if riverErr != nil {
			slog.Warn(
				"River graceful shutdown timed out; cancelling active work",
				"error",
				riverErr,
			)
			forcedCtx, cancelForced := context.WithTimeout(
				context.Background(),
				riverForcedStopTimeout,
			)
			forcedErr := riverClient.StopAndCancel(forcedCtx)
			cancelForced()
			if forcedErr != nil {
				return fmt.Errorf(
					"river forced shutdown after graceful stop failed: %w",
					forcedErr,
				)
			}
		}
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
