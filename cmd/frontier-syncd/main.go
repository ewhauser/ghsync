// frontier-syncd is the Frontier sync engine daemon.
//
// Usage:
//
//	frontier-syncd serve [--roles=all]   run the engine (default command)
//	frontier-syncd migrate               apply River + schema migrations
//	frontier-syncd backfill              start/resume installation backfill
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

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/config"
	"github.com/acme/frontier/internal/derive"
	"github.com/acme/frontier/internal/dispatch"
	"github.com/acme/frontier/internal/drift"
	"github.com/acme/frontier/internal/fetch"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/ingress"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
	streammaint "github.com/acme/frontier/internal/stream"
	"github.com/acme/frontier/internal/sweep"
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
	roleFetch              = "fetch"
	roleSweep              = "sweep"
	roleDrift              = "drift"
	rolePruner             = "pruner"
	roleWatermarker        = "watermarker"
	roleDeriver            = "deriver"
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
	case "backfill":
		return backfill(args)
	case "version":
		fmt.Println(version)
		return nil
	default:
		return fmt.Errorf(
			"unknown command %q (want serve, migrate, requeue, backfill, or version)",
			cmd,
		)
	}
}

func backfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("backfill does not accept positional arguments")
	}
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.RequireDatabase(); err != nil {
		return err
	}
	if cfg.GitHubInstallationID <= 0 {
		return fmt.Errorf("GITHUB_INSTALLATION_ID is required for backfill")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	riverClient, err := queue.NewClient(pool)
	if err != nil {
		return fmt.Errorf("River client: %w", err)
	}
	cursor, err := fetch.StartInstallationBackfill(
		ctx,
		pool,
		riverClient,
		cfg.GitHubInstallationID,
	)
	if err != nil {
		return err
	}
	slog.Info(
		"backfill started or resumed",
		"installation_id",
		cfg.GitHubInstallationID,
		"phase",
		cursor.Phase,
		"page",
		cursor.Page,
	)
	return nil
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
		"comma-separated roles: ingress,dispatch,fetch,sweep,drift,pruner,watermarker,deriver, or all",
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
	var classifier dispatch.Classifier
	if roles[roleDispatch] {
		classifier, err = dispatcherClassifier(cfg.DispatchRulesFile)
		if err != nil {
			return err
		}
	}
	if roles[roleIngress] {
		if err := cfg.RequireWebhookSecret(); err != nil {
			return err
		}
	}
	if roles[roleFetch] || roles[roleSweep] || roles[roleDrift] {
		if err := cfg.RequireFetchCredentials(); err != nil {
			return err
		}
	}
	worksJobs := roles[roleFetch] || roles[roleSweep] ||
		roles[roleDrift] || roles[rolePruner]
	if worksJobs && cfg.GitHubInstallationID <= 0 {
		return fmt.Errorf(
			"GITHUB_INSTALLATION_ID is required for periodic scheduling",
		)
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
	serviceErrors := make(chan error, 8)

	if roles[roleWatermarker] {
		owner, hostErr := os.Hostname()
		if hostErr != nil {
			return fmt.Errorf("watermarker lease owner: %w", hostErr)
		}
		watermarker, createErr := streammaint.NewWatermarker(
			pool,
			streammaint.WatermarkOptions{
				RefreshInterval: cfg.WatermarkRefresh,
				LeaseTTL:        cfg.WatermarkLeaseTTL,
				Owner:           owner,
			},
		)
		if createErr != nil {
			return createErr
		}
		go func() {
			if runErr := watermarker.Run(serviceCtx); runErr != nil {
				serviceErrors <- fmt.Errorf("watermarker: %w", runErr)
			}
		}()
	}
	if roles[roleDeriver] {
		deriver, createErr := derive.New(derive.Options{
			Pool:         pool,
			Deriver:      derive.NoopDeriver{},
			DirtyCap:     cfg.DeriverDirtyCap,
			PollInterval: cfg.DeriverPollInterval,
		})
		if createErr != nil {
			return createErr
		}
		go func() {
			if runErr := deriver.Run(serviceCtx); runErr != nil {
				serviceErrors <- fmt.Errorf("deriver: %w", runErr)
			}
		}()
	}
	if roles[rolePruner] {
		retention, createErr := streammaint.NewRetention(
			pool,
			streammaint.RetentionOptions{
				Age:       cfg.StreamRetentionAge,
				Period:    cfg.StreamRetentionPeriod,
				BatchSize: cfg.StreamRetentionBatch,
			},
		)
		if createErr != nil {
			return createErr
		}
		go func() {
			if runErr := retention.Run(serviceCtx); runErr != nil {
				serviceErrors <- fmt.Errorf(
					"change-event retention: %w", runErr,
				)
			}
		}()
	}

	var riverClient *river.Client[pgx.Tx]
	var githubGate *budget.Gate
	var rest *gh.RESTClient
	var graphQL *gh.GraphQLClient
	var deliveryClient *gh.DeliveriesClient
	var tokens gh.TokenProvider
	budgetCtx, cancelBudget := context.WithCancel(context.Background())
	defer func() {
		if githubGate != nil {
			closeCtx, cancelClose := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			_ = githubGate.Close(closeCtx)
			cancelClose()
		}
		cancelBudget()
	}()
	needsGitHub := roles[roleFetch] || roles[roleSweep] || roles[roleDrift]
	if needsGitHub {
		owner, hostErr := os.Hostname()
		if hostErr != nil {
			return fmt.Errorf("budget lease owner: %w", hostErr)
		}
		githubGate, err = budget.NewLeased(
			budgetCtx,
			http.DefaultClient,
			budget.Options{},
			budget.NewPostgresLeaseStore(pool),
			budget.LeaseOptions{
				InstallationID: cfg.GitHubInstallationID,
				Owner:          owner,
			},
		)
		if err != nil {
			return fmt.Errorf("GitHub budget gate: %w", err)
		}
		var appTokens gh.TokenProvider
		if cfg.GitHubToken != "" {
			tokens = gh.StaticToken(cfg.GitHubToken)
			appTokens = tokens
		} else {
			privateKeyPEM, readErr := os.ReadFile(cfg.GitHubPrivateKeyPath)
			if readErr != nil {
				return fmt.Errorf("read GitHub private key: %w", readErr)
			}
			tokens, err = gh.NewInstallationTokens(
				githubGate,
				gh.InstallationTokenOptions{
					BaseURL:        cfg.GitHubBaseURL,
					AppID:          cfg.GitHubAppID,
					InstallationID: cfg.GitHubInstallationID,
					PrivateKeyPEM:  privateKeyPEM,
				},
			)
			if err != nil {
				return fmt.Errorf("GitHub installation tokens: %w", err)
			}
			appTokens, err = gh.NewAppTokens(
				cfg.GitHubAppID,
				privateKeyPEM,
			)
			if err != nil {
				return fmt.Errorf("GitHub App tokens: %w", err)
			}
		}
		rest, err = gh.NewRESTClient(
			cfg.GitHubBaseURL,
			githubGate,
			tokens,
		)
		if err != nil {
			return fmt.Errorf("GitHub REST client: %w", err)
		}
		graphQL, err = gh.NewGraphQLClient(
			cfg.GitHubBaseURL,
			githubGate,
			tokens,
		)
		if err != nil {
			return fmt.Errorf("GitHub GraphQL client: %w", err)
		}
		if roles[roleSweep] {
			deliveryClient, err = gh.NewDeliveriesClient(
				cfg.GitHubBaseURL,
				githubGate,
				appTokens,
			)
			if err != nil {
				return fmt.Errorf("GitHub deliveries client: %w", err)
			}
		}
	}

	needsRiver := roles[roleDispatch] || roles[roleFetch] ||
		roles[roleSweep] || roles[roleDrift] || roles[rolePruner]
	if needsRiver {
		var fetchHandler *fetch.Handler
		if roles[roleFetch] {
			fetchHandler, err = fetch.New(fetch.Options{
				Pool:           pool,
				REST:           rest,
				GraphQL:        graphQL,
				InstallationID: cfg.GitHubInstallationID,
				OrgID:          cfg.GitHubOrgID,
			})
			if err != nil {
				return err
			}
		}
		var sweepService *sweep.Service
		if roles[roleSweep] || roles[rolePruner] {
			sweepService, err = sweep.New(sweep.Options{
				Pool:       pool,
				REST:       rest,
				Deliveries: deliveryClient,
				Config:     sweepConfig(cfg),
			})
			if err != nil {
				return err
			}
		}
		var driftService *drift.Service
		if roles[roleDrift] {
			driftService, err = drift.New(drift.Options{
				Pool:    pool,
				REST:    rest,
				GraphQL: graphQL,
				Config:  driftConfig(cfg),
			})
			if err != nil {
				return err
			}
		}
		var clientOptions []queue.ClientOption
		plan := riverPlanForRoles(roles)
		clientOptions = append(
			clientOptions,
			queue.WithQueues(plan.queues...),
		)
		if roles[roleFetch] {
			clientOptions = append(
				clientOptions,
				queue.WithRefreshHandler(fetchHandler),
				queue.WithDeadlineObserver(
					queue.LogDeadlineObserver{},
				),
			)
		} else {
			clientOptions = append(
				clientOptions,
				queue.WithoutRefreshWorkers(),
			)
		}
		if worksJobs {
			// River elects one cluster leader. Every leader-eligible client
			// therefore receives the same schedule table, independent of the
			// queues and workers owned by this process's roles.
			clientOptions = append(
				clientOptions,
				queue.WithPeriodicJobs(
					allPeriodicJobs(cfg)...,
				),
			)
		}
		if roles[roleSweep] {
			clientOptions = append(
				clientOptions,
				queue.WithWorkerRegistrar(
					sweepService.RegisterReconciliationWorkers,
				),
			)
		}
		if roles[rolePruner] {
			clientOptions = append(
				clientOptions,
				queue.WithWorkerRegistrar(
					sweepService.RegisterPrunerWorker,
				),
			)
		}
		if driftService != nil {
			clientOptions = append(
				clientOptions,
				queue.WithWorkerRegistrar(driftService.RegisterWorker),
			)
		}
		riverClient, err = queue.NewClient(pool, clientOptions...)
		if err != nil {
			return fmt.Errorf("river client: %w", err)
		}
		if fetchHandler != nil {
			fetchHandler.SetRiverClient(riverClient)
		}
		if sweepService != nil {
			sweepService.SetRiverClient(riverClient)
		}
		if driftService != nil {
			driftService.SetRiverClient(riverClient)
		}
		if worksJobs {
			if err := riverClient.Start(serviceCtx); err != nil {
				return fmt.Errorf("river start: %w", err)
			}
		}
		if roles[roleDispatch] {
			dispatcher := dispatch.New(pool, riverClient, dispatch.Config{
				BatchSize:    cfg.DispatchBatchSize,
				MaxAttempts:  cfg.DispatchMaxAttempts,
				Debounce:     cfg.DispatchDebounce,
				PollInterval: cfg.DispatchPollInterval,
				Classifier:   classifier,
			})
			go func() {
				if err := dispatcher.Run(serviceCtx); err != nil {
					serviceErrors <- fmt.Errorf("dispatcher: %w", err)
				}
			}()
		}
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
	if riverClient != nil &&
		(roles[roleFetch] || roles[roleSweep] ||
			roles[roleDrift] || roles[rolePruner]) {
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
	if githubGate != nil {
		closeCtx, cancelGate := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		gateErr := githubGate.Close(closeCtx)
		cancelGate()
		if gateErr != nil && serviceErr == nil {
			serviceErr = fmt.Errorf("GitHub budget gate shutdown: %w", gateErr)
		}
	}
	return serviceErr
}

func dispatcherClassifier(path string) (dispatch.Classifier, error) {
	if path == "" {
		return dispatch.DefaultClassifier(), nil
	}
	rules, err := dispatch.LoadRulesFile(path)
	if err != nil {
		return dispatch.Classifier{}, err
	}
	return dispatch.NewClassifier(rules), nil
}

func validateRoles(roles string) error {
	_, err := parseRoles(roles)
	return err
}

type riverRolePlan struct {
	queues []string
}

func riverPlanForRoles(roles map[string]bool) riverRolePlan {
	var queues []string
	if roles[roleFetch] {
		queues = append(
			queues,
			queue.QueueInteractive,
			queue.QueueEvent,
			queue.QueueSweep,
		)
	}
	if roles[roleSweep] {
		queues = append(queues, queue.QueueReconcile)
	}
	if roles[roleDrift] {
		queues = append(queues, queue.QueueDrift)
	}
	if roles[rolePruner] {
		queues = append(queues, queue.QueuePruner)
	}
	return riverRolePlan{queues: queues}
}

func sweepConfig(cfg config.Config) sweep.Config {
	return sweep.Config{
		InstallationID:        cfg.GitHubInstallationID,
		OpenStackMaxStaleness: cfg.SweepOpenStackMaxStaleness,
		OpenPRMaxStaleness:    cfg.SweepOpenPRMaxStaleness,
		RepoRulesMaxStaleness: cfg.SweepRepoRulesMaxStaleness,
		ClosedMaxStaleness:    cfg.SweepClosedMaxStaleness,
		RepositoryListPeriod:  cfg.SweepRepositoryListPeriod,
		PageSize:              cfg.SweepPageSize,
		GapHealPeriod:         cfg.GapHealPeriod,
		GapWindow:             cfg.GapWindow,
		GapPageSize:           cfg.GapPageSize,
		GapMaxPages:           cfg.GapMaxPages,
		RetentionPeriod:       cfg.RetentionPeriod,
		RetentionAge:          cfg.RetentionAge,
		RetentionBatchSize:    cfg.RetentionBatchSize,
		Observer:              sweep.LogObserver{},
	}
}

func driftConfig(cfg config.Config) drift.Config {
	return drift.Config{
		InstallationID:     cfg.GitHubInstallationID,
		Period:             cfg.DriftPeriod,
		SampleSize:         cfg.DriftSampleSize,
		PageSize:           cfg.SweepPageSize,
		ResolvedRetention:  cfg.DriftResolvedRetention,
		RetentionBatchSize: cfg.RetentionBatchSize,
		Observer:           drift.LogObserver{},
	}
}

func allPeriodicJobs(cfg config.Config) []*river.PeriodicJob {
	sweepCfg := sweepConfig(cfg)
	jobs := sweep.ReconciliationPeriodicJobs(sweepCfg)
	jobs = append(jobs, sweep.PrunerPeriodicJobs(sweepCfg)...)
	jobs = append(jobs, drift.PeriodicJobs(driftConfig(cfg))...)
	return jobs
}

func parseRoles(raw string) (map[string]bool, error) {
	if raw == "all" {
		return map[string]bool{
			roleIngress:     true,
			roleDispatch:    true,
			roleFetch:       true,
			roleSweep:       true,
			roleDrift:       true,
			rolePruner:      true,
			roleWatermarker: true,
			roleDeriver:     true,
		}, nil
	}
	roles := make(map[string]bool, 8)
	for _, role := range strings.Split(raw, ",") {
		switch role {
		case roleIngress,
			roleDispatch,
			roleFetch,
			roleSweep,
			roleDrift,
			rolePruner,
			roleWatermarker,
			roleDeriver:
			roles[role] = true
		default:
			return nil, fmt.Errorf(
				"--roles=%q contains unsupported role %q (want ingress, dispatch, fetch, sweep, drift, pruner, watermarker, deriver, or all)",
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
