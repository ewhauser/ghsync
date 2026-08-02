// ghsyncd is the ghsync sync engine daemon.
//
// Usage:
//
//	ghsyncd serve [--roles=all]   run the engine (default command)
//	ghsyncd migrate               apply River + schema migrations
//	ghsyncd backfill              start/resume installation backfill
//	ghsyncd requeue --guid=...    replay parked deliveries
//	ghsyncd version               print the build version
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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/rivercontrib/otelriver"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/derive"
	"github.com/ewhauser/ghsync/internal/dispatch"
	"github.com/ewhauser/ghsync/internal/drift"
	"github.com/ewhauser/ghsync/internal/fetch"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/ingress"
	ghsyncmetrics "github.com/ewhauser/ghsync/internal/metrics"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	streammaint "github.com/ewhauser/ghsync/internal/stream"
	"github.com/ewhauser/ghsync/internal/sweep"
	"github.com/ewhauser/ghsync/internal/telemetry"
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
	roleMetrics            = "metrics"
)

func databaseConnectOptions(
	cfg *config.Config,
	options ...store.ConnectOption,
) []store.ConnectOption {
	if cfg.DatabaseAuth == config.DatabaseAuthRDSIAM {
		options = append(options, store.WithRDSIAMAuthentication())
	}
	return options
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ghsyncd:", err)
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

func shutdownTracing(tracing *telemetry.Tracing) {
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	if err := tracing.Shutdown(shutdownCtx); err != nil {
		slog.Error("tracing shutdown failed", "error", err)
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
	tracing, err := telemetry.NewTracing(
		ctx,
		telemetry.TracingOptions{Version: version, Command: "backfill"},
	)
	if err != nil {
		return err
	}
	defer shutdownTracing(tracing)
	ctx, span := tracing.Provider().Tracer(
		"github.com/ewhauser/ghsync/cmd/ghsyncd",
	).Start(ctx, "ghsync.command.backfill")
	defer span.End()
	pool, err := store.Connect(
		ctx,
		cfg.DatabaseURL,
		databaseConnectOptions(
			&cfg,
			store.WithTracerProvider(tracing.Provider()),
		)...,
	)
	if err != nil {
		return err
	}
	defer pool.Close()
	riverClient, err := queue.NewClient(
		pool,
		queue.WithPlugins(otelriver.NewMiddleware(
			&otelriver.MiddlewareConfig{
				EnableTracePropagation:      true,
				EnableWorkSpanJobKindSuffix: true,
				MeterProvider:               metricnoop.NewMeterProvider(),
				TracerProvider:              tracing.Provider(),
			},
		)),
	)
	if err != nil {
		return fmt.Errorf("river client: %w", err)
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
	guids         []string
	event         string
	errorContains string
}

func parseRequeueOptions(args []string) (requeueOptions, error) {
	fs := flag.NewFlagSet("requeue", flag.ContinueOnError)
	guid := fs.String("guid", "", "parked delivery GUID to requeue")
	guids := fs.String(
		"guids",
		"",
		"comma-separated parked delivery GUIDs to requeue (maximum 100)",
	)
	event := fs.String("event", "", "event name for bounded family replay")
	errorContains := fs.String(
		"error-contains",
		"",
		"error substring for bounded family replay",
	)
	if err := fs.Parse(args); err != nil {
		return requeueOptions{}, err
	}
	if fs.NArg() != 0 {
		return requeueOptions{}, fmt.Errorf("requeue does not accept positional arguments")
	}
	selected := make([]string, 0, 100)
	if value := strings.TrimSpace(*guid); value != "" {
		selected = append(selected, value)
	}
	for value := range strings.SplitSeq(*guids, ",") {
		if value = strings.TrimSpace(value); value != "" {
			selected = append(selected, value)
		}
	}
	selected = uniqueStrings(selected)
	family := strings.TrimSpace(*event) != "" ||
		strings.TrimSpace(*errorContains) != ""
	if (len(selected) > 0) == family ||
		(family && (strings.TrimSpace(*event) == "" ||
			strings.TrimSpace(*errorContains) == "")) {
		return requeueOptions{}, fmt.Errorf(
			"requeue requires a GUID set or both --event and --error-contains",
		)
	}
	if len(selected) > 100 {
		return requeueOptions{}, fmt.Errorf("requeue accepts at most 100 GUIDs")
	}
	if len(selected) == 0 {
		selected = nil
	}
	return requeueOptions{
		guids:         selected,
		event:         strings.TrimSpace(*event),
		errorContains: strings.TrimSpace(*errorContains),
	}, nil
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
	tracing, err := telemetry.NewTracing(
		ctx,
		telemetry.TracingOptions{Version: version, Command: "requeue"},
	)
	if err != nil {
		return err
	}
	defer shutdownTracing(tracing)
	ctx, span := tracing.Provider().Tracer(
		"github.com/ewhauser/ghsync/cmd/ghsyncd",
	).Start(ctx, "ghsync.command.requeue")
	defer span.End()
	pool, err := store.Connect(
		ctx,
		cfg.DatabaseURL,
		databaseConnectOptions(
			&cfg,
			store.WithTracerProvider(tracing.Provider()),
		)...,
	)
	if err != nil {
		return err
	}
	defer pool.Close()
	count, err := dbgen.New(pool).RequeueParkedWebhookDeliveries(
		ctx,
		dbgen.RequeueParkedWebhookDeliveriesParams{
			DeliveryGuids: options.guids,
			Event:         options.event,
			ErrorContains: options.errorContains,
		},
	)
	if err != nil {
		return fmt.Errorf("requeue parked deliveries: %w", err)
	}
	if len(options.guids) > 0 && count != int64(len(options.guids)) {
		return fmt.Errorf(
			"requeued %d of %d selected parked deliveries",
			count,
			len(options.guids),
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
	tracing, err := telemetry.NewTracing(
		ctx,
		telemetry.TracingOptions{Version: version, Command: "migrate"},
	)
	if err != nil {
		return err
	}
	defer shutdownTracing(tracing)
	ctx, span := tracing.Provider().Tracer(
		"github.com/ewhauser/ghsync/cmd/ghsyncd",
	).Start(ctx, "ghsync.command.migrate")
	defer span.End()
	pool, err := store.Connect(
		ctx,
		cfg.DatabaseURL,
		databaseConnectOptions(
			&cfg,
			store.WithTracerProvider(tracing.Provider()),
		)...,
	)
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
		"comma-separated roles: ingress,dispatch,fetch+sweep+drift,pruner,watermarker,deriver,metrics, or all",
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
	if cfg.GitHubInstallationID <= 0 {
		return fmt.Errorf(
			"GITHUB_INSTALLATION_ID is required for every serve role",
		)
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

	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stopSignals()

	tracing, err := telemetry.NewTracing(
		signalCtx,
		telemetry.TracingOptions{
			Version: version,
			Command: "serve",
			Roles:   enabledRoles(roles),
		},
	)
	if err != nil {
		return err
	}
	defer shutdownTracing(tracing)

	pool, err := store.Connect(
		signalCtx,
		cfg.DatabaseURL,
		databaseConnectOptions(
			&cfg,
			store.WithTracerProvider(tracing.Provider()),
		)...,
	)
	if err != nil {
		return err
	}
	defer pool.Close()

	metricRegistry, err := ghsyncmetrics.New()
	if err != nil {
		return err
	}
	runtimeMetrics, err := ghsyncmetrics.NewRuntime(
		ghsyncmetrics.RuntimeOptions{
			Pool:               pool,
			InstallationID:     cfg.GitHubInstallationID,
			Roles:              enabledRoles(roles),
			CollectDatabase:    roles[roleMetrics],
			OpenStackStaleness: cfg.SweepOpenStackMaxStaleness,
			OpenPRStaleness:    cfg.SweepOpenPRMaxStaleness,
			RepoRulesStaleness: cfg.SweepRepoRulesMaxStaleness,
			ClosedStaleness:    cfg.SweepClosedMaxStaleness,
			RepositoryPeriod:   cfg.SweepRepositoryListPeriod,
			StreamRetentionAge: cfg.StreamRetentionAge,
		},
	)
	if err != nil {
		return err
	}
	if err := metricRegistry.Register(
		"github.com/ewhauser/ghsync/ghsyncd",
		runtimeMetrics,
	); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		if shutdownErr := metricRegistry.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Error("metrics shutdown failed", "error", shutdownErr)
		}
		cancel()
	}()

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
				RefreshInterval:  cfg.WatermarkRefresh,
				LeaseTTL:         cfg.WatermarkLeaseTTL,
				FenceLockTimeout: cfg.WatermarkFenceTimeout,
				Owner:            owner,
				Observer:         watermarkMetricsAdapter{runtime: runtimeMetrics},
				InstallationID:   cfg.GitHubInstallationID,
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
		deriver, createErr := derive.New(&derive.Options{
			Pool:         pool,
			Deriver:      derive.NoopDeriver{},
			DirtyCap:     cfg.DeriverDirtyCap,
			PollInterval: cfg.DeriverPollInterval,
			Observer:     runtimeMetrics,
			Tracer: tracing.Provider().Tracer(
				"github.com/ewhauser/ghsync/internal/derive",
			),
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
				OnPrune:   runtimeMetrics.PrunerDelete,
				Tracer: tracing.Provider().Tracer(
					"github.com/ewhauser/ghsync/internal/stream",
				),
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
	defer cancelBudget()
	needsGitHub := roles[roleFetch] || roles[roleSweep] || roles[roleDrift]
	if needsGitHub {
		owner, hostErr := os.Hostname()
		if hostErr != nil {
			return fmt.Errorf("budget lease owner: %w", hostErr)
		}
		githubHTTPClient := &http.Client{
			Transport: otelhttp.NewTransport(
				http.DefaultTransport,
				otelhttp.WithTracerProvider(tracing.Provider()),
				otelhttp.WithPropagators(tracing.Propagator()),
			),
		}
		githubGate, err = budget.NewLeased(
			budgetCtx,
			githubHTTPClient,
			budget.Options{
				MaxConcurrent:          cfg.BudgetMaxConcurrent,
				RESTLimit:              cfg.BudgetRESTLimit,
				GraphQLLimit:           cfg.BudgetGraphQLLimit,
				SweepFloor:             cfg.BudgetSweepFloor,
				EventFloor:             cfg.BudgetEventFloor,
				SecondaryLimitFallback: cfg.BudgetSecondaryFallback,
				OnStarvation:           runtimeMetrics.BudgetStarvation,
				OnRequest:              runtimeMetrics.BudgetRequest,
				Tracer: tracing.Provider().Tracer(
					"github.com/ewhauser/ghsync/internal/budget",
				),
			},
			budget.NewPostgresLeaseStore(pool),
			budget.LeaseOptions{
				InstallationID: cfg.GitHubInstallationID,
				Owner:          owner,
				TTL:            cfg.BudgetLeaseTTL,
				RenewInterval:  cfg.BudgetLeaseRenewInterval,
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
				Pool:             pool,
				REST:             rest,
				GraphQL:          graphQL,
				InstallationID:   cfg.GitHubInstallationID,
				OrgID:            cfg.GitHubOrgID,
				BatchWindow:      cfg.FetchBatchWindow,
				BackfillPageSize: cfg.BackfillPageSize,
				CacheObserver:    runtimeMetrics,
			})
			if err != nil {
				return err
			}
		}
		var sweepService *sweep.Service
		if roles[roleSweep] || roles[rolePruner] {
			sweepCfg := sweepConfig(&cfg)
			sweepCfg.Observer = sweep.Observers{
				sweep.LogObserver{},
				runtimeMetrics,
			}
			sweepCfg.OnPrune = runtimeMetrics.PrunerDelete
			sweepService, err = sweep.New(sweep.Options{
				Pool:       pool,
				REST:       rest,
				Deliveries: deliveryClient,
				Config:     sweepCfg,
			})
			if err != nil {
				return err
			}
		}
		var driftService *drift.Service
		if roles[roleDrift] {
			driftCfg := driftConfig(&cfg)
			driftCfg.Observer = drift.Observers{
				drift.LogObserver{},
				runtimeMetrics,
			}
			driftService, err = drift.New(drift.Options{
				Pool:    pool,
				REST:    rest,
				GraphQL: graphQL,
				Config:  driftCfg,
			})
			if err != nil {
				return err
			}
		}
		var clientOptions []queue.ClientOption
		clientOptions = append(
			clientOptions,
			queue.WithPlugins(otelriver.NewMiddleware(
				&otelriver.MiddlewareConfig{
					EnableTracePropagation:      true,
					EnableWorkSpanJobKindSuffix: true,
					MeterProvider:               metricRegistry.Provider(),
					TracerProvider:              tracing.Provider(),
				},
			)),
		)
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
					queue.DeadlineObservers{
						queue.LogDeadlineObserver{},
						runtimeMetrics,
					},
				),
				queue.WithRefreshObserver(runtimeMetrics),
			)
		}
		clientOptions = append(clientOptions, refreshWorkerOptions(roles)...)
		if worksJobs {
			// River elects one cluster leader. Every leader-eligible client
			// therefore receives the same schedule table, independent of the
			// queues and workers owned by this process's roles.
			clientOptions = append(
				clientOptions,
				queue.WithPeriodicJobs(
					allPeriodicJobs(&cfg)...,
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
			dispatcher, err := dispatch.New(pool, riverClient, dispatch.Config{
				BatchSize:    cfg.DispatchBatchSize,
				MaxAttempts:  cfg.DispatchMaxAttempts,
				Debounce:     cfg.DispatchDebounce,
				PollInterval: cfg.DispatchPollInterval,
				Classifier:   classifier,
				Observer:     runtimeMetrics,
				Tracer: tracing.Provider().Tracer(
					"github.com/ewhauser/ghsync/internal/dispatch",
				),
			})
			if err != nil {
				return fmt.Errorf("dispatcher: %w", err)
			}
			go func() {
				if err := dispatcher.Run(serviceCtx); err != nil {
					serviceErrors <- fmt.Errorf("dispatcher: %w", err)
				}
			}()
		}
	}

	var webhookHandler http.Handler
	if roles[roleIngress] {
		handler := ingress.NewHandler(
			dbgen.New(pool),
			cfg.GitHubWebhookSecret,
			cfg.WebhookMaxBodyBytes,
			webhookRequestTimeout,
		)
		webhookHandler = otelhttp.NewHandler(
			handler,
			"github.webhook",
			otelhttp.WithTracerProvider(tracing.Provider()),
			otelhttp.WithPropagators(tracing.Propagator()),
		)
	}
	// C-O4 role separation: every role has health and metrics, while only an
	// ingress role registers the externally writable webhook route.
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: serviceMux(
			webhookHandler,
			metricRegistry.Handler(),
		),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       httpReadTimeout,
	}
	listener, err := (&net.ListenConfig{}).Listen(
		serviceCtx,
		"tcp",
		cfg.HTTPAddr,
	)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	go func() {
		err := httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			serviceErrors <- fmt.Errorf("health/metrics HTTP server: %w", err)
		}
	}()

	slog.Info("ghsyncd running", "version", version, "roles", *rolesFlag)
	var serviceErr error
	select {
	case <-signalCtx.Done():
	case serviceErr = <-serviceErrors:
	}
	slog.Info("shutting down")
	cancelServices()

	httpCtx, cancelHTTP := context.WithTimeout(
		context.Background(),
		httpShutdownTimeout,
	)
	httpErr := httpServer.Shutdown(httpCtx)
	cancelHTTP()
	if httpErr != nil && serviceErr == nil {
		serviceErr = fmt.Errorf("HTTP graceful shutdown: %w", httpErr)
	}
	if riverClient != nil && worksJobs {
		riverCtx, cancelRiver := context.WithTimeout(
			context.Background(),
			riverShutdownTimeout,
		)
		riverErr := riverClient.Stop(riverCtx)
		cancelRiver()
		if riverErr != nil {
			slog.Warn(
				"River graceful shutdown timed out; canceling active work",
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

func serviceMux(
	webhook http.Handler,
	metricsHandler http.Handler,
) *http.ServeMux {
	mux := ingress.NewMux(webhook)
	mux.Handle("GET "+ghsyncmetrics.Path, metricsHandler)
	return mux
}

func enabledRoles(roles map[string]bool) []string {
	result := make([]string, 0, len(roles))
	for role, enabled := range roles {
		if enabled {
			result = append(result, role)
		}
	}
	sort.Strings(result)
	return result
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

// refreshWorkerOptions decides refresh-worker registration for the enabled
// roles. Fetch installs the real handler-backed workers elsewhere. Dispatch
// keeps River's default (handler-less, never-started) registrations because
// it produces refresh jobs and River validates insert args against the
// client's Workers bundle — opting out crashed dispatch-only processes on
// their first classified refresh (issue #7). Every other fetchless role is
// worker-only for its own queues and opts out.
func refreshWorkerOptions(roles map[string]bool) []queue.ClientOption {
	if roles[roleFetch] || roles[roleDispatch] {
		return nil
	}
	return []queue.ClientOption{queue.WithoutRefreshWorkers()}
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

func sweepConfig(cfg *config.Config) sweep.Config {
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

func driftConfig(cfg *config.Config) drift.Config {
	return drift.Config{
		InstallationID:     cfg.GitHubInstallationID,
		Period:             cfg.DriftPeriod,
		SampleSize:         cfg.DriftSampleSize,
		PageSize:           cfg.DriftPageSize,
		ResolvedRetention:  cfg.DriftResolvedRetention,
		RetentionBatchSize: cfg.RetentionBatchSize,
		Observer:           drift.LogObserver{},
	}
}

func allPeriodicJobs(cfg *config.Config) []*river.PeriodicJob {
	sweepCfg := sweepConfig(cfg)
	jobs := sweep.ReconciliationPeriodicJobs(&sweepCfg)
	jobs = append(jobs, sweep.PrunerPeriodicJobs(&sweepCfg)...)
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
			roleMetrics:     true,
		}, nil
	}
	roles := make(map[string]bool, 9)
	for role := range strings.SplitSeq(raw, ",") {
		switch role {
		case roleIngress,
			roleDispatch,
			roleFetch,
			roleSweep,
			roleDrift,
			rolePruner,
			roleWatermarker,
			roleDeriver,
			roleMetrics:
			roles[role] = true
		default:
			return nil, fmt.Errorf(
				"--roles=%q contains unsupported role %q (want ingress, dispatch, fetch, sweep, drift, pruner, watermarker, deriver, metrics, or all)",
				raw,
				role,
			)
		}
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("--roles must not be empty")
	}
	githubRoles := 0
	for _, role := range []string{roleFetch, roleSweep, roleDrift} {
		if roles[role] {
			githubRoles++
		}
	}
	if githubRoles != 0 && githubRoles != 3 {
		return nil, fmt.Errorf(
			"fetch, sweep, and drift must be enabled together as one singleton GitHub role group",
		)
	}
	return roles, nil
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// watermarkMetricsAdapter bridges the stream package's rich WatermarkProgress
// observer to the metrics runtime's plain-value signature, preserving the
// metrics package's independence from internal/stream (Opus review C6) without
// widening the stream interface.
type watermarkMetricsAdapter struct {
	runtime *ghsyncmetrics.Runtime
}

func (a watermarkMetricsAdapter) WatermarkStep(
	ctx context.Context,
	progress streammaint.WatermarkProgress,
) {
	a.runtime.WatermarkStep(ctx, progress.Advanced)
}
