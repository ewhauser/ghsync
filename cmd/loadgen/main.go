// loadgen replays recorded repository truth through standalone fake GitHub
// and exits successfully only after every end-to-end load assertion holds.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"

	runtimeconfig "github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/replay"
	"github.com/ewhauser/ghsync/internal/store"
)

const (
	defaultEngineURL      = "http://127.0.0.1:8080"
	defaultFakeGitHubURL  = "http://127.0.0.1:9797"
	defaultScrapeInterval = 500 * time.Millisecond
	defaultTrafficTimeout = 3 * time.Minute
	defaultDrainTimeout   = 90 * time.Second
	loadStream            = "entities"
)

type config struct {
	engineURL      string
	fakeGitHubURL  string
	databaseURL    string
	databaseAuth   runtimeconfig.DatabaseAuth
	installationID int64
	recordingPath  string
	speed          float64
	copies         int
	loop           bool
	maxSteps       int
	expectedCount  int
	targetRate     float64
	trafficTimeout time.Duration
	drainTimeout   time.Duration
	scrapeInterval time.Duration
	duplicateEvery int
	reorderWindow  int
	dropEvery      int
	fake500Burst   int
	fake429Burst   int
	fake429Retry   time.Duration
	engineCmd      string
	restartAfter   time.Duration
	httpClient     *http.Client
}

type histogramSnapshot struct {
	count   uint64
	buckets map[float64]uint64
}

type runState struct {
	segmentWatermark     float64
	segmentStarvations   float64
	watermarkAdvanced    bool
	postLoadCaptured     bool
	postLoadDriftPasses  float64
	postLoadDriftSamples float64
	postLoadWatermarks   float64
	lastHistogram        histogramSnapshot
	runHistogram         histogramSnapshot
	histogramWindows     int
	samples              int
	budgetSamples        int
	gateClosedSamples    int
	cR1Samples           int
	requireCR1           bool
	metricRestarts       int
	last500Responses     float64
	last429Responses     float64
	lastGapRedeliveries  float64
	observed500Responses float64
	observed429Responses float64
	observedGapHeals     float64
}

type runResult struct {
	steps           int
	deliveries      int
	dropped         int
	duplicates      int
	achievedRate    float64
	targetRate      float64
	trafficElapsed  time.Duration
	drainElapsed    time.Duration
	streamEvents    int64
	streamRestartAt int64
	cq2P95          float64
	cq2P99          float64
	observed500s    float64
	observed429s    float64
	gapHeals        float64
	engineRestarts  int
}

func main() {
	if err := runMain(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func runMain(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	engineURL := fs.String(
		"engine-url", defaultEngineURL, "ghsyncd base URL",
	)
	fakeGitHubURL := fs.String(
		"fake-github-url",
		defaultFakeGitHubURL,
		"standalone fake GitHub base URL",
	)
	databaseURL := fs.String(
		"database-url",
		os.Getenv("DATABASE_URL"),
		"Postgres URL used for strict assertions",
	)
	databaseAuth := fs.String(
		"database-auth",
		os.Getenv("DATABASE_AUTH"),
		"database authentication mode: password or rds-iam",
	)
	installationID := fs.Int64(
		"installation-id",
		0,
		"GitHub installation whose completed backfill is required",
	)
	recordingPath := fs.String(
		"recording", "", "recording NDJSON to compile and replay",
	)
	speed := fs.Float64(
		"speed", 1, "recorded-time compression multiplier",
	)
	copies := fs.Int("copies", 1, "renumbered replay copies")
	loop := fs.Bool("loop", false, "compile additional renumbered laps")
	maxSteps := fs.Int(
		"steps",
		0,
		"maximum compiled steps (zero means one complete lap)",
	)
	expectedCount := fs.Int(
		"expected-deliveries",
		0,
		"exact compiled delivery count (zero derives it from the recording)",
	)
	targetRate := fs.Float64(
		"target-rate",
		1,
		"minimum base deliveries per second",
	)
	trafficTimeout := fs.Duration(
		"traffic-timeout",
		defaultTrafficTimeout,
		"deadline for applying all configured steps and deliveries",
	)
	drainTimeout := fs.Duration(
		"drain-timeout",
		defaultDrainTimeout,
		"deadline for drain, trust passes, and convergence",
	)
	scrapeInterval := fs.Duration(
		"scrape-interval",
		defaultScrapeInterval,
		"live metrics assertion interval",
	)
	duplicateEvery := fs.Int(
		"duplicate-every",
		0,
		"repeat every Nth delivery with its original GUID (off at zero)",
	)
	reorderWindow := fs.Int(
		"reorder-window",
		0,
		"reverse deliveries within bounded windows of N (off at zero)",
	)
	dropEvery := fs.Int(
		"drop-every",
		0,
		"GitHub-side drop every Nth delivery for C-R4 healing (off at zero)",
	)
	fake500Burst := fs.Int(
		"fake-500-burst",
		0,
		"script this many fake GitHub API 500 responses (off at zero)",
	)
	fake429Burst := fs.Int(
		"fake-429-burst",
		0,
		"script this many fake GitHub API 429 responses (off at zero)",
	)
	fake429Retry := fs.Duration(
		"fake-429-retry-after",
		time.Second,
		"Retry-After used by scripted fake 429 responses",
	)
	engineCmd := fs.String(
		"engine-cmd",
		"",
		"ghsyncd command loadgen owns and may restart",
	)
	restartAfter := fs.Duration(
		"restart-after",
		0,
		"SIGKILL and restart the owned engine after this traffic duration",
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("loadgen does not accept positional arguments")
	}
	parsedDatabaseAuth, err := runtimeconfig.ParseDatabaseAuth(*databaseAuth)
	if err != nil {
		return err
	}
	if *installationID == 0 {
		raw := strings.TrimSpace(os.Getenv("GITHUB_INSTALLATION_ID"))
		if raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("parse GITHUB_INSTALLATION_ID: %w", err)
			}
			*installationID = parsed
		}
	}
	cfg := config{
		engineURL:      strings.TrimRight(*engineURL, "/"),
		fakeGitHubURL:  strings.TrimRight(*fakeGitHubURL, "/"),
		databaseURL:    strings.TrimSpace(*databaseURL),
		databaseAuth:   parsedDatabaseAuth,
		installationID: *installationID,
		recordingPath:  *recordingPath,
		speed:          *speed,
		copies:         *copies,
		loop:           *loop,
		maxSteps:       *maxSteps,
		expectedCount:  *expectedCount,
		targetRate:     *targetRate,
		trafficTimeout: *trafficTimeout,
		drainTimeout:   *drainTimeout,
		scrapeInterval: *scrapeInterval,
		duplicateEvery: *duplicateEvery,
		reorderWindow:  *reorderWindow,
		dropEvery:      *dropEvery,
		fake500Burst:   *fake500Burst,
		fake429Burst:   *fake429Burst,
		fake429Retry:   *fake429Retry,
		engineCmd:      strings.TrimSpace(*engineCmd),
		restartAfter:   *restartAfter,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}
	result, err := run(context.Background(), cfg)
	if err != nil {
		return err
	}
	fmt.Printf(
		"loadgen passed: steps=%d deliveries=%d dropped=%d duplicates=%d "+
			"required_rate=%.2f/s achieved_rate=%.2f/s traffic=%s "+
			"drain=%s stream_events=%d stream_restart_seq=%d "+
			"cq2_p95=%.1fs cq2_p99=%.1fs http_500s=%.0f http_429s=%.0f "+
			"gap_heals=%.0f engine_restarts=%d\n",
		result.steps,
		result.deliveries,
		result.dropped,
		result.duplicates,
		result.targetRate,
		result.achievedRate,
		result.trafficElapsed.Round(time.Millisecond),
		result.drainElapsed.Round(time.Millisecond),
		result.streamEvents,
		result.streamRestartAt,
		result.cq2P95,
		result.cq2P99,
		result.observed500s,
		result.observed429s,
		result.gapHeals,
		result.engineRestarts,
	)
	return nil
}

func validateConfig(cfg config) error {
	if _, err := runtimeconfig.ParseDatabaseAuth(string(cfg.databaseAuth)); err != nil {
		return err
	}
	if cfg.engineURL == "" || cfg.fakeGitHubURL == "" ||
		cfg.databaseURL == "" || cfg.recordingPath == "" {
		return fmt.Errorf(
			"engine, fake GitHub, database, and recording are required",
		)
	}
	if cfg.installationID <= 0 || cfg.speed <= 0 || cfg.copies <= 0 ||
		cfg.maxSteps < 0 || cfg.expectedCount < 0 || cfg.targetRate <= 0 ||
		cfg.trafficTimeout <= 0 || cfg.drainTimeout <= 0 ||
		cfg.scrapeInterval <= 0 || cfg.duplicateEvery < 0 ||
		cfg.reorderWindow < 0 || cfg.dropEvery < 0 ||
		cfg.fake500Burst < 0 || cfg.fake429Burst < 0 ||
		cfg.fake429Retry <= 0 || cfg.restartAfter < 0 {
		return fmt.Errorf("counts, rates, and durations are invalid")
	}
	if cfg.loop && cfg.maxSteps == 0 {
		return fmt.Errorf("--loop requires a finite --steps limit")
	}
	if !cfg.loop && cfg.maxSteps > 0 {
		return fmt.Errorf("--steps requires --loop; one-shot verification uses a complete lap")
	}
	if cfg.reorderWindow == 1 {
		return fmt.Errorf("--reorder-window must be zero (off) or at least two")
	}
	if cfg.restartAfter > 0 && cfg.engineCmd == "" {
		return fmt.Errorf("--restart-after requires --engine-cmd")
	}
	if cfg.restartAfter >= cfg.trafficTimeout {
		return fmt.Errorf("--restart-after must be less than --traffic-timeout")
	}
	return nil
}

func run(ctx context.Context, cfg config) (runResult, error) {
	steps, err := compileSteps(cfg)
	if err != nil {
		return runResult{}, err
	}
	plan, err := newReplayPlan(steps, replayChaos{
		duplicateEvery: cfg.duplicateEvery,
		reorderWindow:  cfg.reorderWindow,
		dropEvery:      cfg.dropEvery,
	})
	if err != nil {
		return runResult{}, err
	}
	if cfg.expectedCount > 0 && plan.deliveryCount != cfg.expectedCount {
		return runResult{}, fmt.Errorf(
			"compiled delivery count %d does not match configured exact count %d",
			plan.deliveryCount,
			cfg.expectedCount,
		)
	}
	engine := &engineProcess{command: cfg.engineCmd}
	if cfg.engineCmd != "" {
		if err := engine.Start(ctx); err != nil {
			return runResult{}, err
		}
		defer engine.Close()
		if err := waitOwnedEngineHealthy(
			ctx,
			cfg,
			engine,
			45*time.Second,
		); err != nil {
			return runResult{}, fmt.Errorf("owned engine health: %w", err)
		}
	} else {
		if err := waitHealthy(
			ctx,
			cfg.httpClient,
			cfg.engineURL+ingress.HealthPath,
			30*time.Second,
		); err != nil {
			return runResult{}, fmt.Errorf("engine health: %w", err)
		}
	}
	if err := waitHealthy(
		ctx,
		cfg.httpClient,
		cfg.fakeGitHubURL+"/healthz",
		30*time.Second,
	); err != nil {
		return runResult{}, fmt.Errorf("fake GitHub health: %w", err)
	}
	var connectOptions []store.ConnectOption
	if cfg.databaseAuth == runtimeconfig.DatabaseAuthRDSIAM {
		connectOptions = append(connectOptions, store.WithRDSIAMAuthentication())
	}
	pool, err := store.Connect(ctx, cfg.databaseURL, connectOptions...)
	if err != nil {
		return runResult{}, fmt.Errorf("connect assertion database: %w", err)
	}
	defer pool.Close()
	if err := waitForCacheSeed(ctx, cfg, pool); err != nil {
		return runResult{}, err
	}

	initial, err := scrape(ctx, cfg)
	if err != nil {
		return runResult{}, err
	}
	state := newRunState(initial)
	state.requireCR1 = cfg.dropEvery > 0
	if err := assertLive(initial, state); err != nil {
		return runResult{}, fmt.Errorf("initial metrics: %w", err)
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	streamConsumer, err := newLoadStreamConsumer(ctx, pool, runID)
	if err != nil {
		return runResult{}, err
	}
	defer streamConsumer.cleanup(ctx)
	if err := streamConsumer.start(ctx); err != nil {
		return runResult{}, err
	}
	if cfg.fake500Burst > 0 || cfg.fake429Burst > 0 {
		if err := configureFaults(ctx, cfg); err != nil {
			return runResult{}, err
		}
	}

	startedAt := time.Now()
	trafficCtx, cancelTraffic := context.WithDeadline(
		ctx,
		startedAt.Add(cfg.trafficTimeout),
	)
	defer cancelTraffic()
	replayCh := make(chan replayResult, 1)
	go func() {
		replayCh <- executeReplay(
			trafficCtx,
			cfg,
			plan,
			startedAt,
		)
	}()
	ticker := time.NewTicker(cfg.scrapeInterval)
	defer ticker.Stop()
	var restartTimer *time.Timer
	var restartC <-chan time.Time
	if cfg.restartAfter > 0 {
		restartTimer = time.NewTimer(cfg.restartAfter)
		restartC = restartTimer.C
		defer restartTimer.Stop()
	}
	var replayed replayResult
	streamRestarted := false
	engineRestarted := false
	for {
		select {
		case <-ctx.Done():
			return runResult{}, ctx.Err()
		case replayed = <-replayCh:
			if replayed.err != nil {
				return runResult{}, replayed.err
			}
			goto trafficComplete
		case <-ticker.C:
			if err := streamConsumer.check(); err != nil {
				return runResult{}, err
			}
			if !streamRestarted &&
				replayedProgress(plan) >= max(plan.deliveryCount/2, 1) {
				counts, countsErr := streamConsumer.counts(ctx)
				if countsErr != nil {
					return runResult{}, countsErr
				}
				if counts.cursor > streamConsumer.initialSeq &&
					counts.total > 0 {
					restartErr := streamConsumer.restart(ctx)
					if restartErr != nil {
						return runResult{}, restartErr
					}
					streamRestarted = true
				}
			}
			families, scrapeErr := scrape(ctx, cfg)
			if scrapeErr != nil {
				return runResult{}, scrapeErr
			}
			if err := assertLive(families, state); err != nil {
				return runResult{}, fmt.Errorf("live assertion: %w", err)
			}
		case <-restartC:
			beforeRestart, scrapeErr := scrape(ctx, cfg)
			if scrapeErr != nil {
				return runResult{}, scrapeErr
			}
			if err := assertLive(beforeRestart, state); err != nil {
				return runResult{}, fmt.Errorf(
					"pre-restart live assertion: %w",
					err,
				)
			}
			if err := engine.Restart(ctx); err != nil {
				return runResult{}, err
			}
			if err := waitOwnedEngineHealthy(
				ctx,
				cfg,
				engine,
				45*time.Second,
			); err != nil {
				return runResult{}, fmt.Errorf(
					"restarted engine health: %w",
					err,
				)
			}
			families, scrapeErr := scrape(ctx, cfg)
			if scrapeErr != nil {
				return runResult{}, scrapeErr
			}
			if err := state.resetSegment(families); err != nil {
				return runResult{}, err
			}
			engineRestarted = true
			restartC = nil
		}
	}

trafficComplete:
	if !streamRestarted {
		if err := streamConsumer.restartAfterProgress(
			ctx,
			min(cfg.drainTimeout/2, 15*time.Second),
		); err != nil {
			return runResult{}, err
		}
	}
	if cfg.restartAfter > 0 && !engineRestarted {
		return runResult{}, fmt.Errorf("configured engine restart did not occur")
	}
	if replayed.deliveries != plan.deliveryCount {
		return runResult{}, fmt.Errorf(
			"configured delivery count missed: completed %d, required %d",
			replayed.deliveries,
			plan.deliveryCount,
		)
	}
	trafficElapsed := time.Since(startedAt)
	achievedRate := float64(replayed.deliveries) / trafficElapsed.Seconds()
	if achievedRate < cfg.targetRate {
		return runResult{}, fmt.Errorf(
			"achieved rate %.2f/s is below configured target %.2f/s",
			achievedRate,
			cfg.targetRate,
		)
	}

	drainStarted := time.Now()
	final, err := drainAndAssert(
		ctx,
		cfg,
		pool,
		streamConsumer,
		state,
		plan,
	)
	if err != nil {
		return runResult{}, err
	}
	stopCtx, cancelStop := context.WithTimeout(ctx, 5*time.Second)
	err = streamConsumer.stop(stopCtx)
	cancelStop()
	if err != nil {
		return runResult{}, err
	}
	counts, err := streamConsumer.assertFinal(ctx)
	if err != nil {
		return runResult{}, err
	}
	if err := assertFinal(
		final,
		*state,
		cfg.fake500Burst,
		cfg.fake429Burst,
		plan.dropped,
	); err != nil {
		return runResult{}, err
	}
	cq2P95, err := histogramQuantileSnapshot(state.runHistogram, 0.95)
	if err != nil {
		return runResult{}, err
	}
	cq2P99, err := histogramQuantileSnapshot(state.runHistogram, 0.99)
	if err != nil {
		return runResult{}, err
	}
	return runResult{
		steps:           len(steps),
		deliveries:      replayed.deliveries,
		dropped:         replayed.dropped,
		duplicates:      replayed.duplicates,
		achievedRate:    achievedRate,
		targetRate:      cfg.targetRate,
		trafficElapsed:  trafficElapsed,
		drainElapsed:    time.Since(drainStarted),
		streamEvents:    counts.total,
		streamRestartAt: streamConsumer.restartSeq,
		cq2P95:          cq2P95,
		cq2P99:          cq2P99,
		observed500s:    state.observed500Responses,
		observed429s:    state.observed429Responses,
		gapHeals:        state.observedGapHeals,
		engineRestarts:  state.metricRestarts,
	}, nil
}

func compileSteps(cfg config) ([]replay.Step, error) {
	file, err := os.Open(cfg.recordingPath)
	if err != nil {
		return nil, fmt.Errorf("open recording: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	recording, err := replay.Read(file)
	if err != nil {
		return nil, err
	}
	program, err := replay.Compile(recording, replay.CompileOptions{
		Speed:  cfg.speed,
		Copies: cfg.copies,
		Loop:   cfg.loop,
	})
	if err != nil {
		return nil, err
	}
	var steps []replay.Step
	for {
		lap, nextErr := program.NextLap()
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				break
			}
			return nil, nextErr
		}
		steps = append(steps, lap...)
		if cfg.maxSteps > 0 && len(steps) >= cfg.maxSteps {
			steps = steps[:cfg.maxSteps]
			break
		}
		if !cfg.loop {
			break
		}
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("compiled replay has no steps")
	}
	return steps, nil
}

type engineProcess struct {
	mu      sync.Mutex
	command string
	cmd     *exec.Cmd
	done    chan error
}

func (p *engineProcess) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.startLocked(ctx)
}

func (p *engineProcess) startLocked(ctx context.Context) error {
	if p.command == "" {
		return fmt.Errorf("engine command is empty")
	}
	if p.cmd != nil {
		return fmt.Errorf("engine process is already running")
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "exec "+p.command)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start engine command: %w", err)
	}
	p.cmd = cmd
	p.done = make(chan error, 1)
	go func(done chan<- error) {
		done <- cmd.Wait()
	}(p.done)
	return nil
}

func (p *engineProcess) Restart(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapExitedLocked()
	if p.cmd == nil || p.cmd.Process == nil {
		return p.startLocked(ctx)
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("SIGKILL engine: %w", err)
	}
	if err := <-p.done; err == nil {
		return fmt.Errorf("SIGKILLed engine exited without a signal error")
	}
	p.cmd = nil
	p.done = nil
	return p.startLocked(ctx)
}

func (p *engineProcess) EnsureRunning(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapExitedLocked()
	if p.cmd != nil {
		return nil
	}
	return p.startLocked(ctx)
}

func (p *engineProcess) reapExitedLocked() {
	if p.done == nil {
		return
	}
	select {
	case <-p.done:
		p.cmd = nil
		p.done = nil
	default:
	}
}

func (p *engineProcess) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapExitedLocked()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	p.cmd = nil
	p.done = nil
}

func drainAndAssert(
	ctx context.Context,
	cfg config,
	pool *pgxpool.Pool,
	consumer *loadStreamConsumer,
	state *runState,
	plan *replayPlan,
) (map[string]*dto.MetricFamily, error) {
	deadline := time.Now().Add(cfg.drainTimeout)
	var final map[string]*dto.MetricFamily
	for {
		if err := consumer.check(); err != nil {
			return nil, err
		}
		var err error
		final, err = scrape(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if err := assertLive(final, state); err != nil {
			return nil, fmt.Errorf("drain assertion: %w", err)
		}
		if !state.postLoadCaptured {
			state.postLoadCaptured = true
			state.postLoadDriftPasses = operationValue(
				final,
				"ghsync_c_o4_operation_successes",
				"drift",
				"detector",
			)
			state.postLoadDriftSamples = operationValue(
				final,
				"ghsync_c_o4_operation_samples",
				"drift",
				"detector",
			)
			state.postLoadWatermarks = operationValue(
				final,
				"ghsync_c_o4_operation_successes",
				"watermarker",
				"entities",
			)
		}
		processed, deliveryErr := completedDeliveries(
			ctx,
			pool,
			plan.guids,
		)
		convergenceErr := error(nil)
		streamReady := false
		if deliveryErr == nil && processed == len(plan.guids) &&
			pipelineDrained(final) &&
			postPopulationTrustCompleted(final, *state) {
			convergenceErr = assertConverged(
				ctx,
				cfg,
				pool,
				plan.expected,
			)
			if convergenceErr == nil {
				deliveryErr = assertDroppedDeliveriesHealed(
					ctx,
					pool,
					plan.droppedAt,
					minimumStalenessBound(final),
				)
				if deliveryErr == nil {
					streamReady, deliveryErr = consumer.caughtUp(ctx)
				}
			}
		}
		if deliveryErr == nil && convergenceErr == nil && streamReady &&
			openDriftFindings(final) == 0 {
			return final, nil
		}
		if time.Now().After(deadline) {
			if convergenceErr == nil {
				convergenceErr = assertConverged(
					ctx,
					cfg,
					pool,
					plan.expected,
				)
			}
			return nil, fmt.Errorf(
				"pipeline did not drain and converge within %s "+
					"(deliveries=%d/%d event_queue=%.0f generations=%.0f "+
					"watermark_lag=%.0f open_drift=%.0f "+
					"convergence=%v trust_passes=%t "+
					"delivery_error=%v)",
				cfg.drainTimeout,
				processed,
				len(plan.guids),
				metricValue(
					final,
					"ghsync_c_p2_queue_depth",
					map[string]string{"queue": "event"},
				),
				metricValue(
					final,
					"ghsync_c_q2_outstanding_generations",
					nil,
				),
				metricValue(
					final,
					"ghsync_c_s2_watermark_lag_sequences",
					nil,
				),
				openDriftFindings(final),
				convergenceErr,
				postPopulationTrustCompleted(final, *state),
				deliveryErr,
			)
		}
		if err := wait(ctx, cfg.scrapeInterval); err != nil {
			return nil, err
		}
	}
}
