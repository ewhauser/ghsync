// soak drives the standalone fake GitHub against a running frontier-syncd and
// exits successfully only after the configured load, run-scoped assertions,
// durable trust passes, and exact fake-to-cache convergence all succeed.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/ingress"
	frontiermetrics "github.com/ewhauser/ghsync/internal/metrics"
	"github.com/ewhauser/ghsync/pkg/streamclient"
)

const (
	defaultEngineURL      = "http://127.0.0.1:8080"
	defaultFakeGitHubURL  = "http://127.0.0.1:9797"
	defaultRecordedRate   = 1.0
	defaultMultiplier     = 10.0
	defaultScrapeInterval = 2 * time.Second
	defaultDrainTimeout   = 90 * time.Second
	emitterConcurrency    = 16
	soakStream            = "entities"
)

type event struct {
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

type recordedEvent struct {
	GUID    string          `json:"guid"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

type config struct {
	engineURL      string
	fakeGitHubURL  string
	databaseURL    string
	installationID int64
	profile        string
	duration       time.Duration
	recordedRate   float64
	multiplier     float64
	eventsFile     string
	scrapeInterval time.Duration
	drainTimeout   time.Duration
	httpClient     *http.Client
}

type histogramSnapshot struct {
	count   uint64
	buckets map[float64]uint64
}

type runState struct {
	startWatermark       float64
	startStarvations     float64
	postLoadCaptured     bool
	postLoadDriftPasses  float64
	postLoadDriftSamples float64
	postLoadWatermarks   float64
	lastHistogram        histogramSnapshot
	runHistogram         histogramSnapshot
	histogramWindows     int
	samples              int
	budgetSamples        int
}

type soakStreamConsumer struct {
	parent      context.Context
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

func main() {
	if err := runMain(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(1)
	}
}

func runMain(args []string) error {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	engineURL := fs.String(
		"engine-url", defaultEngineURL, "running frontier-syncd base URL",
	)
	fakeGitHubURL := fs.String(
		"fake-github-url",
		defaultFakeGitHubURL,
		"standalone fake GitHub base URL",
	)
	databaseURL := fs.String(
		"database-url",
		os.Getenv("DATABASE_URL"),
		"Postgres URL used to prove final cache convergence",
	)
	installationID := fs.Int64(
		"installation-id",
		0,
		"GitHub installation whose completed cache seed is required (defaults to GITHUB_INSTALLATION_ID)",
	)
	profile := fs.String("profile", "smoke", "smoke, 48h, or custom")
	duration := fs.Duration(
		"duration", 0, "run duration (profile default when omitted)",
	)
	recordedRate := fs.Float64(
		"recorded-rate", defaultRecordedRate, "base events per second",
	)
	multiplier := fs.Float64(
		"multiplier", defaultMultiplier, "multiple of the base event rate",
	)
	eventsFile := fs.String(
		"events", "", "optional recorded event JSON array",
	)
	scrapeInterval := fs.Duration(
		"scrape-interval",
		defaultScrapeInterval,
		"metrics assertion interval",
	)
	drainTimeout := fs.Duration(
		"drain-timeout",
		defaultDrainTimeout,
		"post-replay pipeline drain timeout",
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("soak does not accept positional arguments")
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
	selectedDuration, err := profileDuration(*profile, *duration)
	if err != nil {
		return err
	}
	cfg := config{
		engineURL:      strings.TrimRight(*engineURL, "/"),
		fakeGitHubURL:  strings.TrimRight(*fakeGitHubURL, "/"),
		databaseURL:    strings.TrimSpace(*databaseURL),
		installationID: *installationID,
		profile:        *profile,
		duration:       selectedDuration,
		recordedRate:   *recordedRate,
		multiplier:     *multiplier,
		eventsFile:     *eventsFile,
		scrapeInterval: *scrapeInterval,
		drainTimeout:   *drainTimeout,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}
	return run(context.Background(), cfg)
}

func profileDuration(profile string, override time.Duration) (time.Duration, error) {
	if override > 0 {
		return override, nil
	}
	switch profile {
	case "smoke":
		return 2 * time.Minute, nil
	case "48h":
		return 48 * time.Hour, nil
	case "custom":
		return 0, fmt.Errorf("custom profile requires --duration")
	default:
		return 0, fmt.Errorf("unknown profile %q", profile)
	}
}

func validateConfig(cfg config) error {
	if cfg.duration <= 0 || cfg.recordedRate <= 0 || cfg.multiplier <= 0 ||
		cfg.scrapeInterval <= 0 || cfg.drainTimeout <= 0 {
		return fmt.Errorf("durations and event rates must be positive")
	}
	if cfg.engineURL == "" || cfg.fakeGitHubURL == "" ||
		cfg.databaseURL == "" {
		return fmt.Errorf("engine, fake GitHub, and database URLs are required")
	}
	if cfg.installationID <= 0 {
		return fmt.Errorf(
			"installation ID is required and must be positive",
		)
	}
	if expectedEventCount(
		cfg.duration,
		cfg.recordedRate,
		cfg.multiplier,
	) <= 0 {
		return fmt.Errorf("configured rate and duration produce no events")
	}
	return nil
}

// expectedEventCount is the load contract. The smoke defaults are exactly:
// 120 seconds * 1 recorded event/second * 10 = 1,200 successful deliveries.
func expectedEventCount(
	duration time.Duration,
	recordedRate float64,
	multiplier float64,
) int64 {
	return int64(math.Floor(
		duration.Seconds()*recordedRate*multiplier + 1e-9,
	))
}

func run(ctx context.Context, cfg config) error {
	events, err := loadEvents(cfg.eventsFile)
	if err != nil {
		return err
	}
	if err := waitHealthy(
		ctx,
		cfg.httpClient,
		cfg.engineURL+ingress.HealthPath,
		30*time.Second,
	); err != nil {
		return fmt.Errorf("engine health: %w", err)
	}
	if err := waitHealthy(
		ctx,
		cfg.httpClient,
		cfg.fakeGitHubURL+"/healthz",
		30*time.Second,
	); err != nil {
		return fmt.Errorf("fake GitHub health: %w", err)
	}
	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("connect convergence database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping convergence database: %w", err)
	}
	if err := waitForCacheSeed(ctx, cfg, pool); err != nil {
		return err
	}

	initial, err := scrape(ctx, cfg)
	if err != nil {
		return err
	}
	initialHistogram, err := histogramState(
		initial["frontier_c_q2_event_to_cache_latency_seconds"],
	)
	if err != nil {
		return err
	}
	state := runState{
		startWatermark: metricValue(
			initial,
			"frontier_c_s2_watermark_advances_total",
			nil,
		),
		startStarvations: metricValue(
			initial,
			"frontier_c_b3_starvations_total",
			nil,
		),
		lastHistogram: initialHistogram,
		runHistogram: histogramSnapshot{
			buckets: make(map[float64]uint64),
		},
	}
	if err := assertLive(initial, &state); err != nil {
		return fmt.Errorf("initial metrics: %w", err)
	}

	target := expectedEventCount(
		cfg.duration,
		cfg.recordedRate,
		cfg.multiplier,
	)
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	streamConsumer, err := newSoakStreamConsumer(ctx, pool, runID)
	if err != nil {
		return err
	}
	defer streamConsumer.cleanup()
	if err := streamConsumer.start(); err != nil {
		return err
	}
	startedAt := time.Now()
	loadCtx, cancelLoad := context.WithDeadline(
		ctx,
		startedAt.Add(cfg.duration),
	)
	defer cancelLoad()
	emittedCh := make(chan emissionResult, 1)
	go func() {
		emittedCh <- emitConfiguredLoad(
			loadCtx,
			cfg,
			events,
			runID,
			target,
			startedAt,
		)
	}()
	scrapeTicker := time.NewTicker(cfg.scrapeInterval)
	defer scrapeTicker.Stop()
	restartTimer := time.NewTimer(cfg.duration / 2)
	defer restartTimer.Stop()
	var emission emissionResult
loadLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-restartTimer.C:
			restartCtx, cancelRestart := context.WithTimeout(
				ctx, 5*time.Second,
			)
			restartErr := streamConsumer.restart(restartCtx)
			cancelRestart()
			if restartErr != nil {
				return restartErr
			}
		case emission = <-emittedCh:
			if emission.err != nil {
				return emission.err
			}
			break loadLoop
		case <-scrapeTicker.C:
			if err := streamConsumer.check(); err != nil {
				return err
			}
			families, scrapeErr := scrape(ctx, cfg)
			if scrapeErr != nil {
				return scrapeErr
			}
			if err := assertLive(families, &state); err != nil {
				return fmt.Errorf(
					"live assertion after %d events: %w",
					emissionCount(&emission),
					err,
				)
			}
		}
	}

	if err := streamConsumer.check(); err != nil {
		return err
	}
	if emission.count != target {
		return fmt.Errorf(
			"configured load missed: emitted %d, required %d",
			emission.count,
			target,
		)
	}
	achievedRate := float64(emission.count) / cfg.duration.Seconds()
	targetRate := float64(target) / cfg.duration.Seconds()

	drainDeadline := time.Now().Add(cfg.drainTimeout)
	var final map[string]*dto.MetricFamily
	for {
		if err := streamConsumer.check(); err != nil {
			return err
		}
		final, err = scrape(ctx, cfg)
		if err != nil {
			return err
		}
		if err := assertLive(final, &state); err != nil {
			return fmt.Errorf("drain assertion: %w", err)
		}
		if !state.postLoadCaptured {
			state.postLoadCaptured = true
			state.postLoadDriftPasses = operationValue(
				final, "frontier_c_o4_operation_successes", "drift", "detector",
			)
			state.postLoadDriftSamples = operationValue(
				final, "frontier_c_o4_operation_samples", "drift", "detector",
			)
			state.postLoadWatermarks = operationValue(
				final, "frontier_c_o4_operation_successes", "watermarker", "entities",
			)
		}
		streamReady := false
		if pipelineDrained(final) {
			if err := assertConverged(ctx, cfg, pool); err == nil &&
				postPopulationTrustCompleted(final, state) {
				caughtUp, streamErr := streamConsumer.caughtUp(ctx)
				if streamErr != nil {
					return streamErr
				}
				streamReady = caughtUp
			}
		}
		if streamReady {
			break
		}
		if time.Now().After(drainDeadline) {
			convergenceErr := assertConverged(ctx, cfg, pool)
			return fmt.Errorf(
				"pipeline did not drain, converge, and complete "+
					"post-load trust passes within %s "+
					"(delivery_age=%.1fs event_queue=%.0f generations=%.0f "+
					"watermark_lag=%.0f convergence=%v trust_passes=%t)",
				cfg.drainTimeout,
				metricValue(
					final,
					"frontier_c_q2_oldest_unprocessed_delivery_age_seconds",
					nil,
				),
				metricValue(
					final,
					"frontier_c_p2_queue_depth",
					map[string]string{"queue": "event"},
				),
				metricValue(
					final,
					"frontier_c_q2_outstanding_generations",
					nil,
				),
				metricValue(
					final,
					"frontier_c_s2_watermark_lag_sequences",
					nil,
				),
				convergenceErr,
				postPopulationTrustCompleted(final, state),
			)
		}
		if err := wait(ctx, cfg.scrapeInterval); err != nil {
			return err
		}
	}
	stopCtx, cancelStop := context.WithTimeout(ctx, 5*time.Second)
	err = streamConsumer.stop(stopCtx)
	cancelStop()
	if err != nil {
		return err
	}
	counts, err := streamConsumer.assertFinal(ctx)
	if err != nil {
		return err
	}
	if err := assertFinal(final, state); err != nil {
		return err
	}
	fmt.Printf(
		"soak %s passed: duration=%s expected=%d emitted=%d "+
			"required_rate=%.2f/s achieved_rate=%.2f/s samples=%d "+
			"stream_events=%d stream_restart_seq=%d\n",
		cfg.profile,
		cfg.duration,
		target,
		emission.count,
		targetRate,
		achievedRate,
		state.samples,
		counts.total,
		streamConsumer.restartSeq,
	)
	return nil
}

func newSoakStreamConsumer(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
) (*soakStreamConsumer, error) {
	table := "frontier_soak_applied_" + runID
	tableSQL := pgx.Identifier{table}.Sanitize()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE `+tableSQL+` (
		    seq BIGINT PRIMARY KEY,
		    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
		)
	`); err != nil {
		return nil, fmt.Errorf("create soak stream applied set: %w", err)
	}
	consumer := &soakStreamConsumer{
		parent:   ctx,
		pool:     pool,
		consumer: "frontier-soak-" + runID,
		tableSQL: tableSQL,
	}
	client, err := streamclient.New(pool, streamclient.Config{
		BatchSize:    256,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		consumer.cleanup()
		return nil, fmt.Errorf("create soak stream client: %w", err)
	}
	snapshot, err := client.Bootstrap(ctx, consumer.consumer, soakStream)
	if err != nil {
		consumer.cleanup()
		return nil, fmt.Errorf("bootstrap soak stream consumer: %w", err)
	}
	consumer.initialSeq = snapshot.SafeSeq
	if err := snapshot.Tx.Commit(ctx); err != nil {
		_ = snapshot.Tx.Rollback(context.WithoutCancel(ctx))
		consumer.cleanup()
		return nil, fmt.Errorf("commit soak stream bootstrap: %w", err)
	}
	return consumer, nil
}

func (c *soakStreamConsumer) start() error {
	if c.cancel != nil || c.done != nil {
		return fmt.Errorf("soak stream consumer is already running")
	}
	client, err := streamclient.New(c.pool, streamclient.Config{
		BatchSize:    256,
		PollInterval: 100 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("restart soak stream client: %w", err)
	}
	tailCtx, cancel := context.WithCancel(c.parent)
	c.cancel = cancel
	c.done = make(chan error, 1)
	go func(done chan<- error) {
		done <- client.Tail(
			tailCtx,
			c.consumer,
			soakStream,
			func(ctx context.Context, tx pgx.Tx, event streamclient.Event) error {
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

func (c *soakStreamConsumer) check() error {
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
		c.terminalErr = fmt.Errorf("soak stream consumer stopped: %w", err)
		return c.terminalErr
	default:
		return nil
	}
}

func (c *soakStreamConsumer) restart(ctx context.Context) error {
	if c.restarted {
		return fmt.Errorf("soak stream consumer restarted more than once")
	}
	if err := c.stop(ctx); err != nil {
		return fmt.Errorf("stop soak stream consumer for restart: %w", err)
	}
	counts, err := c.counts(ctx)
	if err != nil {
		return err
	}
	if counts.cursor <= c.initialSeq || counts.total == 0 {
		return fmt.Errorf(
			"mid-soak stream restart observed no committed progress "+
				"(initial_seq=%d cursor=%d applied=%d)",
			c.initialSeq,
			counts.cursor,
			counts.total,
		)
	}
	c.restartSeq = counts.cursor
	c.restartRows = counts.total
	c.restarted = true
	if err := c.start(); err != nil {
		return err
	}
	return nil
}

func (c *soakStreamConsumer) stop(ctx context.Context) error {
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
			return fmt.Errorf("stop soak stream consumer: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *soakStreamConsumer) caughtUp(ctx context.Context) (bool, error) {
	counts, err := c.counts(ctx)
	if err != nil {
		return false, err
	}
	return counts.total == counts.distinct &&
		counts.total == counts.expected &&
		counts.cursor == counts.maxSeq, nil
}

func (c *soakStreamConsumer) assertFinal(
	ctx context.Context,
) (streamCounts, error) {
	if !c.restarted {
		return streamCounts{}, fmt.Errorf(
			"soak stream consumer never completed its mid-run restart",
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
			"stream cursor did not reach the final entity sequence: "+
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

func (c *soakStreamConsumer) counts(ctx context.Context) (streamCounts, error) {
	var result streamCounts
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
	`, soakStream, c.initialSeq, c.consumer).Scan(
		&result.total,
		&result.distinct,
		&result.expected,
		&result.cursor,
		&result.maxSeq,
	)
	if err != nil {
		return streamCounts{}, fmt.Errorf(
			"read soak stream applied set: %w", err,
		)
	}
	return result, nil
}

func (c *soakStreamConsumer) cleanup() {
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.stop(ctx)
	if c.tableSQL != "" {
		_, _ = c.pool.Exec(ctx, "DROP TABLE IF EXISTS "+c.tableSQL)
	}
	if c.consumer != "" {
		_, _ = c.pool.Exec(ctx, `
			DELETE FROM consumer_cursors
			WHERE consumer = $1 AND stream = $2
		`, c.consumer, soakStream)
	}
}

func operationValue(
	families map[string]*dto.MetricFamily,
	metricName string,
	component string,
	operation string,
) float64 {
	return metricValue(
		families,
		metricName,
		map[string]string{
			"component": component,
			"operation": operation,
		},
	)
}

func postPopulationTrustCompleted(
	families map[string]*dto.MetricFamily,
	state runState,
) bool {
	return state.postLoadCaptured &&
		operationValue(
			families,
			"frontier_c_o4_operation_successes",
			"drift",
			"detector",
		) > state.postLoadDriftPasses &&
		operationValue(
			families,
			"frontier_c_o4_operation_samples",
			"drift",
			"detector",
		) > state.postLoadDriftSamples &&
		operationValue(
			families,
			"frontier_c_o4_operation_successes",
			"watermarker",
			"entities",
		) > state.postLoadWatermarks
}

func waitForCacheSeed(
	ctx context.Context,
	cfg config,
	pool *pgxpool.Pool,
) error {
	deadline := time.Now().Add(cfg.drainTimeout)
	var lastState string
	for {
		var installationDone bool
		var repositories int64
		var incompleteRepos int64
		var pendingChildren int64
		var cacheWriters int64
		err := pool.QueryRow(ctx, `
			SELECT
			    EXISTS (
			        SELECT 1
			        FROM installation_backfill_cursors
			        WHERE installation_id = $1
			          AND phase = 'done'
			          AND completed_at IS NOT NULL
			    ),
			    (SELECT count(*)
			     FROM backfill_cursors
			     WHERE installation_id = $1),
			    (SELECT count(*)
			     FROM backfill_cursors
			     WHERE installation_id = $1
			       AND phase <> 'done'),
			    (SELECT count(*)
			     FROM backfill_children
			     WHERE installation_id = $1
			       AND completed_at IS NULL),
			    (SELECT count(*)
			     FROM river_job
			     WHERE queue IN (
			         'interactive', 'event', 'sweep', 'reconcile'
			     )
			       AND state IN (
			           'available', 'pending', 'retryable',
			           'running', 'scheduled'
			       ))
		`, cfg.installationID).Scan(
			&installationDone,
			&repositories,
			&incompleteRepos,
			&pendingChildren,
			&cacheWriters,
		)
		if err != nil {
			return fmt.Errorf("check completed cache seed: %w", err)
		}
		lastState = fmt.Sprintf(
			"installation_done=%t repositories=%d incomplete_repositories=%d "+
				"pending_children=%d cache_writer_jobs=%d",
			installationDone,
			repositories,
			incompleteRepos,
			pendingChildren,
			cacheWriters,
		)
		if installationDone && repositories > 0 &&
			incompleteRepos == 0 && pendingChildren == 0 &&
			cacheWriters == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"cache seed did not complete within %s (%s); "+
					"run frontier-syncd backfill before soak",
				cfg.drainTimeout,
				lastState,
			)
		}
		if err := wait(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

type emissionResult struct {
	count int64
	err   error
}

func emissionCount(result *emissionResult) int64 {
	if result == nil {
		return 0
	}
	return result.count
}

func emitConfiguredLoad(
	ctx context.Context,
	cfg config,
	events []event,
	runID string,
	target int64,
	startedAt time.Time,
) emissionResult {
	rate := cfg.recordedRate * cfg.multiplier
	jobs := make(chan int64)
	errs := make(chan error, emitterConcurrency)
	var succeeded atomic.Int64
	var workers sync.WaitGroup
	for worker := 0; worker < emitterConcurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				item := events[int((sequence-1)%int64(len(events)))]
				if err := emit(
					ctx,
					cfg,
					item,
					fmt.Sprintf("soak-%s-%012d", runID, sequence),
					sequence,
				); err != nil {
					select {
					case errs <- fmt.Errorf(
						"emit event %d: %w",
						sequence,
						err,
					):
					default:
					}
					return
				}
				succeeded.Add(1)
			}
		}()
	}
	scheduleErr := error(nil)
	for sequence := int64(1); sequence <= target; sequence++ {
		offset := time.Duration(
			(float64(sequence-1) / rate) * float64(time.Second),
		)
		if err := waitUntil(ctx, startedAt.Add(offset)); err != nil {
			scheduleErr = fmt.Errorf(
				"configured load deadline reached after %d/%d events: %w",
				succeeded.Load(),
				target,
				err,
			)
			break
		}
		select {
		case jobs <- sequence:
		case err := <-errs:
			scheduleErr = err
		case <-ctx.Done():
			scheduleErr = fmt.Errorf(
				"configured load deadline reached after %d/%d events: %w",
				succeeded.Load(),
				target,
				ctx.Err(),
			)
		}
		if scheduleErr != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-errs:
		if scheduleErr == nil {
			scheduleErr = err
		}
	default:
	}
	count := succeeded.Load()
	if scheduleErr == nil && count != target {
		scheduleErr = fmt.Errorf(
			"configured load emitted %d successful events, want %d",
			count,
			target,
		)
	}
	return emissionResult{count: count, err: scheduleErr}
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func loadEvents(path string) ([]event, error) {
	if path == "" {
		result := make([]event, 0, 4)
		for _, number := range []int{4812, 4815, 4816, 4820} {
			payload, err := json.Marshal(map[string]any{
				"action":     "synchronize",
				"number":     number,
				"repository": map[string]any{"full_name": "acme/monolith"},
				"pull_request": map[string]any{
					"number": number,
					"stack":  nil,
				},
			})
			if err != nil {
				return nil, err
			}
			result = append(result, event{
				Event: "pull_request", Payload: payload,
			})
		}
		return result, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	var recorded []recordedEvent
	if err := json.Unmarshal(body, &recorded); err != nil {
		return nil, fmt.Errorf("decode recorded events: %w", err)
	}
	result := make([]event, 0, len(recorded))
	for index, item := range recorded {
		if item.Event == "" || len(item.Payload) == 0 {
			return nil, fmt.Errorf("recorded event %d is incomplete", index)
		}
		result = append(result, event{
			Event: item.Event, Payload: item.Payload,
		})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("recorded event file is empty")
	}
	return result, nil
}

func emit(
	ctx context.Context,
	cfg config,
	item event,
	guid string,
	revision int64,
) error {
	var payload map[string]any
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return fmt.Errorf("decode event payload: %w", err)
	}
	payload["soak_revision"] = revision
	requestBody, err := json.Marshal(map[string]any{
		"target_url": cfg.engineURL + ingress.WebhookPath,
		"event":      item.Event,
		"guid":       guid,
		"mutate":     true,
		"payload":    payload,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		cfg.fakeGitHubURL+fakegithub.ControlEmitPath,
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"fake control returned %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(message)),
		)
	}
	return nil
}

func waitHealthy(
	ctx context.Context,
	client *http.Client,
	target string,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		request, err := http.NewRequestWithContext(
			ctx, http.MethodGet, target, nil,
		)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode <= 299 {
				return nil
			}
			lastErr = fmt.Errorf("status %d", response.StatusCode)
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		if err := wait(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func scrape(
	ctx context.Context,
	cfg config,
) (map[string]*dto.MetricFamily, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		cfg.engineURL+frontiermetrics.Path,
		nil,
	)
	if err != nil {
		return nil, err
	}
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("scrape metrics: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf(
			"scrape metrics status %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(message)),
		)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	return families, nil
}

func assertLive(
	families map[string]*dto.MetricFamily,
	state *runState,
) error {
	state.samples++
	required := []string{
		"frontier_c_b3_starvations_total",
		"frontier_c_i5_parked_deliveries",
		"frontier_c_o3_drift_findings",
		"frontier_c_o4_operation_successes",
		"frontier_c_o4_operation_samples",
		"frontier_c_o4_last_operation_sample_age_seconds",
		"frontier_c_p2_queue_depth",
		"frontier_c_q2_oldest_unprocessed_delivery_age_seconds",
		"frontier_c_q2_outstanding_generations",
		"frontier_c_s2_watermark_lag_sequences",
	}
	for _, name := range required {
		if families[name] == nil {
			return fmt.Errorf("required metric %s is absent", name)
		}
	}
	if parked := metricValue(
		families, "frontier_c_i5_parked_deliveries", nil,
	); parked > 0 {
		return fmt.Errorf("C-I5 parked deliveries = %.0f", parked)
	}
	if open := metricValue(
		families,
		"frontier_c_o3_drift_findings",
		map[string]string{"state": "open", "entity_kind": "all"},
	); open > 0 {
		return fmt.Errorf("C-O3 open drift findings = %.0f", open)
	}
	starvations := metricValue(
		families,
		"frontier_c_b3_starvations_total",
		nil,
	)
	if starvations > state.startStarvations {
		return fmt.Errorf(
			"C-B3 starvation counter increased by %.0f",
			starvations-state.startStarvations,
		)
	}
	if budget := families["frontier_c_b3_budget_remaining"]; budget != nil && len(budget.Metric) > 0 {
		state.budgetSamples++
	}
	current, err := histogramState(
		families["frontier_c_q2_event_to_cache_latency_seconds"],
	)
	if err != nil {
		return err
	}
	window, err := subtractHistogram(current, state.lastHistogram)
	if err != nil {
		return fmt.Errorf("C-Q2 scrape-window histogram delta: %w", err)
	}
	if window.count > 0 {
		p95, err := histogramQuantileSnapshot(window, 0.95)
		if err != nil {
			return err
		}
		p99, err := histogramQuantileSnapshot(window, 0.99)
		if err != nil {
			return err
		}
		if p95 > 20 {
			return fmt.Errorf(
				"C-Q2 scrape-window p95 %.1fs exceeds 20s",
				p95,
			)
		}
		if p99 > 60 {
			return fmt.Errorf(
				"C-Q2 scrape-window p99 %.1fs exceeds 60s",
				p99,
			)
		}
	}
	addHistogram(&state.runHistogram, window)
	if window.count > 0 {
		state.histogramWindows++
	}
	state.lastHistogram = current
	return nil
}

func pipelineDrained(families map[string]*dto.MetricFamily) bool {
	return metricValue(
		families,
		"frontier_c_q2_oldest_unprocessed_delivery_age_seconds",
		nil,
	) == 0 &&
		metricValue(
			families,
			"frontier_c_p2_queue_depth",
			map[string]string{"queue": "event"},
		) == 0 &&
		metricValue(
			families,
			"frontier_c_q2_outstanding_generations",
			nil,
		) == 0 &&
		metricValue(
			families,
			"frontier_c_s2_watermark_lag_sequences",
			nil,
		) == 0
}

func assertFinal(
	families map[string]*dto.MetricFamily,
	state runState,
) error {
	if state.budgetSamples == 0 {
		return fmt.Errorf("C-B3 budget metrics were never observed")
	}
	endWatermark := metricValue(
		families,
		"frontier_c_s2_watermark_advances_total",
		nil,
	)
	if endWatermark <= state.startWatermark {
		return fmt.Errorf(
			"C-S2 watermark did not advance (start %.0f, end %.0f)",
			state.startWatermark,
			endWatermark,
		)
	}
	endWatermarkPasses := metricValue(
		families,
		"frontier_c_o4_operation_successes",
		map[string]string{
			"component": "watermarker",
			"operation": "entities",
		},
	)
	if endWatermarkPasses <= state.postLoadWatermarks {
		return fmt.Errorf(
			"C-S2 no post-load durable watermark pass completed "+
				"(post-load %.0f, end %.0f)",
			state.postLoadWatermarks,
			endWatermarkPasses,
		)
	}
	endDriftPasses := metricValue(
		families,
		"frontier_c_o4_operation_successes",
		map[string]string{
			"component": "drift",
			"operation": "detector",
		},
	)
	if endDriftPasses <= state.postLoadDriftPasses {
		return fmt.Errorf(
			"C-O3 no post-population drift pass completed "+
				"(post-load %.0f, end %.0f)",
			state.postLoadDriftPasses,
			endDriftPasses,
		)
	}
	endDriftSamples := metricValue(
		families,
		"frontier_c_o4_operation_samples",
		map[string]string{
			"component": "drift",
			"operation": "detector",
		},
	)
	if endDriftSamples <= state.postLoadDriftSamples {
		return fmt.Errorf(
			"C-O3 post-population drift pass inspected no samples "+
				"(post-load %.0f, end %.0f)",
			state.postLoadDriftSamples,
			endDriftSamples,
		)
	}
	if state.runHistogram.count == 0 || state.histogramWindows == 0 {
		return fmt.Errorf("C-Q2 run-scoped histogram has no new samples")
	}
	p95, err := histogramQuantileSnapshot(state.runHistogram, 0.95)
	if err != nil {
		return err
	}
	p99, err := histogramQuantileSnapshot(state.runHistogram, 0.99)
	if err != nil {
		return err
	}
	if p95 > 20 {
		return fmt.Errorf("C-Q2 run-scoped p95 %.1fs exceeds 20s", p95)
	}
	if p99 > 60 {
		return fmt.Errorf("C-Q2 run-scoped p99 %.1fs exceeds 60s", p99)
	}
	return nil
}

func assertConverged(
	ctx context.Context,
	cfg config,
	pool *pgxpool.Pool,
) error {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		cfg.fakeGitHubURL+fakegithub.ControlTruthPath,
		nil,
	)
	if err != nil {
		return err
	}
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("read fake truth: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("read fake truth status %d", response.StatusCode)
	}
	var truth fakegithub.SoakTruth
	if err := json.NewDecoder(response.Body).Decode(&truth); err != nil {
		return fmt.Errorf("decode fake truth: %w", err)
	}
	if len(truth.PullRequests) == 0 {
		return fmt.Errorf("fake truth contains no mutated pull requests")
	}
	rows, err := pool.Query(ctx, `
		SELECT pull.number, pull.title
		FROM pull_requests AS pull
		JOIN repos AS repo ON repo.id = pull.repo_id
		WHERE repo.full_name = $1
		  AND repo.tombstoned_at IS NULL
		  AND pull.tombstoned_at IS NULL
	`, truth.Repository)
	if err != nil {
		return fmt.Errorf("query cached truth: %w", err)
	}
	cached := make(map[int]string)
	for rows.Next() {
		var number int
		var title string
		if err := rows.Scan(&number, &title); err != nil {
			rows.Close()
			return err
		}
		cached[number] = title
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, pull := range truth.PullRequests {
		got, ok := cached[pull.Number]
		if !ok {
			return fmt.Errorf("cache is missing truth PR %d", pull.Number)
		}
		if got != pull.Title {
			return fmt.Errorf(
				"cache PR %d title %q does not match fake truth %q",
				pull.Number,
				got,
				pull.Title,
			)
		}
	}
	return nil
}

func histogramState(
	family *dto.MetricFamily,
) (histogramSnapshot, error) {
	if family == nil {
		return histogramSnapshot{buckets: make(map[float64]uint64)}, nil
	}
	result := histogramSnapshot{buckets: make(map[float64]uint64)}
	for _, item := range family.Metric {
		histogram := item.GetHistogram()
		if histogram == nil {
			continue
		}
		result.count += histogram.GetSampleCount()
		for _, bucket := range histogram.Bucket {
			result.buckets[bucket.GetUpperBound()] +=
				bucket.GetCumulativeCount()
		}
	}
	return result, nil
}

func subtractHistogram(
	current histogramSnapshot,
	prior histogramSnapshot,
) (histogramSnapshot, error) {
	if current.count < prior.count {
		return histogramSnapshot{}, fmt.Errorf(
			"sample count regressed from %d to %d",
			prior.count,
			current.count,
		)
	}
	result := histogramSnapshot{
		count:   current.count - prior.count,
		buckets: make(map[float64]uint64),
	}
	for bound, value := range current.buckets {
		before := prior.buckets[bound]
		if value < before {
			return histogramSnapshot{}, fmt.Errorf(
				"bucket %v regressed from %d to %d",
				bound,
				before,
				value,
			)
		}
		result.buckets[bound] = value - before
	}
	return result, nil
}

func addHistogram(target *histogramSnapshot, delta histogramSnapshot) {
	if target.buckets == nil {
		target.buckets = make(map[float64]uint64)
	}
	target.count += delta.count
	for bound, value := range delta.buckets {
		target.buckets[bound] += value
	}
}

func histogramQuantile(
	family *dto.MetricFamily,
	quantile float64,
) (float64, uint64, error) {
	snapshot, err := histogramState(family)
	if err != nil {
		return 0, 0, err
	}
	value, err := histogramQuantileSnapshot(snapshot, quantile)
	return value, snapshot.count, err
}

func histogramQuantileSnapshot(
	histogram histogramSnapshot,
	quantile float64,
) (float64, error) {
	if histogram.count == 0 {
		return 0, nil
	}
	target := uint64(math.Ceil(float64(histogram.count) * quantile))
	bounds := make([]float64, 0, len(histogram.buckets))
	for bound := range histogram.buckets {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	for _, bound := range bounds {
		if histogram.buckets[bound] >= target {
			return bound, nil
		}
	}
	return 0, fmt.Errorf(
		"C-Q2 p%.0f exceeds the largest histogram bucket",
		quantile*100,
	)
}

func metricValue(
	families map[string]*dto.MetricFamily,
	name string,
	labels map[string]string,
) float64 {
	family := families[name]
	if family == nil {
		return 0
	}
	if labels == nil {
		var total float64
		for _, item := range family.Metric {
			total += sampleValue(item)
		}
		return total
	}
	for _, item := range family.Metric {
		if labelsMatch(labelsOf(item), labels) {
			return sampleValue(item)
		}
	}
	return 0
}

func sampleValue(item *dto.Metric) float64 {
	switch {
	case item.Gauge != nil:
		return item.Gauge.GetValue()
	case item.Counter != nil:
		return item.Counter.GetValue()
	case item.Untyped != nil:
		return item.Untyped.GetValue()
	default:
		return 0
	}
}

func labelsOf(item *dto.Metric) map[string]string {
	labels := make(map[string]string, len(item.Label))
	for _, label := range item.Label {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

func labelsMatch(got map[string]string, want map[string]string) bool {
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
