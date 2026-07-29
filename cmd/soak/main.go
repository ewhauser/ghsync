// soak drives the standalone fake GitHub against a running frontier-syncd and
// fails unless the C-O4 operational contract remains healthy.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/ingress"
	frontiermetrics "github.com/acme/frontier/internal/metrics"
)

const (
	defaultEngineURL      = "http://127.0.0.1:8080"
	defaultFakeGitHubURL  = "http://127.0.0.1:9797"
	defaultRecordedRate   = 1.0
	defaultMultiplier     = 10.0
	defaultScrapeInterval = 2 * time.Second
	defaultDrainTimeout   = 90 * time.Second
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
	profile        string
	duration       time.Duration
	recordedRate   float64
	multiplier     float64
	eventsFile     string
	scrapeInterval time.Duration
	drainTimeout   time.Duration
	httpClient     *http.Client
}

type runState struct {
	startWatermark float64
	maxParked      float64
	samples        int
	budgetSamples  int
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
	selectedDuration, err := profileDuration(*profile, *duration)
	if err != nil {
		return err
	}
	cfg := config{
		engineURL:      strings.TrimRight(*engineURL, "/"),
		fakeGitHubURL:  strings.TrimRight(*fakeGitHubURL, "/"),
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return run(ctx, cfg)
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
	if cfg.engineURL == "" || cfg.fakeGitHubURL == "" {
		return fmt.Errorf("engine and fake GitHub URLs are required")
	}
	return nil
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

	initial, err := scrape(ctx, cfg)
	if err != nil {
		return err
	}
	state := runState{
		startWatermark: metricValue(
			initial,
			"frontier_c_s2_watermark_advances_total",
			nil,
		),
	}
	if err := assertLive(initial, &state); err != nil {
		return fmt.Errorf("initial metrics: %w", err)
	}

	rate := cfg.recordedRate * cfg.multiplier
	emitInterval := time.Duration(float64(time.Second) / rate)
	if emitInterval < time.Millisecond {
		emitInterval = time.Millisecond
	}
	emitTicker := time.NewTicker(emitInterval)
	defer emitTicker.Stop()
	scrapeTicker := time.NewTicker(cfg.scrapeInterval)
	defer scrapeTicker.Stop()
	finish := time.NewTimer(cfg.duration)
	defer finish.Stop()
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	var emitted int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-finish.C:
			goto drain
		case <-emitTicker.C:
			item := events[int(emitted)%len(events)]
			emitted++
			if err := emit(
				ctx,
				cfg,
				item,
				fmt.Sprintf("soak-%s-%012d", runID, emitted),
				emitted,
			); err != nil {
				return fmt.Errorf("emit event %d: %w", emitted, err)
			}
		case <-scrapeTicker.C:
			families, err := scrape(ctx, cfg)
			if err != nil {
				return err
			}
			if err := assertLive(families, &state); err != nil {
				return fmt.Errorf("live assertion after %d events: %w", emitted, err)
			}
		}
	}

drain:
	drainDeadline := time.Now().Add(cfg.drainTimeout)
	var final map[string]*dto.MetricFamily
	for {
		final, err = scrape(ctx, cfg)
		if err != nil {
			return err
		}
		if err := assertLive(final, &state); err != nil {
			return fmt.Errorf("drain assertion: %w", err)
		}
		if pipelineDrained(final) {
			break
		}
		if time.Now().After(drainDeadline) {
			return fmt.Errorf(
				"pipeline did not drain within %s (oldest delivery %.1fs, watermark lag %.0f)",
				cfg.drainTimeout,
				metricValue(
					final,
					"frontier_c_q2_oldest_unprocessed_delivery_age_seconds",
					nil,
				),
				metricValue(
					final,
					"frontier_c_s2_watermark_lag_sequences",
					nil,
				),
			)
		}
		timer := time.NewTimer(cfg.scrapeInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err := assertFinal(final, state); err != nil {
		return err
	}
	fmt.Printf(
		"soak %s passed: duration=%s events=%d rate=%.2fx samples=%d\n",
		cfg.profile,
		cfg.duration,
		emitted,
		cfg.multiplier,
		state.samples,
	)
	return nil
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
	defer response.Body.Close()
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
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
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
	parked := metricValue(
		families, "frontier_c_i5_parked_deliveries", nil,
	)
	state.maxParked = math.Max(state.maxParked, parked)
	if parked > 0 {
		return fmt.Errorf("C-I5 parked deliveries = %.0f", parked)
	}
	if open := metricValue(
		families,
		"frontier_c_o3_drift_findings",
		map[string]string{"state": "open", "entity_kind": "all"},
	); open > 0 {
		return fmt.Errorf("C-O3 open drift findings = %.0f", open)
	}
	checked, err := assertBudgetFloors(families)
	if err != nil {
		return err
	}
	if checked {
		state.budgetSamples++
	}
	return nil
}

func assertBudgetFloors(
	families map[string]*dto.MetricFamily,
) (bool, error) {
	remainingFamily := families["frontier_c_b3_budget_remaining"]
	if remainingFamily == nil {
		return false, nil
	}
	checked := false
	for _, remainingMetric := range remainingFamily.Metric {
		labels := labelsOf(remainingMetric)
		class := labels["class"]
		if class != "event" && class != "sweep" {
			continue
		}
		remaining := sampleValue(remainingMetric)
		floor := metricValue(
			families,
			"frontier_c_b3_budget_floor",
			labels,
		)
		checked = true
		if remaining < floor {
			return true, fmt.Errorf(
				"C-B3 %s/%s remaining %.0f breached floor %.0f",
				class,
				labels["resource"],
				remaining,
				floor,
			)
		}
	}
	return checked, nil
}

func pipelineDrained(families map[string]*dto.MetricFamily) bool {
	return metricValue(
		families,
		"frontier_c_q2_oldest_unprocessed_delivery_age_seconds",
		nil,
	) == 0 && metricValue(
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
	histogram := families["frontier_c_q2_event_to_cache_latency_seconds"]
	if histogram == nil {
		return fmt.Errorf("C-Q2 event-to-cache histogram was not observed")
	}
	p95, count, err := histogramQuantile(histogram, 0.95)
	if err != nil {
		return err
	}
	p99, _, err := histogramQuantile(histogram, 0.99)
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("C-Q2 event-to-cache histogram has no samples")
	}
	if p95 > 20 {
		return fmt.Errorf("C-Q2 p95 %.1fs exceeds 20s", p95)
	}
	if p99 > 60 {
		return fmt.Errorf("C-Q2 p99 %.1fs exceeds 60s", p99)
	}
	return nil
}

func histogramQuantile(
	family *dto.MetricFamily,
	quantile float64,
) (float64, uint64, error) {
	var count uint64
	cumulativeByBound := make(map[float64]uint64)
	for _, item := range family.Metric {
		histogram := item.GetHistogram()
		if histogram == nil {
			continue
		}
		count += histogram.GetSampleCount()
		for _, bucket := range histogram.Bucket {
			cumulativeByBound[bucket.GetUpperBound()] +=
				bucket.GetCumulativeCount()
		}
	}
	if count == 0 {
		return 0, 0, nil
	}
	target := uint64(math.Ceil(float64(count) * quantile))
	bounds := make([]float64, 0, len(cumulativeByBound))
	for bound := range cumulativeByBound {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	for _, bound := range bounds {
		if cumulativeByBound[bound] >= target {
			return bound, count, nil
		}
	}
	return 0, count, fmt.Errorf(
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
