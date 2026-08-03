package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/ingress"
	ghsyncmetrics "github.com/ewhauser/ghsync/internal/metrics"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func newRunState(
	families map[string]*dto.MetricFamily,
) *runState {
	histogram := histogramState(
		families["ghsync_c_q2_event_to_cache_latency_seconds"],
	)
	state := &runState{
		segmentWatermark: metricValue(
			families,
			"ghsync_c_s2_watermark_advances_total",
			nil,
		),
		segmentStarvations: metricValue(
			families,
			"ghsync_c_b3_starvations_total",
			nil,
		),
		lastHistogram: histogram,
		runHistogram: histogramSnapshot{
			buckets: make(map[float64]uint64),
		},
	}
	state.last500Responses = githubStatusResponses(families, 500)
	state.last429Responses = githubStatusResponses(families, 429)
	state.lastGapRedeliveries = metricValue(
		families,
		"ghsync_c_r4_gap_heal_requests_total",
		nil,
	)
	return state
}

func (state *runState) resetSegment(
	families map[string]*dto.MetricFamily,
) error {
	histogram := histogramState(
		families["ghsync_c_q2_event_to_cache_latency_seconds"],
	)
	state.segmentWatermark = metricValue(
		families,
		"ghsync_c_s2_watermark_advances_total",
		nil,
	)
	state.segmentStarvations = metricValue(
		families,
		"ghsync_c_b3_starvations_total",
		nil,
	)
	if state.segmentStarvations != 0 {
		return fmt.Errorf(
			"C-B3 restarted process already recorded %.0f starvation increments",
			state.segmentStarvations,
		)
	}
	if err := state.observeFreshCounters(families); err != nil {
		return err
	}
	if histogram.count > 0 {
		addHistogram(&state.runHistogram, histogram)
		state.histogramWindows++
	}
	state.lastHistogram = histogram
	state.metricRestarts++
	return nil
}

func (state *runState) observeFreshCounters(
	families map[string]*dto.MetricFamily,
) error {
	current500 := githubStatusResponses(families, 500)
	current429 := githubStatusResponses(families, 429)
	currentGap := metricValue(
		families,
		"ghsync_c_r4_gap_heal_requests_total",
		nil,
	)
	if current500 < 0 || current429 < 0 || currentGap < 0 {
		return fmt.Errorf("fresh process exposed a negative counter")
	}
	state.observed500Responses += current500
	state.observed429Responses += current429
	state.observedGapHeals += currentGap
	state.last500Responses = current500
	state.last429Responses = current429
	state.lastGapRedeliveries = currentGap
	return nil
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
			"ghsync_c_o4_operation_successes",
			"drift",
			"detector",
		) > state.postLoadDriftPasses &&
		operationValue(
			families,
			"ghsync_c_o4_operation_samples",
			"drift",
			"detector",
		) > state.postLoadDriftSamples &&
		operationValue(
			families,
			"ghsync_c_o4_operation_successes",
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
		state, err := dbgen.New(pool).GetLoadgenCacheSeedState(
			ctx,
			cfg.installationID,
		)
		if err != nil {
			return fmt.Errorf("check completed cache seed: %w", err)
		}
		lastState = fmt.Sprintf(
			"installation_done=%t repositories=%d incomplete_repositories=%d "+
				"pending_children=%d backfill_jobs=%d",
			state.InstallationDone,
			state.Repositories,
			state.IncompleteRepos,
			state.PendingChildren,
			state.BackfillJobs,
		)
		if state.InstallationDone &&
			state.Repositories == int64(cfg.copies) &&
			state.IncompleteRepos == 0 && state.PendingChildren == 0 &&
			state.BackfillJobs == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"cache seed did not complete within %s (%s); "+
					"initialize fake repositories before backfill",
				cfg.drainTimeout,
				lastState,
			)
		}
		if err := wait(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
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
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
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

func waitOwnedEngineHealthy(
	ctx context.Context,
	cfg config,
	engine *engineProcess,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	target := cfg.engineURL + ingress.HealthPath
	var lastErr error
	for {
		if err := engine.EnsureRunning(ctx); err != nil {
			lastErr = err
		} else {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
			if err != nil {
				return err
			}
			response, err := cfg.httpClient.Do(request)
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode >= 200 &&
					response.StatusCode <= 299 {
					return nil
				}
				lastErr = fmt.Errorf(
					"status %d",
					response.StatusCode,
				)
			} else {
				lastErr = err
			}
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		if err := wait(ctx, 2*time.Second); err != nil {
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
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.engineURL+ghsyncmetrics.Path, http.NoBody)
	if err != nil {
		return nil, err
	}
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("scrape metrics: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
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
		"ghsync_c_b2_gate_closed",
		"ghsync_c_b3_starvations_total",
		"ghsync_c_i5_parked_deliveries",
		"ghsync_c_o3_drift_findings",
		"ghsync_c_o4_last_operation_sample_age_seconds",
		"ghsync_c_o4_operation_successes",
		"ghsync_c_o4_operation_samples",
		"ghsync_c_p2_queue_depth",
		"ghsync_c_q2_oldest_unprocessed_delivery_age_seconds",
		"ghsync_c_q2_outstanding_generations",
		"ghsync_c_r1_cache_staleness_seconds",
		"ghsync_c_r1_staleness_bound_seconds",
		"ghsync_c_s2_watermark_lag_sequences",
	}
	for _, name := range required {
		if families[name] == nil {
			return fmt.Errorf("required metric %s is absent", name)
		}
	}
	if parked := metricValue(
		families,
		"ghsync_c_i5_parked_deliveries",
		nil,
	); parked > 0 {
		return fmt.Errorf("C-I5 parked deliveries = %.0f", parked)
	}
	starvations := metricValue(
		families,
		"ghsync_c_b3_starvations_total",
		nil,
	)
	if starvations > state.segmentStarvations {
		return fmt.Errorf(
			"C-B3 starvation counter increased by %.0f",
			starvations-state.segmentStarvations,
		)
	}
	watermarks := metricValue(
		families,
		"ghsync_c_s2_watermark_advances_total",
		nil,
	)
	if watermarks > state.segmentWatermark {
		state.watermarkAdvanced = true
	}
	if err := observeCounterDelta(
		githubStatusResponses(families, 500),
		&state.last500Responses,
		&state.observed500Responses,
		"GitHub HTTP 500 responses",
	); err != nil {
		return err
	}
	if err := observeCounterDelta(
		githubStatusResponses(families, 429),
		&state.last429Responses,
		&state.observed429Responses,
		"GitHub HTTP 429 responses",
	); err != nil {
		return err
	}
	if err := observeCounterDelta(
		metricValue(
			families,
			"ghsync_c_r4_gap_heal_requests_total",
			nil,
		),
		&state.lastGapRedeliveries,
		&state.observedGapHeals,
		"C-R4 gap redeliveries",
	); err != nil {
		return err
	}
	if budget := families["ghsync_c_b3_budget_remaining"]; budget != nil &&
		len(budget.Metric) > 0 {
		state.budgetSamples++
	}
	if metricValue(
		families,
		"ghsync_c_b2_gate_closed",
		nil,
	) > 0 {
		state.gateClosedSamples++
	}
	cR1Samples, err := assertStalenessWithinBounds(families)
	if err != nil {
		return err
	}
	state.cR1Samples += cR1Samples
	current := histogramState(
		families["ghsync_c_q2_event_to_cache_latency_seconds"],
	)
	window, err := subtractHistogram(current, state.lastHistogram)
	if err != nil {
		return fmt.Errorf("C-Q2 scrape-window histogram delta: %w", err)
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
		"ghsync_c_q2_oldest_unprocessed_delivery_age_seconds",
		nil,
	) == 0 &&
		metricValue(
			families,
			"ghsync_c_p2_queue_depth",
			map[string]string{"queue": "event"},
		) == 0 &&
		metricValue(
			families,
			"ghsync_c_q2_outstanding_generations",
			nil,
		) == 0 &&
		metricValue(
			families,
			"ghsync_c_s2_watermark_lag_sequences",
			nil,
		) == 0
}

func assertFinal(
	families map[string]*dto.MetricFamily,
	state runState,
	expected500s int,
	expected429s int,
	expectedDrops int,
) error {
	if state.budgetSamples == 0 {
		return fmt.Errorf("C-B3 budget metrics were never observed")
	}
	if open := openDriftFindings(families); open > 0 {
		return fmt.Errorf("C-O3 open drift findings = %.0f", open)
	}
	if expected429s > 0 && state.gateClosedSamples == 0 {
		return fmt.Errorf("fake 429 chaos never engaged the C-B2 backoff gate")
	}
	if state.observed500Responses < float64(expected500s) {
		return fmt.Errorf(
			"engine observed %.0f GitHub HTTP 500 responses, want at least %d",
			state.observed500Responses,
			expected500s,
		)
	}
	if state.observed429Responses < float64(expected429s) {
		return fmt.Errorf(
			"engine observed %.0f GitHub HTTP 429 responses, want at least %d",
			state.observed429Responses,
			expected429s,
		)
	}
	if state.observedGapHeals < float64(expectedDrops) {
		return fmt.Errorf(
			"C-R4 redelivered %.0f dropped webhooks, want at least %d",
			state.observedGapHeals,
			expectedDrops,
		)
	}
	if state.requireCR1 && state.cR1Samples == 0 {
		return fmt.Errorf("dropped-delivery chaos observed no C-R1 samples")
	}
	if !state.watermarkAdvanced {
		return fmt.Errorf("C-S2 watermark did not advance during the run")
	}
	endWatermarkPasses := operationValue(
		families,
		"ghsync_c_o4_operation_successes",
		"watermarker",
		"entities",
	)
	if endWatermarkPasses <= state.postLoadWatermarks {
		return fmt.Errorf(
			"C-S2 no post-load durable watermark pass completed "+
				"(post-load %.0f, end %.0f)",
			state.postLoadWatermarks,
			endWatermarkPasses,
		)
	}
	endDriftPasses := operationValue(
		families,
		"ghsync_c_o4_operation_successes",
		"drift",
		"detector",
	)
	if endDriftPasses <= state.postLoadDriftPasses {
		return fmt.Errorf(
			"C-O3 no post-population drift pass completed "+
				"(post-load %.0f, end %.0f)",
			state.postLoadDriftPasses,
			endDriftPasses,
		)
	}
	endDriftSamples := operationValue(
		families,
		"ghsync_c_o4_operation_samples",
		"drift",
		"detector",
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
	return assertHistogramBounds(state.runHistogram, "run-scoped")
}

func openDriftFindings(
	families map[string]*dto.MetricFamily,
) float64 {
	return metricValue(
		families,
		"ghsync_c_o3_drift_findings",
		map[string]string{"state": "open", "entity_kind": "all"},
	)
}

func githubStatusResponses(
	families map[string]*dto.MetricFamily,
	status int,
) float64 {
	return metricValue(
		families,
		"ghsync_c_b1_github_requests_total",
		map[string]string{"status": fmt.Sprintf("%d", status)},
	)
}

func observeCounterDelta(
	current float64,
	last *float64,
	total *float64,
	name string,
) error {
	if current < *last {
		return fmt.Errorf(
			"%s counter regressed from %.0f to %.0f",
			name,
			*last,
			current,
		)
	}
	*total += current - *last
	*last = current
	return nil
}

func assertHistogramBounds(
	histogram histogramSnapshot,
	scope string,
) error {
	p95, err := histogramQuantileSnapshot(histogram, 0.95)
	if err != nil {
		return err
	}
	p99, err := histogramQuantileSnapshot(histogram, 0.99)
	if err != nil {
		return err
	}
	if p95 > 20 {
		return fmt.Errorf("C-Q2 %s p95 %.1fs exceeds 20s", scope, p95)
	}
	if p99 > 60 {
		return fmt.Errorf("C-Q2 %s p99 %.1fs exceeds 60s", scope, p99)
	}
	return nil
}

func assertStalenessWithinBounds(
	families map[string]*dto.MetricFamily,
) (int, error) {
	staleness := families["ghsync_c_r1_cache_staleness_seconds"]
	bounds := families["ghsync_c_r1_staleness_bound_seconds"]
	if staleness == nil || bounds == nil {
		return 0, fmt.Errorf("C-R1 staleness or bound metric is absent")
	}
	samples := 0
	for _, item := range staleness.Metric {
		entityClass := labelsOf(item)["entity_class"]
		if entityClass == "" {
			return samples, fmt.Errorf(
				"C-R1 staleness sample has no entity_class",
			)
		}
		bound, ok := labeledMetricValue(
			bounds,
			map[string]string{"entity_class": entityClass},
		)
		if !ok || bound <= 0 {
			return samples, fmt.Errorf(
				"C-R1 bound is absent for %s",
				entityClass,
			)
		}
		age := sampleValue(item)
		if age > bound {
			return samples, fmt.Errorf(
				"C-R1 %s cache age %.1fs exceeds %.1fs bound",
				entityClass,
				age,
				bound,
			)
		}
		samples++
	}
	return samples, nil
}

func completedDeliveries(
	ctx context.Context,
	pool *pgxpool.Pool,
	guids []string,
) (int, error) {
	counts, err := dbgen.New(pool).CountLoadgenRunDeliveries(ctx, guids)
	if err != nil {
		return 0, fmt.Errorf("count run deliveries: %w", err)
	}
	if counts.Total > int64(len(guids)) {
		return 0, fmt.Errorf(
			"run delivery GUIDs stored %d rows, want at most %d",
			counts.Total,
			len(guids),
		)
	}
	return int(counts.Completed), nil
}

func assertDroppedDeliveriesHealed(
	ctx context.Context,
	pool *pgxpool.Pool,
	droppedAt map[string]time.Time,
	bound time.Duration,
) error {
	if len(droppedAt) == 0 {
		return nil
	}
	if bound <= 0 {
		return fmt.Errorf(
			"dropped-delivery healing has no positive C-R1 bound",
		)
	}
	guids := make([]string, 0, len(droppedAt))
	for guid := range droppedAt {
		guids = append(guids, guid)
	}
	rows, err := dbgen.New(pool).ListLoadgenDroppedDeliveries(ctx, guids)
	if err != nil {
		return fmt.Errorf("query healed dropped deliveries: %w", err)
	}
	seen := make(map[string]struct{}, len(droppedAt))
	for _, row := range rows {
		if row.Status != "processed" {
			return fmt.Errorf(
				"dropped delivery %s healed with status %q",
				row.DeliveryGuid,
				row.Status,
			)
		}
		delay := max(row.ReceivedAt.Time.Sub(droppedAt[row.DeliveryGuid]), 0)
		if delay > bound {
			return fmt.Errorf(
				"dropped delivery %s was redelivered after %s, "+
					"exceeding the strictest C-R1 bound %s",
				row.DeliveryGuid,
				delay.Round(time.Millisecond),
				bound,
			)
		}
		seen[row.DeliveryGuid] = struct{}{}
	}
	if len(seen) != len(droppedAt) {
		return fmt.Errorf(
			"C-R4 healed %d/%d dropped deliveries within %s",
			len(seen),
			len(droppedAt),
			bound,
		)
	}
	return nil
}

func minimumStalenessBound(
	families map[string]*dto.MetricFamily,
) time.Duration {
	family := families["ghsync_c_r1_staleness_bound_seconds"]
	if family == nil {
		return 0
	}
	minimum := math.Inf(1)
	for _, item := range family.Metric {
		value := sampleValue(item)
		if value > 0 && value < minimum {
			minimum = value
		}
	}
	if math.IsInf(minimum, 1) {
		return 0
	}
	return time.Duration(minimum * float64(time.Second))
}

func assertConverged(
	ctx context.Context,
	cfg config,
	pool *pgxpool.Pool,
	expected map[string]expectedEntityKeys,
) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.fakeGitHubURL+fakegithub.ControlTruthPath, http.NoBody)
	if err != nil {
		return err
	}
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("read fake truth: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("read fake truth status %d", response.StatusCode)
	}
	var truth fakegithub.TruthSnapshot
	if err := json.NewDecoder(response.Body).Decode(&truth); err != nil {
		return fmt.Errorf("decode fake truth: %w", err)
	}
	if len(truth.Repositories) == 0 {
		return fmt.Errorf("fake truth contains no mutated repositories")
	}
	if len(truth.Repositories) != len(expected) {
		return fmt.Errorf(
			"fake truth repository count %d does not match recording count %d",
			len(truth.Repositories),
			len(expected),
		)
	}
	if truth.Faults.Applied500 != truth.Faults.Configured500 ||
		truth.Faults.Applied429 != truth.Faults.Configured429 {
		return fmt.Errorf(
			"fake fault burst was not fully exercised "+
				"(500=%d/%d 429=%d/%d)",
			truth.Faults.Applied500,
			truth.Faults.Configured500,
			truth.Faults.Applied429,
			truth.Faults.Configured429,
		)
	}
	for _, fixture := range truth.Repositories {
		keys, ok := expected[fixture.Repository.FullName]
		if !ok {
			return fmt.Errorf(
				"fake truth contains repository %s absent from recording",
				fixture.Repository.FullName,
			)
		}
		if err := assertFixtureEntityCounts(fixture, keys); err != nil {
			return fmt.Errorf(
				"repository %s: %w",
				fixture.Repository.FullName,
				err,
			)
		}
		if err := assertFixtureConverged(ctx, pool, fixture); err != nil {
			return fmt.Errorf(
				"repository %s: %w",
				fixture.Repository.FullName,
				err,
			)
		}
	}
	return nil
}

func assertFixtureEntityCounts(
	fixture fakegithub.TruthFixtureSnapshot,
	expected expectedEntityKeys,
) error {
	counts := []struct {
		name string
		got  int
		want int
	}{
		{"pull requests", len(fixture.PullRequests), len(expected.pulls)},
		{"stacks", len(fixture.Stacks), len(expected.stacks)},
		{"check runs", len(fixture.CheckRuns), len(expected.checks)},
		{"review threads", len(fixture.ReviewThreads), len(expected.threads)},
	}
	for _, count := range counts {
		if count.want == 0 {
			return fmt.Errorf(
				"recording has vacuous zero %s coverage",
				count.name,
			)
		}
		if count.got != count.want {
			return fmt.Errorf(
				"fake truth has %d %s, recording requires %d",
				count.got,
				count.name,
				count.want,
			)
		}
	}
	actualPulls := make(map[int]struct{}, len(fixture.PullRequests))
	for _, pull := range fixture.PullRequests {
		actualPulls[pull.Number] = struct{}{}
	}
	actualStacks := make(map[int]struct{}, len(fixture.Stacks))
	for _, stack := range fixture.Stacks {
		actualStacks[stack.Number] = struct{}{}
	}
	actualChecks := make(map[int64]struct{}, len(fixture.CheckRuns))
	for _, check := range fixture.CheckRuns {
		actualChecks[check.ID] = struct{}{}
	}
	actualThreads := make(map[string]struct{}, len(fixture.ReviewThreads))
	for _, thread := range fixture.ReviewThreads {
		actualThreads[thread.ID] = struct{}{}
	}
	if !reflect.DeepEqual(actualPulls, expected.pulls) ||
		!reflect.DeepEqual(actualStacks, expected.stacks) ||
		!reflect.DeepEqual(actualChecks, expected.checks) ||
		!reflect.DeepEqual(actualThreads, expected.threads) {
		return fmt.Errorf(
			"fake truth entity identities do not match the compiled recording",
		)
	}
	return nil
}

type oraclePull struct {
	ID             int64
	NodeID         string
	Number         int
	Title          string
	State          string
	Draft          bool
	AuthorLogin    string
	ReviewDecision string
	MergeableState string
	HeadRef        string
	HeadSHA        string
	BaseRef        string
	BaseSHA        string
	StackNumber    *int
	StackPosition  *int
	UpdatedAt      time.Time
}

type oracleReviewRequest struct {
	Pull        int
	Kind        string
	ID          int64
	NodeID      string
	Login       string
	RequestedAt *time.Time
	HeadSHA     string
}

type oraclePullRequestReview struct {
	Pull         int
	ID           int64
	NodeID       string
	AuthorKind   string
	AuthorNodeID string
	AuthorLogin  string
	State        string
	SubmittedAt  *time.Time
	CommitOID    string
	UpdatedAt    time.Time
	HeadSHA      string
}

type oraclePullRequestComment struct {
	Pull         int
	ID           int64
	NodeID       string
	AuthorKind   string
	AuthorNodeID string
	AuthorLogin  string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	HeadSHA      string
}

type oracleStack struct {
	ID        int64
	NodeID    string
	Number    int
	BaseRef   string
	BaseSHA   string
	Open      bool
	Entries   []store.StackEntry
	UpdatedAt time.Time
	HeadSHA   string
}

type oracleCheck struct {
	ID          int64
	NodeID      string
	HeadSHA     string
	Name        string
	Status      string
	Conclusion  string
	DetailsURL  string
	AppSlug     string
	StartedAt   *time.Time
	CompletedAt *time.Time
	UpdatedAt   *time.Time
	Semantic    string
}

type oracleThread struct {
	ID         string
	Pull       int
	IsResolved bool
	IsOutdated bool
	Path       string
	Line       *int
	Comments   []store.ReviewCommentRecord
	UpdatedAt  time.Time
	HeadSHA    string
}

func assertFixtureConverged(
	ctx context.Context,
	pool *pgxpool.Pool,
	truth fakegithub.TruthFixtureSnapshot,
) error {
	if len(truth.PullRequests) == 0 || len(truth.Stacks) == 0 ||
		len(truth.CheckRuns) == 0 || len(truth.ReviewThreads) == 0 {
		return fmt.Errorf(
			"full-record oracle is vacuous "+
				"(pull_requests=%d stacks=%d check_runs=%d review_threads=%d)",
			len(truth.PullRequests),
			len(truth.Stacks),
			len(truth.CheckRuns),
			len(truth.ReviewThreads),
		)
	}
	repo := truth.Repository.FullName
	expectedPulls := make([]oraclePull, 0, len(truth.PullRequests))
	for _, pull := range truth.PullRequests {
		// Empty is the shared fixture/cache sentinel for a GitHub-null base
		// SHA, so unknown truth converges instead of oscillating.
		expected := oraclePull{
			ID:             pull.ID,
			NodeID:         pull.NodeID,
			Number:         pull.Number,
			Title:          pull.Title,
			State:          pull.State,
			Draft:          pull.Draft,
			AuthorLogin:    pull.AuthorLogin,
			ReviewDecision: pull.ReviewDecision,
			MergeableState: pull.MergeableState,
			HeadRef:        pull.Head.Ref,
			HeadSHA:        pull.Head.SHA,
			BaseRef:        pull.Base.Ref,
			BaseSHA:        pull.Base.SHA,
			UpdatedAt:      pull.UpdatedAt.UTC(),
		}
		if pull.Stack != nil {
			number := pull.Stack.Number
			position := pull.Stack.Position
			expected.StackNumber = &number
			expected.StackPosition = &position
		}
		expectedPulls = append(expectedPulls, expected)
	}
	cachedPulls, err := readCachedPulls(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOraclePulls(expectedPulls)
	sortOraclePulls(cachedPulls)
	if !reflect.DeepEqual(expectedPulls, cachedPulls) {
		return fmt.Errorf(
			"pull-request cache mismatch\ntruth=%+v\ncache=%+v",
			expectedPulls,
			cachedPulls,
		)
	}
	// Review requests are part of the convergence oracle: stable identity,
	// current login/slug, request timestamp when available, and observed head
	// must match fixture truth. first_seen_at is intentionally excluded because
	// it is a cache-local observation time, not GitHub fixture truth.
	expectedRequests := make([]oracleReviewRequest, 0)
	for _, pull := range truth.PullRequests {
		for _, request := range pull.ReviewRequests {
			if request.Kind != "user" && request.Kind != "team" {
				continue
			}
			expectedRequests = append(expectedRequests, oracleReviewRequest{
				Pull:    pull.Number,
				Kind:    request.Kind,
				ID:      request.ID,
				NodeID:  request.NodeID,
				Login:   request.Login,
				HeadSHA: pull.Head.SHA,
			})
		}
	}
	cachedRequests, err := readCachedReviewRequests(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOracleReviewRequests(expectedRequests)
	sortOracleReviewRequests(cachedRequests)
	if !reflect.DeepEqual(expectedRequests, cachedRequests) {
		return fmt.Errorf(
			"pull-request review-request cache mismatch\ntruth=%+v\ncache=%+v",
			expectedRequests,
			cachedRequests,
		)
	}

	expectedReviews := make([]oraclePullRequestReview, 0)
	expectedComments := make([]oraclePullRequestComment, 0)
	for _, pull := range truth.PullRequests {
		for _, review := range pull.Reviews {
			expectedReviews = append(expectedReviews, oraclePullRequestReview{
				Pull:         pull.Number,
				ID:           review.ID,
				NodeID:       review.NodeID,
				AuthorKind:   review.Author.Kind,
				AuthorNodeID: review.Author.NodeID,
				AuthorLogin:  review.Author.Login,
				State:        review.State,
				SubmittedAt:  utcTimePointer(review.SubmittedAt),
				CommitOID:    review.CommitOID,
				UpdatedAt:    review.UpdatedAt.UTC(),
				HeadSHA:      pull.Head.SHA,
			})
		}
		for _, comment := range pull.Comments {
			expectedComments = append(expectedComments, oraclePullRequestComment{
				Pull:         pull.Number,
				ID:           comment.ID,
				NodeID:       comment.NodeID,
				AuthorKind:   comment.Author.Kind,
				AuthorNodeID: comment.Author.NodeID,
				AuthorLogin:  comment.Author.Login,
				CreatedAt:    comment.CreatedAt.UTC(),
				UpdatedAt:    comment.UpdatedAt.UTC(),
				HeadSHA:      pull.Head.SHA,
			})
		}
	}
	cachedReviews, err := readCachedPullRequestReviews(ctx, pool, repo)
	if err != nil {
		return err
	}
	cachedComments, err := readCachedPullRequestComments(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOraclePullRequestReviews(expectedReviews)
	sortOraclePullRequestReviews(cachedReviews)
	sortOraclePullRequestComments(expectedComments)
	sortOraclePullRequestComments(cachedComments)
	if !reflect.DeepEqual(expectedReviews, cachedReviews) {
		return fmt.Errorf(
			"pull-request review cache mismatch\ntruth=%+v\ncache=%+v",
			expectedReviews,
			cachedReviews,
		)
	}
	if !reflect.DeepEqual(expectedComments, cachedComments) {
		return fmt.Errorf(
			"pull-request comment cache mismatch\ntruth=%+v\ncache=%+v",
			expectedComments,
			cachedComments,
		)
	}

	expectedStacks := make([]oracleStack, 0, len(truth.Stacks))
	for _, stack := range truth.Stacks {
		entries := make([]store.StackEntry, 0, len(stack.PullRequests))
		for _, entry := range stack.PullRequests {
			entries = append(entries, store.StackEntry{
				Number:    entry.Number,
				State:     entry.State,
				Draft:     entry.Draft,
				MergedAt:  utcTimePointer(entry.MergedAt),
				UpdatedAt: entry.UpdatedAt.UTC(),
				HeadRef:   entry.Head.Ref,
				HeadSHA:   entry.Head.SHA,
			})
		}
		expectedStacks = append(expectedStacks, oracleStack{
			ID:        stack.ID,
			NodeID:    stack.NodeID,
			Number:    stack.Number,
			BaseRef:   stack.Base.Ref,
			BaseSHA:   stack.Base.SHA,
			Open:      stack.Open,
			Entries:   entries,
			UpdatedAt: stack.UpdatedAt.UTC(),
			HeadSHA:   stackHeadSHA(entries, stack.Base.SHA),
		})
	}
	cachedStacks, err := readCachedStacks(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOracleStacks(expectedStacks)
	sortOracleStacks(cachedStacks)
	if !reflect.DeepEqual(expectedStacks, cachedStacks) {
		return fmt.Errorf(
			"stack cache mismatch\ntruth=%+v\ncache=%+v",
			expectedStacks,
			cachedStacks,
		)
	}

	expectedChecks := make([]oracleCheck, 0, len(truth.CheckRuns))
	for _, run := range truth.CheckRuns {
		expectedChecks = append(expectedChecks, oracleCheck{
			ID:          run.ID,
			NodeID:      run.NodeID,
			HeadSHA:     run.HeadSHA,
			Name:        run.Name,
			Status:      run.Status,
			Conclusion:  run.Conclusion,
			DetailsURL:  run.DetailsURL,
			AppSlug:     run.AppSlug,
			StartedAt:   utcTimePointer(run.StartedAt),
			CompletedAt: utcTimePointer(run.CompletedAt),
			UpdatedAt:   checkRunUpdatedAt(run),
			Semantic:    checkSemanticVersion(run),
		})
	}
	cachedChecks, err := readCachedChecks(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOracleChecks(expectedChecks)
	sortOracleChecks(cachedChecks)
	if !reflect.DeepEqual(expectedChecks, cachedChecks) {
		return fmt.Errorf(
			"check-run cache mismatch\ntruth=%+v\ncache=%+v",
			expectedChecks,
			cachedChecks,
		)
	}

	pullHeadSHAs := make(map[int]string, len(truth.PullRequests))
	for _, pull := range truth.PullRequests {
		pullHeadSHAs[pull.Number] = pull.Head.SHA
	}
	expectedThreads := make([]oracleThread, 0, len(truth.ReviewThreads))
	for _, thread := range truth.ReviewThreads {
		comments := make(
			[]store.ReviewCommentRecord,
			0,
			len(thread.Comments),
		)
		for _, comment := range thread.Comments {
			comments = append(comments, store.ReviewCommentRecord{
				ID:          comment.ID,
				Body:        comment.Body,
				UpdatedAt:   comment.UpdatedAt.UTC(),
				AuthorLogin: comment.AuthorLogin,
			})
		}
		expectedThreads = append(expectedThreads, oracleThread{
			ID:         thread.ID,
			Pull:       thread.PullRequest,
			IsResolved: thread.IsResolved,
			IsOutdated: thread.IsOutdated,
			Path:       thread.Path,
			Line:       thread.Line,
			Comments:   comments,
			UpdatedAt:  thread.UpdatedAt.UTC(),
			HeadSHA:    pullHeadSHAs[thread.PullRequest],
		})
	}
	cachedThreads, err := readCachedThreads(ctx, pool, repo)
	if err != nil {
		return err
	}
	sortOracleThreads(expectedThreads)
	sortOracleThreads(cachedThreads)
	if !reflect.DeepEqual(expectedThreads, cachedThreads) {
		return fmt.Errorf(
			"review-thread cache mismatch\ntruth=%+v\ncache=%+v",
			expectedThreads,
			cachedThreads,
		)
	}
	return nil
}

func sortOraclePulls(pulls []oraclePull) {
	sort.Slice(pulls, func(i, j int) bool {
		return pulls[i].Number < pulls[j].Number
	})
}

func sortOracleReviewRequests(requests []oracleReviewRequest) {
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].Pull != requests[j].Pull {
			return requests[i].Pull < requests[j].Pull
		}
		if requests[i].Kind != requests[j].Kind {
			return requests[i].Kind < requests[j].Kind
		}
		return requests[i].ID < requests[j].ID
	})
}

func sortOraclePullRequestReviews(reviews []oraclePullRequestReview) {
	sort.Slice(reviews, func(i, j int) bool {
		if reviews[i].Pull != reviews[j].Pull {
			return reviews[i].Pull < reviews[j].Pull
		}
		return reviews[i].NodeID < reviews[j].NodeID
	})
}

func sortOraclePullRequestComments(comments []oraclePullRequestComment) {
	sort.Slice(comments, func(i, j int) bool {
		if comments[i].Pull != comments[j].Pull {
			return comments[i].Pull < comments[j].Pull
		}
		return comments[i].NodeID < comments[j].NodeID
	})
}

func sortOracleStacks(stacks []oracleStack) {
	sort.Slice(stacks, func(i, j int) bool {
		return stacks[i].Number < stacks[j].Number
	})
}

func sortOracleChecks(checks []oracleCheck) {
	sort.Slice(checks, func(i, j int) bool {
		return checks[i].ID < checks[j].ID
	})
}

func sortOracleThreads(threads []oracleThread) {
	sort.Slice(threads, func(i, j int) bool {
		return threads[i].ID < threads[j].ID
	})
}

func readCachedPulls(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oraclePull, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedPullRequests(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached pull requests: %w", err)
	}
	result := make([]oraclePull, 0, len(rows))
	for _, row := range rows {
		result = append(result, oraclePull{
			ID:             row.GhID.Int64,
			NodeID:         row.NodeID,
			Number:         int(row.Number),
			Title:          row.Title,
			State:          row.State,
			Draft:          row.Draft,
			AuthorLogin:    row.AuthorLogin,
			ReviewDecision: row.ReviewDecision,
			MergeableState: row.MergeableState,
			HeadRef:        row.HeadRef,
			HeadSHA:        row.HeadSha,
			BaseRef:        row.BaseRef,
			BaseSHA:        row.BaseSha,
			StackNumber:    pgInt4Pointer(row.StackNumber),
			StackPosition:  pgInt4Pointer(row.StackPosition),
			UpdatedAt:      row.GhUpdatedAt.Time.UTC(),
		})
	}
	return result, nil
}

func readCachedReviewRequests(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oracleReviewRequest, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedPullRequestReviewRequests(
		ctx,
		repo,
	)
	if err != nil {
		return nil, fmt.Errorf("query cached PR review requests: %w", err)
	}
	result := make([]oracleReviewRequest, 0, len(rows))
	for _, row := range rows {
		result = append(result, oracleReviewRequest{
			Pull:        int(row.PrNumber),
			Kind:        row.ReviewerKind,
			ID:          row.ReviewerGhID,
			NodeID:      row.ReviewerNodeID,
			Login:       row.ReviewerLogin,
			RequestedAt: pgTimestampPointer(row.RequestedAt),
			HeadSHA:     row.HeadSha,
		})
	}
	return result, nil
}

func readCachedPullRequestReviews(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oraclePullRequestReview, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedPullRequestReviews(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached PR reviews: %w", err)
	}
	result := make([]oraclePullRequestReview, 0, len(rows))
	for _, row := range rows {
		result = append(result, oraclePullRequestReview{
			Pull:         int(row.PrNumber),
			ID:           row.GhID.Int64,
			NodeID:       row.NodeID,
			AuthorKind:   row.AuthorKind,
			AuthorNodeID: row.AuthorNodeID.String,
			AuthorLogin:  row.AuthorLogin.String,
			State:        row.State,
			SubmittedAt:  pgTimestampPointer(row.SubmittedAt),
			CommitOID:    row.CommitOid.String,
			UpdatedAt:    row.GhUpdatedAt.Time.UTC(),
			HeadSHA:      row.HeadSha,
		})
	}
	return result, nil
}

func readCachedPullRequestComments(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oraclePullRequestComment, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedPullRequestComments(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached PR comments: %w", err)
	}
	result := make([]oraclePullRequestComment, 0, len(rows))
	for _, row := range rows {
		result = append(result, oraclePullRequestComment{
			Pull:         int(row.PrNumber),
			ID:           row.GhID.Int64,
			NodeID:       row.NodeID,
			AuthorKind:   row.AuthorKind,
			AuthorNodeID: row.AuthorNodeID.String,
			AuthorLogin:  row.AuthorLogin.String,
			CreatedAt:    row.CreatedAt.Time.UTC(),
			UpdatedAt:    row.GhUpdatedAt.Time.UTC(),
			HeadSHA:      row.HeadSha,
		})
	}
	return result, nil
}

func readCachedStacks(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oracleStack, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedStacks(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached stacks: %w", err)
	}
	result := make([]oracleStack, 0, len(rows))
	for _, row := range rows {
		stack := oracleStack{
			ID:        row.GhID.Int64,
			NodeID:    row.NodeID,
			Number:    int(row.Number),
			BaseRef:   row.BaseRef,
			BaseSHA:   row.BaseSha,
			Open:      row.Open,
			UpdatedAt: row.GhUpdatedAt.Time.UTC(),
			HeadSHA:   row.HeadSha,
		}
		if err := json.Unmarshal(row.Entries, &stack.Entries); err != nil {
			return nil, fmt.Errorf("decode cached stack entries: %w", err)
		}
		for index := range stack.Entries {
			stack.Entries[index].UpdatedAt =
				stack.Entries[index].UpdatedAt.UTC()
			stack.Entries[index].MergedAt =
				utcTimePointer(stack.Entries[index].MergedAt)
		}
		result = append(result, stack)
	}
	return result, nil
}

func readCachedChecks(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oracleCheck, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedCheckRuns(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached check runs: %w", err)
	}
	result := make([]oracleCheck, 0, len(rows))
	for _, row := range rows {
		result = append(result, oracleCheck{
			ID:          row.GhID,
			NodeID:      row.NodeID,
			HeadSHA:     row.HeadSha,
			Name:        row.Name,
			Status:      row.Status,
			Conclusion:  row.Conclusion,
			DetailsURL:  row.DetailsUrl,
			AppSlug:     row.AppSlug,
			StartedAt:   pgTimestampPointer(row.StartedAt),
			CompletedAt: pgTimestampPointer(row.CompletedAt),
			UpdatedAt:   pgTimestampPointer(row.GhUpdatedAt),
			Semantic:    row.SemanticVersion,
		})
	}
	return result, nil
}

func readCachedThreads(
	ctx context.Context,
	pool *pgxpool.Pool,
	repo string,
) ([]oracleThread, error) {
	rows, err := dbgen.New(pool).ListLoadgenCachedReviewThreads(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("query cached review threads: %w", err)
	}
	result := make([]oracleThread, 0, len(rows))
	for _, row := range rows {
		thread := oracleThread{
			ID:         row.ID,
			Pull:       int(row.PrNumber),
			IsResolved: row.IsResolved,
			IsOutdated: row.IsOutdated,
			Path:       row.Path,
			Line:       pgInt4Pointer(row.Line),
			UpdatedAt:  row.GhUpdatedAt.Time.UTC(),
			HeadSHA:    row.HeadSha,
		}
		if err := json.Unmarshal(row.Comments, &thread.Comments); err != nil {
			return nil, fmt.Errorf(
				"decode cached review-thread comments: %w",
				err,
			)
		}
		for index := range thread.Comments {
			thread.Comments[index].UpdatedAt =
				thread.Comments[index].UpdatedAt.UTC()
		}
		result = append(result, thread)
	}
	return result, nil
}

func pgInt4Pointer(value pgtype.Int4) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int32)
	return &result
}

func pgTimestampPointer(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func checkRunUpdatedAt(
	run fakegithub.TruthCheckRunSnapshot,
) *time.Time {
	if run.CompletedAt != nil {
		return utcTimePointer(run.CompletedAt)
	}
	return utcTimePointer(run.StartedAt)
}

func stackHeadSHA(entries []store.StackEntry, fallback string) string {
	if len(entries) == 0 {
		return fallback
	}
	return entries[len(entries)-1].HeadSHA
}

func checkSemanticVersion(
	run fakegithub.TruthCheckRunSnapshot,
) string {
	type semanticCheck struct {
		Status      string     `json:"status"`
		Conclusion  string     `json:"conclusion"`
		DetailsURL  string     `json:"details_url"`
		AppSlug     string     `json:"app_slug"`
		StartedAt   *time.Time `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at"`
	}
	encoded, _ := json.Marshal(semanticCheck{
		Status:      run.Status,
		Conclusion:  run.Conclusion,
		DetailsURL:  run.DetailsURL,
		AppSlug:     run.AppSlug,
		StartedAt:   run.StartedAt,
		CompletedAt: run.CompletedAt,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func histogramState(
	family *dto.MetricFamily,
) histogramSnapshot {
	if family == nil {
		return histogramSnapshot{buckets: make(map[float64]uint64)}
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
	return result
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

func addHistogram(
	target *histogramSnapshot,
	delta histogramSnapshot,
) {
	if target.buckets == nil {
		target.buckets = make(map[float64]uint64)
	}
	target.count += delta.count
	for bound, value := range delta.buckets {
		target.buckets[bound] += value
	}
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
	var total float64
	for _, item := range family.Metric {
		if labelsMatch(labelsOf(item), labels) {
			total += sampleValue(item)
		}
	}
	return total
}

func labeledMetricValue(
	family *dto.MetricFamily,
	labels map[string]string,
) (float64, bool) {
	if family == nil {
		return 0, false
	}
	for _, item := range family.Metric {
		if labelsMatch(labelsOf(item), labels) {
			return sampleValue(item), true
		}
	}
	return 0, false
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

func labelsMatch(got, want map[string]string) bool {
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
