package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/metric"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

type counterRegistrar struct {
	counter metric.Int64Counter
}

func (r *counterRegistrar) RegisterMetrics(meter metric.Meter) error {
	var err error
	r.counter, err = meter.Int64Counter("ghsync_c_o4_registry_test")
	return err
}

func TestRegistryIsolatedPrometheusExposition(t *testing.T) {
	t.Parallel()
	registry, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Shutdown(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	registrar := &counterRegistrar{}
	if err := registry.Register("test", registrar); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("test", registrar); err == nil {
		t.Fatal("duplicate metrics scope was accepted")
	}
	registrar.counter.Add(context.Background(), 2)

	request := httptest.NewRequestWithContext(t.Context(), "GET", Path, http.NoBody)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("metrics status = %d", response.Code)
	}
	if body := response.Body.String(); !strings.Contains(
		body,
		"ghsync_c_o4_registry_test_total 2",
	) {
		t.Fatalf("metrics body omitted registry counter:\n%s", body)
	}
}

func TestRuntimeMetricsExposeConstraintState(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database := testdb.New(t)
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO installation_budgets (
		    installation_id, class, remaining, rate_limit, reset_at,
		    backoff_until
		)
		VALUES
		    (1, 'rest', 14000, 15000,
		     clock_timestamp() + interval '1 hour', NULL),
		    (1, 'app_jwt_rest', 4800, 5000,
		     clock_timestamp() + interval '1 hour',
		     clock_timestamp() + interval '1 minute'),
		    (1, 'graphql', 4900, 5000,
		     clock_timestamp() + interval '1 hour', NULL)
	`); err != nil {
		t.Fatal(err)
	}
	eventTx, err := database.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer eventTx.Rollback(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, eventTx); err != nil {
		t.Fatal(err)
	}
	rows, err := eventTx.Query(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES
		    ('entities', 'pull_request.changed', 'pr:1:1:1',
		     clock_timestamp() - interval '8 days', '{"version":1}'),
		    ('entities', 'pull_request.changed', 'pr:1:1:2',
		     clock_timestamp() - interval '8 days', '{"version":1}'),
		    ('entities', 'pull_request.changed', 'pr:1:1:3',
		     clock_timestamp() - interval '8 days', '{"version":1}')
		RETURNING seq
	`)
	if err != nil {
		t.Fatal(err)
	}
	var sequences []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		sequences = append(sequences, seq)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if len(sequences) != 3 {
		t.Fatalf("seeded change event sequences = %v", sequences)
	}
	if err := eventTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		UPDATE stream_watermark
		SET safe_seq = $1, updated_at = clock_timestamp()
		WHERE singleton
	`, sequences[2]); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO consumer_cursors (
		    consumer, stream, seq, updated_at, resync_count, last_resync_at
		)
		VALUES (
		    'metrics-test-consumer', 'entities', $1,
		    clock_timestamp() - interval '1 minute', 4,
		    clock_timestamp() - interval '2 minutes'
		)
	`, sequences[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO sweep_cursors (
		    installation_id, sweep_kind, scope_key, started_at,
		    updated_at, completed_at
		)
		VALUES (
		    1, 'stacks', '', clock_timestamp() - interval '20 seconds',
		    clock_timestamp() - interval '5 seconds',
		    clock_timestamp() - interval '5 seconds'
		)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO drift_findings (
		    installation_id, entity_kind, entity_key, detected_at,
		    cache_snapshot, upstream_snapshot, diff, refresh_enqueued_at,
		    diff_hash, first_seen_at, last_seen_at, resolved_at
		)
		VALUES
		    (1, 'pull_request', 'pr:1', clock_timestamp() - interval '1 minute',
		     '{}', '{}', '{}', clock_timestamp() - interval '1 minute',
		     'open-pr', clock_timestamp() - interval '1 minute',
		     clock_timestamp() - interval '1 minute', NULL),
		    (1, 'stack', 'stack:1', clock_timestamp() - interval '2 minutes',
		     '{}', '{}', '{}', clock_timestamp() - interval '2 minutes',
		     'resolved-stack', clock_timestamp() - interval '2 minutes',
		     clock_timestamp() - interval '1 minute',
		     clock_timestamp() - interval '1 minute')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO operation_heartbeats (
		    installation_id, component, operation, success_count,
		    last_success_at, sample_count, last_sample_at
		)
		VALUES
		    (1, 'drift', 'detector', 7,
		     clock_timestamp() - interval '10 seconds', 11,
		     clock_timestamp() - interval '5 seconds'),
		    (1, 'deriver', 'dirty_sets', 21,
		     clock_timestamp() - interval '30 seconds', 23,
		     clock_timestamp() - interval '25 seconds'),
		    (1, 'sweep', 'repositories', 13,
		     clock_timestamp() - interval '20 seconds', 17,
		     clock_timestamp() - interval '15 seconds')
	`); err != nil {
		t.Fatal(err)
	}

	runtimeMetrics, err := NewRuntime(RuntimeOptions{
		Pool:               database.Pool,
		InstallationID:     1,
		Roles:              []string{"dispatch", "fetch"},
		CollectDatabase:    true,
		OpenStackStaleness: 5 * time.Minute,
		OpenPRStaleness:    10 * time.Minute,
		RepoRulesStaleness: time.Hour,
		ClosedStaleness:    24 * time.Hour,
		RepositoryPeriod:   time.Hour,
		StreamRetentionAge: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Shutdown(context.Background()) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := registry.Register("runtime-test", runtimeMetrics); err != nil {
		t.Fatal(err)
	}
	runtimeMetrics.BudgetRequest(budget.RequestObservation{
		Class:          budget.Sweep,
		Resource:       budget.REST,
		AuthContext:    budget.InstallationAuth,
		EndpointFamily: "pull_request_metadata",
		StatusCode:     304,
		Conditional:    true,
		NotModified:    true,
	})
	runtimeMetrics.BudgetRequest(budget.RequestObservation{
		Class:          budget.Sweep,
		Resource:       budget.REST,
		AuthContext:    budget.AppJWTAuth,
		EndpointFamily: "app_hook_deliveries",
		StatusCode:     403,
	})
	runtimeMetrics.BudgetStarvation(budget.Starvation{
		Class:       budget.Sweep,
		Resource:    budget.REST,
		AuthContext: budget.AppJWTAuth,
	})
	runtimeMetrics.CacheWrite(ctx, "pull_request", true, false)
	runtimeMetrics.CacheWrite(ctx, "pull_request", true, false)
	runtimeMetrics.CacheWrite(ctx, "pull_request", false, false)
	runtimeMetrics.OutboxFence(ctx, outbox.FenceObservation{
		Role:         outbox.SharedWriterFence,
		WaitDuration: 2 * time.Millisecond,
		HoldDuration: 3 * time.Millisecond,
		Acquired:     true,
	})
	receivedAt := time.Now().UTC()
	runtimeMetrics.RefreshFinished(ctx, &queue.RefreshObservation{
		Kind:            queue.KindRefreshBranch,
		Queue:           queue.QueueEvent,
		EventReceivedAt: receivedAt,
	})
	runtimeMetrics.RefreshFinished(ctx, &queue.RefreshObservation{
		Kind:             queue.KindRefreshPR,
		Queue:            queue.QueueEvent,
		EventReceivedAt:  receivedAt,
		CacheCommittedAt: receivedAt.Add(-time.Second),
	})
	runtimeMetrics.RefreshFinished(ctx, &queue.RefreshObservation{
		Kind:             queue.KindRefreshPR,
		Queue:            queue.QueueEvent,
		EventReceivedAt:  receivedAt,
		CacheCommittedAt: receivedAt.Add(time.Second),
	})

	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(
		response,
		httptest.NewRequestWithContext(t.Context(), "GET", Path, http.NoBody),
	)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"ghsync_c_b3_budget_remaining",
		"ghsync_c_b4_conditional_304s_total",
		"ghsync_c_b1_request_attribution_total",
		"ghsync_c_c2_cache_cas_reject_ratio",
		"ghsync_c_i5_parked_deliveries",
		"ghsync_c_o3_drift_findings",
		"ghsync_c_s2_watermark_lag_sequences",
		`ghsync_c_s2_outbox_fence_wait_seconds_count{outcome="acquired",role="shared_writer"} 1`,
		`ghsync_c_s2_outbox_fence_hold_seconds_count{outcome="acquired",role="shared_writer"} 1`,
		"ghsync_c_q2_outstanding_generations",
		"ghsync_c_o4_operation_samples",
		"ghsync_c_o4_last_operation_sample_age_seconds",
		// Issue #22: the expected-operation roster must include the deriver
		// heartbeat, and unseeded expected operations still export series.
		`ghsync_c_o4_last_operation_success_age_seconds{component="deriver",operation="dirty_sets"}`,
		`ghsync_c_o4_last_operation_sample_age_seconds{component="deriver",operation="dirty_sets"}`,
		`ghsync_c_o4_last_operation_success_age_seconds{component="sweep",operation="repo_rules"}`,
		`ghsync_c_o4_last_operation_success_age_seconds{component="sweep",operation="closed_tracked"}`,
		"ghsync_c_p5_deriver_dirty_backlog",
		`ghsync_c_r2_sweep_period_seconds{sweep_kind="stacks"} 225`,
		`ghsync_c_o4_role_enabled{role="fetch"} 1`,
		`ghsync_c_q2_event_to_cache_latency_seconds_count{kind="refresh_pr"} 1`,
		`ghsync_c_q2_invalid_event_cache_latency_total{kind="refresh_pr"} 1`,
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("metrics exposition omitted %q", name)
		}
	}
	for _, name := range []string{
		"ghsync_c_b3_budget_remaining",
		"ghsync_c_b3_budget_limit",
		"ghsync_c_b3_starvations_total",
	} {
		assertEveryPrometheusMetricHasLabel(t, body, name, "auth_context")
	}
	assertPrometheusValue(
		t, body,
		"ghsync_c_b3_budget_remaining",
		map[string]string{
			"installation_id": "1",
			"class":           "sweep",
			"resource":        "rest",
			"auth_context":    "installation",
		},
		14000,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_b3_budget_remaining",
		map[string]string{
			"installation_id": "1",
			"class":           "sweep",
			"resource":        "rest",
			"auth_context":    "app_jwt",
		},
		4800,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_b3_starvations_total",
		map[string]string{
			"class":        "sweep",
			"resource":     "rest",
			"auth_context": "app_jwt",
		},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_b2_gate_closed",
		map[string]string{
			"installation_id": "1",
			"resource":        "rest",
			"auth_context":    "app_jwt",
		},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_b1_request_attribution_total",
		map[string]string{
			"auth_context":    "installation",
			"endpoint_family": "pull_request_metadata",
			"outcome":         "304",
		},
		1,
	)
	assertPrometheusLabelShape(
		t,
		body,
		"ghsync_c_b1_request_attribution_total",
		"auth_context",
		"endpoint_family",
		"outcome",
	)
	assertPrometheusValue(
		t,
		body,
		"ghsync_c_b4_conditional_requests_total",
		map[string]string{"class": "sweep", "resource": "rest"},
		1,
	)
	assertPrometheusValue(
		t,
		body,
		"ghsync_c_b4_conditional_304s_total",
		map[string]string{"class": "sweep", "resource": "rest"},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_b1_request_attribution_total",
		map[string]string{
			"auth_context":    "app_jwt",
			"endpoint_family": "app_hook_deliveries",
			"outcome":         "403",
		},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_c2_cache_cas_reject_ratio",
		map[string]string{"entity_kind": "pull_request"},
		1.0/3.0,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_s4_consumer_outstanding_events",
		map[string]string{
			"consumer": "metrics-test-consumer",
			"stream":   "entities",
		},
		2,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_s4_resyncs_total",
		map[string]string{
			"consumer": "metrics-test-consumer",
			"stream":   "entities",
		},
		4,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_s7_prunable_outbox_depth",
		map[string]string{"stream": "entities"},
		3,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_s7_prunable_outbox_depth",
		map[string]string{"stream": "all"},
		3,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_o3_drift_findings",
		map[string]string{"state": "open", "entity_kind": "pull_request"},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_o3_drift_findings",
		map[string]string{"state": "resolved", "entity_kind": "stack"},
		1,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_o4_operation_successes",
		map[string]string{"component": "drift", "operation": "detector"},
		7,
	)
	assertPrometheusValue(
		t, body,
		"ghsync_c_o4_operation_samples",
		map[string]string{"component": "sweep", "operation": "repositories"},
		17,
	)
	if age := prometheusValue(
		t, body,
		"ghsync_c_s4_oldest_outstanding_event_age_seconds",
		map[string]string{
			"consumer": "metrics-test-consumer",
			"stream":   "entities",
		},
	); age < (7 * 24 * time.Hour).Seconds() {
		t.Fatalf("oldest consumer event age = %v, want at least 7d", age)
	}
	if duration := prometheusValue(
		t, body,
		"ghsync_c_r2_sweep_duration_seconds",
		map[string]string{"sweep_kind": "stacks"},
	); duration < 14 || duration > 16 {
		t.Fatalf("stacks sweep duration = %v, want about 15s", duration)
	}
}

func TestRequestAttributionOutcomeIsBounded(t *testing.T) {
	t.Parallel()
	tests := map[int]string{
		0:   "transport_error",
		200: "200",
		201: "other_2xx",
		304: "304",
		302: "other_3xx",
		403: "403",
		429: "other_4xx",
		503: "other_5xx",
		999: "other",
	}
	for status, want := range tests {
		if got := requestAttributionOutcome(status); got != want {
			t.Errorf("status %d outcome = %q, want %q", status, got, want)
		}
	}
}

func TestBatchLabelUsesConfiguredDirtyCap(t *testing.T) {
	t.Parallel()
	options := RuntimeOptions{DeriverDirtyCap: 7}
	for _, test := range []struct {
		count int
		want  string
	}{
		{count: 0, want: "empty"},
		{count: 6, want: "partial"},
		{count: 7, want: "capped"},
		{count: 8, want: "capped"},
	} {
		if got := batchLabel(test.count, &options); got != test.want {
			t.Errorf(
				"batchLabel(%d, dirty cap %d) = %q, want %q",
				test.count,
				options.DeriverDirtyCap,
				got,
				test.want,
			)
		}
	}
}

func assertPrometheusValue(
	t *testing.T,
	body []byte,
	name string,
	labels map[string]string,
	want float64,
) {
	t.Helper()
	if got := prometheusValue(t, body, name, labels); got != want {
		t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func assertEveryPrometheusMetricHasLabel(
	t *testing.T,
	body []byte,
	name string,
	labelName string,
) {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	family := families[name]
	if family == nil || len(family.Metric) == 0 {
		t.Fatalf("metrics exposition omitted family %q", name)
	}
	for _, item := range family.Metric {
		found := false
		for _, label := range item.Label {
			if label.GetName() == labelName && label.GetValue() != "" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s metric omitted %q: %+v", name, labelName, item)
		}
	}
}

func prometheusValue(
	t *testing.T,
	body []byte,
	name string,
	labels map[string]string,
) float64 {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	family := families[name]
	if family == nil {
		t.Fatalf("metrics exposition omitted family %q", name)
	}
	for _, item := range family.Metric {
		if metricLabelsMatch(item, labels) {
			switch {
			case item.Gauge != nil:
				return item.Gauge.GetValue()
			case item.Counter != nil:
				return item.Counter.GetValue()
			case item.Untyped != nil:
				return item.Untyped.GetValue()
			}
		}
	}
	t.Fatalf("metrics exposition omitted %s%v", name, labels)
	return 0
}

func metricLabelsMatch(item *dto.Metric, want map[string]string) bool {
	got := make(map[string]string, len(item.Label))
	for _, label := range item.Label {
		got[label.GetName()] = label.GetValue()
	}
	for name, value := range want {
		if got[name] != value {
			return false
		}
	}
	return true
}

func assertPrometheusLabelShape(
	t *testing.T,
	body []byte,
	name string,
	want ...string,
) {
	t.Helper()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	family := families[name]
	if family == nil || len(family.Metric) == 0 {
		t.Fatalf("metrics exposition omitted family %q", name)
	}
	slices.Sort(want)
	for _, item := range family.Metric {
		got := make([]string, 0, len(item.Label))
		for _, label := range item.Label {
			got = append(got, label.GetName())
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("%s label shape = %v, want %v", name, got, want)
		}
	}
}

// CollectDeliveryMetrics feeds the C-I5 parked gauges and the C-Q2 oldest
// unprocessed delivery age. It was rewritten from one FILTERed pass over
// webhook_deliveries into per-status aggregates that ride
// webhook_deliveries_unfinished_received_idx; this pins the values that
// rewrite must keep producing, including LEAST()'s NULL handling when only
// one of pending/processing is present.
func TestCollectDeliveryMetricsPerStatusAggregates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool
	queries := dbgen.New(pool)

	row, err := queries.CollectDeliveryMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if row.OldestUnprocessed != 0 || row.Parked != 0 || row.OldestParked != 0 {
		t.Fatalf("empty delivery table metrics = %+v, want zeroes", row)
	}

	// 'processed' is the terminal status and must contribute to nothing.
	if _, err := pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, headers, received_at, status, next_attempt_at
		) VALUES (
		    'done-1', 'pull_request', '{}'::jsonb,
		    clock_timestamp() - interval '9 hours', 'processed',
		    clock_timestamp()
		)
	`); err != nil {
		t.Fatal(err)
	}
	row, err = queries.CollectDeliveryMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if row.OldestUnprocessed != 0 || row.Parked != 0 || row.OldestParked != 0 {
		t.Fatalf("processed-only metrics = %+v, want zeroes", row)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, headers, received_at, status, next_attempt_at
		) VALUES
		    ('pending-1', 'pull_request', '{}'::jsonb,
		     clock_timestamp() - interval '600 seconds', 'pending',
		     clock_timestamp()),
		    ('processing-1', 'pull_request', '{}'::jsonb,
		     clock_timestamp() - interval '900 seconds', 'processing',
		     clock_timestamp()),
		    ('parked-1', 'pull_request', '{}'::jsonb,
		     clock_timestamp() - interval '1200 seconds', 'parked',
		     clock_timestamp()),
		    ('parked-2', 'pull_request', '{}'::jsonb,
		     clock_timestamp() - interval '300 seconds', 'parked',
		     clock_timestamp())
	`); err != nil {
		t.Fatal(err)
	}
	row, err = queries.CollectDeliveryMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The unprocessed age spans pending AND processing, and parked never
	// leaks into it even though parked is older.
	assertDeliveryAge(t, "oldest_unprocessed", row.OldestUnprocessed, 900)
	if row.Parked != 2 {
		t.Fatalf("parked = %d, want 2", row.Parked)
	}
	assertDeliveryAge(t, "oldest_parked", row.OldestParked, 1200)

	// With processing gone, LEAST() must fall through to the pending side
	// rather than collapsing to NULL and then to the COALESCE zero.
	if _, err := pool.Exec(ctx,
		`DELETE FROM webhook_deliveries WHERE delivery_guid = 'processing-1'`,
	); err != nil {
		t.Fatal(err)
	}
	row, err = queries.CollectDeliveryMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertDeliveryAge(t, "oldest_unprocessed", row.OldestUnprocessed, 600)

	// With neither present the gauge reports zero, not a negative clock age.
	if _, err := pool.Exec(ctx,
		`DELETE FROM webhook_deliveries WHERE delivery_guid = 'pending-1'`,
	); err != nil {
		t.Fatal(err)
	}
	row, err = queries.CollectDeliveryMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if row.OldestUnprocessed != 0 {
		t.Fatalf("oldest_unprocessed = %v, want 0", row.OldestUnprocessed)
	}
	if row.Parked != 2 {
		t.Fatalf("parked = %d, want 2", row.Parked)
	}
}

func assertDeliveryAge(t *testing.T, name string, got, want float64) {
	t.Helper()
	// Row ages are seeded off clock_timestamp() and read back moments later.
	if got < want || got > want+60 {
		t.Fatalf("%s = %v seconds, want ~%v", name, got, want)
	}
}
