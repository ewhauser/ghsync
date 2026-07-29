package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/testdb"
)

type counterRegistrar struct {
	counter metric.Int64Counter
}

func (r *counterRegistrar) RegisterMetrics(meter metric.Meter) error {
	var err error
	r.counter, err = meter.Int64Counter("frontier_c_o4_registry_test")
	return err
}

func TestRegistryIsolatedPrometheusExposition(t *testing.T) {
	registry, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Shutdown(context.Background()) //nolint:errcheck
	registrar := &counterRegistrar{}
	if err := registry.Register("test", registrar); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("test", registrar); err == nil {
		t.Fatal("duplicate metrics scope was accepted")
	}
	registrar.counter.Add(context.Background(), 2)

	request := httptest.NewRequest("GET", Path, nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("metrics status = %d", response.Code)
	}
	if body := response.Body.String(); !strings.Contains(
		body,
		"frontier_c_o4_registry_test_total 2",
	) {
		t.Fatalf("metrics body omitted registry counter:\n%s", body)
	}
}

func TestRuntimeMetricsExposeConstraintState(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, databaseURL, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO installation_budgets (
		    installation_id, class, remaining, rate_limit, reset_at
		)
		VALUES
		    (1, 'rest', 14000, 15000, clock_timestamp() + interval '1 hour'),
		    (1, 'graphql', 4900, 5000, clock_timestamp() + interval '1 hour')
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
	defer registry.Shutdown(context.Background()) //nolint:errcheck
	if err := registry.Register("runtime-test", runtimeMetrics); err != nil {
		t.Fatal(err)
	}
	runtimeMetrics.BudgetRequest(budget.RequestObservation{
		Class:       budget.Sweep,
		Resource:    budget.REST,
		StatusCode:  304,
		Conditional: true,
		NotModified: true,
	})
	runtimeMetrics.CacheWrite(ctx, "pull_request", false, false)
	receivedAt := time.Now().UTC()
	runtimeMetrics.RefreshFinished(ctx, queue.RefreshObservation{
		Kind:            queue.KindRefreshBranch,
		Queue:           queue.QueueEvent,
		EventReceivedAt: receivedAt,
	})
	runtimeMetrics.RefreshFinished(ctx, queue.RefreshObservation{
		Kind:             queue.KindRefreshPR,
		Queue:            queue.QueueEvent,
		EventReceivedAt:  receivedAt,
		CacheCommittedAt: receivedAt.Add(-time.Second),
	})
	runtimeMetrics.RefreshFinished(ctx, queue.RefreshObservation{
		Kind:             queue.KindRefreshPR,
		Queue:            queue.QueueEvent,
		EventReceivedAt:  receivedAt,
		CacheCommittedAt: receivedAt.Add(time.Second),
	})

	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(
		response,
		httptest.NewRequest("GET", Path, nil),
	)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"frontier_c_b3_budget_remaining",
		"frontier_c_b4_conditional_304s_total",
		"frontier_c_c2_cache_cas_reject_ratio",
		"frontier_c_i5_parked_deliveries",
		"frontier_c_o3_drift_findings",
		"frontier_c_s2_watermark_lag_sequences",
		"frontier_c_q2_outstanding_generations",
		"frontier_c_o4_operation_samples",
		"frontier_c_o4_last_operation_sample_age_seconds",
		"frontier_c_p5_deriver_dirty_backlog",
		`frontier_c_r2_sweep_period_seconds{sweep_kind="stacks"} 225`,
		`frontier_c_o4_role_enabled{role="fetch"} 1`,
		`frontier_c_q2_event_to_cache_latency_seconds_count{kind="refresh_pr"} 1`,
		`frontier_c_q2_invalid_event_cache_latency_total{kind="refresh_pr"} 1`,
	} {
		if !strings.Contains(string(body), name) {
			t.Errorf("metrics exposition omitted %q", name)
		}
	}
}
