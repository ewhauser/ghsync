package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/dispatch"
	"github.com/ewhauser/ghsync/internal/ingress"
	ghsyncmetrics "github.com/ewhauser/ghsync/internal/metrics"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

func TestServiceMuxKeepsWebhookRoleSeparated(t *testing.T) {
	t.Parallel()
	metricsHandler := http.HandlerFunc(func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		_, _ = w.Write([]byte("ghsync_c_o4_test 1\n"))
	})
	workerMux := serviceMux(nil, metricsHandler)
	for path, wantStatus := range map[string]int{
		ingress.HealthPath:  http.StatusNoContent,
		ghsyncmetrics.Path:  http.StatusOK,
		ingress.WebhookPath: http.StatusNotFound,
	} {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
		if path == ingress.WebhookPath {
			request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, http.NoBody)
		}
		response := httptest.NewRecorder()
		workerMux.ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("%s status = %d, want %d", path, response.Code, wantStatus)
		}
	}

	ingressMux := serviceMux(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}),
		metricsHandler,
	)
	response := httptest.NewRecorder()
	ingressMux.ServeHTTP(
		response,
		httptest.NewRequestWithContext(t.Context(), http.MethodPost, ingress.WebhookPath, http.NoBody),
	)
	if response.Code != http.StatusAccepted {
		t.Fatalf("ingress webhook status = %d", response.Code)
	}
	metricsResponse := httptest.NewRecorder()
	ingressMux.ServeHTTP(
		metricsResponse,
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, ghsyncmetrics.Path, http.NoBody),
	)
	if !strings.Contains(metricsResponse.Body.String(), "ghsync_c_o4_test") {
		t.Fatal("ingress mux omitted metrics handler")
	}
}

func TestValidateRoles(t *testing.T) {
	t.Parallel()
	for _, roles := range []string{
		"all",
		"ingress",
		"dispatch",
		"pruner",
		"watermarker",
		"deriver",
		"metrics",
		"fetch,sweep,drift",
		"ingress,dispatch,fetch,sweep,drift,pruner,watermarker,deriver,metrics",
	} {
		if err := validateRoles(roles); err != nil {
			t.Fatalf("%q rejected: %v", roles, err)
		}
	}
	for _, roles := range []string{
		"",
		"bogus",
		"all,event",
		"ingress,event",
		"fetch",
		"sweep",
		"drift",
		"fetch,sweep",
	} {
		if err := validateRoles(roles); err == nil {
			t.Fatalf("roles %q accepted", roles)
		}
	}
}

func TestRolePlansPollOnlyOwnedQueueFamilies(t *testing.T) {
	t.Parallel()
	tests := map[string][]string{
		"dispatch": nil,
		"fetch,sweep,drift": {
			queue.QueueInteractive,
			queue.QueueEvent,
			queue.QueueSweep,
			queue.QueueReconcile,
			queue.QueueDrift,
		},
		"pruner":          {queue.QueuePruner},
		"watermarker":     nil,
		"deriver":         nil,
		"dispatch,pruner": {queue.QueuePruner},
		"fetch,sweep,drift,pruner": {
			queue.QueueInteractive,
			queue.QueueEvent,
			queue.QueueSweep,
			queue.QueueReconcile,
			queue.QueueDrift,
			queue.QueuePruner,
		},
	}
	for raw, want := range tests {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			roles, err := parseRoles(raw)
			if err != nil {
				t.Fatal(err)
			}
			got := riverPlanForRoles(roles).queues
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("queues = %v, want %v", got, want)
			}
		})
	}
}

func TestDriftPageSizeIsIndependentFromSweepPageSize(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		SweepPageSize: 25,
		DriftPageSize: 75,
	}
	if got := sweepConfig(&cfg).PageSize; got != 25 {
		t.Fatalf("sweep page size = %d, want 25", got)
	}
	if got := driftConfig(&cfg).PageSize; got != 75 {
		t.Fatalf("drift page size = %d, want 75", got)
	}
}

func TestEveryLeaderEligibleRoleGetsIdenticalPeriodicTable(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		GitHubInstallationID:       1,
		SweepOpenStackMaxStaleness: 5 * time.Minute,
		SweepOpenPRMaxStaleness:    10 * time.Minute,
		SweepRepoRulesMaxStaleness: time.Hour,
		SweepClosedMaxStaleness:    24 * time.Hour,
		SweepRepositoryListPeriod:  time.Hour,
		SweepPageSize:              100,
		GapHealPeriod:              5 * time.Minute,
		GapWindow:                  6 * time.Hour,
		GapPageSize:                100,
		GapMaxPages:                10,
		DriftPeriod:                time.Hour,
		DriftSampleSize:            10,
		DriftPageSize:              100,
		DriftResolvedRetention:     30 * 24 * time.Hour,
		RetentionPeriod:            24 * time.Hour,
		RetentionAge:               90 * 24 * time.Hour,
		RetentionBatchSize:         1000,
	}
	want := len(allPeriodicJobs(&cfg))
	if want == 0 {
		t.Fatal("periodic table is empty")
	}
	for _, raw := range []string{
		"fetch,sweep,drift",
		"pruner",
		"dispatch,pruner",
	} {
		roles, err := parseRoles(raw)
		if err != nil {
			t.Fatal(err)
		}
		worksJobs := roles[roleFetch] || roles[roleSweep] ||
			roles[roleDrift] || roles[rolePruner]
		got := 0
		if worksJobs {
			got = len(allPeriodicJobs(&cfg))
		}
		if got != want {
			t.Fatalf("%s periodic jobs = %d, want %d", raw, got, want)
		}
	}
}

func TestBadDispatcherRulesFileFailsClosed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "bad-rules.yaml")
	if err := os.WriteFile(path, []byte(`rules:
  - event: pull_request
    action: "*"
    target: pull_request
    stacked_targte: stack
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcherClassifier(path); err == nil {
		t.Fatal("bad dispatcher rules silently fell back to defaults")
	}
}

func TestParseRequeueOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		args []string
		want requeueOptions
	}{
		{
			args: []string{"--guid=delivery-1"},
			want: requeueOptions{guids: []string{"delivery-1"}},
		},
		{
			args: []string{"--guids=delivery-1,delivery-2"},
			want: requeueOptions{
				guids: []string{"delivery-1", "delivery-2"},
			},
		},
		{
			args: []string{
				"--event=pull_request",
				"--error-contains=unsupported action",
			},
			want: requeueOptions{
				event:         "pull_request",
				errorContains: "unsupported action",
			},
		},
	}
	for _, test := range tests {
		got, err := parseRequeueOptions(test.args)
		if err != nil {
			t.Fatalf("%v rejected: %v", test.args, err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%v parsed as %+v, want %+v", test.args, got, test.want)
		}
	}
	for _, args := range [][]string{
		nil,
		{"--guid=delivery-1", "--event=pull_request", "--error-contains=x"},
		{"--guid="},
		{"--event=pull_request"},
		{"--error-contains=x"},
		{"--guid=delivery-1", "extra"},
	} {
		if _, err := parseRequeueOptions(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}

// seedPushDelivery stores one webhook delivery that DefaultClassifier maps
// to a refresh_branch intent.
func seedPushDelivery(
	t *testing.T,
	pool *pgxpool.Pool,
	guid string,
) {
	t.Helper()
	payload := []byte(`{
		"ref": "refs/heads/main",
		"repository": {"full_name": "acme/monolith"}
	}`)
	headers := []byte(`{
		"x-github-delivery": "` + guid + `",
		"x-github-event": "push"
	}`)
	if _, err := dbgen.New(pool).InsertWebhookDelivery(
		context.Background(),
		dbgen.InsertWebhookDeliveryParams{
			DeliveryGuid: guid,
			Event:        "push",
			RawBody:      payload,
			Headers:      headers,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func issue7Dispatcher(
	t *testing.T,
	pool *pgxpool.Pool,
	riverClient *river.Client[pgx.Tx],
) *dispatch.Dispatcher {
	t.Helper()
	dispatcher, err := dispatch.New(pool, riverClient, dispatch.Config{
		BatchSize:    10,
		MaxAttempts:  3,
		Debounce:     time.Millisecond,
		PollInterval: time.Millisecond,
		// Stay ahead of the row's DB-clock received_at so the debounce
		// window has always elapsed.
		Now:        func() time.Time { return time.Now().Add(time.Minute) },
		Classifier: dispatch.DefaultClassifier(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

// Regression test for issue #7: a dispatch-only process is a producer of
// refresh jobs, so its never-started River client must still register the
// refresh job kinds for insert validation.
func TestDispatchOnlyClientInsertsClassifiedRefreshJobs(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	dispatchRoles := map[string]bool{roleDispatch: true}

	clientOptions := append(
		[]queue.ClientOption{
			queue.WithQueues(riverPlanForRoles(dispatchRoles).queues...),
		},
		refreshWorkerOptions(dispatchRoles)...,
	)
	riverClient, err := queue.NewClient(database.Pool, clientOptions...)
	if err != nil {
		t.Fatal(err)
	}

	seedPushDelivery(t, database.Pool, "issue-7-dispatch-only")
	count, err := issue7Dispatcher(t, database.Pool, riverClient).
		DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch-only refresh insert: %v", err)
	}
	if count != 1 {
		t.Fatalf("claimed %d deliveries, want 1", count)
	}

	// The client was never started: the inserted job must exist and remain
	// unworked (available or debounce-scheduled), proving dispatch produced
	// it without running refresh workers locally.
	var pending int
	if err := database.Pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM river_job
		 WHERE kind = 'refresh_branch'
		   AND state IN ('available', 'scheduled')`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("unworked refresh_branch jobs = %d, want 1", pending)
	}
}

// Pins the mechanism issue #7 exposed: a client that opts out of refresh
// registrations (correct for worker-only roles such as the pruner) cannot
// insert refresh jobs, which is exactly why dispatch must not opt out.
func TestWithoutRefreshWorkersRejectsRefreshInserts(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	prunerRoles := map[string]bool{rolePruner: true}

	workerOnlyOptions := refreshWorkerOptions(prunerRoles)
	if len(workerOnlyOptions) == 0 {
		t.Fatal("fetchless worker-only roles must opt out of refresh registrations")
	}
	riverClient, err := queue.NewClient(
		database.Pool,
		append(
			[]queue.ClientOption{
				queue.WithQueues(riverPlanForRoles(prunerRoles).queues...),
			},
			workerOnlyOptions...,
		)...,
	)
	if err != nil {
		t.Fatal(err)
	}

	seedPushDelivery(t, database.Pool, "issue-7-worker-only")
	_, err = issue7Dispatcher(t, database.Pool, riverClient).
		DispatchBatch(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf(
			"worker-only refresh insert error = %v, want unregistered-kind rejection",
			err,
		)
	}
}
