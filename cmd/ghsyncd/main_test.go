package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/ingress"
	ghsyncmetrics "github.com/ewhauser/ghsync/internal/metrics"
	"github.com/ewhauser/ghsync/internal/queue"
)

func TestServiceMuxKeepsWebhookRoleSeparated(t *testing.T) {
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
		request := httptest.NewRequest(http.MethodGet, path, nil)
		if path == ingress.WebhookPath {
			request = httptest.NewRequest(http.MethodPost, path, nil)
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
		httptest.NewRequest(http.MethodPost, ingress.WebhookPath, nil),
	)
	if response.Code != http.StatusAccepted {
		t.Fatalf("ingress webhook status = %d", response.Code)
	}
	metricsResponse := httptest.NewRecorder()
	ingressMux.ServeHTTP(
		metricsResponse,
		httptest.NewRequest(http.MethodGet, ghsyncmetrics.Path, nil),
	)
	if !strings.Contains(metricsResponse.Body.String(), "ghsync_c_o4_test") {
		t.Fatal("ingress mux omitted metrics handler")
	}
}

func TestValidateRoles(t *testing.T) {
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
	cfg := config.Config{
		SweepPageSize: 25,
		DriftPageSize: 75,
	}
	if got := sweepConfig(cfg).PageSize; got != 25 {
		t.Fatalf("sweep page size = %d, want 25", got)
	}
	if got := driftConfig(cfg).PageSize; got != 75 {
		t.Fatalf("drift page size = %d, want 75", got)
	}
}

func TestEveryLeaderEligibleRoleGetsIdenticalPeriodicTable(t *testing.T) {
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
	want := len(allPeriodicJobs(cfg))
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
			got = len(allPeriodicJobs(cfg))
		}
		if got != want {
			t.Fatalf("%s periodic jobs = %d, want %d", raw, got, want)
		}
	}
}

func TestBadDispatcherRulesFileFailsClosed(t *testing.T) {
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
