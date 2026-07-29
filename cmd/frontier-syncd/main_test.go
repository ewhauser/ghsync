package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/acme/frontier/internal/config"
	"github.com/acme/frontier/internal/queue"
)

func TestValidateRoles(t *testing.T) {
	for _, roles := range []string{
		"all",
		"ingress",
		"dispatch",
		"fetch",
		"sweep",
		"drift",
		"pruner",
		"ingress,dispatch,fetch,sweep,drift,pruner",
	} {
		if err := validateRoles(roles); err != nil {
			t.Fatalf("%q rejected: %v", roles, err)
		}
	}
	for _, roles := range []string{"", "bogus", "all,event", "ingress,event"} {
		if err := validateRoles(roles); err == nil {
			t.Fatalf("roles %q accepted", roles)
		}
	}
}

func TestRolePlansPollOnlyOwnedQueueFamilies(t *testing.T) {
	tests := map[string][]string{
		"dispatch": nil,
		"fetch": {
			queue.QueueInteractive,
			queue.QueueEvent,
			queue.QueueSweep,
		},
		"sweep":           {queue.QueueReconcile},
		"drift":           {queue.QueueDrift},
		"pruner":          {queue.QueuePruner},
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
		"fetch",
		"sweep",
		"drift",
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
			want: requeueOptions{guid: "delivery-1"},
		},
		{
			args: []string{"--all-parked"},
			want: requeueOptions{allParked: true},
		},
	}
	for _, test := range tests {
		got, err := parseRequeueOptions(test.args)
		if err != nil {
			t.Fatalf("%v rejected: %v", test.args, err)
		}
		if got != test.want {
			t.Fatalf("%v parsed as %+v, want %+v", test.args, got, test.want)
		}
	}
	for _, args := range [][]string{
		nil,
		{"--guid=delivery-1", "--all-parked"},
		{"--guid="},
		{"--all-parked", "extra"},
	} {
		if _, err := parseRequeueOptions(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
