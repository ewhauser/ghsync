package main

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func TestProfileDuration(t *testing.T) {
	if got, err := profileDuration("smoke", 0); err != nil ||
		got != 2*time.Minute {
		t.Fatalf("smoke duration = %s, %v", got, err)
	}
	if got, err := profileDuration("48h", 0); err != nil ||
		got != 48*time.Hour {
		t.Fatalf("48h duration = %s, %v", got, err)
	}
	if got, err := profileDuration("custom", 3*time.Minute); err != nil ||
		got != 3*time.Minute {
		t.Fatalf("custom duration = %s, %v", got, err)
	}
	if _, err := profileDuration("custom", 0); err == nil {
		t.Fatal("custom profile without duration was accepted")
	}
}

func TestSmokeLoadArithmeticIsExactlyTenTimesRecordedRate(t *testing.T) {
	const recordedRate = 1.0
	const multiplier = 10.0
	got := expectedEventCount(2*time.Minute, recordedRate, multiplier)
	if got != 1200 {
		t.Fatalf("smoke event count = %d, want 1200", got)
	}
	achieved := float64(got) / (2 * time.Minute).Seconds()
	if achieved != recordedRate*multiplier {
		t.Fatalf(
			"smoke achieved rate = %.2f/s, want %.2f/s",
			achieved,
			recordedRate*multiplier,
		)
	}
}

func TestValidateConfigRequiresPositiveInstallation(t *testing.T) {
	cfg := config{
		engineURL:      "http://engine",
		fakeGitHubURL:  "http://fake",
		databaseURL:    "postgres://database",
		duration:       time.Second,
		recordedRate:   1,
		multiplier:     10,
		scrapeInterval: time.Second,
		drainTimeout:   time.Second,
	}
	if err := validateConfig(cfg); err == nil {
		t.Fatal("soak accepted an absent installation ID")
	}
	cfg.installationID = 1
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("valid strict soak config rejected: %v", err)
	}
}

func TestHistogramQuantileUsesConstraintBuckets(t *testing.T) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE frontier_c_q2_event_to_cache_latency_seconds histogram
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="20"} 95
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="60"} 99
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="+Inf"} 100
frontier_c_q2_event_to_cache_latency_seconds_sum 1000
frontier_c_q2_event_to_cache_latency_seconds_count 100
`))
	if err != nil {
		t.Fatal(err)
	}
	p95, count, err := histogramQuantile(
		families["frontier_c_q2_event_to_cache_latency_seconds"],
		0.95,
	)
	if err != nil || p95 != 20 || count != 100 {
		t.Fatalf("p95=%v count=%d err=%v", p95, count, err)
	}
	p99, _, err := histogramQuantile(
		families["frontier_c_q2_event_to_cache_latency_seconds"],
		0.99,
	)
	if err != nil || p99 != 60 {
		t.Fatalf("p99=%v err=%v", p99, err)
	}
}

func TestHistogramDeltasExcludePreexistingSamples(t *testing.T) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	priorFamilies, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE frontier_c_q2_event_to_cache_latency_seconds histogram
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="20"} 90
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="60"} 99
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="+Inf"} 100
frontier_c_q2_event_to_cache_latency_seconds_sum 1000
frontier_c_q2_event_to_cache_latency_seconds_count 100
`))
	if err != nil {
		t.Fatal(err)
	}
	currentFamilies, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE frontier_c_q2_event_to_cache_latency_seconds histogram
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="20"} 100
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="60"} 109
frontier_c_q2_event_to_cache_latency_seconds_bucket{le="+Inf"} 110
frontier_c_q2_event_to_cache_latency_seconds_sum 1100
frontier_c_q2_event_to_cache_latency_seconds_count 110
`))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := histogramState(
		priorFamilies["frontier_c_q2_event_to_cache_latency_seconds"],
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err := histogramState(
		currentFamilies["frontier_c_q2_event_to_cache_latency_seconds"],
	)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := subtractHistogram(current, prior)
	if err != nil {
		t.Fatal(err)
	}
	if delta.count != 10 || delta.buckets[20] != 10 {
		t.Fatalf("histogram delta = %+v", delta)
	}
}
