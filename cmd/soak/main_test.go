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

func TestAssertBudgetFloors(t *testing.T) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE frontier_c_b3_budget_remaining gauge
frontier_c_b3_budget_remaining{class="sweep",resource="rest"} 2999
# TYPE frontier_c_b3_budget_floor gauge
frontier_c_b3_budget_floor{class="sweep",resource="rest"} 3000
`))
	if err != nil {
		t.Fatal(err)
	}
	checked, err := assertBudgetFloors(families)
	if !checked || err == nil {
		t.Fatalf("checked=%v err=%v, want breach", checked, err)
	}
}
