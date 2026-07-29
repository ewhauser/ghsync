package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/testdb"
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

func TestMetricValueWithoutLabelsSumsZeroInitAndLabeledSeries(t *testing.T) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE frontier_c_b3_starvations_total counter
frontier_c_b3_starvations_total 0
frontier_c_b3_starvations_total{class="event",resource="rest"} 3
frontier_c_b3_starvations_total{class="sweep",resource="graphql"} 2
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(
		families,
		"frontier_c_b3_starvations_total",
		nil,
	); got != 5 {
		t.Fatalf("unfiltered starvation total = %v, want 5", got)
	}
	if got := metricValue(
		families,
		"frontier_c_b3_starvations_total",
		map[string]string{"class": "event", "resource": "rest"},
	); got != 3 {
		t.Fatalf("labeled starvation total = %v, want 3", got)
	}
}

func TestSoakStreamConsumerAppliesExactlyOnceAcrossRestart(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, databaseURL, "soak_stream")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	consumer, err := newSoakStreamConsumer(ctx, database.Pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.cleanup()
	if err := consumer.start(); err != nil {
		t.Fatal(err)
	}

	insertSoakStreamEvents(t, ctx, database.Pool, "before", 2)
	waitForSoakStreamConsumer(t, ctx, consumer, 2)
	if err := consumer.restart(ctx); err != nil {
		t.Fatal(err)
	}

	insertSoakStreamEvents(t, ctx, database.Pool, "after", 2)
	waitForSoakStreamConsumer(t, ctx, consumer, 4)
	if err := consumer.stop(ctx); err != nil {
		t.Fatal(err)
	}
	counts, err := consumer.assertFinal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.total != 4 || counts.distinct != 4 || counts.expected != 4 {
		t.Fatalf("stream counts after restart = %+v, want four exactly once", counts)
	}
}

func insertSoakStreamEvents(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	prefix string,
	count int,
) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var maxSeq int64
	for index := range count {
		if err := tx.QueryRow(ctx, `
			INSERT INTO change_events (
			    stream, kind, entity_key, payload
			)
			VALUES (
			    'entities', 'pull_request.changed', $1, '{"version":1}'
			)
			RETURNING seq
		`, prefix+"-"+strconv.Itoa(index)).Scan(&maxSeq); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE stream_watermark
		SET safe_seq = GREATEST(safe_seq, $1),
		    updated_at = clock_timestamp()
		WHERE singleton
	`, maxSeq); err != nil {
		t.Fatal(err)
	}
}

func waitForSoakStreamConsumer(
	t *testing.T,
	ctx context.Context,
	consumer *soakStreamConsumer,
	want int64,
) {
	t.Helper()
	for {
		if err := consumer.check(); err != nil {
			t.Fatal(err)
		}
		counts, err := consumer.counts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts.total == want &&
			counts.distinct == want &&
			counts.expected == want &&
			counts.cursor == counts.maxSeq {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"stream consumer did not reach %d applications: %+v (%v)",
				want,
				counts,
				ctx.Err(),
			)
		case <-time.After(25 * time.Millisecond):
		}
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
