// Package metrics owns ghsync's OpenTelemetry meter provider and Prometheus
// exposition. Runtime packages expose narrow observer interfaces; the daemon
// registers their implementations here so instrumentation never creates an
// import cycle.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/otlptranslator"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const Path = "/metrics"

// Registrar lets a component register instruments against the process-local
// provider without importing the registry or another runtime package.
type Registrar interface {
	RegisterMetrics(metric.Meter) error
}

// Registry is one isolated OTel provider backed by one Prometheus registry.
// Tests and role-separated processes can therefore coexist without global
// collector collisions.
type Registry struct {
	provider *sdkmetric.MeterProvider
	handler  http.Handler

	mu     sync.Mutex
	scopes map[string]struct{}
}

func New() (*Registry, error) {
	promRegistry := prometheus.NewRegistry()
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(promRegistry),
		otelprom.WithoutScopeInfo(),
		otelprom.WithoutTargetInfo(),
		otelprom.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithSuffixes),
	)
	if err != nil {
		return nil, fmt.Errorf("create Prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "ghsync_c_q2_event_to_cache_latency_seconds",
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{
						1, 2, 5, 10, 15, 20, 30, 60, 120,
					},
					NoMinMax: true,
				},
			},
		)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "ghsync_c_p2_dispatch_batch_size",
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{1, 5, 10, 25, 50, 100, 250, 500},
					NoMinMax:   true,
				},
			},
		)),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "ghsync_c_p5_deriver_pass_duration_seconds",
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{
						0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30,
					},
					NoMinMax: true,
				},
			},
		)),
	)
	return &Registry{
		provider: provider,
		handler: promhttp.HandlerFor(
			promRegistry,
			promhttp.HandlerOpts{EnableOpenMetrics: true},
		),
		scopes: make(map[string]struct{}),
	}, nil
}

// Register installs one component's instruments under a unique OTel scope.
func (r *Registry) Register(scope string, registrar Registrar) error {
	if scope == "" || registrar == nil {
		return fmt.Errorf("metrics registration requires scope and registrar")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.scopes[scope]; exists {
		return fmt.Errorf("metrics scope %q is already registered", scope)
	}
	if err := registrar.RegisterMetrics(r.provider.Meter(scope)); err != nil {
		return fmt.Errorf("register metrics scope %q: %w", scope, err)
	}
	r.scopes[scope] = struct{}{}
	return nil
}

func (r *Registry) Handler() http.Handler {
	return r.handler
}

// Provider exposes the process-local provider to instrumentation libraries
// such as River's OpenTelemetry plugin. The provider remains registry-owned and
// must not be shut down by callers.
func (r *Registry) Provider() metric.MeterProvider {
	return r.provider
}

func (r *Registry) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}
