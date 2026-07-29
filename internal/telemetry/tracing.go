// Package telemetry owns ghsync's OpenTelemetry tracing runtime.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const defaultServiceName = "ghsyncd"

// TracingOptions identifies one running command or role set.
type TracingOptions struct {
	Version string
	Command string
	Roles   []string
}

// Tracing is an explicitly injected trace provider and W3C propagator.
// Tracing is disabled unless OTEL_TRACES_EXPORTER is explicitly set to otlp.
type Tracing struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
	shutdown   func(context.Context) error
	enabled    bool
}

// NewTracing constructs the process trace pipeline. OTLP/HTTP exporter
// configuration is read from the standard OTEL_EXPORTER_OTLP_* environment
// variables by the OpenTelemetry exporter.
func NewTracing(ctx context.Context, options TracingOptions) (*Tracing, error) {
	propagator := propagation.TraceContext{}
	exporterName := strings.ToLower(strings.TrimSpace(
		os.Getenv("OTEL_TRACES_EXPORTER"),
	))
	if exporterName == "" || exporterName == "none" {
		return &Tracing{
			provider:   noop.NewTracerProvider(),
			propagator: propagator,
			shutdown:   func(context.Context) error { return nil },
		}, nil
	}
	if exporterName != "otlp" {
		return nil, fmt.Errorf(
			"OTEL_TRACES_EXPORTER must be otlp or none, got %q",
			exporterName,
		)
	}

	sampler, err := samplerFromEnv()
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
		attribute.String("service.version", options.Version),
		attribute.String("ghsync.command", options.Command),
	}
	if len(options.Roles) > 0 {
		attrs = append(attrs, attribute.StringSlice("ghsync.roles", options.Roles))
	}
	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithHost(),
		resource.WithAttributes(attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	return &Tracing{
		provider:   provider,
		propagator: propagator,
		shutdown:   provider.Shutdown,
		enabled:    true,
	}, nil
}

func samplerFromEnv() (sdktrace.Sampler, error) {
	name := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")))
	if name == "" {
		name = "parentbased_always_on"
	}
	switch name {
	case "always_on":
		return sdktrace.AlwaysSample(), nil
	case "always_off":
		return sdktrace.NeverSample(), nil
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case "traceidratio", "parentbased_traceidratio":
		ratio, err := samplerRatio()
		if err != nil {
			return nil, err
		}
		sampler := sdktrace.TraceIDRatioBased(ratio)
		if name == "parentbased_traceidratio" {
			return sdktrace.ParentBased(sampler), nil
		}
		return sampler, nil
	default:
		return nil, fmt.Errorf(
			"unsupported OTEL_TRACES_SAMPLER %q",
			name,
		)
	}
}

func samplerRatio() (float64, error) {
	raw := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
	if raw == "" {
		return 1, nil
	}
	ratio, err := strconv.ParseFloat(raw, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, fmt.Errorf(
			"OTEL_TRACES_SAMPLER_ARG must be a number between 0 and 1",
		)
	}
	return ratio, nil
}

// Provider returns the explicitly owned trace provider.
func (t *Tracing) Provider() trace.TracerProvider {
	return t.provider
}

// Propagator returns the W3C Trace Context propagator. Baggage is deliberately
// excluded so internal baggage cannot be forwarded to GitHub.
func (t *Tracing) Propagator() propagation.TextMapPropagator {
	return t.propagator
}

func (t *Tracing) Enabled() bool {
	return t.enabled
}

func (t *Tracing) Shutdown(ctx context.Context) error {
	return t.shutdown(ctx)
}
