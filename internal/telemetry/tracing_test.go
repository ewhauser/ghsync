package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTracingDisabledByDefault(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	tracing, err := NewTracing(context.Background(), TracingOptions{
		Version: "test",
		Command: "serve",
		Roles:   []string{"ingress"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if tracing.Enabled() {
		t.Fatal("tracing unexpectedly enabled")
	}
	if err := tracing.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTracingRejectsUnsupportedExporter(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	if _, err := NewTracing(
		context.Background(),
		TracingOptions{Version: "test", Command: "serve"},
	); err == nil {
		t.Fatal("unsupported trace exporter was accepted")
	}
}

func TestSamplerRatioValidation(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1.5")
	if _, err := samplerFromEnv(); err == nil {
		t.Fatal("invalid trace ratio was accepted")
	}

	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	if _, err := samplerFromEnv(); err != nil {
		t.Fatalf("valid trace ratio rejected: %v", err)
	}
}

func TestTracingExportsOTLPHTTP(t *testing.T) {
	type exportRequest struct {
		method      string
		path        string
		contentType string
		bodyBytes   int
	}
	requests := make(chan exportRequest, 1)
	collector := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			requests <- exportRequest{
				method:      r.Method,
				path:        r.URL.Path,
				contentType: r.Header.Get("Content-Type"),
				bodyBytes:   len(body),
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(http.StatusOK)
		},
	))
	defer collector.Close()

	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	tracing, err := NewTracing(context.Background(), TracingOptions{
		Version: "test",
		Command: "serve",
		Roles:   []string{"ingress"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracing.Provider().Tracer("test").Start(
		context.Background(),
		"ghsync.test",
	)
	span.End()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tracing.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}

	select {
	case request := <-requests:
		if request.method != http.MethodPost {
			t.Errorf("export method = %q, want POST", request.method)
		}
		if request.path != "/v1/traces" {
			t.Errorf("export path = %q, want /v1/traces", request.path)
		}
		if request.contentType != "application/x-protobuf" {
			t.Errorf(
				"export content type = %q, want application/x-protobuf",
				request.contentType,
			)
		}
		if request.bodyBytes == 0 {
			t.Error("export body is empty")
		}
	default:
		t.Fatal("collector did not receive an export")
	}
}
