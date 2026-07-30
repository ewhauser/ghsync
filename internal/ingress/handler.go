// Package ingress implements the intentionally small webhook commit path.
package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	// WebhookPath is the GitHub App webhook target served by ghsyncd.
	WebhookPath = "/webhooks/github"
	// HealthPath is the process liveness endpoint.
	HealthPath = "/healthz"
)

// DeliveryInserter is the single allowed ingress persistence operation
// (SYNC_ENGINE C-I1/C-P1).
type DeliveryInserter interface {
	InsertWebhookDelivery(
		ctx context.Context,
		arg dbgen.InsertWebhookDeliveryParams,
	) (int64, error)
}

// Handler verifies and durably records GitHub webhook deliveries.
type Handler struct {
	deliveries     DeliveryInserter
	secret         []byte
	maxBodyBytes   int64
	requestTimeout time.Duration
}

// NewHandler constructs a webhook handler around sqlc's generated insert.
func NewHandler(
	deliveries DeliveryInserter,
	webhookSecret string,
	maxBodyBytes int64,
	requestTimeout time.Duration,
) *Handler {
	if deliveries == nil {
		panic("ingress delivery inserter is required")
	}
	if webhookSecret == "" {
		panic("ingress webhook secret is required")
	}
	if maxBodyBytes <= 0 {
		panic("ingress max body bytes must be positive")
	}
	if requestTimeout <= 0 {
		panic("ingress request timeout must be positive")
	}
	return &Handler{
		deliveries:     deliveries,
		secret:         []byte(webhookSecret),
		maxBodyBytes:   maxBodyBytes,
		requestTimeout: requestTimeout,
	}
}

// NewMux composes liveness with an optional GitHub webhook route.
func NewMux(webhook http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+HealthPath, func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		w.WriteHeader(http.StatusNoContent)
	})
	if webhook != nil {
		mux.Handle("POST "+WebhookPath, webhook)
	}
	return mux
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestCtx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	r = r.WithContext(requestCtx)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read webhook body", http.StatusBadRequest)
		return
	}

	// C-I2: signature verification is deliberately before header validation,
	// JSON decoding, classification, or persistence.
	if !gh.VerifySignature(
		h.secret,
		body,
		r.Header.Get("X-Hub-Signature-256"),
	) {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	guid := r.Header.Get("X-GitHub-Delivery")
	event := r.Header.Get("X-GitHub-Event")
	if guid == "" || event == "" {
		http.Error(w, "missing GitHub delivery headers", http.StatusBadRequest)
		return
	}
	if _, err := gh.DecodeWebhookPayload(
		r.Header.Get("Content-Type"),
		body,
	); err != nil {
		if errors.Is(err, gh.ErrUnsupportedWebhookContentType) {
			http.Error(
				w,
				"unsupported webhook content type",
				http.StatusUnsupportedMediaType,
			)
			return
		}
		http.Error(w, "invalid webhook payload", http.StatusBadRequest)
		return
	}
	headers, err := json.Marshal(requestEnvelope{
		Headers:          r.Header.Clone(),
		Host:             r.Host,
		ContentLength:    r.ContentLength,
		TransferEncoding: append([]string(nil), r.TransferEncoding...),
	})
	if err != nil {
		http.Error(w, "encode webhook headers", http.StatusInternalServerError)
		return
	}

	// C-I1/C-I3/C-P1: exactly one durable insert. A zero row count is the
	// expected duplicate-GUID no-op and still acknowledges with 200.
	traceCarrier := webhookTraceCarrier(r.Context()) //nolint:contextcheck // inject the request's remote span context
	if _, err := h.deliveries.InsertWebhookDelivery( //nolint:contextcheck // the HTTP request context is propagated directly
		r.Context(),
		dbgen.InsertWebhookDeliveryParams{
			DeliveryGuid: guid,
			Event:        event,
			RawBody:      body,
			Headers:      headers,
			Traceparent:  traceCarrier.Get("traceparent"),
			Tracestate:   traceCarrier.Get("tracestate"),
		}); err != nil {
		http.Error(w, "store webhook delivery", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func webhookTraceCarrier(ctx context.Context) propagation.MapCarrier {
	carrier := make(propagation.MapCarrier)
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier
}

// requestEnvelope preserves the semantically relevant request metadata needed
// for C-R4 gap healing. Go has already canonicalized headers, so wire-exact
// casing and ordering are intentionally not retained.
type requestEnvelope struct {
	Headers          http.Header `json:"headers"`
	Host             string      `json:"host"`
	ContentLength    int64       `json:"content_length"`
	TransferEncoding []string    `json:"transfer_encoding"`
}
