// Package ingress implements the intentionally small webhook commit path.
package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store/dbgen"
)

const (
	// WebhookPath is the GitHub App webhook target served by frontier-syncd.
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
	deliveries   DeliveryInserter
	secret       []byte
	maxBodyBytes int64
}

// NewHandler constructs a webhook handler around sqlc's generated insert.
func NewHandler(
	deliveries DeliveryInserter,
	webhookSecret string,
	maxBodyBytes int64,
) *Handler {
	if deliveries == nil {
		panic("ingress delivery inserter is required")
	}
	if maxBodyBytes <= 0 {
		panic("ingress max body bytes must be positive")
	}
	return &Handler{
		deliveries:   deliveries,
		secret:       []byte(webhookSecret),
		maxBodyBytes: maxBodyBytes,
	}
}

// Mux returns the ingress and health routes.
func (h *Handler) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST "+WebhookPath, h)
	mux.HandleFunc("GET "+HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	headers, err := json.Marshal(r.Header)
	if err != nil {
		http.Error(w, "encode webhook headers", http.StatusInternalServerError)
		return
	}

	// C-I1/C-I3/C-P1: exactly one durable insert. A zero row count is the
	// expected duplicate-GUID no-op and still acknowledges with 200.
	if _, err := h.deliveries.InsertWebhookDelivery(
		r.Context(),
		dbgen.InsertWebhookDeliveryParams{
			DeliveryGuid: guid,
			Event:        event,
			RawBody:      body,
			Headers:      headers,
		}); err != nil {
		http.Error(w, "store webhook delivery", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
