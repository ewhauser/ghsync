package conformance_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

const conformanceWebhookSecret = "frontier-conformance-secret"

func TestIngressCorpus(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	queries := dbgen.New(database.Pool)
	handler := ingress.NewMux(ingress.NewHandler(
		queries,
		conformanceWebhookSecret,
		1<<20,
		5*time.Second,
	))

	for _, payload := range loadCorpusPayloads(t) {
		t.Run(payload.Filename, func(t *testing.T) {
			guid := corpusGUID(payload.Filename)
			response := serveWebhook(
				t,
				handler,
				payload.Body,
				payload.Event,
				guid,
				conformanceWebhookSecret,
				"application/json",
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"accept status = %d, body = %q",
					response.Code,
					response.Body.String(),
				)
			}

			stored, err := queries.GetWebhookDelivery(t.Context(), guid)
			if err != nil {
				t.Fatalf("get stored delivery: %v", err)
			}
			if stored.Event != payload.Event ||
				!bytes.Equal(stored.RawBody, payload.Body) {
				t.Fatalf(
					"stored event/body = %q/%d bytes, want %q/%d bytes",
					stored.Event,
					len(stored.RawBody),
					payload.Event,
					len(payload.Body),
				)
			}

			response = serveWebhook(
				t,
				handler,
				[]byte(`{"dedupe_probe":"must not overwrite"}`),
				"dedupe_probe",
				guid,
				conformanceWebhookSecret,
				"application/json",
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"redelivery status = %d, body = %q",
					response.Code,
					response.Body.String(),
				)
			}
			assertStoredDeliveryCount(t, database, guid, 1)
			storedAfterRedelivery, err := queries.GetWebhookDelivery(
				t.Context(),
				guid,
			)
			if err != nil {
				t.Fatalf("get redelivered delivery: %v", err)
			}
			if storedAfterRedelivery.Event != payload.Event ||
				!bytes.Equal(storedAfterRedelivery.RawBody, payload.Body) {
				t.Fatalf(
					"redelivery overwrote first event/body with %q/%q",
					storedAfterRedelivery.Event,
					storedAfterRedelivery.RawBody,
				)
			}

			formBody := []byte(url.Values{
				"payload": {string(payload.Body)},
			}.Encode())
			formGUID := guid + "-form"
			response = serveWebhook(
				t,
				handler,
				formBody,
				payload.Event,
				formGUID,
				conformanceWebhookSecret,
				"application/x-www-form-urlencoded",
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"form accept status = %d, body = %q",
					response.Code,
					response.Body.String(),
				)
			}
			storedForm, err := queries.GetWebhookDelivery(
				t.Context(),
				formGUID,
			)
			if err != nil {
				t.Fatalf("get stored form delivery: %v", err)
			}
			if storedForm.Event != payload.Event ||
				!bytes.Equal(storedForm.RawBody, formBody) {
				t.Fatalf(
					"stored form event/body = %q/%q, want %q/%q",
					storedForm.Event,
					storedForm.RawBody,
					payload.Event,
					formBody,
				)
			}

			trimmed := bytes.TrimSpace(payload.Body)
			if len(trimmed) < 2 {
				t.Fatalf("payload has only %d non-space bytes", len(trimmed))
			}
			truncated := trimmed[:len(trimmed)-1]
			rejections := []struct {
				name          string
				body          []byte
				signingSecret string
				contentType   string
				wantStatus    int
			}{
				{
					name:          "truncated_body",
					body:          truncated,
					signingSecret: conformanceWebhookSecret,
					contentType:   "application/json",
					wantStatus:    http.StatusBadRequest,
				},
				{
					name:          "bad_signature",
					body:          payload.Body,
					signingSecret: "wrong-secret",
					contentType:   "application/json",
					wantStatus:    http.StatusUnauthorized,
				},
				{
					name:          "wrong_content_type",
					body:          payload.Body,
					signingSecret: conformanceWebhookSecret,
					contentType:   "text/plain",
					wantStatus:    http.StatusUnsupportedMediaType,
				},
				{
					name:          "non_object_json",
					body:          []byte(`[]`),
					signingSecret: conformanceWebhookSecret,
					contentType:   "application/json",
					wantStatus:    http.StatusBadRequest,
				},
				{
					name:          "missing_form_payload",
					body:          []byte(`other=value`),
					signingSecret: conformanceWebhookSecret,
					contentType:   "application/x-www-form-urlencoded",
					wantStatus:    http.StatusBadRequest,
				},
			}
			for _, rejection := range rejections {
				t.Run(rejection.name, func(t *testing.T) {
					rejectionGUID := guid + "-" + rejection.name
					response := serveWebhook(
						t,
						handler,
						rejection.body,
						payload.Event,
						rejectionGUID,
						rejection.signingSecret,
						rejection.contentType,
					)
					if response.Code != rejection.wantStatus {
						t.Fatalf(
							"status = %d, want %d; body = %q",
							response.Code,
							rejection.wantStatus,
							response.Body.String(),
						)
					}
					assertStoredDeliveryCount(
						t,
						database,
						rejectionGUID,
						0,
					)
				})
			}
		})
	}
}

func serveWebhook(
	t *testing.T,
	handler http.Handler,
	body []byte,
	event string,
	guid string,
	signingSecret string,
	contentType string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		ingress.WebhookPath,
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("X-GitHub-Delivery", guid)
	request.Header.Set("X-GitHub-Event", event)
	request.Header.Set(
		"X-Hub-Signature-256",
		gh.SignBody([]byte(signingSecret), body),
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertStoredDeliveryCount(
	t *testing.T,
	database *testdb.Database,
	guid string,
	want int,
) {
	t.Helper()
	var count int
	if err := database.Pool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM webhook_deliveries
		WHERE delivery_guid = $1
	`, guid).Scan(&count); err != nil {
		t.Fatalf("count stored delivery: %v", err)
	}
	if count != want {
		t.Fatalf("stored delivery count = %d, want %d", count, want)
	}
}

func corpusGUID(filename string) string {
	sum := sha256.Sum256([]byte(filename))
	return "conformance-" + hex.EncodeToString(sum[:16])
}
