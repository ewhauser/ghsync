package ingress

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store/dbgen"
)

type recordingInserter struct {
	calls  int
	rows   int64
	err    error
	params dbgen.InsertWebhookDeliveryParams
}

func (r *recordingInserter) InsertWebhookDelivery(
	_ context.Context,
	params dbgen.InsertWebhookDeliveryParams,
) (int64, error) {
	r.calls++
	r.params = params
	return r.rows, r.err
}

func TestHandlerVerifiesThenStoresRawDelivery(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024)
	body := []byte(`not-json-at-ingress`)
	request := signedRequest(t, body, "secret", "guid-1", "pull_request")
	request.Header.Set("X-Extra-Test-Header", "preserved")
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", response.Code, response.Body.String())
	}
	if store.calls != 1 {
		t.Fatalf("insert calls = %d, want 1", store.calls)
	}
	if store.params.DeliveryGuid != "guid-1" ||
		store.params.Event != "pull_request" ||
		!bytes.Equal(store.params.RawBody, body) {
		t.Fatalf("stored delivery = %+v", store.params)
	}
	if !bytes.Contains(store.params.Headers, []byte("X-Extra-Test-Header")) {
		t.Fatalf("stored headers = %s", store.params.Headers)
	}
}

func TestHandlerRejectsUnverifiedWithoutStoring(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024)
	request := signedRequest(
		t,
		[]byte(`{"action":"opened"}`),
		"wrong-secret",
		"guid-2",
		"pull_request",
	)
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if store.calls != 0 {
		t.Fatalf("unverified delivery was stored %d times", store.calls)
	}
}

func TestHandlerAcknowledgesDuplicateGUID(t *testing.T) {
	store := &recordingInserter{rows: 0}
	handler := NewHandler(store, "secret", 1024)
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(
		response,
		signedRequest(t, []byte(`{}`), "secret", "duplicate", "push"),
	)

	if response.Code != http.StatusOK || store.calls != 1 {
		t.Fatalf("status=%d insert calls=%d", response.Code, store.calls)
	}
}

func TestHandlerRejectsOversizeBeforeStoring(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 4)
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(
		response,
		signedRequest(t, []byte(`12345`), "secret", "guid-3", "push"),
	)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
	if store.calls != 0 {
		t.Fatalf("oversize delivery was stored %d times", store.calls)
	}
}

func TestHandlerRequiresDeliveryHeadersAfterVerification(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024)
	request := signedRequest(t, []byte(`{}`), "secret", "", "push")
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || store.calls != 0 {
		t.Fatalf("status=%d insert calls=%d", response.Code, store.calls)
	}
}

func TestHandlerDoesNotAcknowledgeFailedCommit(t *testing.T) {
	store := &recordingInserter{err: errors.New("database unavailable")}
	handler := NewHandler(store, "secret", 1024)
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(
		response,
		signedRequest(t, []byte(`{}`), "secret", "guid-4", "push"),
	)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
}

func TestHealth(t *testing.T) {
	handler := NewHandler(&recordingInserter{}, "secret", 1024)
	request := httptest.NewRequest(http.MethodGet, HealthPath, nil)
	response := httptest.NewRecorder()

	handler.Mux().ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
}

func signedRequest(
	t *testing.T,
	body []byte,
	secret string,
	guid string,
	event string,
) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, WebhookPath, bytes.NewReader(body))
	request.Header.Set("X-Hub-Signature-256", gh.SignBody([]byte(secret), body))
	if guid != "" {
		request.Header.Set("X-GitHub-Delivery", guid)
	}
	if event != "" {
		request.Header.Set("X-GitHub-Event", event)
	}
	return request
}
