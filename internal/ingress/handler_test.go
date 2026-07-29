package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store/dbgen"
)

type recordingInserter struct {
	calls       int
	rows        int64
	err         error
	params      dbgen.InsertWebhookDeliveryParams
	hasDeadline bool
}

func (r *recordingInserter) InsertWebhookDelivery(
	ctx context.Context,
	params dbgen.InsertWebhookDeliveryParams,
) (int64, error) {
	r.calls++
	r.params = params
	_, r.hasDeadline = ctx.Deadline()
	return r.rows, r.err
}

func TestHandlerVerifiesThenStoresRawDelivery(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024, time.Second)
	body := []byte(`not-json-at-ingress`)
	request := signedRequest(t, body, "secret", "guid-1", "pull_request")
	request.Header.Set("X-Extra-Test-Header", "preserved")
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

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
	var envelope requestEnvelope
	if err := json.Unmarshal(store.params.Headers, &envelope); err != nil {
		t.Fatalf("decode stored request envelope: %v", err)
	}
	if envelope.Headers.Get("X-Extra-Test-Header") != "preserved" ||
		envelope.Host != request.Host ||
		envelope.ContentLength != int64(len(body)) ||
		len(envelope.TransferEncoding) != 0 {
		t.Fatalf("stored request envelope = %+v", envelope)
	}
	if !store.hasDeadline {
		t.Fatal("insert context has no request deadline")
	}
}

func TestHandlerRejectsUnverifiedWithoutStoring(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024, time.Second)
	request := signedRequest(
		t,
		[]byte(`{"action":"opened"}`),
		"wrong-secret",
		"guid-2",
		"pull_request",
	)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if store.calls != 0 {
		t.Fatalf("unverified delivery was stored %d times", store.calls)
	}
}

func TestHandlerAcknowledgesDuplicateGUID(t *testing.T) {
	store := &recordingInserter{rows: 0}
	handler := NewHandler(store, "secret", 1024, time.Second)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(
		response,
		signedRequest(t, []byte(`{}`), "secret", "duplicate", "push"),
	)

	if response.Code != http.StatusOK || store.calls != 1 {
		t.Fatalf("status=%d insert calls=%d", response.Code, store.calls)
	}
}

func TestHandlerRejectsOversizeBeforeStoring(t *testing.T) {
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 4, time.Second)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(
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
	handler := NewHandler(store, "secret", 1024, time.Second)
	request := signedRequest(t, []byte(`{}`), "secret", "", "push")
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || store.calls != 0 {
		t.Fatalf("status=%d insert calls=%d", response.Code, store.calls)
	}
}

func TestHandlerDoesNotAcknowledgeFailedCommit(t *testing.T) {
	store := &recordingInserter{err: errors.New("database unavailable")}
	handler := NewHandler(store, "secret", 1024, time.Second)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(
		response,
		signedRequest(t, []byte(`{}`), "secret", "guid-4", "push"),
	)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
}

type deadlineInserter struct{}

func (deadlineInserter) InsertWebhookDelivery(
	ctx context.Context,
	_ dbgen.InsertWebhookDeliveryParams,
) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestHandlerRequestDeadlineCoversInsert(t *testing.T) {
	handler := NewHandler(deadlineInserter{}, "secret", 1024, 10*time.Millisecond)
	response := httptest.NewRecorder()
	started := time.Now()

	NewMux(handler).ServeHTTP(
		response,
		signedRequest(t, []byte(`{}`), "secret", "guid-deadline", "push"),
	)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("request deadline took %s", elapsed)
	}
}

func TestHealth(t *testing.T) {
	handler := NewHandler(&recordingInserter{}, "secret", 1024, time.Second)
	request := httptest.NewRequest(http.MethodGet, HealthPath, nil)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

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
