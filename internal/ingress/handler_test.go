package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
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
	params dbgen.InsertWebhookDeliveryParams, //nolint:gocritic // test double implements the generated value-parameter interface
) (int64, error) {
	r.calls++
	r.params = params
	_, r.hasDeadline = ctx.Deadline()
	return r.rows, r.err
}

func TestHandlerVerifiesThenStoresRawDelivery(t *testing.T) {
	t.Parallel()
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024, time.Second)
	body := []byte(`{"raw":"preserved"}`)
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

func TestHandlerPersistsCurrentTraceContext(t *testing.T) {
	t.Parallel()
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		},
		SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		TraceFlags: trace.FlagsSampled,
		TraceState: trace.TraceState{},
	})
	ctx := trace.ContextWithSpanContext(t.Context(), spanContext)
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024, time.Second)
	request := signedRequest(t, []byte(`{}`), "secret", "traced", "push").
		WithContext(ctx)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", response.Code, response.Body.String())
	}
	const wantTraceparent = "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"
	if store.params.Traceparent != wantTraceparent {
		t.Fatalf(
			"stored traceparent = %q, want %q",
			store.params.Traceparent,
			wantTraceparent,
		)
	}
}

func TestHandlerRejectsUnverifiedWithoutStoring(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestHandlerAcceptsFormDeliveryAndPreservesWireBody(t *testing.T) {
	t.Parallel()
	store := &recordingInserter{rows: 1}
	handler := NewHandler(store, "secret", 1024, time.Second)
	formBody := []byte(url.Values{
		"payload": {`{"action":"opened"}`},
	}.Encode())
	request := signedRequest(
		t,
		formBody,
		"secret",
		"form-guid",
		"pull_request",
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if store.calls != 1 || !bytes.Equal(store.params.RawBody, formBody) {
		t.Fatalf(
			"insert calls = %d, raw body = %q, want exact form body %q",
			store.calls,
			store.params.RawBody,
			formBody,
		)
	}
}

func TestHandlerRejectsMalformedOrUnsupportedPayloadWithoutStoring(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name        string
		body        []byte
		contentType string
		wantStatus  int
	}{
		{
			name:        "truncated JSON",
			body:        []byte(`{"action":"opened"`),
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "non-object JSON",
			body:        []byte(`[]`),
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing form payload",
			body:        []byte(`other=value`),
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "unsupported content type",
			body:        []byte(`{"action":"opened"}`),
			contentType: "text/plain",
			wantStatus:  http.StatusUnsupportedMediaType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := &recordingInserter{rows: 1}
			handler := NewHandler(store, "secret", 1024, time.Second)
			request := signedRequest(
				t,
				test.body,
				"secret",
				"rejected-guid",
				"pull_request",
			)
			request.Header.Set("Content-Type", test.contentType)
			response := httptest.NewRecorder()

			NewMux(handler).ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %q",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if store.calls != 0 {
				t.Fatalf("rejected delivery was stored %d times", store.calls)
			}
		})
	}
}

func TestHandlerRejectsOversizeBeforeStoring(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	_ dbgen.InsertWebhookDeliveryParams, //nolint:gocritic // test double implements the generated value-parameter interface
) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestHandlerRequestDeadlineCoversInsert(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	handler := NewHandler(&recordingInserter{}, "secret", 1024, time.Second)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, HealthPath, http.NoBody)
	response := httptest.NewRecorder()

	NewMux(handler).ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
}

func TestNewHandlerRejectsEmptyWebhookSecret(t *testing.T) {
	t.Parallel()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("NewHandler accepted an empty webhook secret")
		}
	}()
	NewHandler(&recordingInserter{}, "", 1024, time.Second)
}

func signedRequest(
	t *testing.T,
	body []byte,
	secret string,
	guid string,
	event string,
) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, WebhookPath, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Hub-Signature-256", gh.SignBody([]byte(secret), body))
	if guid != "" {
		request.Header.Set("X-GitHub-Delivery", guid)
	}
	if event != "" {
		request.Header.Set("X-GitHub-Event", event)
	}
	return request
}
