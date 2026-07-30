package gh

import (
	"bytes"
	"errors"
	"net/url"
	"testing"
)

func TestDecodeWebhookPayload(t *testing.T) {
	t.Parallel()
	jsonBody := []byte(`{"action":"opened","message":"a+b & c"}`)
	formBody := []byte(url.Values{
		"payload": {string(jsonBody)},
	}.Encode())

	tests := []struct {
		name        string
		contentType string
		body        []byte
		want        []byte
		wantErr     bool
		unsupported bool
	}{
		{
			name:        "json",
			contentType: "application/json; charset=utf-8",
			body:        jsonBody,
			want:        jsonBody,
		},
		{
			name:        "form",
			contentType: WebhookFormContentType,
			body:        formBody,
			want:        jsonBody,
		},
		{
			name:        "truncated json",
			contentType: WebhookJSONContentType,
			body:        []byte(`{"action":"opened"`),
			wantErr:     true,
		},
		{
			name:        "non-object json",
			contentType: WebhookJSONContentType,
			body:        []byte(`[]`),
			wantErr:     true,
		},
		{
			name:        "missing form payload",
			contentType: WebhookFormContentType,
			body:        []byte(`other=value`),
			wantErr:     true,
		},
		{
			name:        "duplicate form payload",
			contentType: WebhookFormContentType,
			body:        []byte(`payload=%7B%7D&payload=%7B%7D`),
			wantErr:     true,
		},
		{
			name:        "malformed form encoding",
			contentType: WebhookFormContentType,
			body:        []byte(`payload=%zz`),
			wantErr:     true,
		},
		{
			name:        "unsupported content type",
			contentType: "text/plain",
			body:        jsonBody,
			wantErr:     true,
			unsupported: true,
		},
		{
			name:        "malformed content type",
			contentType: "application/json; charset",
			body:        jsonBody,
			wantErr:     true,
			unsupported: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := DecodeWebhookPayload(test.contentType, test.body)
			if (err != nil) != test.wantErr {
				t.Fatalf("DecodeWebhookPayload() error = %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf(
					"DecodeWebhookPayload() = %q, want %q",
					got,
					test.want,
				)
			}
			if errors.Is(err, ErrUnsupportedWebhookContentType) !=
				test.unsupported {
				t.Fatalf(
					"unsupported error = %v, want %v; err = %v",
					errors.Is(err, ErrUnsupportedWebhookContentType),
					test.unsupported,
					err,
				)
			}
		})
	}
}
