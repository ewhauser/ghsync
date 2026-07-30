package gh

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/url"
)

const (
	// WebhookJSONContentType identifies GitHub's direct JSON delivery mode.
	WebhookJSONContentType = "application/json"
	// WebhookFormContentType identifies GitHub's form-wrapped JSON mode.
	WebhookFormContentType = "application/x-www-form-urlencoded"
)

// ErrUnsupportedWebhookContentType marks content types GitHub does not use.
var ErrUnsupportedWebhookContentType = errors.New(
	"unsupported webhook content type",
)

// DecodeWebhookPayload validates a GitHub webhook body and returns its JSON
// payload. Form deliveries keep their encoded wire body in durable storage;
// callers use the decoded bytes only for classification.
func DecodeWebhookPayload(contentType string, body []byte) ([]byte, error) {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %q: %w",
			ErrUnsupportedWebhookContentType,
			contentType,
			err,
		)
	}

	payload := body
	switch mediaType {
	case WebhookJSONContentType:
	case WebhookFormContentType:
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return nil, fmt.Errorf("decode form webhook body: %w", err)
		}
		payloads := values["payload"]
		if len(payloads) != 1 {
			return nil, fmt.Errorf(
				"form webhook body has %d payload fields, want exactly one",
				len(payloads),
			)
		}
		payload = []byte(payloads[0])
	default:
		return nil, fmt.Errorf(
			"%w: %q",
			ErrUnsupportedWebhookContentType,
			mediaType,
		)
	}

	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return nil, fmt.Errorf("webhook payload must be a valid JSON object")
	}
	return payload, nil
}
