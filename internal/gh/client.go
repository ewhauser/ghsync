package gh

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ewhauser/ghsync/internal/budget"
)

const (
	apiVersion               = "2022-11-28"
	maxHTTPErrorMessageBytes = 16 << 10
)

// TokenProvider supplies an installation token without exposing it outside
// the GitHub client layer.
type TokenProvider interface {
	Token(context.Context) (string, error)
}

// StaticToken is useful for fake-GitHub conformance tests.
type StaticToken string

// Token returns the static test token.
func (t StaticToken) Token(context.Context) (string, error) {
	return string(t), nil
}

type client struct {
	baseURL     *url.URL
	gate        budget.Doer
	tokens      TokenProvider
	authContext budget.AuthContext
}

func newClient(
	baseURL string,
	gate budget.Doer,
	tokens TokenProvider,
	authContext budget.AuthContext,
) (client, error) {
	if gate == nil {
		return client{}, fmt.Errorf("GitHub budget gate is required")
	}
	if tokens == nil {
		return client{}, fmt.Errorf("GitHub token provider is required")
	}
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/") + "/")
	if err != nil {
		return client{}, fmt.Errorf("parse GitHub base URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return client{}, fmt.Errorf("GitHub base URL must be absolute")
	}
	return client{
		baseURL:     parsed,
		gate:        gate,
		tokens:      tokens,
		authContext: authContext,
	}, nil
}

func (c client) request(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body io.Reader,
) (*http.Request, error) {
	relative := &url.URL{Path: strings.TrimPrefix(path, "/"), RawQuery: query.Encode()}
	endpoint := c.baseURL.ResolveReference(relative)
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	return req, nil
}

func (c client) authorize(ctx context.Context, req *http.Request) error {
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("GitHub installation token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

func (c client) restRequest(req *http.Request) *budget.Request {
	if c.authContext == budget.AppJWTAuth {
		return budget.NewAppRESTRequest(req)
	}
	return budget.NewInstallationRESTRequest(req)
}

// HTTPError is a non-success response from GitHub. Response bodies are
// bounded before being retained.
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("GitHub returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("GitHub returned HTTP %d: %s", e.StatusCode, e.Message)
}

func decodeHTTPError(resp *http.Response) error {
	if resp == nil {
		return &HTTPError{}
	}
	if resp.Body == nil {
		return &HTTPError{StatusCode: resp.StatusCode}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(
		io.LimitReader(resp.Body, maxHTTPErrorMessageBytes),
	)
	if err != nil {
		return fmt.Errorf("read GitHub error response: %w", err)
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		payload.Message = strings.TrimSpace(string(body))
	}
	return &HTTPError{StatusCode: resp.StatusCode, Message: payload.Message}
}

func closeResponseBody(resp *http.Response) error {
	if resp == nil || resp.Body == nil {
		return nil
	}
	return resp.Body.Close()
}
