package gh

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ewhauser/ghsync/internal/budget"
)

func TestGetJSONRetainsRequestETagWhen304OmitsResponseETag(t *testing.T) {
	const validator = `"known-validator"`
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("If-None-Match"); got != validator {
				t.Errorf("If-None-Match = %q, want %q", got, validator)
			}
			w.WriteHeader(http.StatusNotModified)
		},
	))
	t.Cleanup(server.Close)
	gate := budget.New(server.Client(), budget.Options{})
	client, err := NewRESTClient(server.URL, gate, StaticToken("unused"))
	if err != nil {
		t.Fatal(err)
	}
	_, response, err := client.ListStacks(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		ListStacksOptions{},
		validator,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !response.NotModified || response.ETag != validator {
		t.Fatalf("304 metadata = %+v", response)
	}
}

func TestDecodeHTTPErrorBoundsRetainedMessage(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body: io.NopCloser(
			strings.NewReader(strings.Repeat("x", 2*maxHTTPErrorMessageBytes)),
		),
	}
	var httpErr *HTTPError
	if err := decodeHTTPError(response); !errors.As(err, &httpErr) {
		t.Fatalf("decodeHTTPError = %v", err)
	}
	if got := len(httpErr.Message); got > maxHTTPErrorMessageBytes {
		t.Fatalf(
			"retained HTTP error message = %d bytes, max %d",
			got,
			maxHTTPErrorMessageBytes,
		)
	}
}

func TestCloseResponseBodyAllowsBodylessResponses(t *testing.T) {
	for _, response := range []*http.Response{
		nil,
		{},
		{Body: http.NoBody},
	} {
		if err := closeResponseBody(response); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientsHandleBodylessGateErrors(t *testing.T) {
	gateErr := errors.New("gate observation failed")
	gate := bodylessErrorGate{err: gateErr}
	rest, err := NewRESTClient("http://github.test", gate, StaticToken("unused"))
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := NewGraphQLClient(
		"http://github.test",
		gate,
		StaticToken("unused"),
	)
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := NewDeliveriesClient(
		"http://github.test",
		gate,
		StaticToken("unused"),
	)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := NewInstallationTokens(gate, InstallationTokenOptions{
		BaseURL:        "http://github.test",
		AppID:          99,
		InstallationID: 1234,
		PrivateKey:     privateKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	calls := []func() error{
		func() error {
			_, _, callErr := rest.ListStacks(
				context.Background(),
				budget.Interactive,
				"acme",
				"monolith",
				ListStacksOptions{},
				"",
			)
			return callErr
		},
		func() error {
			_, callErr := graphQL.Call(
				context.Background(),
				budget.Interactive,
				`query { rateLimit { cost limit remaining resetAt } }`,
				nil,
				nil,
			)
			return callErr
		},
		func() error {
			return deliveries.RedeliverAppHookDelivery(
				context.Background(),
				1,
			)
		},
		func() error {
			_, callErr := tokens.Token(context.Background())
			return callErr
		},
	}
	for index, call := range calls {
		if err := call(); !errors.Is(err, gateErr) {
			t.Fatalf("bodyless error call %d = %v", index, err)
		}
	}
}

type bodylessErrorGate struct {
	err error
}

func (g bodylessErrorGate) Do(
	context.Context,
	budget.Class,
	*budget.Request,
) (*budget.Response, error) {
	return &budget.Response{
		HTTP: &http.Response{StatusCode: http.StatusForbidden},
	}, g.err
}
