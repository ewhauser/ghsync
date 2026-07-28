package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/acme/frontier/internal/budget"
)

type GraphQLClient struct {
	client client
}

func NewGraphQLClient(
	baseURL string,
	gate budget.Doer,
	tokens TokenProvider,
) (*GraphQLClient, error) {
	common, err := newClient(baseURL, gate, tokens)
	if err != nil {
		return nil, err
	}
	return &GraphQLClient{client: common}, nil
}

type GraphQLError struct {
	Type       string         `json:"type"`
	Message    string         `json:"message"`
	Path       []any          `json:"path"`
	Extensions map[string]any `json:"extensions"`
}

type GraphQLErrors []GraphQLError

func (e GraphQLErrors) Error() string {
	if len(e) == 0 {
		return "GitHub GraphQL error"
	}
	return "GitHub GraphQL: " + e[0].Message
}

type GraphQLResponse struct {
	RateLimit budget.GraphQLRate
	Errors    []GraphQLError
}

// Call executes a query that includes a top-level data.rateLimit block.
// extractGraphQLRate reads that block for Gate before Call decodes data.
func (c *GraphQLClient) Call(
	ctx context.Context,
	class budget.Class,
	query string,
	variables map[string]any,
	target any,
) (*GraphQLResponse, error) {
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal GraphQL request: %w", err)
	}
	req, err := c.client.request(
		ctx,
		http.MethodPost,
		"graphql",
		nil,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	gated, err := c.client.gate.Do(
		ctx,
		class,
		budget.NewGraphQLRequest(req, extractGraphQLRate),
	)
	if err != nil {
		if gated != nil && gated.HTTP != nil {
			gated.HTTP.Body.Close()
		}
		return nil, err
	}
	resp := gated.HTTP
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, decodeHTTPError(resp)
	}
	defer resp.Body.Close()
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []GraphQLError  `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode GraphQL response: %w", err)
	}
	if gated.GraphQLRate == nil {
		return nil, fmt.Errorf("GraphQL response omitted data.rateLimit")
	}
	result := &GraphQLResponse{
		RateLimit: *gated.GraphQLRate,
		Errors:    envelope.Errors,
	}
	if target != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return result, fmt.Errorf("decode GraphQL data: %w", err)
		}
	}
	if len(envelope.Errors) > 0 {
		return result, GraphQLErrors(envelope.Errors)
	}
	return result, nil
}

func extractGraphQLRate(resp *http.Response) (budget.GraphQLRate, bool, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return budget.GraphQLRate{}, false, fmt.Errorf("read GraphQL rateLimit: %w", err)
	}
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	var envelope struct {
		Data struct {
			RateLimit *struct {
				Cost      int64     `json:"cost"`
				Limit     int64     `json:"limit"`
				Remaining int64     `json:"remaining"`
				ResetAt   time.Time `json:"resetAt"`
			} `json:"rateLimit"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return budget.GraphQLRate{}, false, nil
	}
	if envelope.Data.RateLimit == nil {
		return budget.GraphQLRate{}, false, nil
	}
	rate := budget.GraphQLRate{
		Cost:      envelope.Data.RateLimit.Cost,
		Limit:     envelope.Data.RateLimit.Limit,
		Remaining: envelope.Data.RateLimit.Remaining,
		ResetAt:   envelope.Data.RateLimit.ResetAt,
	}
	if rate.Limit <= 0 || rate.Remaining < 0 || rate.ResetAt.IsZero() {
		return budget.GraphQLRate{}, false, fmt.Errorf("invalid GraphQL rateLimit block")
	}
	return rate, true, nil
}
