// Package fakegithub is a scriptable stand-in for the GitHub API used by the
// sync engine's tests and local development (IMPLEMENTATION_PLAN M0). It
// serves the REST endpoints the engine consumes (including the gh-stack
// preview Stacks API), a minimal GraphQL endpoint, rate-limit headers, and
// emits HMAC-signed webhooks at a target URL.
package fakegithub

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/acme/frontier/internal/gh"
)

type StackRef struct {
	ID       string `json:"id"`
	Number   int    `json:"number"`
	Size     int    `json:"size"`
	Position int    `json:"position"`
	Base     Base   `json:"base"`
}

type Base struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type PullRequestBranch struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type PullRequest struct {
	Number    int               `json:"number"`
	Title     string            `json:"title"`
	State     string            `json:"state"`
	Head      PullRequestBranch `json:"head"`
	Base      PullRequestBranch `json:"base"`
	UpdatedAt time.Time         `json:"updated_at"`
	Stack     *StackRef         `json:"stack"`
}

type Stack struct {
	ID           string `json:"id"`
	Number       int    `json:"number"`
	Base         Base   `json:"base"`
	Open         bool   `json:"open"`
	PullRequests []int  `json:"pull_requests"` // bottom → top, PR numbers
}

type Fixture struct {
	Owner        string
	Repo         string
	Stacks       []Stack
	PullRequests []PullRequest
}

type rateBudget struct {
	limit     int64
	remaining int64
	resetAt   time.Time
	resource  string
}

type rateSnapshot struct {
	limit     int64
	remaining int64
	resetAt   time.Time
	resource  string
}

// Option customizes the fake server for a scenario.
type Option func(*Server)

// WithRateLimits sets independent REST-request and GraphQL-point limits.
func WithRateLimits(rest, graphql int64) Option {
	if rest < 0 || graphql < 0 {
		panic("rate limits cannot be negative")
	}
	return func(s *Server) {
		s.restBudget.limit = rest
		s.restBudget.remaining = rest
		s.graphQLBudget.limit = graphql
		s.graphQLBudget.remaining = graphql
	}
}

// Server implements http.Handler for the API surface and can emit webhooks.
// All mutable state is guarded so tests can mutate the fixture mid-scenario.
type Server struct {
	mu            sync.Mutex
	fixture       Fixture
	webhookSecret []byte
	restBudget    rateBudget
	graphQLBudget rateBudget
	mux           *http.ServeMux
	client        *http.Client
}

func New(fixture Fixture, webhookSecret string, options ...Option) *Server {
	resetAt := nextRateReset(time.Now())
	s := &Server{
		fixture:       fixture,
		webhookSecret: []byte(webhookSecret),
		restBudget: rateBudget{
			limit:     15000, // GHEC installation REST budget
			remaining: 15000,
			resetAt:   resetAt,
			resource:  "core",
		},
		graphQLBudget: rateBudget{
			limit:     5000,
			remaining: 5000,
			resetAt:   resetAt,
			resource:  "graphql",
		},
		client: &http.Client{Timeout: 10 * time.Second},
	}
	for _, option := range options {
		option(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks", s.listStacks)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks/{number}", s.getStack)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls", s.listPulls)
	mux.HandleFunc("POST /graphql", s.graphql)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		s.mux.ServeHTTP(w, r)
		return
	}

	resource := "core"
	if r.URL.Path == "/graphql" {
		resource = "graphql"
	}
	budget, allowed := s.consume(resource, 1)
	setRateHeaders(w.Header(), budget)
	if !allowed {
		if resource == "graphql" {
			writeGraphQLRateLimitExceeded(w, budget)
		} else {
			writeRESTRateLimitExceeded(w, budget)
		}
		return
	}
	s.mux.ServeHTTP(w, r)
}

// SetFixture swaps the served state; tests use this to script scenarios.
func (s *Server) SetFixture(fixture Fixture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fixture = fixture
}

// Remaining reports the simulated REST request budget left.
func (s *Server) Remaining() int64 {
	return s.snapshot("core").remaining
}

// GraphQLRemaining reports the simulated GraphQL point budget left.
func (s *Server) GraphQLRemaining() int64 {
	return s.snapshot("graphql").remaining
}

func (s *Server) checkRepo(w http.ResponseWriter, r *http.Request) (Fixture, bool) {
	s.mu.Lock()
	fx := s.fixture
	s.mu.Unlock()
	if r.PathValue("owner") != fx.Owner || r.PathValue("repo") != fx.Repo {
		http.NotFound(w, r)
		return Fixture{}, false
	}
	return fx, true
}

func (s *Server) listStacks(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	writeJSON(w, fx.Stacks)
}

func (s *Server) getStack(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		http.Error(w, "bad stack number", http.StatusBadRequest)
		return
	}
	for _, stack := range fx.Stacks {
		if stack.Number == number {
			writeJSON(w, stack)
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) listPulls(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	state := r.URL.Query().Get("state")
	pulls := make([]PullRequest, 0, len(fx.PullRequests))
	for _, pull := range fx.PullRequests {
		if state == "" || state == "all" || pull.State == state {
			pulls = append(pulls, pull)
		}
	}
	writeJSON(w, pulls)
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	budget := s.snapshot("graphql")
	writeJSON(w, map[string]any{
		"data": map[string]any{
			"rateLimit": map[string]any{
				"cost":      1,
				"limit":     budget.limit,
				"remaining": budget.remaining,
				"resetAt":   budget.resetAt.Format(time.RFC3339),
				"used":      budget.limit - budget.remaining,
			},
		},
	})
}

// EmitWebhook signs and POSTs a webhook delivery to targetURL, returning the
// globally unique delivery GUID it generated. Non-2xx responses are errors, mirroring
// GitHub's delivery-failure semantics.
func (s *Server) EmitWebhook(ctx context.Context, targetURL, event string, payload any) (string, error) {
	return s.EmitWebhookWithGUID(ctx, targetURL, event, "", payload)
}

// EmitWebhookWithGUID emits a delivery with an explicit GUID. Tests use it to
// model GitHub retries and verify GUID deduplication; an empty GUID generates a
// new UUID just like EmitWebhook.
func (s *Server) EmitWebhookWithGUID(
	ctx context.Context,
	targetURL string,
	event string,
	guid string,
	payload any,
) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	if guid == "" {
		guid, err = newDeliveryGUID()
		if err != nil {
			return "", fmt.Errorf("generate delivery GUID: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", guid)
	req.Header.Set("X-Hub-Signature-256", gh.SignBody(s.webhookSecret, body))

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("webhook target returned %d", resp.StatusCode)
	}
	return guid, nil
}

func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) consume(resource string, cost int64) (rateSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.resetIfExpired(time.Now())
	if budget.remaining < cost {
		return budget.snapshot(), false
	}
	budget.remaining -= cost
	return budget.snapshot(), true
}

func (s *Server) snapshot(resource string) rateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.resetIfExpired(time.Now())
	return budget.snapshot()
}

func (s *Server) budget(resource string) *rateBudget {
	if resource == "graphql" {
		return &s.graphQLBudget
	}
	return &s.restBudget
}

func (b *rateBudget) resetIfExpired(now time.Time) {
	if now.Before(b.resetAt) {
		return
	}
	b.remaining = b.limit
	b.resetAt = nextRateReset(now)
}

func (b *rateBudget) snapshot() rateSnapshot {
	return rateSnapshot{
		limit:     b.limit,
		remaining: b.remaining,
		resetAt:   b.resetAt,
		resource:  b.resource,
	}
}

func nextRateReset(now time.Time) time.Time {
	return now.UTC().Truncate(time.Hour).Add(time.Hour)
}

func setRateHeaders(header http.Header, budget rateSnapshot) {
	header.Set("X-RateLimit-Limit", strconv.FormatInt(budget.limit, 10))
	header.Set("X-RateLimit-Remaining", strconv.FormatInt(budget.remaining, 10))
	header.Set("X-RateLimit-Reset", strconv.FormatInt(budget.resetAt.Unix(), 10))
	header.Set("X-RateLimit-Resource", budget.resource)
	header.Set("X-RateLimit-Used", strconv.FormatInt(budget.limit-budget.remaining, 10))
}

func writeRESTRateLimitExceeded(w http.ResponseWriter, budget rateSnapshot) {
	untilReset := time.Until(budget.resetAt)
	retryAfter := max(
		int64((untilReset+time.Second-1)/time.Second),
		1,
	)
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"message": "API rate limit exceeded for installation.",
		"documentation_url": "https://docs.github.com/rest/using-the-rest-api/" +
			"rate-limits-for-the-rest-api",
		"status": "403",
	}); err != nil {
		return
	}
}

func writeGraphQLRateLimitExceeded(w http.ResponseWriter, budget rateSnapshot) {
	writeJSON(w, map[string]any{
		"data": map[string]any{
			"rateLimit": map[string]any{
				"cost":      1,
				"limit":     budget.limit,
				"remaining": budget.remaining,
				"resetAt":   budget.resetAt.Format(time.RFC3339),
				"used":      budget.limit - budget.remaining,
			},
		},
		"errors": []map[string]any{
			{
				"type":    "RATE_LIMITED",
				"message": "API rate limit exceeded for this GraphQL resource.",
			},
		},
	})
}

func newDeliveryGUID() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		uuid[0:4],
		uuid[4:6],
		uuid[6:8],
		uuid[8:10],
		uuid[10:16],
	), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
