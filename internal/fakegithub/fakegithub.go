// Package fakegithub is a scriptable stand-in for the GitHub API used by the
// sync engine's tests and local development (IMPLEMENTATION_PLAN M0). It
// serves the REST endpoints the engine consumes (including the gh-stack
// preview Stacks API), a minimal GraphQL endpoint, rate-limit headers, and
// emits HMAC-signed webhooks at a target URL.
package fakegithub

import (
	"bytes"
	"context"
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

type PullRequest struct {
	Number    int       `json:"number"`
	Title     string    `json:"title"`
	State     string    `json:"state"`
	HeadRef   string    `json:"head_ref"`
	HeadSHA   string    `json:"head_sha"`
	BaseRef   string    `json:"base_ref"`
	UpdatedAt time.Time `json:"updated_at"`
	Stack     *StackRef `json:"stack"`
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

// Server implements http.Handler for the API surface and can emit webhooks.
// All mutable state is guarded so tests can mutate the fixture mid-scenario.
type Server struct {
	mu            sync.Mutex
	fixture       Fixture
	webhookSecret []byte
	remaining     int64
	limit         int64
	deliverySeq   int
	mux           *http.ServeMux
	client        *http.Client
}

func New(fixture Fixture, webhookSecret string) *Server {
	s := &Server{
		fixture:       fixture,
		webhookSecret: []byte(webhookSecret),
		limit:         15000, // GHEC installation budget
		remaining:     15000,
		client:        &http.Client{Timeout: 10 * time.Second},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks", s.listStacks)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks/{number}", s.getStack)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls", s.listPulls)
	mux.HandleFunc("POST /graphql", s.graphql)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.remaining > 0 {
		s.remaining--
	}
	remaining := s.remaining
	limit := s.limit
	s.mu.Unlock()

	w.Header().Set("x-ratelimit-limit", strconv.FormatInt(limit, 10))
	w.Header().Set("x-ratelimit-remaining", strconv.FormatInt(remaining, 10))
	w.Header().Set("x-ratelimit-reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	s.mux.ServeHTTP(w, r)
}

// SetFixture swaps the served state; tests use this to script scenarios.
func (s *Server) SetFixture(fixture Fixture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fixture = fixture
}

// Remaining reports the simulated REST budget left.
func (s *Server) Remaining() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remaining
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
	s.mu.Lock()
	remaining := s.remaining
	s.mu.Unlock()
	writeJSON(w, map[string]any{
		"data": map[string]any{
			"rateLimit": map[string]any{
				"cost":      1,
				"remaining": remaining,
				"resetAt":   time.Now().Add(time.Hour).Format(time.RFC3339),
			},
		},
	})
}

// EmitWebhook signs and POSTs a webhook delivery to targetURL, returning the
// delivery GUID it generated. Non-2xx responses are errors, mirroring
// GitHub's delivery-failure semantics.
func (s *Server) EmitWebhook(ctx context.Context, targetURL, event string, payload any) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	s.mu.Lock()
	s.deliverySeq++
	guid := fmt.Sprintf("fake-delivery-%d", s.deliverySeq)
	s.mu.Unlock()

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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
