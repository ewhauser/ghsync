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
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/acme/frontier/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

type StackRef struct {
	ID       int64 `json:"id"`
	Number   int   `json:"number"`
	Size     int   `json:"size"`
	Position int   `json:"position"`
	Base     Base  `json:"base"`
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
	ID           int64              `json:"id"`
	Number       int                `json:"number"`
	NodeID       string             `json:"node_id"`
	URL          string             `json:"url"`
	Base         Base               `json:"base"`
	Open         bool               `json:"open"`
	CreatedAt    time.Time          `json:"created_at"`
	PullRequests []StackPullRequest `json:"pull_requests"` // bottom → top
}

type StackPullRequest struct {
	Number   int               `json:"number"`
	State    string            `json:"state"`
	Draft    bool              `json:"draft"`
	MergedAt *time.Time        `json:"merged_at"`
	Head     PullRequestBranch `json:"head"`
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

// RateLimitStep scripts one response's server-authoritative rate state.
// Secondary distinguishes GitHub's documented secondary-limit response shape
// from ordinary primary-budget exhaustion. RetryAfter may be zero to exercise
// GitHub's headerless secondary-limit fallback.
type RateLimitStep struct {
	Limit      int64
	Remaining  int64
	ResetAt    time.Time
	StatusCode int
	RetryAfter time.Duration
	Secondary  bool
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

func WithRESTRateSteps(steps ...RateLimitStep) Option {
	return func(s *Server) {
		s.restSteps = append([]RateLimitStep(nil), steps...)
	}
}

func WithGraphQLRateSteps(steps ...RateLimitStep) Option {
	return func(s *Server) {
		s.graphQLSteps = append([]RateLimitStep(nil), steps...)
	}
}

func WithResponseDelay(delay time.Duration) Option {
	if delay < 0 {
		panic("response delay cannot be negative")
	}
	return func(s *Server) {
		s.responseDelay = delay
	}
}

func WithInstallationTokenTTL(ttl time.Duration) Option {
	if ttl <= 0 {
		panic("installation token TTL must be positive")
	}
	return func(s *Server) {
		s.tokenTTL = ttl
	}
}

// WithAppAuthentication configures the public key and issuer expected by the
// installation-token endpoint.
func WithAppAuthentication(appID int64, publicKey *rsa.PublicKey) Option {
	if appID <= 0 || publicKey == nil {
		panic("App authentication requires a positive App ID and public key")
	}
	return func(s *Server) {
		s.appID = appID
		s.appPublicKey = publicKey
	}
}

// WithNow supplies deterministic server time for token-expiry tests.
func WithNow(now func() time.Time) Option {
	if now == nil {
		panic("now function is required")
	}
	return func(s *Server) {
		s.now = now
	}
}

// Server implements http.Handler for the API surface and can emit webhooks.
// All mutable state is guarded so tests can mutate the fixture mid-scenario.
type Server struct {
	mu             sync.Mutex
	fixture        Fixture
	webhookSecret  []byte
	restBudget     rateBudget
	graphQLBudget  rateBudget
	restSteps      []RateLimitStep
	graphQLSteps   []RateLimitStep
	restStep       int
	graphQLStep    int
	responseDelay  time.Duration
	active         int
	maxActive      int
	tokenTTL       time.Duration
	tokenRequests  int
	authorizations []string
	appID          int64
	appPublicKey   *rsa.PublicKey
	now            func() time.Time
	mux            *http.ServeMux
	client         *http.Client
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
		tokenTTL: time.Hour,
		now:      time.Now,
		client:   &http.Client{Timeout: 10 * time.Second},
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
	mux.HandleFunc("POST /app/installations/{id}/access_tokens", s.installationToken)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		s.mux.ServeHTTP(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/app/installations/") {
		s.mux.ServeHTTP(w, r)
		return
	}

	delay := s.beginRequest()
	defer s.endRequest()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
	}

	resource := "core"
	if r.URL.Path == "/graphql" {
		resource = "graphql"
	}
	s.mu.Lock()
	s.authorizations = append(s.authorizations, r.Header.Get("Authorization"))
	s.mu.Unlock()
	rate, scripted, allowed, status, retryAfter, secondary := s.nextRate(resource, 1)
	setRateHeaders(w.Header(), rate)
	if scripted {
		r = r.WithContext(context.WithValue(r.Context(), scriptedRateKey{}, true))
	}
	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		if retryAfter > 0 {
			w.Header().Set(
				"Retry-After",
				strconv.FormatInt(int64((retryAfter+time.Second-1)/time.Second), 10),
			)
		}
		if secondary || retryAfter > 0 {
			writeSecondaryRateLimitExceeded(w, status, resource, rate)
		} else if resource == "graphql" {
			writeGraphQLRateLimitExceeded(w, rate, status)
		} else {
			writeRESTRateLimitExceeded(w, rate, status)
		}
		return
	}
	if !allowed {
		if resource == "graphql" {
			writeGraphQLRateLimitExceeded(w, rate, http.StatusOK)
		} else {
			writeRESTRateLimitExceeded(w, rate, http.StatusForbidden)
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

func (s *Server) MaxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

func (s *Server) TokenRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenRequests
}

func (s *Server) Authorizations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.authorizations...)
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
	stacks := fx.Stacks
	if raw := r.URL.Query().Get("pull_request"); raw != "" {
		number, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "bad pull_request", http.StatusBadRequest)
			return
		}
		stacks = nil
		for _, stack := range fx.Stacks {
			for _, pull := range stack.PullRequests {
				if pull.Number == number {
					stacks = append(stacks, stack)
					break
				}
			}
		}
	}
	s.writeConditionalJSON(w, r, "core", stacks)
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
			s.writeConditionalJSON(w, r, "core", stack)
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
	s.writeConditionalJSON(w, r, "core", pulls)
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

func (s *Server) installationToken(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept") != "application/vnd.github+json" ||
		r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" ||
		!strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "invalid GitHub API headers", http.StatusBadRequest)
		return
	}
	const bearer = "Bearer "
	rawAuthorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(rawAuthorization, bearer) ||
		s.appPublicKey == nil || s.appID <= 0 {
		http.Error(w, "valid App JWT required", http.StatusUnauthorized)
		return
	}
	auth := strings.TrimPrefix(rawAuthorization, bearer)
	claims := &fakeAppClaims{}
	token, err := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
	).ParseWithClaims(auth, claims, func(token *jwt.Token) (any, error) {
		if token.Header["typ"] != "JWT" {
			return nil, fmt.Errorf("JWT typ header is required")
		}
		return s.appPublicKey, nil
	})
	now := s.now()
	expectedIssuer := strconv.FormatInt(s.appID, 10)
	if err != nil || !token.Valid ||
		claims.Issuer != expectedIssuer ||
		claims.IssuedAt == nil ||
		claims.ExpiresAt == nil ||
		claims.IssuedAt.Time.After(now) ||
		claims.IssuedAt.Time.Before(now.Add(-time.Minute)) ||
		!claims.ExpiresAt.Time.After(now) ||
		claims.ExpiresAt.Time.Sub(claims.IssuedAt.Time) > 10*time.Minute {
		http.Error(w, "valid App JWT required", http.StatusUnauthorized)
		return
	}
	installationID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || installationID <= 0 {
		http.Error(w, "bad installation ID", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.tokenRequests++
	requestNumber := s.tokenRequests
	ttl := s.tokenTTL
	s.mu.Unlock()
	writeJSONStatus(w, http.StatusCreated, map[string]any{
		"token": fmt.Sprintf(
			"fake-installation-%d-token-%d",
			installationID,
			requestNumber,
		),
		"expires_at": now.Add(ttl).UTC().Format(time.RFC3339Nano),
	})
}

type fakeAppClaims struct {
	jwt.RegisteredClaims
}

// Valid deliberately leaves time validation to installationToken's injected
// clock while jwt.Parser still verifies the signature and signing algorithm.
func (*fakeAppClaims) Valid() error {
	return nil
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

func (s *Server) nextRate(
	resource string,
	cost int64,
) (rateSnapshot, bool, bool, int, time.Duration, bool) {
	s.mu.Lock()
	var steps []RateLimitStep
	var index *int
	if resource == "graphql" {
		steps = s.graphQLSteps
		index = &s.graphQLStep
	} else {
		steps = s.restSteps
		index = &s.restStep
	}
	if *index < len(steps) {
		step := steps[*index]
		*index++
		budget := s.budget(resource)
		budget.resetIfExpired(time.Now())
		if step.Limit <= 0 {
			step.Limit = budget.limit
		}
		if step.ResetAt.IsZero() {
			step.ResetAt = budget.resetAt
		}
		budget.limit = step.Limit
		budget.remaining = step.Remaining
		budget.resetAt = step.ResetAt
		snapshot := budget.snapshot()
		s.mu.Unlock()
		return snapshot, true, true, step.StatusCode, step.RetryAfter, step.Secondary
	}
	s.mu.Unlock()
	snapshot, allowed := s.consume(resource, cost)
	return snapshot, false, allowed, 0, 0, false
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

func (s *Server) beginRequest() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	return s.responseDelay
}

func (s *Server) endRequest() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

type scriptedRateKey struct{}

func (s *Server) writeConditionalJSON(
	w http.ResponseWriter,
	r *http.Request,
	resource string,
	value any,
) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sum := sha256.Sum256(body)
	etag := fmt.Sprintf(`"%x"`, sum[:16])
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		if scripted, _ := r.Context().Value(scriptedRateKey{}).(bool); !scripted {
			rate := s.refund(resource, 1)
			setRateHeaders(w.Header(), rate)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	body = append(body, '\n')
	_, _ = w.Write(body)
}

func (s *Server) refund(resource string, cost int64) rateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.remaining = min(budget.remaining+cost, budget.limit)
	return budget.snapshot()
}

func setRateHeaders(header http.Header, budget rateSnapshot) {
	header.Set("X-RateLimit-Limit", strconv.FormatInt(budget.limit, 10))
	header.Set("X-RateLimit-Remaining", strconv.FormatInt(budget.remaining, 10))
	header.Set("X-RateLimit-Reset", strconv.FormatInt(budget.resetAt.Unix(), 10))
	header.Set("X-RateLimit-Resource", budget.resource)
	header.Set("X-RateLimit-Used", strconv.FormatInt(budget.limit-budget.remaining, 10))
}

func writeRESTRateLimitExceeded(
	w http.ResponseWriter,
	budget rateSnapshot,
	status int,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"message": "API rate limit exceeded for installation.",
		"documentation_url": "https://docs.github.com/rest/using-the-rest-api/" +
			"rate-limits-for-the-rest-api",
		"status": strconv.Itoa(status),
	}); err != nil {
		return
	}
}

func writeGraphQLRateLimitExceeded(w http.ResponseWriter, budget rateSnapshot, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
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

func writeSecondaryRateLimitExceeded(
	w http.ResponseWriter,
	status int,
	resource string,
	budget rateSnapshot,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if resource == "graphql" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"rateLimit": map[string]any{
					"cost":      1,
					"limit":     budget.limit,
					"remaining": budget.remaining,
					"resetAt":   budget.resetAt.Format(time.RFC3339),
				},
			},
			"errors": []map[string]any{{
				"type":    "RATE_LIMITED",
				"message": "You have exceeded a secondary rate limit.",
			}},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "You have exceeded a secondary rate limit. Please wait " +
			"a few minutes before you try again.",
		"documentation_url": "https://docs.github.com/rest/using-the-rest-api/" +
			"rate-limits-for-the-rest-api#about-secondary-rate-limits",
		"status": strconv.Itoa(status),
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
	writeJSONStatus(w, http.StatusOK, v)
}

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return
	}
}
