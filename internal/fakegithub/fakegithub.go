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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/acme/frontier/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

// ControlEmitPath is a development-only control surface used by cmd/soak to
// ask the standalone fake to record and emit a signed delivery.
const (
	ControlEmitPath  = "/_frontier/emit"
	ControlTruthPath = "/_frontier/truth"
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
	ID             int64             `json:"id"`
	NodeID         string            `json:"node_id"`
	Number         int               `json:"number"`
	Title          string            `json:"title"`
	State          string            `json:"state"`
	Draft          bool              `json:"draft"`
	AuthorLogin    string            `json:"-"`
	ReviewDecision string            `json:"review_decision"`
	MergeableState string            `json:"mergeable_state"`
	Head           PullRequestBranch `json:"head"`
	Base           PullRequestBranch `json:"base"`
	UpdatedAt      time.Time         `json:"updated_at"`
	CreatedAt      time.Time         `json:"-"`
	Stack          *StackRef         `json:"stack"`
	ReviewThreads  []ReviewThread    `json:"-"`
}

type Stack struct {
	ID           int64              `json:"id"`
	Number       int                `json:"number"`
	NodeID       string             `json:"node_id"`
	URL          string             `json:"url"`
	Base         Base               `json:"base"`
	Open         bool               `json:"open"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
	PullRequests []StackPullRequest `json:"pull_requests"` // bottom → top
}

type StackPullRequest struct {
	Number    int               `json:"number"`
	State     string            `json:"state"`
	Draft     bool              `json:"draft"`
	MergedAt  *time.Time        `json:"merged_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Head      PullRequestBranch `json:"head"`
}

type Repository struct {
	ID               int64     `json:"id"`
	NodeID           string    `json:"node_id"`
	Owner            string    `json:"-"`
	Name             string    `json:"name"`
	FullName         string    `json:"full_name"`
	DefaultBranch    string    `json:"default_branch"`
	DefaultBranchSHA string    `json:"-"`
	Archived         bool      `json:"archived"`
	UpdatedAt        time.Time `json:"updated_at"`
	PushedAt         time.Time `json:"pushed_at"`
}

func (r Repository) MarshalJSON() ([]byte, error) {
	type wireRepository Repository
	return json.Marshal(struct {
		wireRepository
		Owner map[string]string `json:"owner"`
	}{
		wireRepository: wireRepository(r),
		Owner:          map[string]string{"login": r.Owner},
	})
}

type ReviewThread struct {
	ID         string
	IsResolved bool
	IsOutdated bool
	Path       string
	Line       *int
	Comments   []ReviewComment
}

type ReviewComment struct {
	ID          string
	Body        string
	UpdatedAt   time.Time
	AuthorLogin string
}

type CheckRun struct {
	ID          int64      `json:"id"`
	NodeID      string     `json:"node_id"`
	HeadSHA     string     `json:"head_sha"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion"`
	DetailsURL  string     `json:"details_url"`
	AppSlug     string     `json:"-"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

type RepositoryRule struct {
	ID          int64            `json:"id"`
	Name        string           `json:"name"`
	Target      string           `json:"target"`
	Enforcement string           `json:"enforcement"`
	UpdatedAt   *time.Time       `json:"updated_at,omitempty"`
	Rules       []map[string]any `json:"rules"`
}

// HookDelivery is the fake's public /app/hook/deliveries representation.
type HookDelivery struct {
	ID             int64     `json:"id"`
	GUID           string    `json:"guid"`
	DeliveredAt    time.Time `json:"delivered_at"`
	Redelivery     bool      `json:"redelivery"`
	Status         string    `json:"status"`
	StatusCode     int       `json:"status_code"`
	Event          string    `json:"event"`
	Action         string    `json:"action"`
	InstallationID int64     `json:"installation_id"`
	RepositoryID   int64     `json:"repository_id"`
}

type storedHookDelivery struct {
	HookDelivery
	targetURL string
	body      []byte
}

func (c CheckRun) MarshalJSON() ([]byte, error) {
	type wireCheckRun CheckRun
	return json.Marshal(struct {
		wireCheckRun
		App map[string]string `json:"app"`
	}{
		wireCheckRun: wireCheckRun(c),
		App:          map[string]string{"slug": c.AppSlug},
	})
}

type Fixture struct {
	Owner        string
	Repo         string
	Repository   Repository
	Repositories []Repository
	RepoRules    []RepositoryRule
	Stacks       []Stack
	PullRequests []PullRequest
	CheckRuns    []CheckRun
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
	Cost       int64
	StatusCode int
	RetryAfter time.Duration
	Secondary  bool
}

// Option customizes the fake server for a scenario.
type Option func(*Server)

func WithFixture(fixture Fixture) Option {
	return func(s *Server) {
		s.fixture = cloneFixture(fixture)
	}
}

// WithRequestHook mutates fixture truth at a deterministic request boundary.
// Tests use it to expose mutable-pagination races without timing sleeps.
func WithRequestHook(
	hook func(method string, path string, count int, fixture *Fixture),
) Option {
	if hook == nil {
		panic("request hook is required")
	}
	return func(s *Server) {
		s.requestHook = hook
	}
}

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
	tokenMaxActive int
	tokenTTL       time.Duration
	tokenRequests  int
	authorizations []string
	appID          int64
	appPublicKey   *rsa.PublicKey
	now            func() time.Time
	mux            *http.ServeMux
	client         *http.Client
	requestCounts  map[string]int
	notModified    map[string]int
	notFound       map[string]int
	notFoundAt     map[string]map[int]struct{}
	requestHook    func(string, string, int, *Fixture)
	deliveries     []storedHookDelivery
	nextDeliveryID int64
	redeliveries   []int64
	soakTruth      map[int]string
}

func New(fixture Fixture, webhookSecret string, options ...Option) *Server {
	resetAt := nextRateReset(time.Now())
	s := &Server{
		fixture:       cloneFixture(fixture),
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
		tokenTTL:       time.Hour,
		now:            time.Now,
		client:         &http.Client{Timeout: 10 * time.Second},
		requestCounts:  make(map[string]int),
		notModified:    make(map[string]int),
		notFound:       make(map[string]int),
		notFoundAt:     make(map[string]map[int]struct{}),
		nextDeliveryID: 1,
		soakTruth:      make(map[int]string),
	}
	for _, option := range options {
		option(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("POST "+ControlEmitPath, s.controlEmit)
	mux.HandleFunc("GET "+ControlTruthPath, s.controlTruth)
	mux.HandleFunc("GET /repos/{owner}/{repo}", s.getRepository)
	mux.HandleFunc("GET /installation/repositories", s.listInstallationRepositories)
	mux.HandleFunc("GET /repos/{owner}/{repo}/rulesets", s.listRepositoryRules)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks", s.listStacks)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks/{number}", s.getStack)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls", s.listPulls)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls/{number}", s.getPull)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/commits/{sha}/check-runs",
		s.listCheckRuns,
	)
	mux.HandleFunc("POST /graphql", s.graphql)
	mux.HandleFunc("POST /app/installations/{id}/access_tokens", s.installationToken)
	mux.HandleFunc("GET /app/hook/deliveries", s.listAppHookDeliveries)
	mux.HandleFunc(
		"POST /app/hook/deliveries/{id}/attempts",
		s.redeliverAppHookDelivery,
	)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestKey := r.Method + " " + r.URL.Path
	s.mu.Lock()
	s.requestCounts[requestKey]++
	requestCount := s.requestCounts[requestKey]
	if s.requestHook != nil {
		s.requestHook(r.Method, r.URL.Path, requestCount, &s.fixture)
	}
	_, scriptedAt := s.notFoundAt[requestKey][requestCount]
	if scriptedAt {
		delete(s.notFoundAt[requestKey], requestCount)
	}
	if s.notFound[requestKey] > 0 || scriptedAt {
		if s.notFound[requestKey] > 0 {
			s.notFound[requestKey]--
		}
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	s.mu.Unlock()

	if r.URL.Path == "/healthz" || strings.HasPrefix(r.URL.Path, "/_frontier/") {
		s.mux.ServeHTTP(w, r)
		return
	}
	delay := s.beginRequest(r.URL.Path)
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

	// Installation-token exchanges use GitHub's separate App rate bucket, but
	// they still share the fake's request-concurrency accounting.
	if strings.HasPrefix(r.URL.Path, "/app/installations/") {
		s.mux.ServeHTTP(w, r)
		return
	}

	resource := "core"
	if r.URL.Path == "/graphql" {
		resource = "graphql"
	}
	if strings.HasPrefix(r.URL.Path, "/app/hook/deliveries") {
		if !s.validateAppAuthorization(w, r) {
			return
		}
	} else if !validateInstallationAuthorization(w, r) {
		return
	}
	s.mu.Lock()
	s.authorizations = append(s.authorizations, r.Header.Get("Authorization"))
	s.mu.Unlock()
	rate := s.nextRate(resource, 1)
	setRateHeaders(w.Header(), rate.snapshot)
	if rate.scripted {
		r = r.WithContext(context.WithValue(r.Context(), scriptedRateKey{}, true))
	}
	r = r.WithContext(context.WithValue(r.Context(), rateCostKey{}, rate.cost))
	if rate.status == http.StatusForbidden ||
		rate.status == http.StatusTooManyRequests {
		if rate.retryAfter > 0 {
			w.Header().Set(
				"Retry-After",
				strconv.FormatInt(
					int64((rate.retryAfter+time.Second-1)/time.Second),
					10,
				),
			)
		}
		if rate.secondary || rate.retryAfter > 0 {
			writeSecondaryRateLimitExceeded(
				w,
				rate.status,
				resource,
				rate.snapshot,
				rate.cost,
			)
		} else if resource == "graphql" {
			writeGraphQLRateLimitExceeded(
				w,
				rate.snapshot,
				rate.status,
				rate.cost,
			)
		} else {
			writeRESTRateLimitExceeded(w, rate.snapshot, rate.status)
		}
		return
	}
	if !rate.allowed {
		if resource == "graphql" {
			writeGraphQLRateLimitExceeded(
				w,
				rate.snapshot,
				http.StatusOK,
				rate.cost,
			)
		} else {
			writeRESTRateLimitExceeded(
				w,
				rate.snapshot,
				http.StatusForbidden,
			)
		}
		return
	}
	s.mux.ServeHTTP(w, r)
}

type controlEmitRequest struct {
	TargetURL string          `json:"target_url"`
	Event     string          `json:"event"`
	GUID      string          `json:"guid,omitempty"`
	Mutate    bool            `json:"mutate,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func (s *Server) controlEmit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request controlEmitRequest
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "decode emit request", http.StatusBadRequest)
		return
	}
	target, err := url.Parse(request.TargetURL)
	if err != nil || target.Scheme != "http" || !loopbackHost(target.Hostname()) {
		http.Error(
			w,
			"target_url must be an http loopback address",
			http.StatusBadRequest,
		)
		return
	}
	if request.Event == "" || len(request.Payload) == 0 {
		http.Error(w, "event and payload are required", http.StatusBadRequest)
		return
	}
	if request.Mutate {
		s.applySoakMutation(request.Event, request.Payload)
	}
	guid, err := s.EmitWebhookWithGUID(
		r.Context(),
		request.TargetURL,
		request.Event,
		request.GUID,
		request.Payload,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"guid": guid})
}

type SoakTruthPullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
}

type SoakTruth struct {
	Repository   string                 `json:"repository"`
	PullRequests []SoakTruthPullRequest `json:"pull_requests"`
}

func (s *Server) controlTruth(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	truth := SoakTruth{
		Repository:   s.fixture.Owner + "/" + s.fixture.Repo,
		PullRequests: make([]SoakTruthPullRequest, 0, len(s.soakTruth)),
	}
	for number, title := range s.soakTruth {
		truth.PullRequests = append(
			truth.PullRequests,
			SoakTruthPullRequest{Number: number, Title: title},
		)
	}
	s.mu.Unlock()
	sort.Slice(truth.PullRequests, func(i, j int) bool {
		return truth.PullRequests[i].Number < truth.PullRequests[j].Number
	})
	writeJSON(w, truth)
}

func (s *Server) applySoakMutation(event string, payload json.RawMessage) {
	if event != "pull_request" {
		return
	}
	var envelope struct {
		Number       int   `json:"number"`
		SoakRevision int64 `json:"soak_revision"`
	}
	if json.Unmarshal(payload, &envelope) != nil || envelope.Number <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for index := range s.fixture.PullRequests {
		pull := &s.fixture.PullRequests[index]
		if pull.Number != envelope.Number {
			continue
		}
		pull.Title = fmt.Sprintf(
			"Soak revision %d for PR %d",
			envelope.SoakRevision,
			envelope.Number,
		)
		s.soakTruth[pull.Number] = pull.Title
		pull.UpdatedAt = s.now().UTC()
		for stackIndex := range s.fixture.Stacks {
			for prIndex := range s.fixture.Stacks[stackIndex].PullRequests {
				stackPull := &s.fixture.Stacks[stackIndex].PullRequests[prIndex]
				if stackPull.Number == envelope.Number {
					stackPull.UpdatedAt = pull.UpdatedAt
				}
			}
		}
		return
	}
}

func loopbackHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

// ScriptNotFound makes the next count exact method/path requests return 404.
// It is used to exercise C-C4 without mutating unrelated fixture entities.
func (s *Server) ScriptNotFound(method, path string, count int) {
	if count < 0 {
		panic("not-found count cannot be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notFound[method+" "+path] = count
}

// ScriptNotFoundOnRequest makes one absolute request ordinal for an exact
// method/path return 404. It allows pagination tests to fail page N without
// failing the entry request for the listing resource.
func (s *Server) ScriptNotFoundOnRequest(
	method string,
	path string,
	requestNumber int,
) {
	if requestNumber <= 0 {
		panic("not-found request number must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := method + " " + path
	if s.notFoundAt[key] == nil {
		s.notFoundAt[key] = make(map[int]struct{})
	}
	s.notFoundAt[key][requestNumber] = struct{}{}
}

func (s *Server) RequestCount(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestCounts[method+" "+path]
}

// NotModifiedCount reports conditional requests satisfied with a 304.
func (s *Server) NotModifiedCount(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notModified[method+" "+path]
}

// SetFixture swaps the served state; tests use this to script scenarios.
func (s *Server) SetFixture(fixture Fixture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fixture = cloneFixture(fixture)
}

// RedeliveryRequests reports the delivery IDs requested through the fake
// redelivery endpoint.
func (s *Server) RedeliveryRequests() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.redeliveries...)
}

// Deliveries returns the recorded delivery-list skeletons newest first.
func (s *Server) Deliveries() []HookDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]HookDelivery, 0, len(s.deliveries))
	for index := len(s.deliveries) - 1; index >= 0; index-- {
		result = append(result, s.deliveries[index].HookDelivery)
	}
	return result
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

// Concurrent reports requests currently inside the fake's concurrency
// accounting. Tests use it only to establish deterministic burst boundaries.
func (s *Server) Concurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

// TokenMaxConcurrent reports the highest active-request count observed while
// an installation-token exchange was active.
func (s *Server) TokenMaxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenMaxActive
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
	fx := cloneFixture(s.fixture)
	s.mu.Unlock()
	if r.PathValue("owner") != fx.Owner || r.PathValue("repo") != fx.Repo {
		http.NotFound(w, r)
		return Fixture{}, false
	}
	return fx, true
}

func (s *Server) getRepository(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	s.writeConditionalJSON(w, r, "core", fx.Repository)
}

func (s *Server) listInstallationRepositories(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.mu.Lock()
	fx := cloneFixture(s.fixture)
	repositories := fx.Repositories
	if repositories == nil {
		repositories = []Repository{fx.Repository}
	}
	s.mu.Unlock()
	sort.SliceStable(repositories, func(i, j int) bool {
		return repositories[i].ID < repositories[j].ID
	})
	repositories = paginate(repositories, r, w)
	s.writeConditionalJSON(w, r, "core", map[string]any{
		"total_count":  len(repositories),
		"repositories": repositories,
	})
}

func (s *Server) listRepositoryRules(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	rules := append([]RepositoryRule(nil), fx.RepoRules...)
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})
	s.writeConditionalJSON(w, r, "core", rules)
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
	stacks = paginate(stacks, r, w)
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
	sortBy := r.URL.Query().Get("sort")
	if sortBy != "" {
		if sortBy != "updated" && sortBy != "created" {
			http.Error(w, "bad sort", http.StatusBadRequest)
			return
		}
		sort.SliceStable(pulls, func(i, j int) bool {
			if sortBy == "updated" {
				if pulls[i].UpdatedAt.Equal(pulls[j].UpdatedAt) {
					return pulls[i].Number < pulls[j].Number
				}
				return pulls[i].UpdatedAt.Before(pulls[j].UpdatedAt)
			}
			if pulls[i].CreatedAt.Equal(pulls[j].CreatedAt) {
				return pulls[i].Number < pulls[j].Number
			}
			return pulls[i].CreatedAt.Before(pulls[j].CreatedAt)
		})
		direction := r.URL.Query().Get("direction")
		if direction == "" {
			direction = "desc"
		}
		if direction == "desc" {
			slicesReverse(pulls)
		} else if direction != "asc" {
			http.Error(w, "bad direction", http.StatusBadRequest)
			return
		}
	}
	pulls = paginate(pulls, r, w)
	s.writeConditionalJSON(w, r, "core", pulls)
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Query     string                     `json:"query"`
		Variables map[string]json.RawMessage `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil &&
		!errors.Is(err, io.EOF) {
		http.Error(w, "invalid GraphQL request", http.StatusBadRequest)
		return
	}
	budget := s.snapshot("graphql")
	cost, _ := r.Context().Value(rateCostKey{}).(int64)
	if cost <= 0 {
		cost = 1
	}
	data := map[string]any{
		"rateLimit": map[string]any{
			"cost":      cost,
			"limit":     budget.limit,
			"remaining": budget.remaining,
			"resetAt":   budget.resetAt.Format(time.RFC3339),
			"used":      budget.limit - budget.remaining,
		},
	}
	s.mu.Lock()
	fx := cloneFixture(s.fixture)
	s.mu.Unlock()
	var ids []string
	if raw := request.Variables["ids"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &ids)
	}
	if len(ids) > 0 {
		nodes := make([]any, 0, len(ids))
		for _, id := range ids {
			var node any
			for _, pull := range fx.PullRequests {
				if pull.NodeID == id {
					node = graphQLPullRequest(fx.Repository, pull)
					break
				}
			}
			nodes = append(nodes, node)
		}
		data["nodes"] = nodes
	} else if strings.Contains(
		request.Query,
		"FrontierPullRequestReviewThreadsPage",
	) {
		id, after := graphQLCursorVariables(request.Variables)
		for _, pull := range fx.PullRequests {
			if pull.NodeID == id {
				data["node"] = map[string]any{
					"reviewThreads": graphQLReviewThreads(
						pull.ReviewThreads,
						after,
					),
				}
				break
			}
		}
	} else if strings.Contains(
		request.Query,
		"FrontierReviewThreadCommentsPage",
	) {
		id, after := graphQLCursorVariables(request.Variables)
		for _, pull := range fx.PullRequests {
			for _, thread := range pull.ReviewThreads {
				if thread.ID == id {
					data["node"] = map[string]any{
						"comments": graphQLReviewComments(
							thread.Comments,
							after,
						),
					}
					break
				}
			}
		}
	}
	writeJSON(w, map[string]any{
		"data": data,
	})
}

func (s *Server) getPull(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		http.Error(w, "bad pull request number", http.StatusBadRequest)
		return
	}
	for _, pull := range fx.PullRequests {
		if pull.Number == number {
			// GitHub's REST pull shape carries the author under user.login.
			// Keep AuthorLogin out of the fixture's serialized list golden,
			// but include the real field on authoritative detail fetches.
			s.writeConditionalJSON(w, r, "core", struct {
				PullRequest
				User map[string]string `json:"user"`
			}{
				PullRequest: pull,
				User: map[string]string{
					"login": pull.AuthorLogin,
				},
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) listCheckRuns(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	headSHA := r.PathValue("sha")
	runs := make([]CheckRun, 0)
	for _, run := range fx.CheckRuns {
		if run.HeadSHA == headSHA {
			runs = append(runs, run)
		}
	}
	runs = paginate(runs, r, w)
	s.writeConditionalJSON(w, r, "core", map[string]any{
		"total_count": len(runs),
		"check_runs":  runs,
	})
}

func graphQLPullRequest(repository Repository, pull PullRequest) map[string]any {
	return map[string]any{
		"id":             pull.NodeID,
		"databaseId":     pull.ID,
		"number":         pull.Number,
		"title":          pull.Title,
		"state":          strings.ToUpper(pull.State),
		"isDraft":        pull.Draft,
		"updatedAt":      pull.UpdatedAt.Format(time.RFC3339Nano),
		"reviewDecision": pull.ReviewDecision,
		"mergeable":      strings.ToUpper(pull.MergeableState),
		"headRefName":    pull.Head.Ref,
		"headRefOid":     pull.Head.SHA,
		"baseRefName":    pull.Base.Ref,
		"baseRefOid":     pull.Base.SHA,
		"author":         map[string]any{"login": pull.AuthorLogin},
		"repository": map[string]any{
			"id":            repository.NodeID,
			"databaseId":    repository.ID,
			"name":          repository.Name,
			"nameWithOwner": repository.FullName,
			"isArchived":    repository.Archived,
			"updatedAt":     repository.UpdatedAt.Format(time.RFC3339Nano),
			"owner":         map[string]any{"login": repository.Owner},
			"defaultBranchRef": map[string]any{
				"name": repository.DefaultBranch,
				"target": map[string]any{
					"oid": repository.DefaultBranchSHA,
				},
			},
		},
		"reviewThreads": graphQLReviewThreads(pull.ReviewThreads, 0),
	}
}

const fakeGraphQLConnectionLimit = 100

func graphQLReviewThreads(
	threads []ReviewThread,
	after int,
) map[string]any {
	start, end, pageInfo := graphQLPage(len(threads), after)
	nodes := make([]map[string]any, 0, end-start)
	for _, thread := range threads[start:end] {
		nodes = append(nodes, map[string]any{
			"id":         thread.ID,
			"isResolved": thread.IsResolved,
			"isOutdated": thread.IsOutdated,
			"path":       thread.Path,
			"line":       thread.Line,
			"comments":   graphQLReviewComments(thread.Comments, 0),
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLReviewComments(
	comments []ReviewComment,
	after int,
) map[string]any {
	start, end, pageInfo := graphQLPage(len(comments), after)
	nodes := make([]map[string]any, 0, end-start)
	for _, comment := range comments[start:end] {
		author := any(nil)
		if comment.AuthorLogin != "" {
			author = map[string]any{"login": comment.AuthorLogin}
		}
		nodes = append(nodes, map[string]any{
			"id":        comment.ID,
			"body":      comment.Body,
			"updatedAt": comment.UpdatedAt.Format(time.RFC3339Nano),
			"author":    author,
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLPage(length, after int) (int, int, map[string]any) {
	start := min(max(after, 0), length)
	end := min(start+fakeGraphQLConnectionLimit, length)
	var endCursor any
	if end > 0 {
		endCursor = strconv.Itoa(end)
	}
	return start, end, map[string]any{
		"hasNextPage": end < length,
		"endCursor":   endCursor,
	}
}

func graphQLCursorVariables(
	variables map[string]json.RawMessage,
) (string, int) {
	var id string
	_ = json.Unmarshal(variables["id"], &id)
	var cursor string
	_ = json.Unmarshal(variables["after"], &cursor)
	after, _ := strconv.Atoi(cursor)
	return id, after
}

func slicesReverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right =
		left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func paginate[T any](values []T, r *http.Request, w http.ResponseWriter) []T {
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if perPage <= 0 {
		perPage = 30
	}
	if page <= 0 {
		page = 1
	}
	start := min((page-1)*perPage, len(values))
	end := min(start+perPage, len(values))
	if end < len(values) {
		next := *r.URL
		query := next.Query()
		query.Set("page", strconv.Itoa(page+1))
		query.Set("per_page", strconv.Itoa(perPage))
		next.RawQuery = query.Encode()
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		w.Header().Set(
			"Link",
			fmt.Sprintf("<%s://%s%s>; rel=\"next\"", scheme, r.Host, next.String()),
		)
	}
	return values[start:end]
}

func (s *Server) installationToken(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept") != "application/vnd.github+json" ||
		r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" ||
		!strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "invalid GitHub API headers", http.StatusBadRequest)
		return
	}
	if !s.validateAppAuthorization(w, r) {
		return
	}
	installationID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || installationID <= 0 {
		http.Error(w, "bad installation ID", http.StatusBadRequest)
		return
	}
	now := s.now()
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

func (s *Server) validateAppAuthorization(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	const bearer = "Bearer "
	rawAuthorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(rawAuthorization, bearer) ||
		s.appPublicKey == nil || s.appID <= 0 {
		http.Error(w, "valid App JWT required", http.StatusUnauthorized)
		return false
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
		return false
	}
	return true
}

func validateInstallationAuthorization(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	if !strings.HasPrefix(
		r.Header.Get("Authorization"),
		"Bearer fake-installation-",
	) {
		http.Error(
			w,
			"valid installation bearer required",
			http.StatusUnauthorized,
		)
		return false
	}
	return true
}

func (s *Server) listAppHookDeliveries(
	w http.ResponseWriter,
	r *http.Request,
) {
	perPage := 30
	if raw := r.URL.Query().Get("per_page"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || value > 100 {
			http.Error(w, "bad per_page", http.StatusUnprocessableEntity)
			return
		}
		perPage = value
	}
	var beforeID int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		value, err := decodeDeliveryCursor(raw)
		if err != nil {
			http.Error(w, "bad cursor", http.StatusUnprocessableEntity)
			return
		}
		beforeID = value
	}
	deliveries := s.Deliveries()
	if beforeID > 0 {
		filtered := deliveries[:0]
		for _, delivery := range deliveries {
			if delivery.ID < beforeID {
				filtered = append(filtered, delivery)
			}
		}
		deliveries = filtered
	}
	end := min(perPage, len(deliveries))
	page := deliveries[:end]
	if end < len(deliveries) {
		nextURL := *r.URL
		query := nextURL.Query()
		query.Set(
			"cursor",
			encodeDeliveryCursor(page[len(page)-1].ID),
		)
		nextURL.RawQuery = query.Encode()
		w.Header().Set(
			"Link",
			fmt.Sprintf("<%s>; rel=\"next\"", nextURL.String()),
		)
	}
	s.writeConditionalJSON(w, r, "core", page)
}

func (s *Server) redeliverAppHookDelivery(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "bad delivery ID", http.StatusUnprocessableEntity)
		return
	}
	s.mu.Lock()
	var original *storedHookDelivery
	for index := range s.deliveries {
		if s.deliveries[index].ID == id {
			copy := s.deliveries[index]
			original = &copy
			break
		}
	}
	if original != nil {
		s.redeliveries = append(s.redeliveries, id)
	}
	s.mu.Unlock()
	if original == nil {
		http.Error(w, "delivery not found", http.StatusUnprocessableEntity)
		return
	}
	// GitHub acknowledges a redelivery request before attempting the webhook.
	// Use a background context because the request context is cancelled as
	// soon as this 202 response completes.
	w.WriteHeader(http.StatusAccepted)
	go func(delivery storedHookDelivery) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		statusCode, _ := s.sendWebhook(
			ctx,
			delivery.targetURL,
			delivery.Event,
			delivery.GUID,
			delivery.body,
		)
		s.recordHookDelivery(
			delivery.targetURL,
			delivery.Event,
			delivery.GUID,
			delivery.body,
			true,
			statusCode,
		)
	}(*original)
}

func encodeDeliveryCursor(beforeID int64) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf("before:%d", beforeID)),
	)
}

func decodeDeliveryCursor(cursor string) (int64, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimPrefix(string(decoded), "before:")
	if raw == string(decoded) {
		return 0, fmt.Errorf("cursor prefix is invalid")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("cursor boundary is invalid")
	}
	return value, nil
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
	statusCode, deliveryErr := s.sendWebhook(
		ctx,
		targetURL,
		event,
		guid,
		body,
	)
	s.recordHookDelivery(
		targetURL,
		event,
		guid,
		body,
		false,
		statusCode,
	)
	return guid, deliveryErr
}

// DropWebhook records a GitHub-side delivery without sending it to ingress.
// C-R4 tests use this to model an outage/drop and then verify redelivery.
func (s *Server) DropWebhook(
	targetURL string,
	event string,
	payload any,
) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	guid, err := newDeliveryGUID()
	if err != nil {
		return "", fmt.Errorf("generate delivery GUID: %w", err)
	}
	s.recordHookDelivery(targetURL, event, guid, body, false, 0)
	return guid, nil
}

func (s *Server) sendWebhook(
	ctx context.Context,
	targetURL string,
	event string,
	guid string,
	body []byte,
) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		targetURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", guid)
	req.Header.Set("X-Hub-Signature-256", gh.SignBody(s.webhookSecret, body))
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf(
			"webhook target returned %d",
			resp.StatusCode,
		)
	}
	return resp.StatusCode, nil
}

func (s *Server) recordHookDelivery(
	targetURL string,
	event string,
	guid string,
	body []byte,
	redelivery bool,
	statusCode int,
) int64 {
	status := "DROPPED"
	if statusCode >= 200 && statusCode <= 399 {
		status = "OK"
	} else if statusCode > 0 {
		status = "FAILED"
	}
	var action string
	var envelope struct {
		Action string `json:"action"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		action = envelope.Action
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextDeliveryID
	s.nextDeliveryID++
	s.deliveries = append(s.deliveries, storedHookDelivery{
		HookDelivery: HookDelivery{
			ID:           id,
			GUID:         guid,
			DeliveredAt:  s.now().UTC(),
			Redelivery:   redelivery,
			Status:       status,
			StatusCode:   statusCode,
			Event:        event,
			Action:       action,
			RepositoryID: s.fixture.Repository.ID,
		},
		targetURL: targetURL,
		body:      append([]byte(nil), body...),
	})
	return id
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

type rateOutcome struct {
	snapshot   rateSnapshot
	scripted   bool
	allowed    bool
	status     int
	retryAfter time.Duration
	secondary  bool
	cost       int64
}

func (s *Server) nextRate(resource string, defaultCost int64) rateOutcome {
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
		cost := step.Cost
		if cost <= 0 {
			cost = defaultCost
		}
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
		return rateOutcome{
			snapshot:   snapshot,
			scripted:   true,
			allowed:    true,
			status:     step.StatusCode,
			retryAfter: step.RetryAfter,
			secondary:  step.Secondary,
			cost:       cost,
		}
	}
	s.mu.Unlock()
	snapshot, allowed := s.consume(resource, defaultCost)
	return rateOutcome{
		snapshot: snapshot,
		allowed:  allowed,
		cost:     defaultCost,
	}
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

func (s *Server) beginRequest(path string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	if strings.HasPrefix(path, "/app/installations/") {
		s.tokenMaxActive = max(s.tokenMaxActive, s.active)
	}
	return s.responseDelay
}

func (s *Server) endRequest() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

type scriptedRateKey struct{}
type rateCostKey struct{}

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
		s.mu.Lock()
		s.notModified[r.Method+" "+r.URL.Path]++
		s.mu.Unlock()
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

func writeGraphQLRateLimitExceeded(
	w http.ResponseWriter,
	budget rateSnapshot,
	status int,
	cost int64,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": map[string]any{
			"rateLimit": map[string]any{
				"cost":      cost,
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
	cost int64,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if resource == "graphql" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"rateLimit": map[string]any{
					"cost":      cost,
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
