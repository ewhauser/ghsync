package fakegithub

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ControlEmitPath is a development-only control surface used by cmd/soak to
// ask the standalone fake to record and emit a signed delivery.
const (
	ControlEmitPath = "/_ghsync/emit"
	// ControlTruthPath exposes current soak truth to the local oracle.
	ControlTruthPath          = "/_ghsync/truth"
	maxRecordedAuthorizations = 1024
)

// StackRef is the preview stack reference embedded in a pull request.
type StackRef struct {
	ID       int64 `json:"id"`
	Number   int   `json:"number"`
	Size     int   `json:"size"`
	Position int   `json:"position"`
	Base     Base  `json:"base"`
}

// Base identifies a stack's base ref and commit.
type Base struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// PullRequestBranch identifies one pull request branch.
type PullRequestBranch struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// PullRequest is fixture truth for REST and GraphQL pull responses.
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

// Stack is fixture truth for the gh-stack preview API.
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

// StackPullRequest is one ordered stack layer.
type StackPullRequest struct {
	Number    int               `json:"number"`
	State     string            `json:"state"`
	Draft     bool              `json:"draft"`
	MergedAt  *time.Time        `json:"merged_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Head      PullRequestBranch `json:"head"`
}

// Repository is fixture truth for repository endpoints.
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

// MarshalJSON emits GitHub's nested owner shape.
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

// ReviewThread is fixture truth for one GraphQL review thread.
type ReviewThread struct {
	ID         string
	IsResolved bool
	IsOutdated bool
	Path       string
	Line       *int
	Comments   []ReviewComment
}

// ReviewComment is fixture truth for one review comment.
type ReviewComment struct {
	ID          string
	Body        string
	UpdatedAt   time.Time
	AuthorLogin string
}

// CheckRun is fixture truth for one check run.
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

// RepositoryRule is fixture truth for one repository rule.
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

// MarshalJSON emits GitHub's nested App shape.
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

// Fixture is the complete mutable upstream truth served by a Server.
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

// WithFixture replaces the initial fixture after the base server is built.
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

// WithRESTRateSteps scripts successive REST response budgets.
func WithRESTRateSteps(steps ...RateLimitStep) Option {
	return func(s *Server) {
		s.restSteps = append([]RateLimitStep(nil), steps...)
	}
}

// WithGraphQLRateSteps scripts successive GraphQL response budgets.
func WithGraphQLRateSteps(steps ...RateLimitStep) Option {
	return func(s *Server) {
		s.graphQLSteps = append([]RateLimitStep(nil), steps...)
	}
}

// WithResponseDelay delays every counted API request.
func WithResponseDelay(delay time.Duration) Option {
	if delay < 0 {
		panic("response delay cannot be negative")
	}
	return func(s *Server) {
		s.responseDelay = delay
	}
}

// WithInstallationTokenTTL sets the fake token lifetime.
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
	restBudget     rateState
	graphQLBudget  rateState
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

// New constructs a scriptable fake GitHub server.
func New(fixture Fixture, webhookSecret string, options ...Option) *Server {
	resetAt := nextRateReset(time.Now())
	s := &Server{
		fixture:       cloneFixture(fixture),
		webhookSecret: []byte(webhookSecret),
		restBudget: rateState{
			limit:     15000, // GHEC installation REST budget
			remaining: 15000,
			resetAt:   resetAt,
			resource:  "core",
		},
		graphQLBudget: rateState{
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

// ServeHTTP applies scripts, authentication, concurrency, and rate accounting
// before routing a fake GitHub request.
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

	if r.URL.Path == "/healthz" || strings.HasPrefix(r.URL.Path, "/_ghsync/") {
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
	s.recordAuthorization(r.Header.Get("Authorization"))
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

func (s *Server) recordAuthorization(authorization string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.authorizations) < maxRecordedAuthorizations {
		s.authorizations = append(s.authorizations, authorization)
		return
	}
	copy(s.authorizations, s.authorizations[1:])
	s.authorizations[len(s.authorizations)-1] = authorization
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

// SoakTruthPullRequest is the oracle's current pull-request state.
type SoakTruthPullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
}

// SoakTruth is the fake's current mutation sequence and pull state.
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

// RequestCount reports the number of exact method/path requests.
func (s *Server) RequestCount(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestCounts[method+" "+path]
}

// NotModifiedCount reports conditional requests satisfied with 304.
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

// MaxConcurrent reports the highest counted API concurrency observed.
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

// TokenRequests reports installation-token exchanges.
func (s *Server) TokenRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenRequests
}

// Authorizations returns the latest bounded window of API Authorization
// headers in request order.
func (s *Server) Authorizations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.authorizations...)
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
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
