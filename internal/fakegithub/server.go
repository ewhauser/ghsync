package fakegithub

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ControlEmitPath is a development-only control surface used by the load
// verifier to ask the standalone fake to record and emit a signed delivery.
const (
	ControlEmitPath = "/_ghsync/emit"
	// ControlTruthPath exposes the fake's mutation truth to the local oracle.
	ControlTruthPath = "/_ghsync/truth"
	// ControlFaultPath configures flag-gated API failures for load tests.
	ControlFaultPath          = "/_ghsync/faults"
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

// Base identifies a pull request or stack base ref and commit.
type Base struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// MarshalJSON emits GitHub's wire representation for an unresolved commit:
// the ref remains present while sha is null.
func (b Base) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Ref string `json:"ref"`
		SHA any    `json:"sha"`
	}{
		Ref: b.Ref,
		SHA: nullableSHA(b.SHA),
	})
}

// PullRequestBranch identifies one pull request branch.
type PullRequestBranch struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

func nullableSHA(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// PullRequest is fixture truth for REST and GraphQL pull responses.
type PullRequest struct {
	ID                  int64               `json:"id"`
	NodeID              string              `json:"node_id"`
	Number              int                 `json:"number"`
	Title               string              `json:"title"`
	State               string              `json:"state"`
	Draft               bool                `json:"draft"`
	AuthorLogin         string              `json:"-"`
	ReviewDecision      string              `json:"review_decision"`
	MergeableState      string              `json:"mergeable_state"`
	Head                PullRequestBranch   `json:"head"`
	Base                Base                `json:"base"`
	UpdatedAt           time.Time           `json:"updated_at"`
	CreatedAt           time.Time           `json:"-"`
	MergedAt            *time.Time          `json:"-"`
	Stack               *StackRef           `json:"stack"`
	ReviewThreads       []ReviewThread      `json:"-"`
	ReviewRequests      []ReviewRequest     `json:"-"`
	Reviews             []PullRequestReview `json:"-"`
	Comments            []IssueComment      `json:"-"`
	ChangedFiles        []ChangedFile       `json:"-"`
	ChangedFilesTotal   int                 `json:"-"`
	ChangedFilesOmitted bool                `json:"-"`
}

// ChangedFile is fixture truth for one pull-request changed-file node. The
// prior path is served by REST because GitHub's GraphQL type omits it.
type ChangedFile struct {
	Path         string `json:"filename"`
	PreviousPath string `json:"previous_filename,omitempty"`
	ChangeType   string `json:"status"`
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
func (r Repository) MarshalJSON() ([]byte, error) { //nolint:gocritic // value receiver preserves json.Marshaler for repository values
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
	ID         string          `json:"id"`
	IsResolved bool            `json:"is_resolved"`
	IsOutdated bool            `json:"is_outdated"`
	Path       string          `json:"path"`
	Line       *int            `json:"line,omitempty"`
	Comments   []ReviewComment `json:"comments"`
}

// ReviewComment is fixture truth for one review comment.
type ReviewComment struct {
	ID          string    `json:"id"`
	Body        string    `json:"body"`
	UpdatedAt   time.Time `json:"updated_at"`
	AuthorLogin string    `json:"author_login"`
}

// ReviewRequest is fixture truth for one RequestedReviewer union member.
// Login carries a user login for kind=user and a team slug for kind=team.
// The bot, mannequin, and nil kinds exist to exercise the documented v1
// exclusion policy; they are not projected as user or team requests.
type ReviewRequest struct {
	Kind   string `json:"kind"`
	ID     int64  `json:"id"`
	NodeID string `json:"node_id"`
	Login  string `json:"login"`
}

// Actor preserves the GraphQL author union, including non-user participants
// and deleted authors (Kind "deleted" with empty identity fields).
type Actor struct {
	Kind   string `json:"kind"`
	NodeID string `json:"node_id"`
	Login  string `json:"login"`
}

// PullRequestReview is fixture truth for an identity-keyed review fact.
type PullRequestReview struct {
	ID          int64      `json:"id"`
	NodeID      string     `json:"node_id"`
	Author      Actor      `json:"author"`
	State       string     `json:"state"`
	SubmittedAt *time.Time `json:"submitted_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CommitOID   string     `json:"commit_oid,omitempty"`
}

// IssueComment is fixture truth for ordinary PR issue-comment participation.
// Body exists only so webhook constructors can produce schema-valid payloads;
// authoritative participation fetches never request or persist it.
type IssueComment struct {
	ID        int64     `json:"id"`
	NodeID    string    `json:"node_id"`
	Author    Actor     `json:"author"`
	Body      string    `json:"body,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
func (c CheckRun) MarshalJSON() ([]byte, error) { //nolint:gocritic // value receiver preserves json.Marshaler for check-run values
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
	// Contents is keyed by exact Git ref/SHA and repository-relative path.
	Contents map[string]map[string]string
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
func WithFixture(fixture Fixture) Option { //nolint:gocritic // option takes an ownership snapshot before deep cloning
	return func(s *Server) {
		s.fixture = cloneFixture(&fixture)
	}
}

// WithAdditionalFixtures adds independently addressable repositories to the
// installation. It is used to seed --copies replay namespaces before backfill.
func WithAdditionalFixtures(fixtures ...Fixture) Option {
	return func(s *Server) {
		for index := range fixtures {
			fixture := &fixtures[index]
			clone := cloneFixture(fixture)
			s.additionalFixtures[clone.Owner+"/"+clone.Repo] = &clone
		}
	}
}

// WithAppBearerToken allows the standalone development fake to accept the
// static token ghsyncd uses for App deliveries-API gap healing.
func WithAppBearerToken(token string) Option {
	if strings.TrimSpace(token) == "" {
		panic("App bearer token is required")
	}
	return func(s *Server) {
		s.appBearerToken = token
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
	mu                 sync.Mutex
	fixture            Fixture
	webhookSecret      []byte
	restBudget         rateState
	graphQLBudget      rateState
	restSteps          []RateLimitStep
	graphQLSteps       []RateLimitStep
	restStep           int
	graphQLStep        int
	responseDelay      time.Duration
	active             int
	maxActive          int
	tokenMaxActive     int
	tokenTTL           time.Duration
	tokenRequests      int
	authorizations     []string
	appID              int64
	appPublicKey       *rsa.PublicKey
	now                func() time.Time
	mux                *http.ServeMux
	client             *http.Client
	requestCounts      map[string]int
	notModified        map[string]int
	notFound           map[string]int
	notFoundAt         map[string]map[int]struct{}
	requestHook        func(string, string, int, *Fixture)
	deliveries         []storedHookDelivery
	nextDeliveryID     int64
	redeliveries       []int64
	redeliveryInFlight map[int64]struct{}
	additionalFixtures map[string]*Fixture
	truthKeys          map[string]struct{}
	appBearerToken     string
	faults             []int
	faultRetryAfter    time.Duration
	configured500      int
	configured429      int
	applied500         int
	applied429         int
}

// New constructs a scriptable fake GitHub server.
func New(fixture Fixture, webhookSecret string, options ...Option) *Server { //nolint:gocritic // constructor snapshots caller-owned fixture data
	resetAt := nextRateReset(time.Now())
	s := &Server{
		fixture:       cloneFixture(&fixture),
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
		tokenTTL:           time.Hour,
		now:                time.Now,
		client:             &http.Client{Timeout: 10 * time.Second},
		requestCounts:      make(map[string]int),
		notModified:        make(map[string]int),
		notFound:           make(map[string]int),
		notFoundAt:         make(map[string]map[int]struct{}),
		nextDeliveryID:     1,
		redeliveryInFlight: make(map[int64]struct{}),
		additionalFixtures: make(map[string]*Fixture),
		truthKeys:          make(map[string]struct{}),
	}
	for _, option := range options {
		option(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health)
	mux.HandleFunc("POST "+ControlEmitPath, s.controlEmit)
	mux.HandleFunc("GET "+ControlTruthPath, s.controlTruth)
	mux.HandleFunc("POST "+ControlFaultPath, s.controlFaults)
	mux.HandleFunc("GET /repos/{owner}/{repo}", s.getRepository)
	mux.HandleFunc("GET /installation/repositories", s.listInstallationRepositories)
	mux.HandleFunc("GET /repos/{owner}/{repo}/rulesets", s.listRepositoryRules)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks", s.listStacks)
	mux.HandleFunc("GET /repos/{owner}/{repo}/stacks/{number}", s.getStack)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls", s.listPulls)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls/{number}", s.getPull)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/pulls/{number}/files",
		s.listPullFiles,
	)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/pulls/{number}/reviews",
		s.listPullReviews,
	)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/issues/{number}/comments",
		s.listIssueComments,
	)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/commits/{sha}/check-runs",
		s.listCheckRuns,
	)
	mux.HandleFunc(
		"GET /repos/{owner}/{repo}/contents/{path...}",
		s.getRepositoryContent,
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
	if status, retryAfter, ok := s.takeFault(); ok {
		s.writeFault(w, r, resource, status, retryAfter)
		return
	}
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
		switch {
		case rate.secondary || rate.retryAfter > 0:
			writeSecondaryRateLimitExceeded(
				w,
				rate.status,
				resource,
				rate.snapshot,
				rate.cost,
			)
		case resource == "graphql":
			writeGraphQLRateLimitExceeded(
				w,
				rate.snapshot,
				rate.status,
				rate.cost,
			)
		default:
			writeRESTRateLimitExceeded(w, rate.status)
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

func (s *Server) takeFault() (int, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.faults) == 0 {
		return 0, 0, false
	}
	status := s.faults[0]
	s.faults = s.faults[1:]
	if status == http.StatusTooManyRequests {
		s.applied429++
	} else {
		s.applied500++
	}
	return status, s.faultRetryAfter, true
}

func (s *Server) writeFault(
	w http.ResponseWriter,
	_ *http.Request,
	resource string,
	status int,
	retryAfter time.Duration,
) {
	snapshot := s.snapshot(resource)
	setRateHeaders(w.Header(), snapshot)
	if status == http.StatusTooManyRequests {
		if retryAfter > 0 {
			w.Header().Set(
				"Retry-After",
				strconv.FormatInt(
					int64((retryAfter+time.Second-1)/time.Second),
					10,
				),
			)
		}
		writeSecondaryRateLimitExceeded(
			w,
			status,
			resource,
			snapshot,
			1,
		)
		return
	}
	writeJSONStatus(w, status, map[string]any{
		"message": "scripted fake GitHub internal error",
	})
}

type controlEmitRequest struct {
	TargetURL             string            `json:"target_url"`
	Event                 string            `json:"event"`
	GUID                  string            `json:"guid,omitempty"`
	Mutate                bool              `json:"mutate,omitempty"`
	Payload               json.RawMessage   `json:"payload"`
	Mutation              *TruthMutation    `json:"mutation,omitempty"`
	Deliveries            []ControlDelivery `json:"deliveries,omitempty"`
	AllowDeliveryFailures bool              `json:"allow_delivery_failures,omitempty"`
}

// ControlDelivery is one signed delivery emitted (or GitHub-side dropped)
// after a truth mutation has been applied.
type ControlDelivery struct {
	Event   string          `json:"event"`
	GUID    string          `json:"guid,omitempty"`
	Payload json.RawMessage `json:"payload"`
	Drop    bool            `json:"drop,omitempty"`
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
	deliveries := request.Deliveries
	if request.Event != "" || len(request.Payload) > 0 {
		deliveries = append(deliveries, ControlDelivery{
			Event: request.Event, GUID: request.GUID, Payload: request.Payload,
		})
	}
	if request.Mutation == nil && len(deliveries) == 0 {
		http.Error(w, "mutation or deliveries are required", http.StatusBadRequest)
		return
	}
	if len(deliveries) > 0 {
		target, err := url.Parse(request.TargetURL)
		if err != nil || target.Scheme != "http" ||
			!loopbackHost(target.Hostname()) {
			http.Error(
				w,
				"target_url must be an http loopback address",
				http.StatusBadRequest,
			)
			return
		}
	}
	if request.Mutate {
		http.Error(
			w,
			"legacy mutate is unsupported; send a full mutation",
			http.StatusBadRequest,
		)
		return
	}
	for _, delivery := range deliveries {
		if delivery.Event == "" || len(delivery.Payload) == 0 {
			http.Error(
				w,
				"delivery event and payload are required",
				http.StatusBadRequest,
			)
			return
		}
	}
	if request.Mutation != nil {
		if err := s.applyTruthMutation(*request.Mutation); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	guids := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		var (
			guid string
			err  error
		)
		if delivery.Drop {
			guid, err = s.DropWebhookWithGUID(
				request.TargetURL,
				delivery.Event,
				delivery.GUID,
				delivery.Payload,
			)
		} else {
			guid, err = s.EmitWebhookWithGUID(
				r.Context(),
				request.TargetURL,
				delivery.Event,
				delivery.GUID,
				delivery.Payload,
			)
		}
		if err != nil {
			if request.AllowDeliveryFailures {
				guids = append(guids, guid)
				continue
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		guids = append(guids, guid)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"guids": guids})
}

func (s *Server) controlTruth(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	truth := TruthSnapshot{
		Repositories: make([]TruthFixtureSnapshot, 0, len(s.truthKeys)),
		Faults: TruthFaultSnapshot{
			Configured500: s.configured500,
			Configured429: s.configured429,
			Applied500:    s.applied500,
			Applied429:    s.applied429,
		},
	}
	for key := range s.truthKeys {
		fixture := s.fixtureByKeyLocked(key)
		if fixture != nil {
			truth.Repositories = append(
				truth.Repositories,
				snapshotFixture(cloneFixture(fixture)),
			)
		}
	}
	s.mu.Unlock()
	sort.Slice(truth.Repositories, func(i, j int) bool {
		return truth.Repositories[i].Repository.FullName <
			truth.Repositories[j].Repository.FullName
	})
	writeJSON(w, truth)
}

func (s *Server) fixtureByKeyLocked(key string) *Fixture {
	if key == s.fixture.Owner+"/"+s.fixture.Repo {
		return &s.fixture
	}
	return s.additionalFixtures[key]
}

type controlFaultRequest struct {
	InternalErrors int           `json:"internal_errors"`
	RateLimits     int           `json:"rate_limits"`
	RetryAfter     time.Duration `json:"retry_after"`
}

func (s *Server) controlFaults(w http.ResponseWriter, r *http.Request) {
	var request controlFaultRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "decode fault request", http.StatusBadRequest)
		return
	}
	if request.InternalErrors < 0 || request.RateLimits < 0 ||
		request.RetryAfter < 0 {
		http.Error(w, "fault counts and retry_after cannot be negative", http.StatusBadRequest)
		return
	}
	if request.RetryAfter == 0 && request.RateLimits > 0 {
		request.RetryAfter = time.Second
	}
	s.mu.Lock()
	s.configured500 += request.InternalErrors
	s.configured429 += request.RateLimits
	for range request.InternalErrors {
		s.faults = append(s.faults, http.StatusInternalServerError)
	}
	for range request.RateLimits {
		s.faults = append(s.faults, http.StatusTooManyRequests)
	}
	s.faultRetryAfter = request.RetryAfter
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
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
func (s *Server) SetFixture(fixture Fixture) { //nolint:gocritic // setter snapshots caller-owned fixture data
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fixture = cloneFixture(&fixture)
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
	for index := range slices.Backward(s.deliveries) {
		v := &s.deliveries[index]
		result = append(result, v.HookDelivery)
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
