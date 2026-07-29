package gh_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/gh"
)

func TestResponseHeadersAreAuthoritative(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(
			fakegithub.RateLimitStep{Limit: 100, Remaining: 91, ResetAt: reset},
			fakegithub.RateLimitStep{Limit: 100, Remaining: 73, ResetAt: reset},
		),
	)
	gate := budget.New(server.Client(), budget.Options{})
	client := newRESTClient(t, baseURL, gate)

	if _, _, err := client.ListStacks(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().REST.Remaining; got != 91 {
		t.Fatalf("remaining after first response = %d, want 91", got)
	}
	if _, _, err := client.ListPulls(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListPullsOptions{State: "all"},
		"",
	); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().REST.Remaining; got != 73 {
		t.Fatalf("remaining after second response = %d, want 73", got)
	}
}

func TestSecondaryLimitClosesGateGloballyForRetryAfter(t *testing.T) {
	clock := newManualClock(time.Now())
	reset := clock.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(fakegithub.RateLimitStep{
			Limit:      100,
			Remaining:  80,
			ResetAt:    reset,
			StatusCode: http.StatusForbidden,
			RetryAfter: time.Second,
		}),
	)
	gate := budget.New(server.Client(), budget.Options{Clock: clock})
	rest := newRESTClient(t, baseURL, gate)
	graphQL := newGraphQLClient(t, baseURL, gate)

	_, _, err := rest.ListStacks(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	)
	var httpErr *gh.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusForbidden {
		t.Fatalf("secondary-limit error = %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, callErr := graphQL.Call(
			context.Background(),
			budget.Interactive,
			`query { rateLimit { cost limit remaining resetAt } }`,
			nil,
			nil,
		)
		result <- callErr
	}()
	clock.Advance(time.Second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestClassFloorsQueueBackgroundAndLeaveInteractiveHeadroom(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(
			fakegithub.RateLimitStep{Limit: 100, Remaining: 19, ResetAt: reset},
			fakegithub.RateLimitStep{Limit: 100, Remaining: 18, ResetAt: reset},
			fakegithub.RateLimitStep{Limit: 100, Remaining: 9, ResetAt: reset},
			fakegithub.RateLimitStep{Limit: 100, Remaining: 8, ResetAt: reset},
		),
	)
	starved := make(chan budget.Starvation, 2)
	gate := budget.New(server.Client(), budget.Options{
		OnStarvation: func(value budget.Starvation) {
			starved <- value
		},
	})
	client := newRESTClient(t, baseURL, gate)
	call := func(ctx context.Context, class budget.Class) error {
		_, _, err := client.ListStacks(
			ctx,
			class,
			"acme",
			"monolith",
			gh.ListStacksOptions{},
			"",
		)
		return err
	}

	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatal(err)
	}
	assertQueued(t, starved, func(ctx context.Context) error {
		return call(ctx, budget.Sweep)
	})
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive below sweep floor: %v", err)
	}
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive establishing event floor: %v", err)
	}
	assertQueued(t, starved, func(ctx context.Context) error {
		return call(ctx, budget.Event)
	})
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive below event floor: %v", err)
	}
}

func TestConcurrencyCeiling(t *testing.T) {
	server, baseURL := startFake(t,
		fakegithub.WithRateLimits(100, 100),
		fakegithub.WithResponseDelay(50*time.Millisecond),
	)
	gate := budget.New(server.Client(), budget.Options{MaxConcurrent: 3})
	client := newRESTClient(t, baseURL, gate)

	start := make(chan struct{})
	errs := make(chan error, 18)
	var wg sync.WaitGroup
	for range 18 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := client.ListStacks(
				context.Background(),
				budget.Interactive,
				"acme",
				"monolith",
				gh.ListStacksOptions{},
				"",
			)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := server.MaxConcurrent(); got != 3 {
		t.Fatalf("fake GitHub max concurrency = %d, want 3", got)
	}
}

func TestRESTAndGraphQLBudgetsAreIndependent(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(fakegithub.RateLimitStep{
			Limit: 100, Remaining: 0, ResetAt: reset,
		}),
		fakegithub.WithGraphQLRateSteps(fakegithub.RateLimitStep{
			Limit: 100, Remaining: 64, ResetAt: reset,
		}),
	)
	gate := budget.New(server.Client(), budget.Options{})
	rest := newRESTClient(t, baseURL, gate)
	graphQL := newGraphQLClient(t, baseURL, gate)

	if _, _, err := rest.ListStacks(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	restResult := make(chan error, 1)
	go func() {
		_, _, restErr := rest.ListStacks(
			ctx,
			budget.Interactive,
			"acme",
			"monolith",
			gh.ListStacksOptions{},
			"",
		)
		restResult <- restErr
	}()
	response, err := graphQL.Call(
		context.Background(),
		budget.Interactive,
		`query { rateLimit { cost limit remaining resetAt } }`,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.RateLimit.Cost != 1 || response.RateLimit.Remaining != 64 {
		t.Fatalf("GraphQL rate = %+v", response.RateLimit)
	}
	snapshot := gate.Snapshot()
	if snapshot.REST.Remaining != 0 || snapshot.GraphQL.Remaining != 64 {
		t.Fatalf("independent budgets = %+v", snapshot)
	}
	cancel()
	if err := <-restResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("exhausted REST error = %v, want context canceled", err)
	}
}

func TestConditional304IsCheapSuccess(t *testing.T) {
	server, baseURL := startFake(t, fakegithub.WithRateLimits(3, 100))
	gate := budget.New(server.Client(), budget.Options{})
	client := newRESTClient(t, baseURL, gate)

	stacks, first, err := client.ListStacks(
		context.Background(),
		budget.Sweep,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 1 || len(stacks[0].PullRequests) != 5 || first.ETag == "" {
		t.Fatalf("first stacks response = %#v, %+v", stacks, first)
	}
	remaining := gate.Snapshot().REST.Remaining
	stacks, second, err := client.ListStacks(
		context.Background(),
		budget.Sweep,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		first.ETag,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !second.NotModified || second.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional response = %+v", second)
	}
	if stacks != nil {
		t.Fatalf("304 payload = %#v, want nil", stacks)
	}
	if got := gate.Snapshot().REST.Remaining; got != remaining {
		t.Fatalf("304 remaining = %d, want unchanged %d", got, remaining)
	}
	if got := server.Remaining(); got != remaining {
		t.Fatalf("fake server remaining = %d, want %d", got, remaining)
	}
}

func TestPullsPreservePreviewStackExtension(t *testing.T) {
	server, baseURL := startFake(t)
	gate := budget.New(server.Client(), budget.Options{})
	client := newRESTClient(t, baseURL, gate)
	pulls, _, err := client.ListPulls(
		context.Background(),
		budget.Event,
		"acme",
		"monolith",
		gh.ListPullsOptions{State: "all"},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 5 || pulls[1].GetNumber() != 4812 {
		t.Fatalf("pulls = %#v", pulls)
	}
	if pulls[1].Stack == nil ||
		pulls[1].Stack.ID != 9876543 ||
		pulls[1].Stack.Position != 2 {
		t.Fatalf("preview stack extension = %+v", pulls[1].Stack)
	}
}

func TestGraphQLPaginatesReviewThreadsAndNestedComments(t *testing.T) {
	fixture := fakegithub.DefaultFixture()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	threads := make([]fakegithub.ReviewThread, 101)
	for index := range threads {
		comments := []fakegithub.ReviewComment{{
			ID:   fmt.Sprintf("comment-%03d-000", index),
			Body: "body", UpdatedAt: now, AuthorLogin: "reviewer",
		}}
		if index == 0 {
			comments = make([]fakegithub.ReviewComment, 101)
			for commentIndex := range comments {
				comments[commentIndex] = fakegithub.ReviewComment{
					ID:   fmt.Sprintf("comment-000-%03d", commentIndex),
					Body: "body", UpdatedAt: now, AuthorLogin: "reviewer",
				}
			}
		}
		threads[index] = fakegithub.ReviewThread{
			ID:       fmt.Sprintf("thread-%03d", index),
			Path:     "file.go",
			Comments: comments,
		}
	}
	fixture.PullRequests[1].ReviewThreads = threads
	server, baseURL := startFake(t, fakegithub.WithFixture(fixture))
	gate := budget.New(server.Client(), budget.Options{})
	client := newGraphQLClient(t, baseURL, gate)
	nodes, _, err := client.BatchPullRequests(
		context.Background(),
		budget.Event,
		[]string{fixture.PullRequests[1].NodeID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || len(nodes[0].ReviewThreads.Nodes) != 101 {
		t.Fatalf("review threads = %#v", nodes)
	}
	if got := len(nodes[0].ReviewThreads.Nodes[0].Comments.Nodes); got != 101 {
		t.Fatalf("first thread comments = %d, want 101", got)
	}
	if got := server.handler.RequestCount(http.MethodPost, "/graphql"); got != 3 {
		t.Fatalf("GraphQL requests = %d, want initial + two page calls", got)
	}
}

func TestInstallationTokenCachingAndSingleFlightRenewal(t *testing.T) {
	clock := newManualClock(time.Now())
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server, baseURL := startFake(t,
		fakegithub.WithInstallationTokenTTL(400*time.Millisecond),
		fakegithub.WithAppAuthentication(99, &key.PublicKey),
		fakegithub.WithNow(clock.Now),
	)
	gate := budget.New(server.Client(), budget.Options{Clock: clock})
	tokens, err := gh.NewInstallationTokens(gate, gh.InstallationTokenOptions{
		BaseURL:        baseURL,
		AppID:          99,
		InstallationID: 1234,
		PrivateKey:     key,
		RefreshBefore:  150 * time.Millisecond,
		Clock:          clock,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	got := make(chan string, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, tokenErr := tokens.Token(context.Background())
			if tokenErr != nil {
				t.Errorf("token: %v", tokenErr)
				return
			}
			got <- token
		}()
	}
	close(start)
	wg.Wait()
	close(got)
	var first string
	for token := range got {
		if first == "" {
			first = token
		}
		if token != first {
			t.Fatalf("single-flight tokens differ: %q and %q", first, token)
		}
	}
	if calls := server.TokenRequests(); calls != 1 {
		t.Fatalf("token endpoint calls = %d, want 1", calls)
	}

	clock.Advance(300 * time.Millisecond)
	second, err := tokens.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatal("expiry-aware renewal returned old token")
	}
	if calls := server.TokenRequests(); calls != 2 {
		t.Fatalf("token endpoint calls after expiry = %d, want 2", calls)
	}
}

func TestQueuedRequestRefreshesTokenAfterAdmissionWithoutNestedDeadlock(t *testing.T) {
	clock := newManualClock(time.Now())
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server, baseURL := startFake(t,
		fakegithub.WithInstallationTokenTTL(400*time.Millisecond),
		fakegithub.WithAppAuthentication(99, &key.PublicKey),
		fakegithub.WithNow(clock.Now),
		fakegithub.WithRESTRateSteps(fakegithub.RateLimitStep{
			Limit:      100,
			Remaining:  80,
			ResetAt:    clock.Now().Add(time.Hour),
			StatusCode: http.StatusForbidden,
			RetryAfter: time.Second,
			Secondary:  true,
		}),
	)
	gate := budget.New(server.Client(), budget.Options{
		Clock:         clock,
		MaxConcurrent: 1,
	})
	tokens, err := gh.NewInstallationTokens(gate, gh.InstallationTokenOptions{
		BaseURL:        baseURL,
		AppID:          99,
		InstallationID: 1234,
		PrivateKey:     key,
		RefreshBefore:  150 * time.Millisecond,
		Clock:          clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstToken, err := tokens.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rest, err := gh.NewRESTClient(baseURL, gate, tokens)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = rest.ListStacks(
		context.Background(),
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	)
	var httpErr *gh.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusForbidden {
		t.Fatalf("secondary response = %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, _, callErr := rest.ListStacks(
			context.Background(),
			budget.Interactive,
			"acme",
			"monolith",
			gh.ListStacksOptions{},
			"",
		)
		result <- callErr
	}()
	clock.Advance(time.Second)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("post-admission token renewal deadlocked at MaxConcurrent=1")
	}
	if calls := server.TokenRequests(); calls != 2 {
		t.Fatalf("token endpoint calls = %d, want 2", calls)
	}
	authorizations := server.Authorizations()
	if len(authorizations) != 2 {
		t.Fatalf("REST authorizations = %q", authorizations)
	}
	if authorizations[0] != "Bearer "+firstToken ||
		authorizations[1] == authorizations[0] {
		t.Fatalf("queued request authorizations = %q", authorizations)
	}
}

func TestGraphQLResponseSizeLimitFailsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = strings.NewReader(strings.Repeat("x", 1024)).WriteTo(w)
	}))
	t.Cleanup(server.Close)
	gate := budget.New(server.Client(), budget.Options{})
	client, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("test-token"),
		gh.GraphQLClientOptions{MaxResponseBytes: 128},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Call(
		context.Background(),
		budget.Interactive,
		`query { rateLimit { cost limit remaining resetAt } }`,
		nil,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds 128 bytes") {
		t.Fatalf("oversized GraphQL error = %v", err)
	}
	if got := gate.Snapshot().InFlight; got != 0 {
		t.Fatalf("in-flight after oversized body = %d", got)
	}
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	clock  *manualClock
	when   time.Time
	ch     chan time.Time
	active bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{
		now:    now,
		timers: make(map[*manualTimer]struct{}),
	}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(delay time.Duration) budget.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualTimer{
		clock:  c,
		when:   c.now.Add(delay),
		ch:     make(chan time.Time, 1),
		active: true,
	}
	c.timers[timer] = struct{}{}
	return timer
}

func (c *manualClock) Advance(by time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(by)
	now := c.now
	var ready []*manualTimer
	for timer := range c.timers {
		if timer.active && !timer.when.After(now) {
			timer.active = false
			ready = append(ready, timer)
		}
	}
	c.mu.Unlock()
	for _, timer := range ready {
		timer.ch <- now
	}
}

func (t *manualTimer) C() <-chan time.Time {
	return t.ch
}

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	delete(t.clock.timers, t)
	return wasActive
}

func assertQueued(
	t *testing.T,
	queued <-chan budget.Starvation,
	call func(context.Context) error,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- call(ctx)
	}()
	<-queued
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued call error = %v, want context canceled", err)
	}
}

type runningFake struct {
	*httptest.Server
	handler *fakegithub.Server
}

func (s *runningFake) MaxConcurrent() int {
	return s.handler.MaxConcurrent()
}

func (s *runningFake) Remaining() int64 {
	return s.handler.Remaining()
}

func (s *runningFake) TokenRequests() int {
	return s.handler.TokenRequests()
}

func (s *runningFake) Authorizations() []string {
	return s.handler.Authorizations()
}

func startFake(t *testing.T, options ...fakegithub.Option) (*runningFake, string) {
	t.Helper()
	handler := fakegithub.New(fakegithub.DefaultFixture(), "secret", options...)
	httpServer := httptest.NewServer(handler)
	server := &runningFake{Server: httpServer, handler: handler}
	t.Cleanup(httpServer.Close)
	return server, httpServer.URL
}

func newRESTClient(
	t *testing.T,
	baseURL string,
	gate budget.Doer,
) *gh.RESTClient {
	t.Helper()
	client, err := gh.NewRESTClient(baseURL, gate, gh.StaticToken("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newGraphQLClient(
	t *testing.T,
	baseURL string,
	gate budget.Doer,
) *gh.GraphQLClient {
	t.Helper()
	client, err := gh.NewGraphQLClient(baseURL, gate, gh.StaticToken("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	return client
}
