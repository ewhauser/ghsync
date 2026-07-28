package gh_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
	reset := time.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(fakegithub.RateLimitStep{
			Limit:      100,
			Remaining:  80,
			ResetAt:    reset,
			StatusCode: http.StatusForbidden,
			RetryAfter: time.Second,
		}),
	)
	gate := budget.New(server.Client(), budget.Options{})
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

	started := time.Now()
	if _, err := graphQL.Call(
		context.Background(),
		budget.Interactive,
		`query { rateLimit { cost limit remaining resetAt } }`,
		nil,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed < 950*time.Millisecond || elapsed > 1400*time.Millisecond {
		t.Fatalf("global Retry-After delay = %v, want approximately 1s", elapsed)
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
	var starved atomic.Int64
	gate := budget.New(server.Client(), budget.Options{
		OnStarvation: func(budget.Starvation) {
			starved.Add(1)
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
	assertQueued(t, func(ctx context.Context) error {
		return call(ctx, budget.Sweep)
	})
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive below sweep floor: %v", err)
	}
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive establishing event floor: %v", err)
	}
	assertQueued(t, func(ctx context.Context) error {
		return call(ctx, budget.Event)
	})
	if err := call(context.Background(), budget.Interactive); err != nil {
		t.Fatalf("interactive below event floor: %v", err)
	}
	if got := starved.Load(); got != 2 {
		t.Fatalf("starvation hook calls = %d, want 2", got)
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
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := rest.ListStacks(
		ctx,
		budget.Interactive,
		"acme",
		"monolith",
		gh.ListStacksOptions{},
		"",
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exhausted REST error = %v, want deadline", err)
	}
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

func TestInstallationTokenCachingAndSingleFlightRenewal(t *testing.T) {
	server, baseURL := startFake(t,
		fakegithub.WithInstallationTokenTTL(400*time.Millisecond),
	)
	gate := budget.New(server.Client(), budget.Options{})
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := gh.NewInstallationTokens(gate, gh.InstallationTokenOptions{
		BaseURL:        baseURL,
		AppID:          99,
		InstallationID: 1234,
		PrivateKey:     key,
		RefreshBefore:  150 * time.Millisecond,
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

	time.Sleep(300 * time.Millisecond)
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

func assertQueued(t *testing.T, call func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := call(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued call error = %v, want deadline", err)
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
