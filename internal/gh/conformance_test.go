package gh_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/clocktest"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
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
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Retry-After waiter did not wake after the deadline")
	}
}

func TestTooManyRequestsSecondaryLimitClosesGateEndToEnd(t *testing.T) {
	clock := newManualClock(time.Now())
	server, baseURL := startFake(t,
		fakegithub.WithRESTRateSteps(fakegithub.RateLimitStep{
			Limit:      100,
			Remaining:  80,
			ResetAt:    clock.Now().Add(time.Hour),
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: 2 * time.Second,
			Secondary:  true,
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
	if !errors.As(err, &httpErr) ||
		httpErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("secondary-limit error = %v", err)
	}
	if got := gate.Snapshot().BackoffUntil; !got.Equal(
		clock.Now().Add(2 * time.Second),
	) {
		t.Fatalf("429 backoff deadline = %v", got)
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
	clock.Advance(2 * time.Second)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("429 Retry-After waiter did not wake")
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

func TestGraphQLFloorAdmissionUsesScriptedDivergentCosts(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	server, baseURL := startFake(t,
		fakegithub.WithGraphQLRateSteps(
			fakegithub.RateLimitStep{
				Limit: 100, Remaining: 40, ResetAt: reset, Cost: 7,
			},
			fakegithub.RateLimitStep{
				Limit: 100, Remaining: 20, ResetAt: reset, Cost: 23,
			},
		),
	)
	starved := make(chan budget.Starvation, 1)
	gate := budget.New(server.Client(), budget.Options{
		GraphQLPointEstimate: 1,
		OnStarvation: func(value budget.Starvation) {
			starved <- value
		},
	})
	client := newGraphQLClient(t, baseURL, gate)
	for index, wantCost := range []int64{7, 23} {
		response, err := client.Call(
			context.Background(),
			budget.Interactive,
			`query { rateLimit { cost limit remaining resetAt } }`,
			nil,
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if response.RateLimit.Cost != wantCost {
			t.Fatalf(
				"GraphQL response %d cost = %d, want %d",
				index,
				response.RateLimit.Cost,
				wantCost,
			)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Call(
			ctx,
			budget.Sweep,
			`query { rateLimit { cost limit remaining resetAt } }`,
			nil,
			nil,
		)
		result <- err
	}()
	select {
	case starvation := <-starved:
		if starvation.Resource != budget.GraphQL ||
			starvation.Remaining != 20 ||
			starvation.Limit != 100 {
			t.Fatalf("GraphQL floor starvation = %+v", starvation)
		}
	case <-time.After(time.Second):
		t.Fatal("GraphQL sweep did not queue at the 20% floor")
	}
	if got := server.handler.RequestCount(http.MethodPost, "/graphql"); got != 2 {
		t.Fatalf("floor-blocked GraphQL request count = %d, want 2", got)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("floor-blocked GraphQL error = %v", err)
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

func TestConcurrencyCeilingIncludesRenewalDuringBurst(t *testing.T) {
	clock := newManualClock(time.Now())
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server, baseURL := startFake(t,
		fakegithub.WithRateLimits(1_000, 1_000),
		fakegithub.WithResponseDelay(150*time.Millisecond),
		fakegithub.WithInstallationTokenTTL(400*time.Millisecond),
		fakegithub.WithAppAuthentication(99, &key.PublicKey),
		fakegithub.WithNow(clock.Now),
	)
	gate := budget.New(server.Client(), budget.Options{
		Clock:         clock,
		MaxConcurrent: 3,
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
	if _, err := tokens.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(300 * time.Millisecond)
	client, err := gh.NewRESTClient(baseURL, gate, tokens)
	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 18)
	var workers sync.WaitGroup
	call := func() {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, _, callErr := client.ListStacks(
				context.Background(),
				budget.Interactive,
				"acme",
				"monolith",
				gh.ListStacksOptions{},
				"",
			)
			errs <- callErr
		}()
	}
	call()
	call()
	deadline := time.After(time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for server.Concurrent() != 2 {
		select {
		case <-deadline:
			t.Fatalf(
				"initial requests did not overlap: active=%d",
				server.Concurrent(),
			)
		case <-poll.C:
		}
	}
	// Expire the token only after the first wave is inside the fake. One of the
	// queued calls must renew while that wave is still active.
	clock.Advance(300 * time.Millisecond)
	for range 16 {
		call()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := server.TokenRequests(); got < 2 {
		t.Fatalf(
			"token exchanges during burst = %d, want an initial exchange and renewal",
			got,
		)
	}
	if got := server.MaxConcurrent(); got != 3 {
		t.Fatalf(
			"fake GitHub max concurrency including renewal = %d, want 3",
			got,
		)
	}
	if got := server.TokenMaxConcurrent(); got != 3 {
		t.Fatalf(
			"active requests when mid-burst renewal entered fake = %d, want 3",
			got,
		)
	}
}

func TestDeliveriesRequireAppJWTAndAcceptAppTokens(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server, baseURL := startFake(
		t,
		fakegithub.WithAppAuthentication(99, &key.PublicKey),
	)
	gate := budget.New(server.Client(), budget.Options{})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	appTokens, err := gh.NewAppTokens(99, privateKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := gh.NewDeliveriesClient(baseURL, gate, appTokens)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := deliveries.ListAppHookDeliveries(
		context.Background(),
		gh.ListAppHookDeliveriesOptions{},
		"",
	); err != nil {
		t.Fatal(err)
	}

	wrongTokens, err := gh.NewDeliveriesClient(
		baseURL,
		gate,
		gh.StaticToken("fake-installation-wrong-kind"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = wrongTokens.ListAppHookDeliveries(
		context.Background(),
		gh.ListAppHookDeliveriesOptions{},
		"",
	)
	var httpErr *gh.HTTPError
	if !errors.As(err, &httpErr) ||
		httpErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("installation token on App endpoint = %v", err)
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

func newManualClock(now time.Time) *clocktest.Manual {
	return clocktest.New(now)
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

func (s *runningFake) Concurrent() int {
	return s.handler.Concurrent()
}

func (s *runningFake) TokenMaxConcurrent() int {
	return s.handler.TokenMaxConcurrent()
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
	client, err := gh.NewRESTClient(
		baseURL,
		gate,
		gh.StaticToken("fake-installation-test"),
	)
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
	client, err := gh.NewGraphQLClient(
		baseURL,
		gate,
		gh.StaticToken("fake-installation-test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
