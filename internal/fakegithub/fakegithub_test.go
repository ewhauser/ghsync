package fakegithub

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/acme/frontier/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

func TestServesStacksWithRateHeaders(t *testing.T) {
	resp := serve(
		New(DefaultFixture(), "secret"),
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/stacks",
		nil,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("x-ratelimit-remaining") == "" {
		t.Fatal("missing rate-limit headers")
	}
	var stacks []Stack
	if err := json.NewDecoder(resp.Body).Decode(&stacks); err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 1 || stacks[0].Number != 142 || len(stacks[0].PullRequests) != 5 {
		t.Fatalf("unexpected stacks payload: %+v", stacks)
	}
}

func TestUnknownRepoIs404(t *testing.T) {
	resp := serve(
		New(DefaultFixture(), "secret"),
		http.MethodGet,
		"http://fake.test/repos/acme/other/stacks",
		nil,
	)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestSinglePullETagChecksAndScripted404(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	path := "/repos/acme/monolith/pulls/4812"
	first := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	etag := first.Header.Get("ETag")
	first.Body.Close()
	if first.StatusCode != http.StatusOK || etag == "" {
		t.Fatalf("first PR status=%d etag=%q", first.StatusCode, etag)
	}
	request := httptest.NewRequest(http.MethodGet, "http://fake.test"+path, nil)
	request.Header.Set("If-None-Match", etag)
	recorder := httptest.NewRecorder()
	fake.ServeHTTP(recorder, request)
	notModified := recorder.Result()
	notModified.Body.Close()
	if notModified.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional PR status=%d, want 304", notModified.StatusCode)
	}

	checks := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/commits/8f31c2d/check-runs",
		nil,
	)
	defer checks.Body.Close()
	var payload struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	if err := json.NewDecoder(checks.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.CheckRuns) != 2 {
		t.Fatalf("check runs = %d, want 2", len(payload.CheckRuns))
	}

	fake.ScriptNotFound(http.MethodGet, path, 1)
	missing := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("scripted status=%d, want 404", missing.StatusCode)
	}
	restored := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	restored.Body.Close()
	if restored.StatusCode != http.StatusOK {
		t.Fatalf("post-script status=%d, want 200", restored.StatusCode)
	}
}

func TestPullsGoldenResponseDecodesThroughClientContract(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	resp := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/pulls?state=all",
		nil,
	)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/list_pulls.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytes.TrimSpace(body), bytes.TrimSpace(golden)) {
		t.Fatalf("pull response does not match golden\n got: %s\nwant: %s", body, golden)
	}

	var extended []PullRequest
	if err := json.Unmarshal(body, &extended); err != nil {
		t.Fatalf("decode preview extension: %v", err)
	}
	if extended[1].Stack == nil || extended[1].Stack.Number != 142 {
		t.Fatalf("stack preview extension lost: %+v", extended[1].Stack)
	}

	pulls, status, err := listPullRequests(
		context.Background(),
		"http://fake.test/repos/acme/monolith/pulls?state=all",
		&http.Client{Transport: handlerRoundTripper{handler: fake}},
	)
	if err != nil {
		t.Fatalf("client list pulls: %v", err)
	}
	if status != http.StatusOK || len(pulls) != 5 {
		t.Fatalf("client response status=%d pulls=%d", status, len(pulls))
	}
	if got := pulls[1].Head.Ref; got != "refactor/bm25f-ranker" {
		t.Fatalf("head ref = %q", got)
	}
	if got := pulls[1].Head.SHA; got != "8f31c2d" {
		t.Fatalf("head sha = %q", got)
	}
	if got := pulls[1].Base.Ref; got != "refactor/tokenizer" {
		t.Fatalf("base ref = %q", got)
	}
}

func TestPullListHonorsSortDirectionAndPagination(t *testing.T) {
	fixture := DefaultFixture()
	for index := range fixture.PullRequests {
		fixture.PullRequests[index].UpdatedAt = time.Date(
			2026,
			7,
			28,
			12,
			index,
			0,
			0,
			time.UTC,
		)
	}
	fake := New(fixture, "secret")
	readNumbers := func(target string) []int {
		t.Helper()
		resp := serve(fake, http.MethodGet, target, nil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", target, resp.StatusCode)
		}
		var pulls []PullRequest
		if err := json.NewDecoder(resp.Body).Decode(&pulls); err != nil {
			t.Fatal(err)
		}
		numbers := make([]int, 0, len(pulls))
		for _, pull := range pulls {
			numbers = append(numbers, pull.Number)
		}
		return numbers
	}
	ascending := readNumbers(
		"http://fake.test/repos/acme/monolith/pulls" +
			"?state=all&sort=updated&direction=asc&per_page=2&page=1",
	)
	descending := readNumbers(
		"http://fake.test/repos/acme/monolith/pulls" +
			"?state=all&sort=updated&direction=desc&per_page=2&page=1",
	)
	if !reflect.DeepEqual(ascending, []int{4810, 4812}) {
		t.Fatalf("ascending first page = %v", ascending)
	}
	if !reflect.DeepEqual(descending, []int{4820, 4816}) {
		t.Fatalf("descending first page = %v", descending)
	}
}

func TestSeparateFixedWindowRateBudgets(t *testing.T) {
	fake := New(DefaultFixture(), "secret", WithRateLimits(2, 1))

	graphQL := func() (int, map[string]any, http.Header) {
		t.Helper()
		resp := serve(
			fake,
			http.MethodPost,
			"http://fake.test/graphql",
			bytes.NewReader(nil),
		)
		defer resp.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body, resp.Header
	}
	rest := func() (int, http.Header) {
		t.Helper()
		resp := serve(
			fake,
			http.MethodGet,
			"http://fake.test/repos/acme/monolith/stacks",
			nil,
		)
		resp.Body.Close()
		return resp.StatusCode, resp.Header
	}

	status, body, headers := graphQL()
	if status != http.StatusOK || headers.Get("X-RateLimit-Resource") != "graphql" {
		t.Fatalf("GraphQL status=%d resource=%q", status, headers.Get("X-RateLimit-Resource"))
	}
	if fake.GraphQLRemaining() != 0 || fake.Remaining() != 2 {
		t.Fatalf(
			"budgets after GraphQL: graphql=%d rest=%d",
			fake.GraphQLRemaining(),
			fake.Remaining(),
		)
	}
	rateLimit := body["data"].(map[string]any)["rateLimit"].(map[string]any)
	if rateLimit["remaining"] != float64(0) || rateLimit["limit"] != float64(1) {
		t.Fatalf("GraphQL rateLimit block = %#v", rateLimit)
	}

	status, firstRESTHeaders := rest()
	if status != http.StatusOK ||
		firstRESTHeaders.Get("X-RateLimit-Resource") != "core" ||
		firstRESTHeaders.Get("X-RateLimit-Remaining") != "1" {
		t.Fatalf("first REST status=%d headers=%v", status, firstRESTHeaders)
	}

	status, body, _ = graphQL()
	if status != http.StatusOK || body["errors"] == nil {
		t.Fatalf("exhausted GraphQL status=%d body=%#v", status, body)
	}

	status, secondRESTHeaders := rest()
	if status != http.StatusOK || secondRESTHeaders.Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("second REST status=%d headers=%v", status, secondRESTHeaders)
	}
	if firstRESTHeaders.Get("X-RateLimit-Reset") !=
		secondRESTHeaders.Get("X-RateLimit-Reset") {
		t.Fatal("REST reset time slid between requests")
	}

	status, exhaustedRESTHeaders := rest()
	if status != http.StatusForbidden ||
		exhaustedRESTHeaders.Get("Retry-After") != "" ||
		exhaustedRESTHeaders.Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("exhausted REST status=%d headers=%v", status, exhaustedRESTHeaders)
	}
}

func TestSecondaryLimitModelsHeaderAndHeaderlessForms(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	fake := New(
		DefaultFixture(),
		"secret",
		WithRESTRateSteps(
			RateLimitStep{
				Limit:      100,
				Remaining:  80,
				ResetAt:    reset,
				StatusCode: http.StatusForbidden,
				RetryAfter: time.Second,
				Secondary:  true,
			},
			RateLimitStep{
				Limit:      100,
				Remaining:  79,
				ResetAt:    reset,
				StatusCode: http.StatusTooManyRequests,
				Secondary:  true,
			},
		),
	)
	for index, wantRetryAfter := range []bool{true, false} {
		resp := serve(
			fake,
			http.MethodGet,
			"http://fake.test/repos/acme/monolith/stacks",
			nil,
		)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Header.Get("Retry-After") != ""; got != wantRetryAfter {
			t.Fatalf("step %d Retry-After presence = %v", index, got)
		}
		if !strings.Contains(strings.ToLower(string(body)), "secondary rate limit") {
			t.Fatalf("step %d body is not a secondary-limit shape: %s", index, body)
		}
	}
}

func TestInstallationTokenEndpointValidatesJWTAndReturnsCreated(t *testing.T) {
	now := time.Now().UTC()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	fake := New(
		DefaultFixture(),
		"secret",
		WithAppAuthentication(99, &key.PublicKey),
		WithNow(func() time.Time { return now }),
	)
	sign := func(issuer string, privateKey *rsa.PrivateKey) string {
		t.Helper()
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
			Issuer:    issuer,
			IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
		})
		signed, signErr := token.SignedString(privateKey)
		if signErr != nil {
			t.Fatal(signErr)
		}
		return signed
	}
	call := func(appJWT string) *http.Response {
		t.Helper()
		req := httptest.NewRequest(
			http.MethodPost,
			"http://fake.test/app/installations/1234/access_tokens",
			strings.NewReader("{}"),
		)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Authorization", "Bearer "+appJWT)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		recorder := httptest.NewRecorder()
		fake.ServeHTTP(recorder, req)
		return recorder.Result()
	}

	valid := call(sign(strconv.FormatInt(99, 10), key))
	valid.Body.Close()
	if valid.StatusCode != http.StatusCreated {
		t.Fatalf("valid token status = %d, want 201", valid.StatusCode)
	}
	if calls := fake.TokenRequests(); calls != 1 {
		t.Fatalf("token request count = %d, want 1", calls)
	}

	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		"not.a.jwt",
		sign("100", key),
		sign("99", wrongKey),
	} {
		resp := call(invalid)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("invalid JWT status = %d, want 401", resp.StatusCode)
		}
	}
	if calls := fake.TokenRequests(); calls != 1 {
		t.Fatalf("invalid JWTs reached token issuance: %d", calls)
	}
}

func TestEmitWebhookSignsAndDelivers(t *testing.T) {
	fake := New(DefaultFixture(), "secret")

	type received struct {
		event, guid, sig string
		body             []byte
	}
	got := make(chan received, 1)
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		got <- received{
			event: r.Header.Get("X-GitHub-Event"),
			guid:  r.Header.Get("X-GitHub-Delivery"),
			sig:   r.Header.Get("X-Hub-Signature-256"),
			body:  body,
		}
		return emptyResponse(http.StatusAccepted), nil
	})}

	payload := map[string]any{"action": "stacked", "number": 4815}
	guid, err := fake.EmitWebhook(
		context.Background(),
		"http://webhook.test",
		"pull_request",
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}

	rec := <-got
	if rec.event != "pull_request" || rec.guid != guid {
		t.Fatalf("delivery headers wrong: %+v", rec)
	}
	if matched, err := regexp.MatchString(
		`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
		guid,
	); err != nil || !matched {
		t.Fatalf("delivery GUID %q is not a UUIDv4", guid)
	}
	if !gh.VerifySignature([]byte("secret"), rec.body, rec.sig) {
		t.Fatal("signature does not verify")
	}
	if gh.VerifySignature([]byte("wrong"), rec.body, rec.sig) {
		t.Fatal("signature verified with wrong secret")
	}
}

func TestEmitWebhookWithGUIDOverride(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	got := make(chan string, 2)
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got <- r.Header.Get("X-GitHub-Delivery")
		return emptyResponse(http.StatusAccepted), nil
	})}

	const guid = "intentional-redelivery-guid"
	for range 2 {
		returned, err := fake.EmitWebhookWithGUID(
			context.Background(),
			"http://webhook.test",
			"pull_request",
			guid,
			map[string]any{"action": "synchronize"},
		)
		if err != nil {
			t.Fatal(err)
		}
		if returned != guid {
			t.Fatalf("returned GUID = %q, want %q", returned, guid)
		}
	}
	if first, second := <-got, <-got; first != guid || second != guid {
		t.Fatalf("override headers = %q, %q", first, second)
	}
}

func TestEmitWebhookFailsOnNon2xx(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	fake.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return emptyResponse(http.StatusServiceUnavailable), nil
	})}
	if _, err := fake.EmitWebhook(
		context.Background(),
		"http://webhook.test",
		"push",
		map[string]any{},
	); err == nil {
		t.Fatal("expected error on 503 target")
	}
}

func TestAppHookDeliveriesPaginateAndRedeliver(t *testing.T) {
	var received int
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received++
			w.WriteHeader(http.StatusNoContent)
		},
	))
	defer target.Close()
	fake := New(DefaultFixture(), "secret")
	for index := range 3 {
		if _, err := fake.DropWebhook(
			target.URL,
			"push",
			map[string]any{
				"action": "test",
				"index":  index,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	first := serve(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2",
		nil,
	)
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK ||
		!strings.Contains(first.Header.Get("Link"), "cursor=2") {
		t.Fatalf(
			"first page status/link = %d/%q",
			first.StatusCode,
			first.Header.Get("Link"),
		)
	}
	var deliveries []HookDelivery
	if err := json.NewDecoder(first.Body).Decode(&deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 || deliveries[0].ID <= deliveries[1].ID {
		t.Fatalf("first page deliveries = %+v", deliveries)
	}
	second := serve(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2&cursor=2",
		nil,
	)
	defer second.Body.Close()
	var remaining []HookDelivery
	if err := json.NewDecoder(second.Body).Decode(&remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("second page deliveries = %+v", remaining)
	}
	redelivery := serve(
		fake,
		http.MethodPost,
		fmt.Sprintf(
			"http://fake.test/app/hook/deliveries/%d/attempts",
			deliveries[0].ID,
		),
		nil,
	)
	defer redelivery.Body.Close()
	if redelivery.StatusCode != http.StatusAccepted {
		t.Fatalf("redelivery status = %d", redelivery.StatusCode)
	}
	if received != 1 ||
		len(fake.RedeliveryRequests()) != 1 ||
		!fake.Deliveries()[0].Redelivery {
		t.Fatalf(
			"received=%d requests=%v deliveries=%+v",
			received,
			fake.RedeliveryRequests(),
			fake.Deliveries(),
		)
	}
}

// clientPullRequest is deliberately independent of the fake's response type.
// Its nested shape mirrors google/go-github's PullRequest and catches fixtures
// that would only decode through flat, fake-only fields.
type clientPullRequest struct {
	Number int                 `json:"number"`
	Head   clientPullReference `json:"head"`
	Base   clientPullReference `json:"base"`
}

type clientPullReference struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

func listPullRequests(
	ctx context.Context,
	endpoint string,
	client *http.Client,
) ([]clientPullRequest, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var pulls []clientPullRequest
	if err := json.NewDecoder(resp.Body).Decode(&pulls); err != nil {
		return nil, resp.StatusCode, err
	}
	return pulls, resp.StatusCode, nil
}

func serve(handler http.Handler, method, target string, body io.Reader) *http.Response {
	req := httptest.NewRequest(method, target, body)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}

type handlerRoundTripper struct {
	handler http.Handler
}

func (rt handlerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	rt.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func emptyResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
	}
}
