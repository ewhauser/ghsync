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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

func TestControlEmitRecordsAndSignsLoopbackWebhook(t *testing.T) {
	received := make(chan *http.Request, 1)
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received <- r.Clone(context.Background())
			w.WriteHeader(http.StatusOK)
		},
	))
	defer target.Close()
	fakeServer := New(DefaultFixture(), "secret")
	fake := httptest.NewServer(fakeServer)
	defer fake.Close()
	body := []byte(fmt.Sprintf(`{
		"target_url": %q,
		"event": "pull_request",
		"guid": "soak-guid",
		"payload": {
			"action": "synchronize",
			"number": 4812,
			"repository": {"full_name": "acme/monolith"}
		}
	}`, target.URL))
	response, err := http.Post(
		fake.URL+ControlEmitPath,
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("control emit status = %d: %s", response.StatusCode, message)
	}
	select {
	case request := <-received:
		if request.Header.Get("X-GitHub-Delivery") != "soak-guid" ||
			request.Header.Get("X-GitHub-Event") != "pull_request" ||
			request.Header.Get("X-Hub-Signature-256") == "" {
			t.Fatalf("emitted headers = %#v", request.Header)
		}
	case <-time.After(time.Second):
		t.Fatal("control emit did not deliver webhook")
	}
	if deliveries := fakeServer.Deliveries(); len(deliveries) != 1 ||
		deliveries[0].GUID != "soak-guid" {
		t.Fatalf("recorded deliveries = %#v", deliveries)
	}
}

func TestControlTruthReportsLatestMutatedPullState(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	payload := json.RawMessage(`{
		"number": 4812,
		"soak_revision": 77
	}`)
	fake.applySoakMutation("pull_request", payload)
	response := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+ControlTruthPath,
		nil,
	)
	defer func() { _ = response.Body.Close() }()
	var truth SoakTruth
	if err := json.NewDecoder(response.Body).Decode(&truth); err != nil {
		t.Fatal(err)
	}
	if truth.Repository != "acme/monolith" ||
		len(truth.PullRequests) != 1 ||
		truth.PullRequests[0].Number != 4812 ||
		truth.PullRequests[0].Title != "Soak revision 77 for PR 4812" {
		t.Fatalf("truth = %+v", truth)
	}
}

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

func TestAPIRequiresFakeInstallationBearer(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	for _, request := range []struct {
		method string
		target string
		body   io.Reader
	}{
		{
			method: http.MethodGet,
			target: "http://fake.test/repos/acme/monolith/stacks",
		},
		{
			method: http.MethodPost,
			target: "http://fake.test/graphql",
			body:   strings.NewReader(`{"query":"query { rateLimit { cost } }"}`),
		},
	} {
		response := serveAuthorized(
			fake,
			request.method,
			request.target,
			request.body,
			"Bearer wrong-token-kind",
		)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf(
				"%s %s status = %d, want 401",
				request.method,
				request.target,
				response.StatusCode,
			)
		}
	}
}

func TestAuthorizationRecordingIsBounded(t *testing.T) {
	server := New(DefaultFixture(), "secret")
	for index := 0; index < maxRecordedAuthorizations+10; index++ {
		server.recordAuthorization(strconv.Itoa(index))
	}
	got := server.Authorizations()
	if len(got) != maxRecordedAuthorizations {
		t.Fatalf(
			"recorded authorizations = %d, want %d",
			len(got),
			maxRecordedAuthorizations,
		)
	}
	if got[0] != "10" ||
		got[len(got)-1] != strconv.Itoa(maxRecordedAuthorizations+9) {
		t.Fatalf("bounded authorization window = %q ... %q", got[0], got[len(got)-1])
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
	_ = first.Body.Close()
	if first.StatusCode != http.StatusOK || etag == "" {
		t.Fatalf("first PR status=%d etag=%q", first.StatusCode, etag)
	}
	request := httptest.NewRequest(http.MethodGet, "http://fake.test"+path, nil)
	request.Header.Set("If-None-Match", etag)
	request.Header.Set("Authorization", "Bearer fake-installation-test")
	recorder := httptest.NewRecorder()
	fake.ServeHTTP(recorder, request)
	notModified := recorder.Result()
	_ = notModified.Body.Close()
	if notModified.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional PR status=%d, want 304", notModified.StatusCode)
	}

	checks := serve(
		fake,
		http.MethodGet,
		"http://fake.test/repos/acme/monolith/commits/8f31c2d/check-runs",
		nil,
	)
	defer func() { _ = checks.Body.Close() }()
	checksETag := checks.Header.Get("ETag")
	var payload struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	if err := json.NewDecoder(checks.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.CheckRuns) != 2 {
		t.Fatalf("check runs = %d, want 2", len(payload.CheckRuns))
	}
	checksPath := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	request = httptest.NewRequest(
		http.MethodGet,
		"http://fake.test"+checksPath,
		nil,
	)
	request.Header.Set("If-None-Match", checksETag)
	request.Header.Set("Authorization", "Bearer fake-installation-test")
	recorder = httptest.NewRecorder()
	fake.ServeHTTP(recorder, request)
	notModified = recorder.Result()
	_ = notModified.Body.Close()
	if notModified.StatusCode != http.StatusNotModified ||
		fake.NotModifiedCount(http.MethodGet, checksPath) != 1 {
		t.Fatalf(
			"conditional checks status=%d count=%d, want 304/1",
			notModified.StatusCode,
			fake.NotModifiedCount(http.MethodGet, checksPath),
		)
	}

	fake.ScriptNotFound(http.MethodGet, path, 1)
	missing := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	_ = missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("scripted status=%d, want 404", missing.StatusCode)
	}
	restored := serve(
		fake,
		http.MethodGet,
		"http://fake.test"+path,
		nil,
	)
	_ = restored.Body.Close()
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

func TestConcurrentListPullsAndSoakMutationUseIsolatedFixtureData(
	t *testing.T,
) {
	fake := New(DefaultFixture(), "secret")
	start := make(chan struct{})
	errs := make(chan error, 9)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 250 {
				response := serve(
					fake,
					http.MethodGet,
					"http://fake.test/repos/acme/monolith/pulls?state=all",
					nil,
				)
				var pulls []PullRequest
				err := json.NewDecoder(response.Body).Decode(&pulls)
				_ = response.Body.Close()
				if err != nil {
					errs <- err
					return
				}
				if response.StatusCode != http.StatusOK || len(pulls) != 5 {
					errs <- fmt.Errorf(
						"list pulls status/count = %d/%d",
						response.StatusCode,
						len(pulls),
					)
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for revision := range 2_000 {
			fake.applySoakMutation(
				"pull_request",
				json.RawMessage(fmt.Sprintf(
					`{"number":4812,"soak_revision":%d}`,
					revision,
				)),
			)
		}
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
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
	_ = valid.Body.Close()
	if valid.StatusCode != http.StatusCreated {
		t.Fatalf("valid token status = %d, want 201", valid.StatusCode)
	}
	if calls := fake.TokenRequests(); calls != 1 {
		t.Fatalf("token request count = %d, want 1", calls)
	}
	if got := fake.MaxConcurrent(); got != 1 {
		t.Fatalf("token endpoint max concurrency = %d, want 1", got)
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
	now := time.Now().UTC()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	appJWT := signAppJWT(t, now, "99", key)
	var received atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			received.Add(1)
			started <- struct{}{}
			<-release
			w.WriteHeader(http.StatusNoContent)
		},
	))
	defer target.Close()
	fake := New(
		DefaultFixture(),
		"secret",
		WithAppAuthentication(99, &key.PublicKey),
		WithNow(func() time.Time { return now }),
	)
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
	unauthorized := serve(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2",
		nil,
	)
	_ = unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf(
			"installation bearer on deliveries status = %d, want 401",
			unauthorized.StatusCode,
		)
	}
	first := serveAuthorized(
		fake,
		http.MethodGet,
		"http://fake.test/app/hook/deliveries?per_page=2",
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = first.Body.Close() }()
	link := first.Header.Get("Link")
	if first.StatusCode != http.StatusOK ||
		!strings.Contains(link, "cursor=") {
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
	// A concurrent new delivery sorts ahead of page one. The opaque boundary
	// cursor must still resume after the last ID actually returned.
	if _, err := fake.DropWebhook(
		target.URL,
		"push",
		map[string]any{"action": "concurrent"},
	); err != nil {
		t.Fatal(err)
	}
	nextURL := strings.TrimSuffix(
		strings.TrimPrefix(strings.Split(link, ";")[0], "<"),
		">",
	)
	second := serveAuthorized(
		fake,
		http.MethodGet,
		nextURL,
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = second.Body.Close() }()
	var remaining []HookDelivery
	if err := json.NewDecoder(second.Body).Decode(&remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("second page deliveries = %+v", remaining)
	}
	redelivery := serveAuthorized(
		fake,
		http.MethodPost,
		fmt.Sprintf(
			"http://fake.test/app/hook/deliveries/%d/attempts",
			deliveries[0].ID,
		),
		nil,
		"Bearer "+appJWT,
	)
	defer func() { _ = redelivery.Body.Close() }()
	if redelivery.StatusCode != http.StatusAccepted {
		t.Fatalf("redelivery status = %d", redelivery.StatusCode)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("asynchronous redelivery did not start")
	}
	if received.Load() != 1 || len(fake.RedeliveryRequests()) != 1 {
		t.Fatalf(
			"received=%d requests=%v deliveries=%+v",
			received.Load(),
			fake.RedeliveryRequests(),
			fake.Deliveries(),
		)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fake.Deliveries()[0].Redelivery {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("redelivery was not recorded after asynchronous arrival")
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
	req.Header.Set("Authorization", "Bearer fake-installation-test")
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
	return serveAuthorized(
		handler,
		method,
		target,
		body,
		"Bearer fake-installation-test",
	)
}

func serveAuthorized(
	handler http.Handler,
	method string,
	target string,
	body io.Reader,
	authorization string,
) *http.Response {
	req := httptest.NewRequest(method, target, body)
	req.Header.Set("Authorization", authorization)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}

func signAppJWT(
	t *testing.T,
	now time.Time,
	issuer string,
	privateKey *rsa.PrivateKey,
) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer:    issuer,
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
	})
	signed, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
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
