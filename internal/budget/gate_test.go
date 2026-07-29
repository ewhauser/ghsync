package budget

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeasedGatePersistsPeriodicallyNotPerRequest(t *testing.T) {
	t.Parallel()
	store := &countingLeaseStore{}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", "90")
		headers.Set(
			"X-RateLimit-Reset",
			strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10),
		)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})}
	gate, err := NewLeased(
		context.Background(),
		client,
		Options{},
		store,
		LeaseOptions{
			InstallationID:   1234,
			Owner:            "process-a",
			TTL:              time.Second,
			RenewInterval:    250 * time.Millisecond,
			SnapshotInterval: 100 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://github.test/resource", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := gate.Do(
			context.Background(),
			Interactive,
			NewRESTRequest(req),
		)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.HTTP.Body.Close()
	}
	if saves := store.saveCount(); saves != 0 {
		t.Fatalf("request-path snapshot saves = %d, want 0", saves)
	}

	deadline := time.Now().Add(time.Second)
	for store.saveCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if saves := store.saveCount(); saves == 0 {
		t.Fatal("periodic snapshot was not saved")
	}
	if err := gate.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.released {
		t.Fatal("lease was not released")
	}
}

func TestRetryAfterDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	got, err := retryAfterDeadline(now, "7")
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(7 * time.Second); !got.Equal(want) {
		t.Fatalf("seconds deadline = %v, want %v", got, want)
	}
	httpDate := now.Add(9 * time.Second).Format(http.TimeFormat)
	got, err = retryAfterDeadline(now, httpDate)
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(9 * time.Second); !got.Equal(want) {
		t.Fatalf("HTTP-date deadline = %v, want %v", got, want)
	}
}

func TestObserveSecondaryLimitAcceptsHTTPDateRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	gate := New(nil, Options{Clock: clock})
	response := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Retry-After": []string{
				now.Add(3 * time.Second).Format(http.TimeFormat),
			},
		},
		Body: http.NoBody,
	}
	if err := gate.observeSecondaryLimit(context.Background(), response); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().BackoffUntil; !got.Equal(
		now.Add(3 * time.Second),
	) {
		t.Fatalf("HTTP-date secondary-limit deadline = %v", got)
	}
}

func TestGateOptionsConfigureBudgetSurface(t *testing.T) {
	t.Parallel()
	gate := New(nil, Options{
		MaxConcurrent:          12,
		RESTLimit:              12000,
		GraphQLLimit:           4000,
		SweepFloor:             0.30,
		EventFloor:             0.15,
		SecondaryLimitFallback: 90 * time.Second,
	})
	if gate.maxConcurrent != 12 ||
		gate.rest.Limit != 12000 ||
		gate.graphql.Limit != 4000 ||
		gate.sweepFloor != 0.30 ||
		gate.eventFloor != 0.15 ||
		gate.secondaryLimitFallback != 90*time.Second {
		t.Fatalf("configured gate surface not preserved: %+v", gate)
	}
}

func TestConcurrencySlotHeldUntilNetworkBodyEOFOrClose(t *testing.T) {
	t.Parallel()
	var active atomic.Int64
	var maxActive atomic.Int64
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maxActive.Load()
			if current <= observed || maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "99")
		w.Header().Set(
			"X-RateLimit-Reset",
			strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10),
		)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "done")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)

	gate := New(server.Client(), Options{MaxConcurrent: 1})
	do := func() (*Response, error) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, http.NoBody)
		if err != nil {
			return nil, err
		}
		return gate.Do(context.Background(), Interactive, NewRESTRequest(req))
	}

	first, err := do()
	if err != nil {
		t.Fatal(err)
	}
	<-started
	secondResult := make(chan *Response, 1)
	secondErr := make(chan error, 1)
	go func() {
		response, callErr := do()
		secondResult <- response
		secondErr <- callErr
	}()
	select {
	case <-started:
		t.Fatal("second request reached transport before first body closed")
	case <-time.After(25 * time.Millisecond):
		// This is the deliberately small real-time smoke test; all policy-time
		// tests below use manual clocks.
	}
	if got := gate.Snapshot().InFlight; got != 1 {
		t.Fatalf("in-flight while body blocked = %d, want 1", got)
	}

	if err := first.HTTP.Body.Close(); err != nil {
		t.Fatal(err)
	}
	<-started
	second := <-secondResult
	if err := <-secondErr; err != nil {
		t.Fatal(err)
	}
	close(release)
	if _, err := io.ReadAll(second.HTTP.Body); err != nil {
		t.Fatal(err)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("server max concurrency = %d, want 1", got)
	}
}

func TestReservationsProtectSweepFloorDuringBurst(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	starved := make(chan Starvation, 7)
	var calls atomic.Int64
	body := newBlockingBody()
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", "20")
		headers.Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       body,
		}, nil
	})}
	gate := New(client, Options{
		Clock:               newManualClock(now),
		RESTRequestEstimate: 1,
		OnStarvation: func(value Starvation) {
			starved <- value
		},
	})
	gate.restore(&Snapshot{REST: ResourceBudget{
		Known: true, Limit: 100, Remaining: 21, ResetAt: now.Add(time.Hour),
	}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		response *Response
		err      error
	}
	results := make(chan outcome, 8)
	for range 8 {
		go func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://github.test/resource", http.NoBody)
			response, err := gate.Do(ctx, Sweep, NewRESTRequest(req))
			results <- outcome{response: response, err: err}
		}()
	}

	first := <-results
	if first.err != nil {
		t.Fatal(first.err)
	}
	for range 7 {
		select {
		case value := <-starved:
			if value.Remaining > 20 {
				t.Fatalf("starvation available remaining = %d, want <= 20", value.Remaining)
			}
		case <-time.After(time.Second):
			t.Fatal("burst caller did not queue behind reserved floor")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transport calls at 21/100 sweep budget = %d, want 1", got)
	}

	cancel()
	_ = first.response.HTTP.Body.Close()
	for range 7 {
		result := <-results
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("queued result = %v, want context canceled", result.err)
		}
	}
}

func TestFloorWaitAdvancesDeterministicallyToReset(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	starved := make(chan Starvation, 1)
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", "99")
		headers.Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})}
	gate := New(client, Options{
		Clock: clock,
		OnStarvation: func(value Starvation) {
			starved <- value
		},
	})
	gate.restore(&Snapshot{REST: ResourceBudget{
		Known: true, Limit: 100, Remaining: 20, ResetAt: now.Add(time.Hour),
	}})
	result := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://github.test/resource", http.NoBody)
		response, err := gate.Do(
			context.Background(),
			Sweep,
			NewRESTRequest(req),
		)
		if response != nil && response.HTTP != nil {
			_, _ = io.Copy(io.Discard, response.HTTP.Body)
			_ = response.HTTP.Body.Close()
		}
		result <- err
	}()
	<-starved
	if got := calls.Load(); got != 0 {
		t.Fatalf("floor-blocked transport calls = %d", got)
	}
	clock.Advance(time.Hour)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transport calls after reset = %d, want 1", got)
	}
}

func TestRateObservationsMergeByResetWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	gate := New(nil, Options{Clock: newManualClock(now)})
	headers := func(remaining int64, reset time.Time) http.Header {
		value := make(http.Header)
		value.Set("X-RateLimit-Limit", "100")
		value.Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
		value.Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
		return value
	}
	window := now.Add(time.Hour)
	if err := gate.observeREST(headers(70, window)); err != nil {
		t.Fatal(err)
	}
	if err := gate.observeREST(headers(90, window)); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().REST.Remaining; got != 70 {
		t.Fatalf("same-window slow observation raised remaining to %d", got)
	}
	if err := gate.observeREST(headers(99, window.Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().REST.Remaining; got != 70 {
		t.Fatalf("older-window observation replaced remaining with %d", got)
	}
	if err := gate.observeREST(headers(95, window.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if got := gate.Snapshot().REST.Remaining; got != 95 {
		t.Fatalf("newer-window remaining = %d, want 95", got)
	}
}

func TestSlowOlderResponseCannotRaiseRemaining(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int64
	response := func(remaining string) *http.Response {
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", remaining)
		headers.Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return response("90"), nil
		}
		return response("70"), nil
	})}
	gate := New(client, Options{
		Clock:         newManualClock(now),
		MaxConcurrent: 2,
	})
	do := func() (*Response, error) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://github.test/resource", http.NoBody)
		return gate.Do(context.Background(), Interactive, NewRESTRequest(req))
	}
	firstResult := make(chan *Response, 1)
	firstErr := make(chan error, 1)
	go func() {
		got, err := do()
		firstResult <- got
		firstErr <- err
	}()
	<-firstStarted
	second, err := do()
	if err != nil {
		t.Fatal(err)
	}
	_ = second.HTTP.Body.Close()
	close(releaseFirst)
	first := <-firstResult
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
	_ = first.HTTP.Body.Close()
	if got := gate.Snapshot().REST.Remaining; got != 70 {
		t.Fatalf("late older response raised remaining to %d", got)
	}
}

func TestHeaderlessSecondaryLimitUsesDeterministicFallback(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		call := calls.Add(1)
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", "80")
		headers.Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))
		if call == 1 {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     headers,
				Body: io.NopCloser(strings.NewReader(
					`{"message":"You have exceeded a secondary rate limit."}`,
				)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})}
	gate := New(client, Options{
		Clock:                  clock,
		SecondaryLimitFallback: time.Minute,
	})
	makeRequest := func(ctx context.Context) (*Response, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://github.test/resource", http.NoBody)
		return gate.Do(ctx, Interactive, NewRESTRequest(req))
	}
	first, err := makeRequest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = first.HTTP.Body.Close()
	if got := gate.Snapshot().BackoffUntil; !got.Equal(now.Add(time.Minute)) {
		t.Fatalf("fallback backoff = %v", got)
	}

	result := make(chan error, 1)
	go func() {
		response, callErr := makeRequest(context.Background())
		if response != nil && response.HTTP != nil {
			_ = response.HTTP.Body.Close()
		}
		result <- callErr
	}()
	clock.Advance(59 * time.Second)
	if got := calls.Load(); got != 1 {
		t.Fatalf("call admitted before fallback elapsed: %d", got)
	}
	clock.Advance(time.Second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls after fallback = %d, want 2", got)
	}
}

type blockingBody struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingBody() *blockingBody {
	return &blockingBody{closed: make(chan struct{})}
}

func (b *blockingBody) Read([]byte) (int, error) {
	<-b.closed
	return 0, io.EOF
}

func (b *blockingBody) Close() error {
	b.once.Do(func() {
		close(b.closed)
	})
	return nil
}

type countingLeaseStore struct {
	mu       sync.Mutex
	saves    int
	released bool
}

func (s *countingLeaseStore) Acquire(
	context.Context,
	int64,
	string,
	time.Duration,
) (Snapshot, time.Time, bool, error) {
	return Snapshot{}, time.Now().Add(time.Minute), true, nil
}

func (s *countingLeaseStore) Renew(
	context.Context,
	int64,
	string,
	time.Duration,
) (time.Time, bool, error) {
	return time.Now().Add(time.Minute), true, nil
}

func (s *countingLeaseStore) Save(
	context.Context,
	int64,
	string,
	Snapshot,
) (bool, error) {
	s.mu.Lock()
	s.saves++
	s.mu.Unlock()
	return true, nil
}

func (s *countingLeaseStore) SaveBackoff(
	context.Context,
	int64,
	string,
	time.Time,
) (bool, error) {
	return true, nil
}

func (s *countingLeaseStore) Release(context.Context, int64, string) error {
	s.mu.Lock()
	s.released = true
	s.mu.Unlock()
	return nil
}

func (s *countingLeaseStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
