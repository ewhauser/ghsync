package budget

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestLeaseWatchdogFailsClosedWhileRenewalStoreCallIsStalled(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newRuntimeLeaseStore(clock)
	store.renewBlock = make(chan struct{})
	requestStarted := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestStarted <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	gate := newRuntimeLeasedGate(t, clock, store, client)

	result := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
		_, err := gate.Do(context.Background(), Interactive, NewRESTRequest(req))
		result <- err
	}()
	<-requestStarted

	clock.Advance(3 * time.Second)
	<-store.renewStarted
	clock.Advance(7 * time.Second)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled admitted request error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lease watchdog did not cancel admitted request at confirmed expiry")
	}
	req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
	if _, err := gate.Do(
		context.Background(),
		Interactive,
		NewRESTRequest(req),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("post-expiry admission error = %v, want ErrLeaseLost", err)
	}

	close(store.renewBlock)
	select {
	case <-gate.lease.done:
	case <-time.After(time.Second):
		t.Fatal("lease maintenance did not stop after watchdog fired")
	}
}

func TestCloseRenewsWhileDrainingThenSnapshotsAndReleases(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newRuntimeLeaseStore(clock)
	body := newBlockingBody()
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	})}
	gate := newRuntimeLeasedGate(t, clock, store, client)
	req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
	response, err := gate.Do(context.Background(), Interactive, NewRESTRequest(req))
	if err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() {
		closed <- gate.Close(context.Background())
	}()
	waitForUnavailable(t, gate, ErrClosed)
	clock.Advance(3 * time.Second)
	<-store.renewStarted
	select {
	case err := <-closed:
		t.Fatalf("Close returned before body drain: %v", err)
	default:
	}
	if store.hasEvent("release") {
		t.Fatal("lease released before admitted body drained")
	}

	if err := response.HTTP.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after body closed")
	}
	events := store.eventSnapshot()
	saveAt, releaseAt := eventIndex(events, "save"), eventIndex(events, "release")
	if saveAt < 0 || releaseAt < 0 || saveAt > releaseAt {
		t.Fatalf("close event order = %v, want save before release", events)
	}
}

func TestCloseDeadlineCancelsStragglerBeforeCleanup(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newRuntimeLeaseStore(clock)
	body := newBlockingBody()
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	})}
	gate := newRuntimeLeasedGate(t, clock, store, client)
	req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
	if _, err := gate.Do(
		context.Background(),
		Interactive,
		NewRESTRequest(req),
	); err != nil {
		t.Fatal(err)
	}

	closeCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := gate.Close(closeCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error = %v, want context canceled", err)
	}
	select {
	case <-body.closed:
	default:
		t.Fatal("drain deadline did not close straggler body")
	}
	events := store.eventSnapshot()
	if eventIndex(events, "save") < 0 || eventIndex(events, "release") < 0 {
		t.Fatalf("bounded cleanup events = %v", events)
	}
}

func TestSecondaryBackoffPersistsImmediatelyAndRestoresOnHandoff(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newRuntimeLeaseStore(clock)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-RateLimit-Limit", "100")
		headers.Set("X-RateLimit-Remaining", "80")
		headers.Set("X-RateLimit-Reset", "1785258000")
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     headers,
			Body: io.NopCloser(
				&singleReadBuffer{
					data: []byte(`{"message":"You have exceeded a secondary rate limit."}`),
				},
			),
		}, nil
	})}
	first := newRuntimeLeasedGate(t, clock, store, client)
	req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
	response, err := first.Do(
		context.Background(),
		Interactive,
		NewRESTRequest(req),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.HTTP.Body.Close()
	select {
	case until := <-store.backoffSaved:
		if !until.Equal(now.Add(time.Minute)) {
			t.Fatalf("persisted backoff = %v", until)
		}
	default:
		t.Fatal("secondary backoff was not persisted before Do returned")
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := newRuntimeLeasedGate(t, clock, store, http.DefaultClient)
	if got := second.Snapshot().BackoffUntil; !got.Equal(now.Add(time.Minute)) {
		t.Fatalf("restored backoff = %v", got)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewLeasedUsesUniqueRuntimeTokenNotOwnerName(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newRuntimeLeaseStore(clock)
	for range 2 {
		gate := newRuntimeLeasedGate(t, clock, store, http.DefaultClient)
		if err := gate.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	tokens := append([]string(nil), store.tokens...)
	store.mu.Unlock()
	if len(tokens) != 2 || tokens[0] == tokens[1] {
		t.Fatalf("runtime lease tokens = %q", tokens)
	}
	for _, token := range tokens {
		if token == "same-process-name" || len(token) != 64 {
			t.Fatalf("lease ownership predicate is not an opaque random token: %q", token)
		}
	}
}

func newRuntimeLeasedGate(
	t *testing.T,
	clock Clock,
	store LeaseStore,
	client *http.Client,
) *Gate {
	t.Helper()
	gate, err := NewLeased(
		context.Background(),
		client,
		Options{Clock: clock},
		store,
		LeaseOptions{
			InstallationID:   1234,
			Owner:            "same-process-name",
			TTL:              10 * time.Second,
			RenewInterval:    3 * time.Second,
			SnapshotInterval: time.Hour,
			StoreTimeout:     time.Second,
			Clock:            clock,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if manual, ok := clock.(*manualClock); ok {
		deadline := time.After(time.Second)
		for manual.TimerCount() < 3 {
			select {
			case <-deadline:
				t.Fatal("lease timers did not start")
			default:
			}
		}
	}
	return gate
}

func waitForUnavailable(t *testing.T, gate *Gate, want error) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if errors.Is(gate.unavailableError(), want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("gate did not become unavailable with %v", want)
		default:
		}
	}
}

func eventIndex(events []string, want string) int {
	for index, event := range events {
		if event == want {
			return index
		}
	}
	return -1
}

type runtimeLeaseStore struct {
	clock        *manualClock
	mu           sync.Mutex
	tokens       []string
	events       []string
	snapshot     Snapshot
	renewBlock   chan struct{}
	renewStarted chan struct{}
	backoffSaved chan time.Time
}

func newRuntimeLeaseStore(clock *manualClock) *runtimeLeaseStore {
	return &runtimeLeaseStore{
		clock:        clock,
		renewStarted: make(chan struct{}, 8),
		backoffSaved: make(chan time.Time, 8),
	}
}

func (s *runtimeLeaseStore) Acquire(
	_ context.Context,
	_ int64,
	token string,
	ttl time.Duration,
) (Snapshot, time.Time, bool, error) {
	s.mu.Lock()
	s.tokens = append(s.tokens, token)
	s.events = append(s.events, "acquire")
	snapshot := s.snapshot
	s.mu.Unlock()
	return snapshot, s.clock.Now().Add(ttl), true, nil
}

func (s *runtimeLeaseStore) Renew(
	_ context.Context,
	_ int64,
	_ string,
	ttl time.Duration,
) (time.Time, bool, error) {
	s.mu.Lock()
	s.events = append(s.events, "renew")
	block := s.renewBlock
	s.mu.Unlock()
	s.renewStarted <- struct{}{}
	if block != nil {
		<-block
	}
	return s.clock.Now().Add(ttl), true, nil
}

func (s *runtimeLeaseStore) Save(
	_ context.Context,
	_ int64,
	_ string,
	snapshot Snapshot,
) (bool, error) {
	s.mu.Lock()
	s.events = append(s.events, "save")
	s.snapshot = snapshot
	s.mu.Unlock()
	return true, nil
}

func (s *runtimeLeaseStore) SaveBackoff(
	_ context.Context,
	_ int64,
	_ string,
	until time.Time,
) (bool, error) {
	s.mu.Lock()
	s.events = append(s.events, "backoff")
	if until.After(s.snapshot.BackoffUntil) {
		s.snapshot.BackoffUntil = until
	}
	s.mu.Unlock()
	s.backoffSaved <- until
	return true, nil
}

func (s *runtimeLeaseStore) Release(context.Context, int64, string) error {
	s.mu.Lock()
	s.events = append(s.events, "release")
	s.mu.Unlock()
	return nil
}

func (s *runtimeLeaseStore) hasEvent(want string) bool {
	return eventIndex(s.eventSnapshot(), want) >= 0
}

func (s *runtimeLeaseStore) eventSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

type singleReadBuffer struct {
	data []byte
}

func (b *singleReadBuffer) Read(target []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(target, b.data)
	b.data = b.data[n:]
	return n, nil
}
