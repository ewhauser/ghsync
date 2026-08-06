package budget

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestNewLeasedStandbyAcquiresAfterCleanRelease(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)

	ownerOptions := standbyTestLeaseOptions(clock, "owner-a")
	owner, err := NewLeased(
		t.Context(),
		http.DefaultClient,
		Options{Clock: clock},
		store,
		ownerOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = owner.Close(context.Background())
	})
	firstAttempt := waitForStandbyAcquire(t, store)
	if !firstAttempt.acquired {
		t.Fatal("initial owner did not acquire lease")
	}

	standbyOptions := standbyTestLeaseOptions(clock, "owner-b")
	// Exercise normalization before the retry cadence is derived.
	standbyOptions.TTL = 0
	standbyOptions.RenewInterval = 0
	standbyOptions.StoreTimeout = 0
	observations := make(chan standbyObservation, 1)
	result := make(chan standbyResult, 1)
	go func() {
		gate, standbyErr := NewLeasedStandby(
			t.Context(),
			http.DefaultClient,
			Options{Clock: clock},
			store,
			standbyOptions,
			func(ownerName string, retryIn time.Duration) {
				observations <- standbyObservation{
					owner:   ownerName,
					retryIn: retryIn,
				}
			},
		)
		result <- standbyResult{gate: gate, err: standbyErr}
	}()

	heldAttempt := waitForStandbyAcquire(t, store)
	if heldAttempt.acquired {
		t.Fatal("standby acquired while the first owner still held the lease")
	}
	observation := waitForStandbyObservation(t, observations)
	if observation.owner != "owner-b" {
		t.Fatalf("standby owner = %q, want owner-b", observation.owner)
	}
	if observation.retryIn < maxStandbyRetryInterval/2 ||
		observation.retryIn > maxStandbyRetryInterval {
		t.Fatalf(
			"normalized standby retry = %v, want in [%v, %v]",
			observation.retryIn,
			maxStandbyRetryInterval/2,
			maxStandbyRetryInterval,
		)
	}
	select {
	case acquired := <-result:
		if acquired.gate != nil {
			_ = acquired.gate.Close(context.Background())
		}
		t.Fatalf("standby returned before release: %v", acquired.err)
	default:
	}
	select {
	case extra := <-store.acquireCalls:
		t.Fatalf("standby retried without its clock timer: %+v", extra)
	default:
	}

	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(maxStandbyRetryInterval)
	acquired := waitForStandbyResult(t, result)
	if acquired.err != nil {
		t.Fatalf("standby acquisition after release: %v", acquired.err)
	}
	if acquired.gate == nil {
		t.Fatal("standby returned no gate after release")
	}
	t.Cleanup(func() {
		_ = acquired.gate.Close(context.Background())
	})
	retryAttempt := waitForStandbyAcquire(t, store)
	if !retryAttempt.acquired {
		t.Fatal("standby did not acquire the released lease")
	}
	if retryAttempt.token == heldAttempt.token {
		t.Fatal("standby reused its lease token across acquisition attempts")
	}
}

func TestNewLeasedStandbyStealsAfterLeaseExpiry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)
	const ttl = 6 * time.Second
	store.seed("dead-owner-token", now.Add(ttl))

	options := standbyTestLeaseOptions(clock, "replacement-owner")
	options.TTL = ttl
	options.RenewInterval = 2 * time.Second
	observations := make(chan standbyObservation, 1)
	result := make(chan standbyResult, 1)
	go func() {
		gate, err := NewLeasedStandby(
			t.Context(),
			http.DefaultClient,
			Options{Clock: clock},
			store,
			options,
			func(ownerName string, retryIn time.Duration) {
				observations <- standbyObservation{
					owner:   ownerName,
					retryIn: retryIn,
				}
			},
		)
		result <- standbyResult{gate: gate, err: err}
	}()

	heldAttempt := waitForStandbyAcquire(t, store)
	if heldAttempt.acquired {
		t.Fatal("standby stole an unexpired lease")
	}
	observation := waitForStandbyObservation(t, observations)
	if observation.retryIn < time.Second || observation.retryIn > 2*time.Second {
		t.Fatalf(
			"standby retry = %v, want in [%v, %v]",
			observation.retryIn,
			time.Second,
			2*time.Second,
		)
	}

	clock.Advance(ttl)
	acquired := waitForStandbyResult(t, result)
	if acquired.err != nil {
		t.Fatalf("standby steal after expiry: %v", acquired.err)
	}
	if acquired.gate == nil {
		t.Fatal("standby returned no gate after expiry")
	}
	t.Cleanup(func() {
		_ = acquired.gate.Close(context.Background())
	})
	retryAttempt := waitForStandbyAcquire(t, store)
	if !retryAttempt.acquired {
		t.Fatal("standby did not steal the expired lease")
	}
	if retryAttempt.token == "dead-owner-token" ||
		retryAttempt.token == heldAttempt.token {
		t.Fatal("standby acquisition did not use a fresh opaque token")
	}
}

func TestNewLeasedStandbyStopsWhenContextIsCanceled(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)
	store.seed("live-owner-token", now.Add(time.Minute))
	ctx, cancel := context.WithCancel(t.Context())
	observations := make(chan standbyObservation, 1)
	result := make(chan standbyResult, 1)
	go func() {
		gate, err := NewLeasedStandby(
			ctx,
			http.DefaultClient,
			Options{Clock: clock},
			store,
			standbyTestLeaseOptions(clock, "stopping-owner"),
			func(ownerName string, retryIn time.Duration) {
				observations <- standbyObservation{
					owner:   ownerName,
					retryIn: retryIn,
				}
			},
		)
		result <- standbyResult{gate: gate, err: err}
	}()

	if attempt := waitForStandbyAcquire(t, store); attempt.acquired {
		t.Fatal("standby acquired a live lease")
	}
	_ = waitForStandbyObservation(t, observations)
	cancel()
	stopped := waitForStandbyResult(t, result)
	if stopped.gate != nil {
		_ = stopped.gate.Close(context.Background())
		t.Fatal("canceled standby returned a gate")
	}
	if !errors.Is(stopped.err, context.Canceled) {
		t.Fatalf("canceled standby error = %v, want context canceled", stopped.err)
	}
	if timers := clock.TimerCount(); timers != 0 {
		t.Fatalf("canceled standby left %d active timers", timers)
	}
	select {
	case extra := <-store.acquireCalls:
		t.Fatalf("canceled standby made another acquisition: %+v", extra)
	default:
	}
}

func TestNewLeasedStandbyReleasesAcquisitionRacingWithCancellation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)
	ctx, cancel := context.WithCancel(t.Context())
	store.afterAcquire = cancel
	standbyCalls := 0

	gate, err := NewLeasedStandby(
		ctx,
		http.DefaultClient,
		Options{Clock: clock},
		store,
		standbyTestLeaseOptions(clock, "canceled-owner"),
		func(string, time.Duration) {
			standbyCalls++
		},
	)
	if gate != nil {
		_ = gate.Close(context.Background())
		t.Fatal("cancellation racing with acquisition returned a gate")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("acquisition cancellation error = %v, want context canceled", err)
	}
	if standbyCalls != 0 {
		t.Fatalf("successful acquisition emitted %d standby observations", standbyCalls)
	}
	if calls, releases := store.counts(); calls != 1 || releases != 1 {
		t.Fatalf(
			"canceled acquisition calls = %d, releases = %d, want 1 and 1",
			calls,
			releases,
		)
	}
	if token := store.currentToken(); token != "" {
		t.Fatalf("canceled acquisition left lease token %q", token)
	}
	if timers := clock.TimerCount(); timers != 0 {
		t.Fatalf("canceled acquisition left %d active timers", timers)
	}
}

func TestNewLeasedStandbyReturnsNonLeaseError(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)
	store.acquireErr = errors.New("budget store unavailable")
	standbyCalls := 0

	gate, err := NewLeasedStandby(
		t.Context(),
		http.DefaultClient,
		Options{Clock: clock},
		store,
		standbyTestLeaseOptions(clock, "fatal-owner"),
		func(string, time.Duration) {
			standbyCalls++
		},
	)
	if gate != nil {
		_ = gate.Close(context.Background())
		t.Fatal("fatal acquisition error returned a gate")
	}
	if !errors.Is(err, store.acquireErr) {
		t.Fatalf("fatal acquisition error = %v, want %v", err, store.acquireErr)
	}
	if standbyCalls != 0 {
		t.Fatalf("fatal acquisition emitted %d standby observations", standbyCalls)
	}
	if timers := clock.TimerCount(); timers != 0 {
		t.Fatalf("fatal acquisition left %d active timers", timers)
	}
}

func TestNewLeasedStandbyTreatsExpiredAcquisitionAsFatal(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	store := newStandbyLeaseStore(clock)
	store.returnExpiredAcquisition = true
	standbyCalls := 0

	gate, err := NewLeasedStandby(
		t.Context(),
		http.DefaultClient,
		Options{Clock: clock},
		store,
		standbyTestLeaseOptions(clock, "expired-owner"),
		func(string, time.Duration) {
			standbyCalls++
		},
	)
	if gate != nil {
		_ = gate.Close(context.Background())
		t.Fatal("expired acquisition returned a gate")
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired acquisition error = %v, want ErrLeaseLost", err)
	}
	if standbyCalls != 0 {
		t.Fatalf("expired acquisition emitted %d standby observations", standbyCalls)
	}
	if calls, releases := store.counts(); calls != 1 || releases != 1 {
		t.Fatalf(
			"expired acquisition calls = %d, releases = %d, want 1 and 1",
			calls,
			releases,
		)
	}
}

func TestStandbyRetryDelayIsPositiveAndCapped(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		ttl  time.Duration
	}{
		{name: "minimum-valid-TTL", ttl: 3 * time.Nanosecond},
		{name: "below-absolute-cap", ttl: 6 * time.Second},
		{name: "above-absolute-cap", ttl: time.Hour},
		{name: "maximum-duration", ttl: time.Duration(1<<63 - 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			maximum := min(test.ttl/3, maxStandbyRetryInterval)
			minimum := max(time.Nanosecond, maximum/2)
			for range 1_000 {
				delay := standbyRetryDelay(test.ttl)
				if delay < minimum || delay > maximum {
					t.Fatalf(
						"standby retry = %v, want in [%v, %v] for TTL %v",
						delay,
						minimum,
						maximum,
						test.ttl,
					)
				}
			}
		})
	}
}

type standbyObservation struct {
	owner   string
	retryIn time.Duration
}

type standbyResult struct {
	gate *Gate
	err  error
}

type standbyAcquire struct {
	token    string
	acquired bool
}

type standbyLeaseStore struct {
	clock *manualClock

	mu                       sync.Mutex
	token                    string
	leaseUntil               time.Time
	acquireErr               error
	returnExpiredAcquisition bool
	afterAcquire             func()
	acquires                 int
	releases                 int
	acquireCalls             chan standbyAcquire
}

func newStandbyLeaseStore(clock *manualClock) *standbyLeaseStore {
	return &standbyLeaseStore{
		clock:        clock,
		acquireCalls: make(chan standbyAcquire, 16),
	}
}

func (s *standbyLeaseStore) seed(token string, leaseUntil time.Time) {
	s.mu.Lock()
	s.token = token
	s.leaseUntil = leaseUntil
	s.mu.Unlock()
}

func (s *standbyLeaseStore) Acquire(
	_ context.Context,
	_ int64,
	token string,
	ttl time.Duration,
) (Snapshot, time.Time, bool, error) {
	s.mu.Lock()
	s.acquires++
	if s.acquireErr != nil {
		err := s.acquireErr
		s.mu.Unlock()
		s.acquireCalls <- standbyAcquire{token: token}
		return Snapshot{}, time.Time{}, false, err
	}
	now := s.clock.Now()
	if s.token != "" && now.Before(s.leaseUntil) {
		s.mu.Unlock()
		s.acquireCalls <- standbyAcquire{token: token}
		return Snapshot{}, s.leaseUntil, false, nil
	}
	s.token = token
	if s.returnExpiredAcquisition {
		s.leaseUntil = now
	} else {
		s.leaseUntil = now.Add(ttl)
	}
	leaseUntil := s.leaseUntil
	afterAcquire := s.afterAcquire
	s.mu.Unlock()
	s.acquireCalls <- standbyAcquire{token: token, acquired: true}
	if afterAcquire != nil {
		afterAcquire()
	}
	return Snapshot{}, leaseUntil, true, nil
}

func (s *standbyLeaseStore) Renew(
	_ context.Context,
	_ int64,
	token string,
	ttl time.Duration,
) (time.Time, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token != s.token || !s.clock.Now().Before(s.leaseUntil) {
		return time.Time{}, false, nil
	}
	s.leaseUntil = s.clock.Now().Add(ttl)
	return s.leaseUntil, true, nil
}

func (s *standbyLeaseStore) Save(
	_ context.Context,
	_ int64,
	token string,
	_ Snapshot, //nolint:gocritic // test double implements LeaseStore's immutable value contract
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return token == s.token, nil
}

func (s *standbyLeaseStore) SaveBackoff(
	_ context.Context,
	_ int64,
	token string,
	_ AuthContext,
	_ time.Time,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return token == s.token, nil
}

func (s *standbyLeaseStore) Release(
	_ context.Context,
	_ int64,
	token string,
) error {
	s.mu.Lock()
	s.releases++
	if token == s.token {
		s.token = ""
		s.leaseUntil = time.Time{}
	}
	s.mu.Unlock()
	return nil
}

func (s *standbyLeaseStore) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquires, s.releases
}

func (s *standbyLeaseStore) currentToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func standbyTestLeaseOptions(
	clock Clock,
	owner string,
) LeaseOptions {
	return LeaseOptions{
		InstallationID:   1234,
		Owner:            owner,
		TTL:              9 * time.Second,
		RenewInterval:    3 * time.Second,
		SnapshotInterval: time.Hour,
		StoreTimeout:     time.Second,
		Clock:            clock,
	}
}

func waitForStandbyAcquire(
	t *testing.T,
	store *standbyLeaseStore,
) standbyAcquire {
	t.Helper()
	select {
	case attempt := <-store.acquireCalls:
		return attempt
	case <-time.After(time.Second):
		t.Fatal("lease acquisition did not run")
		return standbyAcquire{}
	}
}

func waitForStandbyObservation(
	t *testing.T,
	observations <-chan standbyObservation,
) standbyObservation {
	t.Helper()
	select {
	case observation := <-observations:
		return observation
	case <-time.After(time.Second):
		t.Fatal("standby observation was not emitted")
		return standbyObservation{}
	}
}

func waitForStandbyResult(
	t *testing.T,
	result <-chan standbyResult,
) standbyResult {
	t.Helper()
	select {
	case acquired := <-result:
		return acquired
	case <-time.After(time.Second):
		t.Fatal("standby acquisition did not finish")
		return standbyResult{}
	}
}
