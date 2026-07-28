package budget

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestLeasedGatePersistsPeriodicallyNotPerRequest(t *testing.T) {
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
		req, err := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
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
		resp.HTTP.Body.Close()
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
) (Snapshot, bool, error) {
	return Snapshot{}, true, nil
}

func (s *countingLeaseStore) Renew(
	context.Context,
	int64,
	string,
	time.Duration,
) (bool, error) {
	return true, nil
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
