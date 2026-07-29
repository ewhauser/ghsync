package budget

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/acme/frontier/internal/store"
)

func TestPostgresLeaseAcquireRenewAndStealOnExpiry(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	installationID := time.Now().UnixNano()
	t.Cleanup(func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM installation_budgets WHERE installation_id = $1`,
			installationID,
		)
	})
	leases := NewPostgresLeaseStore(pool)
	if _, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"owner-a",
		time.Minute,
	); err != nil || !acquired {
		t.Fatalf("first acquire = %v, %v", acquired, err)
	}
	if _, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"owner-b",
		time.Minute,
	); err != nil || acquired {
		t.Fatalf("contended acquire = %v, %v", acquired, err)
	}
	if _, renewed, err := leases.Renew(
		ctx,
		installationID,
		"owner-a",
		time.Minute,
	); err != nil || !renewed {
		t.Fatalf("renew = %v, %v", renewed, err)
	}

	reset := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	snapshot := Snapshot{
		REST: ResourceBudget{
			Known: true, Limit: 15000, Remaining: 12345, ResetAt: reset,
		},
		GraphQL: ResourceBudget{
			Known: true, Limit: 5000, Remaining: 4321, ResetAt: reset,
		},
		BackoffUntil: reset.Add(-time.Minute),
	}
	if saved, err := leases.Save(
		ctx,
		installationID,
		"owner-a",
		snapshot,
	); err != nil || !saved {
		t.Fatalf("save = %v, %v", saved, err)
	}
	if _, err := pool.Exec(
		ctx,
		`UPDATE installation_budgets
		 SET lease_until = now() - interval '1 second'
		 WHERE installation_id = $1`,
		installationID,
	); err != nil {
		t.Fatal(err)
	}
	restored, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"owner-b",
		time.Minute,
	)
	if err != nil || !acquired {
		t.Fatalf("steal expired lease = %v, %v", acquired, err)
	}
	if restored.REST.Remaining != 12345 ||
		restored.GraphQL.Remaining != 4321 ||
		!restored.BackoffUntil.Equal(snapshot.BackoffUntil) {
		t.Fatalf("restored snapshot = %+v", restored)
	}
	if saved, err := leases.Save(
		ctx,
		installationID,
		"owner-a",
		snapshot,
	); err != nil || saved {
		t.Fatalf("stale owner save = %v, %v", saved, err)
	}
	if err := leases.Release(ctx, installationID, "owner-b"); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresLeaseOwnershipLossStopsRenewalAndAllowsFailover(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	installationID := time.Now().UnixNano()
	t.Cleanup(func() {
		_, _ = pool.Exec(
			context.Background(),
			`DELETE FROM installation_budgets WHERE installation_id = $1`,
			installationID,
		)
	})
	leases := NewPostgresLeaseStore(pool)
	ttl := 2 * time.Second
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("Retry-After", "1")
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader("secondary rate limit")),
		}, nil
	})}
	first, err := NewLeased(
		ctx,
		client,
		Options{},
		&saveBackoffOwnershipLossStore{LeaseStore: leases},
		LeaseOptions{
			InstallationID:   installationID,
			Owner:            "first-process",
			TTL:              ttl,
			RenewInterval:    200 * time.Millisecond,
			SnapshotInterval: time.Hour,
			StoreTimeout:     100 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = first.Close(context.Background())
	})

	lostAt := time.Now()
	req, _ := http.NewRequest(http.MethodGet, "http://github.test/resource", nil)
	response, err := first.Do(
		context.Background(),
		Interactive,
		NewRESTRequest(req),
	)
	if response != nil && response.HTTP != nil {
		_ = response.HTTP.Body.Close()
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("SaveBackoff ownership loss error = %v, want ErrLeaseLost", err)
	}
	select {
	case <-first.lease.done:
	case <-time.After(time.Second):
		t.Fatal("lease runtime kept renewing after proven ownership loss")
	}

	deadline := lostAt.Add(ttl + 500*time.Millisecond)
	var second *Gate
	for time.Now().Before(deadline) {
		second, err = NewLeased(
			ctx,
			http.DefaultClient,
			Options{},
			leases,
			LeaseOptions{
				InstallationID:   installationID,
				Owner:            "replacement-process",
				TTL:              ttl,
				RenewInterval:    200 * time.Millisecond,
				SnapshotInterval: time.Hour,
				StoreTimeout:     100 * time.Millisecond,
			},
		)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrLeaseHeld) {
			t.Fatalf("replacement acquire = %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if second == nil {
		t.Fatalf("replacement did not acquire within lease TTL; last error = %v", err)
	}
	if elapsed := time.Since(lostAt); elapsed > ttl+500*time.Millisecond {
		t.Fatalf("replacement acquisition took %v, TTL = %v", elapsed, ttl)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type saveBackoffOwnershipLossStore struct {
	LeaseStore
}

func (*saveBackoffOwnershipLossStore) SaveBackoff(
	context.Context,
	int64,
	string,
	time.Time,
) (bool, error) {
	return false, nil
}
