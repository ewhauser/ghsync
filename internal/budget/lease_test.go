package budget

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestPostgresLeaseAcquireRenewAndStealOnExpiry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool

	installationID := time.Now().UnixNano()
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
		AppREST: ResourceBudget{
			Known: true, Limit: 5000, Remaining: 3456, ResetAt: reset,
		},
		GraphQL: ResourceBudget{
			Known: true, Limit: 5000, Remaining: 4321, ResetAt: reset,
		},
		BackoffUntil:       reset.Add(-time.Minute),
		AppJWTBackoffUntil: reset.Add(-2 * time.Minute),
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
		restored.AppREST.Remaining != 3456 ||
		restored.GraphQL.Remaining != 4321 ||
		!restored.BackoffUntil.Equal(snapshot.BackoffUntil) ||
		!restored.AppJWTBackoffUntil.Equal(snapshot.AppJWTBackoffUntil) {
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

func TestPostgresLeaseBackoffIsPersistedPerAuthContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool

	installationID := time.Now().UnixNano()
	leases := NewPostgresLeaseStore(pool)
	if _, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"owner-a",
		time.Minute,
	); err != nil || !acquired {
		t.Fatalf("acquire = %v, %v", acquired, err)
	}
	appBackoff := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	if saved, err := leases.SaveBackoff(
		ctx,
		installationID,
		"owner-a",
		AppJWTAuth,
		appBackoff,
	); err != nil || !saved {
		t.Fatalf("save App-JWT backoff = %v, %v", saved, err)
	}
	var appRows, unaffectedInstallationRows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (
		           WHERE class = 'app_jwt_rest' AND backoff_until = $2
		       ),
		       count(*) FILTER (
		           WHERE class IN ('rest', 'graphql') AND backoff_until IS NULL
		       )
		FROM installation_budgets
		WHERE installation_id = $1
	`, installationID, appBackoff).Scan(
		&appRows,
		&unaffectedInstallationRows,
	); err != nil {
		t.Fatal(err)
	}
	if appRows != 1 || unaffectedInstallationRows != 2 {
		t.Fatalf(
			"App-JWT backoff rows = %d, unaffected installation rows = %d",
			appRows,
			unaffectedInstallationRows,
		)
	}

	installationBackoff := appBackoff.Add(time.Minute)
	if saved, err := leases.SaveBackoff(
		ctx,
		installationID,
		"owner-a",
		InstallationAuth,
		installationBackoff,
	); err != nil || !saved {
		t.Fatalf("save installation backoff = %v, %v", saved, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE installation_budgets
		SET lease_until = clock_timestamp() - interval '1 second'
		WHERE installation_id = $1
	`, installationID); err != nil {
		t.Fatal(err)
	}
	restored, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"owner-b",
		time.Minute,
	)
	if err != nil || !acquired {
		t.Fatalf("restore = %v, %v", acquired, err)
	}
	if !restored.BackoffUntil.Equal(installationBackoff) ||
		!restored.AppJWTBackoffUntil.Equal(appBackoff) {
		t.Fatalf("restored per-context backoffs = %+v", restored)
	}
	if err := leases.Release(ctx, installationID, "owner-b"); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresLeaseConcurrentStealHasExactlyOneWinner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool

	installationID := time.Now().UnixNano()
	leases := NewPostgresLeaseStore(pool)
	if _, _, acquired, err := leases.Acquire(
		ctx,
		installationID,
		"expired-owner",
		time.Minute,
	); err != nil || !acquired {
		t.Fatalf("seed acquire = %v, %v", acquired, err)
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

	const contenders = 24
	type outcome struct {
		token    string
		acquired bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan outcome, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		token := fmt.Sprintf("steal-contender-%d", contender)
		workers.Go(func() {
			<-start
			_, _, acquired, acquireErr := leases.Acquire(
				ctx,
				installationID,
				token,
				time.Minute,
			)
			results <- outcome{
				token:    token,
				acquired: acquired,
				err:      acquireErr,
			}
		})
	}
	close(start)
	workers.Wait()
	close(results)

	var winner string
	for result := range results {
		if result.err != nil {
			t.Fatalf("%s acquire: %v", result.token, result.err)
		}
		if !result.acquired {
			continue
		}
		if winner != "" {
			t.Fatalf("multiple lease-steal winners: %q and %q", winner, result.token)
		}
		winner = result.token
	}
	if winner == "" {
		t.Fatal("concurrent lease steal had no winner")
	}

	rows, err := pool.Query(
		ctx,
		`SELECT DISTINCT lease_owner
		 FROM installation_budgets
		 WHERE installation_id = $1`,
		installationID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var persisted []string
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			t.Fatal(err)
		}
		persisted = append(persisted, token)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 || persisted[0] != winner {
		t.Fatalf(
			"persisted lease tokens = %q, winner %q",
			persisted,
			winner,
		)
	}
	if err := leases.Release(ctx, installationID, winner); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresLeaseOwnershipLossStopsRenewalAndAllowsFailover(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.New(t).Pool

	installationID := time.Now().UnixNano()
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
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://github.test/resource", http.NoBody)
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
		// A store call that exceeds the 100ms StoreTimeout on a cold
		// per-test pool is retryable, same as a still-held lease; the
		// loop's TTL deadline is the assertion.
		if !errors.Is(err, ErrLeaseHeld) &&
			!errors.Is(err, context.DeadlineExceeded) {
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
	AuthContext,
	time.Time,
) (bool, error) {
	return false, nil
}
