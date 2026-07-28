package budget

import (
	"context"
	"os"
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
