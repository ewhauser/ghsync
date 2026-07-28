package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// Runs only when TEST_DATABASE_URL points at a disposable Postgres (CI
// provides one as a service container). Verifies migrations are idempotent
// and produce the ingestion table.
func TestMigrateIdempotent(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	for range 2 { // twice: second run must be a no-op
		if err := Migrate(ctx, pool); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM webhook_deliveries`).Scan(&count)
	if err != nil {
		t.Fatalf("webhook_deliveries missing after migrate: %v", err)
	}
	var riverTables int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = 'river_job'`).Scan(&riverTables)
	if err != nil || riverTables != 1 {
		t.Fatalf("river_job table missing (err=%v, count=%d)", err, riverTables)
	}
}
