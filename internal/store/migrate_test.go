package store

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Runs only when TEST_DATABASE_URL points at a disposable Postgres (CI
// provides one as a service container). Verifies migrations are idempotent,
// webhook GUID dedupe works through sqlc, and the ingestion table keeps its
// minimal primary-key plus pending-only partial-index invariant.
func TestMigrateIdempotent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testDatabasePool(t)
	var synchronousCommit string
	if err := pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(
		&synchronousCommit,
	); err != nil || synchronousCommit != "on" {
		t.Fatalf(
			"synchronous_commit = %q (err=%v), want on",
			synchronousCommit,
			err,
		)
	}

	// The test database arrives already migrated (pgtestdb template); both
	// re-runs must be no-ops.
	for range 2 {
		if err := Migrate(ctx, pool); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}

	queries := dbgen.New(pool)
	guid := "migrate-test-" + time.Now().UTC().Format("20060102T150405.000000000")
	rawBody := []byte(`{"action":"stacked","number":4815}`)
	headers := []byte(`{
		"content-type": "application/json",
		"x-github-delivery": "` + guid + `",
		"x-github-event": "pull_request"
	}`)
	params := dbgen.InsertWebhookDeliveryParams{
		DeliveryGuid: guid,
		Event:        "pull_request",
		RawBody:      rawBody,
		Headers:      headers,
	}
	first, err := queries.InsertWebhookDelivery(ctx, params)
	if err != nil {
		t.Fatalf("first webhook insert: %v", err)
	}
	second, err := queries.InsertWebhookDelivery(ctx, params)
	if err != nil {
		t.Fatalf("duplicate webhook insert: %v", err)
	}
	if first != 1 || second != 0 {
		t.Fatalf("affected rows = %d, %d; want 1, 0", first, second)
	}

	stored, err := queries.GetWebhookDelivery(ctx, guid)
	if err != nil {
		t.Fatalf("get webhook delivery: %v", err)
	}
	if !bytes.Equal(stored.RawBody, rawBody) {
		t.Fatalf("raw body = %q, want %q", stored.RawBody, rawBody)
	}
	var storedHeaders, expectedHeaders map[string]string
	if err := json.Unmarshal(stored.Headers, &storedHeaders); err != nil {
		t.Fatalf("decode stored headers: %v", err)
	}
	if err := json.Unmarshal(headers, &expectedHeaders); err != nil {
		t.Fatalf("decode expected headers: %v", err)
	}
	if len(storedHeaders) != len(expectedHeaders) {
		t.Fatalf("stored headers = %#v, want %#v", storedHeaders, expectedHeaders)
	}
	for key, want := range expectedHeaders {
		if got := storedHeaders[key]; got != want {
			t.Fatalf("stored header %q = %q, want %q", key, got, want)
		}
	}

	var deliveryCount int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM webhook_deliveries WHERE delivery_guid = $1`,
		guid).Scan(&deliveryCount)
	if err != nil || deliveryCount != 1 {
		t.Fatalf("stored delivery count = %d (err=%v), want 1", deliveryCount, err)
	}

	var indexCount int
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'webhook_deliveries'
	`).Scan(&indexCount)
	if err != nil || indexCount != 4 {
		t.Fatalf("webhook_deliveries index count = %d (err=%v), want 4", indexCount, err)
	}
	var partialIndexDefinition string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'webhook_deliveries'
		  AND indexname = 'webhook_deliveries_pending_due_idx'
	`).Scan(&partialIndexDefinition)
	if err != nil ||
		!strings.Contains(
			partialIndexDefinition,
			"(next_attempt_at, received_at, delivery_guid)",
		) ||
		!strings.Contains(partialIndexDefinition, "WHERE (status = 'pending'::text)") {
		t.Fatalf(
			"pending partial index = %q (err=%v)",
			partialIndexDefinition,
			err,
		)
	}

	var riverTables int
	err = pool.QueryRow(ctx,
		`SELECT count(*)
		 FROM information_schema.tables
		 WHERE table_schema = current_schema()
		   AND table_name = 'river_job'`).Scan(&riverTables)
	if err != nil || riverTables != 1 {
		t.Fatalf("river_job table missing (err=%v, count=%d)", err, riverTables)
	}

	var historyRetentionIndex string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'check_history'
		  AND indexname = 'check_history_prunable_btree_idx'
	`).Scan(&historyRetentionIndex)
	if err != nil ||
		!strings.Contains(historyRetentionIndex, "(synced_at, id)") {
		t.Fatalf(
			"global check-history retention index = %q (err=%v)",
			historyRetentionIndex,
			err,
		)
	}
	var deliveryRetentionIndex string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'webhook_deliveries'
		  AND indexname = 'webhook_deliveries_prunable_btree_idx'
	`).Scan(&deliveryRetentionIndex)
	if err != nil ||
		!strings.Contains(
			deliveryRetentionIndex,
			"(received_at, delivery_guid)",
		) ||
		!strings.Contains(
			deliveryRetentionIndex,
			"WHERE ((raw_body IS NOT NULL) AND (status = 'processed'::text))",
		) {
		t.Fatalf(
			"delivery retention index = %q (err=%v)",
			deliveryRetentionIndex,
			err,
		)
	}
}

func TestVerifyChecksum(t *testing.T) {
	t.Parallel()
	checksum := []byte("expected")
	if err := verifyChecksum("0001.sql", checksum, checksum); err != nil {
		t.Fatalf("equal checksum rejected: %v", err)
	}
	if err := verifyChecksum("0001.sql", []byte("old"), checksum); err == nil {
		t.Fatal("mismatched checksum accepted")
	}
	if err := verifyChecksum("0001.sql", nil, checksum); err == nil {
		t.Fatal("missing checksum accepted")
	}
}

func TestInstallationBudgetSnapshotPreservesLaterBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDatabasePool(t)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result

	installationID := -time.Now().UnixNano()
	now := time.Now().UTC().Truncate(time.Microsecond)
	wantBackoff := now.Add(10 * time.Minute)
	if _, err := tx.Exec(ctx, `
		INSERT INTO installation_budgets (
		    installation_id, class, lease_owner, lease_until, backoff_until
		) VALUES
		    ($1, 'rest', 'snapshot-test', $2, $3),
		    ($1, 'graphql', 'snapshot-test', $2, $3)
	`, installationID, now.Add(time.Hour), wantBackoff); err != nil {
		t.Fatal(err)
	}
	affected, err := dbgen.New(tx).SaveInstallationBudgetSnapshot(
		ctx,
		dbgen.SaveInstallationBudgetSnapshotParams{
			BackoffUntil: pgtype.Timestamptz{
				Time:  now.Add(time.Minute),
				Valid: true,
			},
			InstallationID: installationID,
			LeaseToken:     "snapshot-test",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatalf("snapshot rows = %d, want 2", affected)
	}
	var earlierRows int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM installation_budgets
		WHERE installation_id = $1
		  AND backoff_until = $2
	`, installationID, wantBackoff).Scan(&earlierRows); err != nil {
		t.Fatal(err)
	}
	if earlierRows != 2 {
		t.Fatalf(
			"rows preserving later backoff = %d, want 2",
			earlierRows,
		)
	}
}
