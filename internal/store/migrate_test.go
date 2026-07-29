package store

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/acme/frontier/internal/store/dbgen"
)

// Runs only when TEST_DATABASE_URL points at a disposable Postgres (CI
// provides one as a service container). Verifies migrations are idempotent,
// webhook GUID dedupe works through sqlc, and the ingestion table keeps its
// minimal primary-key plus pending-only partial-index invariant.
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

	for range 2 { // twice: second run must be a no-op
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
	if err != nil || indexCount != 3 {
		t.Fatalf("webhook_deliveries index count = %d (err=%v), want 3", indexCount, err)
	}
	var partialIndexDefinition string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'webhook_deliveries'
		  AND indexname = 'webhook_deliveries_pending_received_guid_idx'
	`).Scan(&partialIndexDefinition)
	if err != nil ||
		!strings.Contains(partialIndexDefinition, "(received_at, delivery_guid)") ||
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
		  AND indexname = 'check_history_synced_at_brin_idx'
	`).Scan(&historyRetentionIndex)
	if err != nil ||
		!strings.Contains(historyRetentionIndex, "USING brin (synced_at)") {
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
		  AND indexname = 'webhook_deliveries_received_at_brin_idx'
	`).Scan(&deliveryRetentionIndex)
	if err != nil ||
		!strings.Contains(deliveryRetentionIndex, "USING brin (received_at)") {
		t.Fatalf(
			"delivery retention index = %q (err=%v)",
			deliveryRetentionIndex,
			err,
		)
	}
}

func TestVerifyChecksum(t *testing.T) {
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
