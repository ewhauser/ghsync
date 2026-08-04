package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

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

	// The C-O3 drift sampler resolves its keyset and rebuilds check snapshots
	// through this partial index; without it both halves fall back to a full
	// check_runs scan per sample.
	var checkGroupIndex string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'check_runs'
		  AND indexname = 'check_runs_live_group_idx'
	`).Scan(&checkGroupIndex)
	if err != nil ||
		!strings.Contains(checkGroupIndex, "(repo_id, head_sha, gh_id)") ||
		!strings.Contains(checkGroupIndex, "WHERE (tombstoned_at IS NULL)") {
		t.Fatalf(
			"live check-group index = %q (err=%v)",
			checkGroupIndex,
			err,
		)
	}
	var reviewRequestIndex string
	err = pool.QueryRow(ctx, `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename = 'pull_request_review_requests'
		  AND indexname = 'pull_request_review_requests_live_pr_idx'
	`).Scan(&reviewRequestIndex)
	if err != nil ||
		!strings.Contains(
			reviewRequestIndex,
			"(repo_id, pr_number, reviewer_kind, reviewer_gh_id)",
		) ||
		!strings.Contains(reviewRequestIndex, "INCLUDE") ||
		!strings.Contains(reviewRequestIndex, "WHERE (tombstoned_at IS NULL)") {
		t.Fatalf(
			"live PR review-request index = %q (err=%v)",
			reviewRequestIndex,
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

func TestAppJWTBudgetMigrationPreservesExistingBudgetRows(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDatabasePool(t)
	installationID := -time.Now().UnixNano()
	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	// Simulate an upgraded v0.3.3 database: the two original class rows and
	// their constraint exist, while migration 0010 has not been recorded.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if _, err := tx.Exec(
		ctx,
		`DELETE FROM schema_migrations
		 WHERE name = '0010_app_jwt_budget_context.sql'`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		ALTER TABLE installation_budgets
		    DROP CONSTRAINT installation_budgets_class_check;
		ALTER TABLE installation_budgets
		    ADD CONSTRAINT installation_budgets_class_check
		    CHECK (class = ANY (ARRAY['rest'::text, 'graphql'::text]));
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO installation_budgets (
		    installation_id, class, remaining, rate_limit, reset_at
		) VALUES
		    ($1, 'rest', 4321, 5000, $2),
		    ($1, 'graphql', 3210, 5000, $2)
	`, installationID, resetAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("upgrade migration: %v", err)
	}
	type state struct {
		remaining int64
		limit     int64
		resetAt   time.Time
	}
	want := map[string]state{
		"rest":    {remaining: 4321, limit: 5000, resetAt: resetAt},
		"graphql": {remaining: 3210, limit: 5000, resetAt: resetAt},
	}
	rows, err := pool.Query(ctx, `
		SELECT class, remaining, rate_limit, reset_at
		FROM installation_budgets
		WHERE installation_id = $1
	`, installationID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[string]state)
	for rows.Next() {
		var class string
		var value state
		if err := rows.Scan(
			&class,
			&value.remaining,
			&value.limit,
			&value.resetAt,
		); err != nil {
			t.Fatal(err)
		}
		got[class] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("upgraded budget rows = %+v, want %+v", got, want)
	}
	for class, expected := range want {
		actual, ok := got[class]
		if !ok || actual.remaining != expected.remaining ||
			actual.limit != expected.limit ||
			!actual.resetAt.Equal(expected.resetAt) {
			t.Fatalf("upgraded %s row = %+v, want %+v", class, actual, expected)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO installation_budgets (installation_id, class)
		VALUES ($1, 'app_jwt_rest')
	`, installationID); err != nil {
		t.Fatalf("new App-JWT budget class rejected after upgrade: %v", err)
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

func TestMigrationLockSerializesPoolsAndReleasesOnEveryExit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := testDatabaseConfig(t)
	firstPool, err := Connect(ctx, conf.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(firstPool.Close)
	secondPool, err := Connect(ctx, conf.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(secondPool.Close)

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- withMigrationLock(ctx, firstPool, func() error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- withMigrationLock(ctx, secondPool, func() error {
			close(secondEntered)
			return nil
		})
	}()
	serialized := true
	select {
	case <-secondEntered:
		serialized = false
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first migration lock: %v", err)
	}
	if !serialized {
		<-secondDone
		t.Fatal("second migration pool entered while the first held the session lock")
	}
	select {
	case <-secondEntered:
	case <-ctx.Done():
		t.Fatalf("second migration pool did not acquire released lock: %v", ctx.Err())
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second migration lock: %v", err)
	}

	sentinel := errors.New("migration callback failed")
	if err := withMigrationLock(ctx, firstPool, func() error {
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("migration callback error = %v, want sentinel", err)
	}
	assertMigrationLockAvailable(t, ctx, secondPool)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = withMigrationLock(ctx, firstPool, func() error {
			panic("migration callback panicked")
		})
	}()
	if recovered != "migration callback panicked" {
		t.Fatalf("migration panic = %v", recovered)
	}
	assertMigrationLockAvailable(t, ctx, secondPool)
}

func TestMigrationLockWaitFailureClosesHijackedConnection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := testDatabaseConfig(t)
	firstPool, err := Connect(ctx, conf.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(firstPool.Close)
	lockConn, err := acquireMigrationLock(ctx, firstPool)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close(context.WithoutCancel(ctx)) //nolint:errcheck // test cleanup releases the session lock

	applicationName := fmt.Sprintf("migration-lock-timeout-%d", time.Now().UnixNano())
	parsed, err := url.Parse(conf.URL())
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("application_name", applicationName)
	query.Set("pool_max_conns", "1")
	parsed.RawQuery = query.Encode()
	waitingPool, err := Connect(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(waitingPool.Close)

	waitCtx, stopWaiting := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stopWaiting()
	if leakedConn, err := acquireMigrationLock(waitCtx, waitingPool); err == nil {
		_ = leakedConn.Close(context.WithoutCancel(ctx))
		t.Fatal("contended migration lock ignored its context deadline")
	}

	// Backend exit is asynchronous relative to the client-side close of the
	// failed lock connection; poll with a generous bound before declaring a
	// leak.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var remaining int
		if err := firstPool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
		`, applicationName).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"failed migration lock leaked %d hijacked database connections",
				remaining,
			)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertMigrationLockAvailable polls until the migration advisory lock can
// be taken: the server releases a closed holder's session lock only when the
// backend processes the disconnect, which is asynchronous relative to the
// client's Close returning. Lock and unlock are pinned to one acquired
// connection because pg_advisory_unlock on a different pooled session is a
// silent no-op that would leak the lock.
func assertMigrationLockAvailable(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	assertMigrationLockAvailableOn(t, ctx, connection)
}

// migrationLockSession is one pinned database session: pg_advisory_unlock on
// a different pooled session is a silent no-op that would leak the lock.
type migrationLockSession interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// assertMigrationLockAvailableOn polls until the migration advisory lock can
// be taken on the given session: the server releases a closed holder's
// session lock only when the backend processes the disconnect, which is
// asynchronous relative to the client's Close returning.
func assertMigrationLockAvailableOn(
	t *testing.T,
	ctx context.Context,
	session migrationLockSession,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var acquired bool
		if err := session.QueryRow(ctx,
			`SELECT pg_try_advisory_lock(hashtextextended('ghsync_schema_migrations', 0))`).
			Scan(&acquired); err != nil {
			t.Fatal(err)
		}
		if acquired {
			if _, err := session.Exec(ctx,
				`SELECT pg_advisory_unlock(hashtextextended('ghsync_schema_migrations', 0))`); err != nil {
				t.Fatal(err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("migration session lock remained held after holder closed")
		}
		time.Sleep(50 * time.Millisecond)
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
		    ($1, 'app_jwt_rest', 'snapshot-test', $2, $3),
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
	if affected != 3 {
		t.Fatalf("snapshot rows = %d, want 3", affected)
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
	if earlierRows != 3 {
		t.Fatalf(
			"rows preserving later backoff = %d, want 3",
			earlierRows,
		)
	}
}
