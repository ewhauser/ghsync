package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestObservationUnlockFailureDestroysPhysicalConnection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDatabasePool(t)
	key := fmt.Sprintf("observation-unlock-failure-%d", time.Now().UnixNano())
	observation, err := NewEntityWriter(pool).BeginObservation(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	backendPID := observation.conn.Conn().PgConn().PID()

	// Shadow only this session's unqualified unlock function with a deliberate
	// failure. The backend remains healthy and idle after the error, which is
	// the dangerous case: an unconditional pgxpool Release would make this
	// still-locked session reusable.
	schema := fmt.Sprintf("observation_unlock_failure_%d", backendPID)
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s.pg_advisory_unlock(bigint)
		RETURNS boolean
		LANGUAGE plpgsql
		AS $function$
		BEGIN
			RAISE EXCEPTION 'forced observation unlock failure';
		END
		$function$
	`, quotedSchema)); err != nil {
		t.Fatal(err)
	}
	if _, err := observation.conn.Exec(
		ctx,
		"SET search_path TO "+quotedSchema+", pg_catalog, public",
	); err != nil {
		t.Fatal(err)
	}

	var held int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_locks
		WHERE pid = $1
		  AND locktype = 'advisory'
		  AND granted
	`, backendPID).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 1 {
		t.Fatalf("observation backend advisory locks = %d, want 1", held)
	}

	err = observation.CloseContext(ctx)
	if err == nil || !strings.Contains(err.Error(), "forced observation unlock failure") {
		t.Fatalf("observation close error = %v", err)
	}
	assertObservationBackendTerminated(t, ctx, pool, backendPID)

	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	if got := connection.Conn().PgConn().PID(); got == backendPID {
		t.Fatalf("pool reused destroyed observation backend %d", got)
	}
	var acquired bool
	if err := connection.QueryRow(
		ctx,
		`SELECT pg_try_advisory_lock(hashtextextended($1::text, 0))`,
		key,
	).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("replacement pooled session could not acquire released observation lock")
	}
	if _, err := connection.Exec(
		ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1::text, 0))`,
		key,
	); err != nil {
		t.Fatal(err)
	}
}

func TestObservationFalseUnlockDestroysPhysicalConnection(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testDatabasePool(t)
	key := fmt.Sprintf("observation-false-unlock-%d", time.Now().UnixNano())
	observation, err := NewEntityWriter(pool).BeginObservation(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	backendPID := observation.conn.Conn().PgConn().PID()
	var unlocked bool
	if err := observation.conn.QueryRow(
		ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1::text, 0))`,
		key,
	).Scan(&unlocked); err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Fatal("test setup did not release the observation lock")
	}

	err = observation.CloseContext(ctx)
	if err == nil || !strings.Contains(err.Error(), "lock not held") {
		t.Fatalf("observation close after false unlock = %v", err)
	}
	assertObservationBackendTerminated(t, ctx, pool, backendPID)
}

func TestObservationCloseIgnoresCallerCancellationForCleanup(t *testing.T) {
	t.Parallel()
	databaseConfig := testDatabaseConfig(t)
	poolConfig, err := pgxpool.ParseConfig(databaseConfig.URL())
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(t.Context(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	key := fmt.Sprintf("observation-canceled-close-%d", time.Now().UnixNano())
	observation, err := NewEntityWriter(pool).BeginObservation(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	backendPID := observation.conn.Conn().PgConn().PID()
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := observation.CloseContext(canceledCtx); err != nil {
		t.Fatalf("close with canceled caller context: %v", err)
	}

	connection, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	if got := connection.Conn().PgConn().PID(); got != backendPID {
		t.Fatalf(
			"canceled cleanup destroyed backend %d instead of reusing it as %d",
			backendPID,
			got,
		)
	}
	var held int
	if err := connection.QueryRow(t.Context(), `
		SELECT count(*)
		FROM pg_locks
		WHERE pid = $1
		  AND locktype = 'advisory'
		  AND granted
	`, backendPID).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("reused observation backend retained %d advisory locks", held)
	}
}

func TestObservationCanceledAcquireDestroysPhysicalConnection(t *testing.T) {
	t.Parallel()
	pool := testDatabasePool(t)
	key := fmt.Sprintf("observation-canceled-acquire-%d", time.Now().UnixNano())
	blocker, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	var locked bool
	if err := blocker.QueryRow(
		t.Context(),
		`SELECT pg_try_advisory_lock(hashtextextended($1::text, 0))`,
		key,
	).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("test setup could not acquire blocking advisory lock")
	}
	defer func() {
		_, _ = blocker.Exec(
			context.Background(),
			`SELECT pg_advisory_unlock(hashtextextended($1::text, 0))`,
			key,
		)
	}()

	poolConfig, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	applicationName := fmt.Sprintf("observation-canceled-acquire-%d", time.Now().UnixNano())
	poolConfig.MaxConns = 1
	poolConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
	waiterPool, err := pgxpool.NewWithConfig(t.Context(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(waiterPool.Close)
	acquireCtx, cancelAcquire := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := NewEntityWriter(waiterPool).BeginObservation(acquireCtx, key)
		result <- err
	}()
	backendPID := waitForObservationLockWaiter(
		t,
		pool,
		applicationName,
	)
	cancelAcquire()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled observation acquire = %v", err)
	}
	assertObservationBackendTerminated(t, t.Context(), pool, backendPID)
}

func waitForObservationLockWaiter(
	t *testing.T,
	pool *pgxpool.Pool,
	applicationName string,
) uint32 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var backendPID uint32
		err := pool.QueryRow(t.Context(), `
			SELECT pid
			FROM pg_stat_activity
			WHERE application_name = $1
			  AND wait_event = 'advisory'
		`, applicationName).Scan(&backendPID)
		if err == nil {
			return backendPID
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("observation backend %q never waited on advisory lock", applicationName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertObservationBackendTerminated follows the migration-lock helper's
// hardened pattern: backend teardown is asynchronous relative to client Close,
// so poll durable server state instead of sleeping once and assuming release.
func assertObservationBackendTerminated(
	t *testing.T,
	ctx context.Context,
	pool interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	backendPID uint32,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var active bool
		var held int
		if err := pool.QueryRow(ctx, `
			SELECT
			    EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid = $1),
			    (SELECT count(*)
			     FROM pg_locks
			     WHERE pid = $1
			       AND locktype = 'advisory'
			       AND granted)
		`, backendPID).Scan(&active, &held); err != nil {
			t.Fatal(err)
		}
		if !active && held == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"observation backend %d remained active=%t with %d advisory locks after destruction",
				backendPID,
				active,
				held,
			)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
