package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func TestBeginObservationDoesNotReservePoolConnection(t *testing.T) {
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

	observation, err := NewEntityWriter(pool).BeginObservation(
		t.Context(),
		fmt.Sprintf("observation-pool-release-%d", time.Now().UnixNano()),
	)
	if err != nil {
		t.Fatal(err)
	}

	// The only pool connection remains immediately available while the token
	// is retained, which models a slow GitHub request in flight.
	var controlPlaneHealthy bool
	if err := pool.QueryRow(t.Context(), `SELECT true`).Scan(
		&controlPlaneHealthy,
	); err != nil {
		t.Fatal(err)
	}
	if !controlPlaneHealthy {
		t.Fatal("control-plane query did not complete")
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestObservationRejectsResponseAfterNewerCommit(t *testing.T) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	baseTime := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/optimistic-fence", 4200, baseTime)
	initial := storeTestPull(&repository, baseTime, "initial-head")
	if _, err := writer.ApplyPullRequest(t.Context(), initial); err != nil {
		t.Fatal(err)
	}

	key := PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, initial.Number,
	)
	staleObservation, err := writer.BeginObservation(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	newer := initial
	newer.Title = "newer committed truth"
	newer.HeadSHA = "newer-head"
	// Equal upstream timestamps are deliberately accepted when they are the
	// latest observation; the optimistic fence, not timestamp ordering, must
	// reject the response that was already in flight.
	if _, err := writer.ApplyPullRequest(t.Context(), newer); err != nil {
		t.Fatal(err)
	}

	stale := initial
	stale.Title = "stale delayed response"
	stale.HeadSHA = "stale-head"
	if _, err := writer.ApplyPullRequestObserved(
		t.Context(), staleObservation, stale, nil,
	); !errors.Is(err, ErrObservationSuperseded) {
		t.Fatalf("stale observed apply error = %v, want %v", err, ErrObservationSuperseded)
	}

	row, err := dbgen.New(pool).GetPullRequestByKey(
		t.Context(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     int32(initial.Number),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.Title != newer.Title || row.HeadSha != newer.HeadSHA {
		t.Fatalf("stale response overwrote newer row: %+v", row)
	}
}

func TestObservationVersionAdvancesOnlyOnCommit(t *testing.T) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	key := fmt.Sprintf("observation-rollback-%d", time.Now().UnixNano())
	observation, err := writer.BeginObservation(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("force rollback")
	err = writer.withEntityTx(
		t.Context(), observation, key,
		func(entityTx) error { return wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("rolled-back write error = %v, want %v", err, wantErr)
	}

	// The same token remains current because the failed transaction did not
	// advance the committed version.
	if err := writer.withEntityTx(
		t.Context(), observation, key,
		func(entityTx) error { return nil },
	); err != nil {
		t.Fatalf("reuse observation after rollback: %v", err)
	}
}

func TestObservationCloseAfterCancellationIsResourceFree(t *testing.T) {
	t.Parallel()
	pool := testDatabasePool(t)
	observation, err := NewEntityWriter(pool).BeginObservation(
		t.Context(),
		fmt.Sprintf("observation-canceled-close-%d", time.Now().UnixNano()),
	)
	if err != nil {
		t.Fatal(err)
	}
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := observation.CloseContext(canceledCtx); err != nil {
		t.Fatal(err)
	}
}
