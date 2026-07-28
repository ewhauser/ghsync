package budget

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/acme/frontier/internal/store/dbgen"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultLeaseTTL         = 30 * time.Second
	defaultRenewInterval    = 10 * time.Second
	defaultSnapshotInterval = 30 * time.Second
)

// LeaseStore coordinates the one active budgeter for an installation and
// persists periodic C-P6 state snapshots.
type LeaseStore interface {
	Acquire(context.Context, int64, string, time.Duration) (Snapshot, bool, error)
	Renew(context.Context, int64, string, time.Duration) (bool, error)
	Save(context.Context, int64, string, Snapshot) (bool, error)
	Release(context.Context, int64, string) error
}

// LeaseOptions identifies and times a per-installation budgeter lease.
type LeaseOptions struct {
	InstallationID   int64
	Owner            string
	TTL              time.Duration
	RenewInterval    time.Duration
	SnapshotInterval time.Duration
}

type leaseRuntime struct {
	store          LeaseStore
	installationID int64
	owner          string
	cancel         context.CancelFunc
	done           chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

// NewLeased acquires the Postgres-coordinated C-B1/C-O2 singleton before
// returning a usable gate. A live lease held by another owner returns
// ErrLeaseHeld.
func NewLeased(
	ctx context.Context,
	client *http.Client,
	gateOptions Options,
	store LeaseStore,
	leaseOptions LeaseOptions,
) (*Gate, error) {
	if store == nil {
		return nil, fmt.Errorf("budget lease store is required")
	}
	if leaseOptions.TTL <= 0 {
		leaseOptions.TTL = defaultLeaseTTL
	}
	if leaseOptions.RenewInterval <= 0 {
		leaseOptions.RenewInterval = min(defaultRenewInterval, leaseOptions.TTL/3)
	}
	if leaseOptions.SnapshotInterval <= 0 {
		leaseOptions.SnapshotInterval = defaultSnapshotInterval
	}
	if leaseOptions.RenewInterval <= 0 ||
		leaseOptions.RenewInterval >= leaseOptions.TTL {
		return nil, fmt.Errorf("budget lease renew interval must be shorter than TTL")
	}
	if err := validateLeaseIdentity(
		leaseOptions.InstallationID,
		leaseOptions.Owner,
		leaseOptions.TTL,
	); err != nil {
		return nil, err
	}

	persisted, acquired, err := store.Acquire(
		ctx,
		leaseOptions.InstallationID,
		leaseOptions.Owner,
		leaseOptions.TTL,
	)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrLeaseHeld
	}

	gate := New(client, gateOptions)
	gate.restore(persisted)
	leaseCtx, cancel := context.WithCancel(ctx)
	runtime := &leaseRuntime{
		store:          store,
		installationID: leaseOptions.InstallationID,
		owner:          leaseOptions.Owner,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
	gate.lease = runtime
	go gate.runLease(leaseCtx, leaseOptions)
	return gate, nil
}

// ErrLeaseHeld reports that another process owns the unexpired installation
// budgeter lease.
var ErrLeaseHeld = errors.New("GitHub installation budget lease is held")

// PostgresLeaseStore implements lease acquire/renew/steal-on-expiry and
// periodic snapshots against installation_budgets.
type PostgresLeaseStore struct {
	pool *pgxpool.Pool
}

func (g *Gate) runLease(ctx context.Context, options LeaseOptions) {
	defer close(g.lease.done)
	renew := time.NewTicker(options.RenewInterval)
	snapshot := time.NewTicker(options.SnapshotInterval)
	defer renew.Stop()
	defer snapshot.Stop()

	for {
		select {
		case <-ctx.Done():
			g.makeUnavailable(fmt.Errorf("%w: %v", ErrLeaseLost, ctx.Err()))
			return
		case <-renew.C:
			ok, err := g.lease.store.Renew(
				ctx,
				g.lease.installationID,
				g.lease.owner,
				options.TTL,
			)
			if err != nil {
				g.makeUnavailable(fmt.Errorf("%w: renew: %v", ErrLeaseLost, err))
				return
			}
			if !ok {
				g.makeUnavailable(ErrLeaseLost)
				return
			}
		case <-snapshot.C:
			ok, err := g.lease.store.Save(
				ctx,
				g.lease.installationID,
				g.lease.owner,
				g.Snapshot(),
			)
			if err != nil {
				g.makeUnavailable(fmt.Errorf("%w: save snapshot: %v", ErrLeaseLost, err))
				return
			}
			if !ok {
				g.makeUnavailable(ErrLeaseLost)
				return
			}
		}
	}
}

// Close stops admission, waits for admitted calls, writes one final snapshot,
// and releases the lease. Request-path calls never persist state (C-P6).
func (g *Gate) Close(ctx context.Context) error {
	if g.lease == nil {
		g.makeUnavailable(ErrClosed)
		return g.waitIdle(ctx)
	}

	g.lease.closeOnce.Do(func() {
		g.makeUnavailable(ErrClosed)
		g.lease.cancel()
		select {
		case <-ctx.Done():
			g.lease.closeErr = ctx.Err()
			return
		case <-g.lease.done:
		}
		if err := g.waitIdle(ctx); err != nil {
			g.lease.closeErr = err
			return
		}
		saved, saveErr := g.lease.store.Save(
			ctx,
			g.lease.installationID,
			g.lease.owner,
			g.Snapshot(),
		)
		if saveErr == nil && !saved {
			saveErr = ErrLeaseLost
		}
		releaseErr := g.lease.store.Release(
			ctx,
			g.lease.installationID,
			g.lease.owner,
		)
		g.lease.closeErr = joinErrors(saveErr, releaseErr)
	})
	return g.lease.closeErr
}

func NewPostgresLeaseStore(pool *pgxpool.Pool) *PostgresLeaseStore {
	return &PostgresLeaseStore{pool: pool}
}

func (s *PostgresLeaseStore) Acquire(
	ctx context.Context,
	installationID int64,
	owner string,
	ttl time.Duration,
) (Snapshot, bool, error) {
	if err := validateLeaseIdentity(installationID, owner, ttl); err != nil {
		return Snapshot{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("begin budget lease: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	queries := dbgen.New(tx)
	for _, class := range []Resource{REST, GraphQL} {
		if err := queries.EnsureInstallationBudget(
			ctx,
			dbgen.EnsureInstallationBudgetParams{
				InstallationID: installationID,
				Class:          string(class),
			},
		); err != nil {
			return Snapshot{}, false, fmt.Errorf("ensure budget row: %w", err)
		}
	}

	rows, err := queries.LockInstallationBudgets(
		ctx,
		dbgen.LockInstallationBudgetsParams{
			Owner:          text(owner),
			InstallationID: installationID,
		},
	)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("lock budget rows: %w", err)
	}
	state, heldByOther, err := scanBudgetRows(rows)
	if err != nil {
		return Snapshot{}, false, err
	}
	if heldByOther {
		if err := tx.Commit(ctx); err != nil {
			return Snapshot{}, false, err
		}
		return state, false, nil
	}

	affected, err := queries.AcquireInstallationBudgetLease(
		ctx,
		dbgen.AcquireInstallationBudgetLeaseParams{
			Owner:          text(owner),
			TtlSeconds:     ttl.Seconds(),
			InstallationID: installationID,
		},
	)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("acquire budget lease: %w", err)
	}
	if affected != 2 {
		return Snapshot{}, false, fmt.Errorf(
			"acquire budget lease: updated %d rows, want 2",
			affected,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, false, fmt.Errorf("commit budget lease: %w", err)
	}
	return state, true, nil
}

func (s *PostgresLeaseStore) Renew(
	ctx context.Context,
	installationID int64,
	owner string,
	ttl time.Duration,
) (bool, error) {
	if err := validateLeaseIdentity(installationID, owner, ttl); err != nil {
		return false, err
	}
	affected, err := dbgen.New(s.pool).RenewInstallationBudgetLease(
		ctx,
		dbgen.RenewInstallationBudgetLeaseParams{
			TtlSeconds:     ttl.Seconds(),
			InstallationID: installationID,
			Owner:          text(owner),
		},
	)
	if err != nil {
		return false, fmt.Errorf("renew budget lease: %w", err)
	}
	return affected == 2, nil
}

func (s *PostgresLeaseStore) Save(
	ctx context.Context,
	installationID int64,
	owner string,
	snapshot Snapshot,
) (bool, error) {
	restRemaining, restLimit, restReset := persistedValues(snapshot.REST)
	graphRemaining, graphLimit, graphReset := persistedValues(snapshot.GraphQL)
	affected, err := dbgen.New(s.pool).SaveInstallationBudgetSnapshot(
		ctx,
		dbgen.SaveInstallationBudgetSnapshotParams{
			RestRemaining:    restRemaining,
			GraphqlRemaining: graphRemaining,
			RestLimit:        restLimit,
			GraphqlLimit:     graphLimit,
			RestResetAt:      restReset,
			GraphqlResetAt:   graphReset,
			InstallationID:   installationID,
			Owner:            text(owner),
		},
	)
	if err != nil {
		return false, fmt.Errorf("save budget snapshot: %w", err)
	}
	return affected == 2, nil
}

func (s *PostgresLeaseStore) Release(
	ctx context.Context,
	installationID int64,
	owner string,
) error {
	err := dbgen.New(s.pool).ReleaseInstallationBudgetLease(
		ctx,
		dbgen.ReleaseInstallationBudgetLeaseParams{
			InstallationID: installationID,
			LeaseOwner:     text(owner),
		},
	)
	if err != nil {
		return fmt.Errorf("release budget lease: %w", err)
	}
	return nil
}

func validateLeaseIdentity(installationID int64, owner string, ttl time.Duration) error {
	if installationID <= 0 {
		return fmt.Errorf("installation ID must be positive")
	}
	if owner == "" {
		return fmt.Errorf("lease owner is required")
	}
	if ttl <= 0 {
		return fmt.Errorf("lease TTL must be positive")
	}
	return nil
}

func scanBudgetRows(
	rows []dbgen.LockInstallationBudgetsRow,
) (Snapshot, bool, error) {
	var snapshot Snapshot
	heldByOther := false
	for _, row := range rows {
		state := restoredBudget(row.Remaining, row.RateLimit, row.ResetAt)
		switch Resource(row.Class) {
		case REST:
			snapshot.REST = state
		case GraphQL:
			snapshot.GraphQL = state
		default:
			return Snapshot{}, false, fmt.Errorf("unknown persisted budget class %q", row.Class)
		}
		if row.HeldByOther {
			heldByOther = true
		}
	}
	if len(rows) != 2 {
		return Snapshot{}, false, fmt.Errorf("read %d budget rows, want 2", len(rows))
	}
	return snapshot, heldByOther, nil
}

func restoredBudget(
	remaining pgtype.Int8,
	limit pgtype.Int8,
	resetAt pgtype.Timestamptz,
) ResourceBudget {
	if !remaining.Valid || !limit.Valid || !resetAt.Valid {
		return ResourceBudget{}
	}
	return ResourceBudget{
		Known:     true,
		Remaining: remaining.Int64,
		Limit:     limit.Int64,
		ResetAt:   resetAt.Time,
	}
}

func persistedValues(
	state ResourceBudget,
) (pgtype.Int8, pgtype.Int8, pgtype.Timestamptz) {
	if !state.Known {
		return pgtype.Int8{}, pgtype.Int8{}, pgtype.Timestamptz{}
	}
	return pgtype.Int8{Int64: state.Remaining, Valid: true},
		pgtype.Int8{Int64: state.Limit, Valid: true},
		pgtype.Timestamptz{Time: state.ResetAt, Valid: true}
}

func text(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: true}
}
