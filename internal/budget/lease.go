package budget

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ewhauser/ghsync/internal/store/dbgen"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultLeaseTTL         = 30 * time.Second
	defaultRenewInterval    = 10 * time.Second
	defaultSnapshotInterval = 30 * time.Second
	defaultStoreTimeout     = 5 * time.Second
)

// LeaseStore coordinates the one active budgeter for an installation and
// persists periodic C-P6 state snapshots. Acquire and Renew return Postgres's
// authoritative lease expiry; callers must never derive it from local time.
// Acquire false means the store proved that another unexpired owner holds the
// lease. For Renew, Save, and SaveBackoff, false means the store has proven
// that the caller no longer owns the lease. Transport failures must always be
// returned as errors; returning false for a transport failure violates this
// contract. Release returns any cleanup failure rather than translating it
// into ownership loss.
type LeaseStore interface {
	Acquire(
		context.Context,
		int64,
		string,
		time.Duration,
	) (Snapshot, time.Time, bool, error)
	Renew(
		context.Context,
		int64,
		string,
		time.Duration,
	) (time.Time, bool, error)
	Save(context.Context, int64, string, Snapshot) (bool, error)
	SaveBackoff(context.Context, int64, string, time.Time) (bool, error)
	Release(context.Context, int64, string) error
}

// LeaseOptions identifies and times a per-installation budgeter lease. Owner
// is a diagnostic process name only; an unguessable per-runtime token is the
// actual database ownership predicate.
type LeaseOptions struct {
	InstallationID   int64
	Owner            string
	TTL              time.Duration
	RenewInterval    time.Duration
	SnapshotInterval time.Duration
	StoreTimeout     time.Duration
	Clock            Clock
}

type leaseRuntime struct {
	store          LeaseStore
	installationID int64
	owner          string
	token          string
	ttl            time.Duration
	renewInterval  time.Duration
	snapshotEvery  time.Duration
	storeTimeout   time.Duration
	clock          Clock
	cancel         context.CancelFunc
	done           chan struct{}
	expiryUpdates  chan time.Time
	closeOnce      sync.Once
	closeErr       error

	mu          sync.Mutex
	leaseExpiry time.Time
}

// NewLeased acquires the Postgres-coordinated C-B1/C-O2 singleton before
// returning a usable gate. A live lease held by another runtime returns
// ErrLeaseHeld.
func NewLeased(
	ctx context.Context,
	client *http.Client,
	gateOptions Options, //nolint:gocritic // value options are normalized before the gate owns them
	store LeaseStore,
	leaseOptions LeaseOptions,
) (*Gate, error) {
	if ctx == nil {
		return nil, fmt.Errorf("budget lease: nil context")
	}
	if store == nil {
		return nil, fmt.Errorf("budget lease store is required")
	}
	if err := normalizeLeaseOptions(&leaseOptions, gateOptions.Clock); err != nil {
		return nil, err
	}
	token, err := newLeaseToken()
	if err != nil {
		return nil, fmt.Errorf("generate budget lease token: %w", err)
	}

	acquireCtx, cancelAcquire := context.WithTimeout(ctx, leaseOptions.StoreTimeout)
	persisted, leaseUntil, acquired, err := store.Acquire(
		acquireCtx,
		leaseOptions.InstallationID,
		token,
		leaseOptions.TTL,
	)
	cancelAcquire()
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrLeaseHeld
	}
	if leaseUntil.IsZero() || !leaseOptions.Clock.Now().Before(leaseUntil) {
		releaseCtx, cancelRelease := context.WithTimeout(
			context.WithoutCancel(ctx),
			leaseOptions.StoreTimeout,
		)
		_ = store.Release(releaseCtx, leaseOptions.InstallationID, token)
		cancelRelease()
		return nil, fmt.Errorf("%w: acquired lease has expired", ErrLeaseLost)
	}

	gateOptions.Clock = leaseOptions.Clock
	gate := New(client, gateOptions)
	gate.restore(&persisted)
	leaseCtx, cancel := context.WithCancel(ctx)
	runtime := &leaseRuntime{
		store:          store,
		installationID: leaseOptions.InstallationID,
		owner:          leaseOptions.Owner,
		token:          token,
		ttl:            leaseOptions.TTL,
		renewInterval:  leaseOptions.RenewInterval,
		snapshotEvery:  leaseOptions.SnapshotInterval,
		storeTimeout:   leaseOptions.StoreTimeout,
		clock:          leaseOptions.Clock,
		cancel:         cancel,
		done:           make(chan struct{}),
		expiryUpdates:  make(chan time.Time, 1),
		leaseExpiry:    leaseUntil,
	}
	gate.lease = runtime
	gate.setLeaseUntil(leaseUntil)
	go gate.runLease(leaseCtx, runtime)
	return gate, nil
}

// ErrLeaseHeld reports that another process owns the unexpired installation
// budgeter lease.
var ErrLeaseHeld = errors.New("GitHub installation budget lease is held")

// PostgresLeaseStore implements atomic lease acquire/renew/steal-on-expiry and
// periodic snapshots against installation_budgets.
type PostgresLeaseStore struct {
	pool *pgxpool.Pool
}

func normalizeLeaseOptions(options *LeaseOptions, gateClock Clock) error {
	if options.TTL <= 0 {
		options.TTL = defaultLeaseTTL
	}
	if options.RenewInterval <= 0 {
		options.RenewInterval = min(defaultRenewInterval, options.TTL/3)
	}
	if options.SnapshotInterval <= 0 {
		options.SnapshotInterval = defaultSnapshotInterval
	}
	if options.StoreTimeout <= 0 {
		options.StoreTimeout = min(defaultStoreTimeout, options.TTL/4)
	}
	if options.Clock == nil {
		options.Clock = gateClock
	}
	if options.Clock == nil {
		options.Clock = realClock{}
	}
	if options.InstallationID <= 0 {
		return fmt.Errorf("installation ID must be positive")
	}
	if options.Owner == "" {
		return fmt.Errorf("lease owner name is required")
	}
	if options.RenewInterval <= 0 || options.RenewInterval >= options.TTL {
		return fmt.Errorf("budget lease renew interval must be shorter than TTL")
	}
	if options.StoreTimeout <= 0 || options.StoreTimeout >= options.TTL {
		return fmt.Errorf("budget lease store timeout must be shorter than TTL")
	}
	return nil
}

func newLeaseToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func (g *Gate) runLease(ctx context.Context, runtime *leaseRuntime) {
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		g.maintainLease(ctx, runtime)
	}()
	go func() {
		defer workers.Done()
		g.watchLeaseExpiry(ctx, runtime)
	}()
	workers.Wait()
	close(runtime.done)
}

func (g *Gate) maintainLease(
	ctx context.Context,
	runtime *leaseRuntime,
) {
	renew := newClockTimer(
		runtime.clock,
		runtime.clock.Now().Add(runtime.renewInterval),
	)
	snapshot := newClockTimer(
		runtime.clock,
		runtime.clock.Now().Add(runtime.snapshotEvery),
	)
	defer func() {
		renew.stop()
		snapshot.stop()
	}()

	for {
		select {
		case <-ctx.Done():
			if !errors.Is(g.unavailableError(), ErrClosed) {
				g.loseLease(fmt.Errorf("%w: %w", ErrLeaseLost, ctx.Err()))
			}
			return
		case <-renew.channel:
			until, ok, err := runtime.renew(ctx)
			switch {
			case err != nil:
				// A transient failure does not invent lease loss. The independent
				// watchdog still closes exactly at the last confirmed expiry.
				renew = resetClockTimer(
					renew,
					runtime.clock,
					runtime.clock.Now().Add(runtime.renewRetryDelay()),
				)
			case !ok || until.IsZero() || !runtime.clock.Now().Before(until):
				g.loseLease(ErrLeaseLost)
				return
			default:
				runtime.confirmExpiry(until)
				g.setLeaseUntil(until)
				runtime.publishExpiry(until)
				renew = resetClockTimer(
					renew,
					runtime.clock,
					runtime.clock.Now().Add(runtime.renewInterval),
				)
			}
		case <-snapshot.channel:
			current := g.Snapshot()
			ok, err := runtime.save(ctx, &current)
			if err == nil && !ok {
				g.loseLease(ErrLeaseLost)
				return
			}
			// Snapshot transport errors do not disprove ownership; renewal and
			// the expiry watchdog remain the ownership authority.
			snapshot = resetClockTimer(
				snapshot,
				runtime.clock,
				runtime.clock.Now().Add(runtime.snapshotEvery),
			)
		}
	}
}

func (g *Gate) watchLeaseExpiry(
	ctx context.Context,
	runtime *leaseRuntime,
) {
	expiry := runtime.currentExpiry()
	timer := newClockTimer(runtime.clock, expiry)
	defer func() {
		timer.stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case until := <-runtime.expiryUpdates:
			expiry = until
			timer = resetClockTimer(timer, runtime.clock, expiry)
		case <-timer.channel:
			g.loseLease(ErrLeaseLost)
			return
		}
	}
}

type clockTimer struct {
	channel <-chan time.Time
	stop    func() bool
}

func newClockTimer(clock Clock, deadline time.Time) clockTimer {
	channel, stop := clock.NewTimerAt(deadline)
	return clockTimer{channel: channel, stop: stop}
}

func resetClockTimer(
	current clockTimer,
	clock Clock,
	deadline time.Time,
) clockTimer {
	current.stop()
	return newClockTimer(clock, deadline)
}

func (r *leaseRuntime) renew(
	parent context.Context,
) (time.Time, bool, error) {
	ctx, cancel := context.WithTimeout(parent, r.storeTimeout)
	defer cancel()
	return r.store.Renew(ctx, r.installationID, r.token, r.ttl)
}

func (r *leaseRuntime) save(
	parent context.Context,
	snapshot *Snapshot,
) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, r.storeTimeout)
	defer cancel()
	return r.store.Save(ctx, r.installationID, r.token, *snapshot)
}

func (r *leaseRuntime) renewRetryDelay() time.Duration {
	remaining := r.currentExpiry().Sub(r.clock.Now())
	if remaining <= 0 {
		return 0
	}
	return min(r.renewInterval/2, remaining/2)
}

func (r *leaseRuntime) confirmExpiry(until time.Time) {
	r.mu.Lock()
	r.leaseExpiry = until
	r.mu.Unlock()
}

func (r *leaseRuntime) currentExpiry() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaseExpiry
}

func (r *leaseRuntime) publishExpiry(until time.Time) {
	select {
	case r.expiryUpdates <- until:
	default:
		select {
		case <-r.expiryUpdates:
		default:
		}
		r.expiryUpdates <- until
	}
}

func (g *Gate) unavailableError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unavailable
}

func (g *Gate) persistBackoff(
	ctx context.Context,
	until time.Time,
) (bool, error) {
	runtime := g.lease
	opCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		runtime.storeTimeout,
	)
	defer cancel()
	ok, err := runtime.store.SaveBackoff(
		opCtx,
		runtime.installationID,
		runtime.token,
		until,
	)
	return ok, err
}

// Close stops admission, keeps renewal alive while admitted calls drain, then
// snapshots and releases. If the caller's drain deadline expires, stragglers
// are canceled before bounded cleanup continues (C-B1/C-B6).
func (g *Gate) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("budget gate close: nil context")
	}
	if g.lease == nil {
		g.stopAdmission(ErrClosed)
		if err := g.waitIdle(ctx); err != nil {
			g.cancelAdmissions()
			return err
		}
		return nil
	}

	g.lease.closeOnce.Do(func() {
		runtime := g.lease
		g.stopAdmission(ErrClosed)

		drainErr := g.waitIdle(ctx)
		if drainErr != nil {
			g.cancelAdmissions()
			waitCtx, cancelWait := context.WithTimeout(
				context.WithoutCancel(ctx),
				runtime.storeTimeout,
			)
			_ = g.waitIdle(waitCtx)
			cancelWait()
		}

		// Renewal stays live through the entire drain. Only now may maintenance
		// stop, immediately before the final snapshot and token-checked release.
		runtime.cancel()
		stopCtx, cancelStop := context.WithTimeout(
			context.WithoutCancel(ctx),
			runtime.storeTimeout,
		)
		var stopErr error
		select {
		case <-runtime.done:
		case <-stopCtx.Done():
			stopErr = stopCtx.Err()
		}
		cancelStop()
		if stopErr != nil {
			runtime.closeErr = joinErrors(drainErr, stopErr)
			return
		}

		snapshotCtx, cancelSnapshot := context.WithTimeout(
			context.WithoutCancel(ctx),
			runtime.storeTimeout,
		)
		saved, saveErr := runtime.store.Save(
			snapshotCtx,
			runtime.installationID,
			runtime.token,
			g.Snapshot(),
		)
		cancelSnapshot()
		if saveErr == nil && !saved {
			saveErr = ErrLeaseLost
		}

		releaseCtx, cancelRelease := context.WithTimeout(
			context.WithoutCancel(ctx),
			runtime.storeTimeout,
		)
		releaseErr := runtime.store.Release(
			releaseCtx,
			runtime.installationID,
			runtime.token,
		)
		cancelRelease()
		runtime.closeErr = joinErrors(drainErr, saveErr, releaseErr)
	})
	return g.lease.closeErr
}

// NewPostgresLeaseStore constructs a Postgres-backed lease store.
func NewPostgresLeaseStore(pool *pgxpool.Pool) *PostgresLeaseStore {
	return &PostgresLeaseStore{pool: pool}
}

// Acquire obtains or steals an expired installation lease and returns its
// authoritative persisted snapshot.
func (s *PostgresLeaseStore) Acquire(
	ctx context.Context,
	installationID int64,
	token string,
	ttl time.Duration,
) (Snapshot, time.Time, bool, error) {
	if err := validateLeaseIdentity(installationID, token, ttl); err != nil {
		return Snapshot{}, time.Time{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Snapshot{}, time.Time{}, false, fmt.Errorf("begin budget lease: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op
	if err := setLeaseTransactionTimeouts(ctx, tx, ttl); err != nil {
		return Snapshot{}, time.Time{}, false, err
	}

	queries := dbgen.New(tx)
	for _, class := range []Resource{REST, GraphQL} {
		if err := queries.EnsureInstallationBudget(
			ctx,
			dbgen.EnsureInstallationBudgetParams{
				InstallationID: installationID,
				Class:          string(class),
			},
		); err != nil {
			return Snapshot{}, time.Time{}, false, fmt.Errorf("ensure budget row: %w", err)
		}
	}

	rows, err := queries.AcquireInstallationBudgetLease(
		ctx,
		dbgen.AcquireInstallationBudgetLeaseParams{
			LeaseToken:     token,
			InstallationID: installationID,
			TtlSeconds:     ttl.Seconds(),
		},
	)
	if err != nil {
		return Snapshot{}, time.Time{}, false, fmt.Errorf("acquire budget lease: %w", err)
	}
	if len(rows) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return Snapshot{}, time.Time{}, false, err
		}
		return Snapshot{}, time.Time{}, false, nil
	}
	state, leaseUntil, err := scanAcquiredBudgetRows(rows)
	if err != nil {
		return Snapshot{}, time.Time{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, time.Time{}, false, fmt.Errorf("commit budget lease: %w", err)
	}
	return state, leaseUntil, true, nil
}

func setLeaseTransactionTimeouts(
	ctx context.Context,
	tx pgx.Tx,
	ttl time.Duration,
) error {
	timeout := ttl / 3
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	timeout = max(time.Millisecond, timeout)
	value := fmt.Sprintf("%dms", timeout.Milliseconds())
	if _, err := tx.Exec(
		ctx,
		`SELECT set_config('lock_timeout', $1, true),
		        set_config('statement_timeout', $1, true)`,
		value,
	); err != nil {
		return fmt.Errorf("set budget lease transaction timeouts: %w", err)
	}
	return nil
}

// Renew extends a lease only when token still proves ownership.
func (s *PostgresLeaseStore) Renew(
	ctx context.Context,
	installationID int64,
	token string,
	ttl time.Duration,
) (time.Time, bool, error) {
	if err := validateLeaseIdentity(installationID, token, ttl); err != nil {
		return time.Time{}, false, err
	}
	rows, err := dbgen.New(s.pool).RenewInstallationBudgetLease(
		ctx,
		dbgen.RenewInstallationBudgetLeaseParams{
			InstallationID: installationID,
			LeaseToken:     token,
			TtlSeconds:     ttl.Seconds(),
		},
	)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("renew budget lease: %w", err)
	}
	until, ok, err := authoritativeExpiry(rows)
	return until, ok, err
}

// Save persists one budget snapshot only for the active owner.
func (s *PostgresLeaseStore) Save(
	ctx context.Context,
	installationID int64,
	token string,
	snapshot Snapshot, //nolint:gocritic // LeaseStore intentionally accepts immutable snapshot values
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
			BackoffUntil:     timestamp(snapshot.BackoffUntil),
			InstallationID:   installationID,
			LeaseToken:       token,
		},
	)
	if err != nil {
		return false, fmt.Errorf("save budget snapshot: %w", err)
	}
	return affected == 2, nil
}

// SaveBackoff immediately persists a secondary-limit deadline.
func (s *PostgresLeaseStore) SaveBackoff(
	ctx context.Context,
	installationID int64,
	token string,
	until time.Time,
) (bool, error) {
	affected, err := dbgen.New(s.pool).SaveInstallationBudgetBackoff(
		ctx,
		dbgen.SaveInstallationBudgetBackoffParams{
			BackoffUntil:   timestamp(until),
			InstallationID: installationID,
			LeaseToken:     token,
		},
	)
	if err != nil {
		return false, fmt.Errorf("save budget backoff: %w", err)
	}
	return affected == 2, nil
}

// Release clears an installation lease only for the active owner.
func (s *PostgresLeaseStore) Release(
	ctx context.Context,
	installationID int64,
	token string,
) error {
	affected, err := dbgen.New(s.pool).ReleaseInstallationBudgetLease(
		ctx,
		dbgen.ReleaseInstallationBudgetLeaseParams{
			InstallationID: installationID,
			LeaseToken:     token,
		},
	)
	if err != nil {
		return fmt.Errorf("release budget lease: %w", err)
	}
	if affected != 2 {
		return ErrLeaseLost
	}
	return nil
}

func validateLeaseIdentity(installationID int64, token string, ttl time.Duration) error {
	if installationID <= 0 {
		return fmt.Errorf("installation ID must be positive")
	}
	if token == "" {
		return fmt.Errorf("lease token is required")
	}
	if ttl <= 0 {
		return fmt.Errorf("lease TTL must be positive")
	}
	return nil
}

func scanAcquiredBudgetRows(
	rows []dbgen.AcquireInstallationBudgetLeaseRow,
) (Snapshot, time.Time, error) {
	if len(rows) != 2 {
		return Snapshot{}, time.Time{}, fmt.Errorf(
			"acquire budget lease: updated %d rows, want 2",
			len(rows),
		)
	}
	var snapshot Snapshot
	var expiries []pgtype.Timestamptz
	for index := range rows {
		row := &rows[index]
		state := restoredBudget(row.Remaining, row.RateLimit, row.ResetAt)
		switch Resource(row.Class) {
		case REST:
			snapshot.REST = state
		case GraphQL:
			snapshot.GraphQL = state
		default:
			return Snapshot{}, time.Time{}, fmt.Errorf(
				"unknown persisted budget class %q",
				row.Class,
			)
		}
		if row.BackoffUntil.Valid &&
			row.BackoffUntil.Time.After(snapshot.BackoffUntil) {
			snapshot.BackoffUntil = row.BackoffUntil.Time
		}
		expiries = append(expiries, row.LeaseUntil)
	}
	until, ok, err := authoritativeExpiry(expiries)
	if err != nil || !ok {
		return Snapshot{}, time.Time{}, fmt.Errorf(
			"acquire budget lease expiry: %w",
			err,
		)
	}
	return snapshot, until, nil
}

func authoritativeExpiry(
	values []pgtype.Timestamptz,
) (time.Time, bool, error) {
	if len(values) == 0 {
		return time.Time{}, false, nil
	}
	if len(values) != 2 || !values[0].Valid || !values[1].Valid {
		return time.Time{}, false, fmt.Errorf(
			"lease expiry rows = %d, want two valid rows",
			len(values),
		)
	}
	if !values[0].Time.Equal(values[1].Time) {
		return time.Time{}, false, fmt.Errorf(
			"inconsistent lease expiries %v and %v",
			values[0].Time,
			values[1].Time,
		)
	}
	return values[0].Time, true, nil
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

func timestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: !value.IsZero()}
}
