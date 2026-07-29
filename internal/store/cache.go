package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/store/dbgen"
)

type SyncSource string

const (
	SyncSourceWebhook   SyncSource = "webhook"
	SyncSourceReconcile SyncSource = "reconcile"
	SyncSourceBackfill  SyncSource = "backfill"
	SyncSourceManual    SyncSource = "manual"
)

func (s SyncSource) Valid() bool {
	return s == SyncSourceWebhook || s == SyncSourceReconcile ||
		s == SyncSourceBackfill || s == SyncSourceManual
}

type RepositoryRecord struct {
	InstallationID  int64
	OrgID           int64
	GitHubID        int64
	NodeID          string
	Owner           string
	Name            string
	FullName        string
	DefaultBranch   string
	DefaultHeadSHA  string
	Archived        bool
	GitHubUpdatedAt time.Time
	ETag            string
	LastCheckedAt   time.Time
}

type ReviewCommentRecord struct {
	ID          string    `json:"id"`
	Body        string    `json:"body"`
	UpdatedAt   time.Time `json:"updated_at"`
	AuthorLogin string    `json:"author_login"`
}

type ReviewThreadRecord struct {
	ID              string
	IsResolved      bool
	IsOutdated      bool
	Path            string
	Line            *int
	Comments        []ReviewCommentRecord
	GitHubUpdatedAt time.Time
}

type PullRequestRecord struct {
	Repository      RepositoryRecord
	GitHubID        int64
	NodeID          string
	Number          int
	Title           string
	State           string
	Draft           bool
	AuthorLogin     string
	HeadRef         string
	HeadSHA         string
	BaseRef         string
	BaseSHA         string
	ReviewDecision  string
	MergeableState  string
	StackNumber     *int
	StackPosition   *int
	MembershipKnown bool
	GitHubUpdatedAt time.Time
	ReviewThreads   []ReviewThreadRecord
	ThreadsKnown    bool
	ETag            string
	SyncedAt        time.Time
	Source          SyncSource
}

type StackEntry struct {
	Number    int        `json:"number"`
	State     string     `json:"state"`
	Draft     bool       `json:"draft"`
	MergedAt  *time.Time `json:"merged_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	HeadRef   string     `json:"head_ref"`
	HeadSHA   string     `json:"head_sha"`
}

type StackRecord struct {
	Repository      RepositoryRecord
	GitHubID        int64
	NodeID          string
	Number          int
	BaseRef         string
	BaseSHA         string
	Open            bool
	Entries         []StackEntry
	GitHubUpdatedAt time.Time
	ETag            string
	SyncedAt        time.Time
	Source          SyncSource
}

type CheckRunRecord struct {
	GitHubID        int64           `json:"gh_id"`
	NodeID          string          `json:"node_id"`
	Name            string          `json:"name"`
	Status          string          `json:"status"`
	Conclusion      string          `json:"conclusion"`
	DetailsURL      string          `json:"details_url"`
	AppSlug         string          `json:"app_slug"`
	StartedAt       *time.Time      `json:"started_at"`
	CompletedAt     *time.Time      `json:"completed_at"`
	GitHubUpdatedAt *time.Time      `json:"gh_updated_at"`
	SemanticVersion string          `json:"semantic_version"`
	Observed        json.RawMessage `json:"observed"`
}

type ChecksRecord struct {
	Repository RepositoryRecord
	HeadSHA    string
	Runs       []CheckRunRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

type RepoRuleRecord struct {
	Key             string          `json:"rule_key"`
	Rule            json.RawMessage `json:"rule"`
	GitHubUpdatedAt *time.Time      `json:"gh_updated_at"`
	HeadSHA         string          `json:"head_sha"`
}

type RepoRulesRecord struct {
	Repository RepositoryRecord
	Rules      []RepoRuleRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

type ApplyPullRequestResult struct {
	Applied        bool
	DomainChanged  bool
	OldStackNumber *int
	NewStackNumber *int
	OldHeadSHA     string
	NewHeadSHA     string
}

type ApplyStackResult struct {
	Applied        bool
	JoinedPRs      []int
	LeftPRs        []int
	MovedPRs       []int
	PriorStackByPR map[int]int
}

type FetchMetadata struct {
	NodeID         string
	ETag           string
	StackNumber    *int
	StackPosition  *int
	HeadSHA        string
	RepoGitHubID   int64
	InstallationID int64
	RepoFullName   string
}

// TransactionHook runs after the accepted cache mutation, dirty marking, and
// event insert but before commit. Fetch workers use it for durable generation
// bumps and River follow-ups (C-C3).
type TransactionHook func(context.Context, pgx.Tx) error

type PullRequestHook func(ApplyPullRequestResult) TransactionHook
type StackHook func(ApplyStackResult) TransactionHook

type PullRequestApply struct {
	Record      PullRequestRecord
	Observation *Observation
	Hook        PullRequestHook
}

type PullRequestApplyOutcome struct {
	Result ApplyPullRequestResult
	Err    error
}

// Observation owns a session-level advisory lock on one dedicated connection.
// It is held from before the GitHub call until the writer transaction commits.
type Observation struct {
	conn *pgxpool.Conn
	key  string

	mu     sync.Mutex
	closed bool
}

func (o *Observation) Key() string {
	if o == nil {
		return ""
	}
	return o.key
}

func (o *Observation) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := dbgen.New(o.conn).ReleaseEntitySessionLock(ctx, o.key)
	o.conn.Release()
	if err != nil {
		return fmt.Errorf("release observation lock %s: %w", o.key, err)
	}
	return nil
}

func (o *Observation) begin(ctx context.Context) (pgx.Tx, error) {
	if o == nil || o.conn == nil {
		return nil, fmt.Errorf("observation is required")
	}
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("observation %s is closed", o.key)
	}
	return o.conn.Begin(ctx)
}

// EntityWriter owns C-C1..C-C5. Network fetches never enter this package.
type EntityWriter struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewEntityWriter(pool *pgxpool.Pool) *EntityWriter {
	if pool == nil {
		panic("entity writer requires Postgres")
	}
	return &EntityWriter{pool: pool, now: time.Now}
}

func (w *EntityWriter) BeginObservation(
	ctx context.Context,
	entityKey string,
) (*Observation, error) {
	if entityKey == "" {
		return nil, fmt.Errorf("observation entity key is required")
	}
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire observation connection: %w", err)
	}
	if err := dbgen.New(conn).AcquireEntitySessionLock(ctx, entityKey); err != nil {
		conn.Release()
		return nil, fmt.Errorf("lock observation %s: %w", entityKey, err)
	}
	return &Observation{conn: conn, key: entityKey}, nil
}

func (w *EntityWriter) PullRequestMetadata(
	ctx context.Context,
	repo string,
	number int,
) (FetchMetadata, error) {
	row, err := dbgen.New(w.pool).GetPullRequestFetchMetadata(
		ctx,
		dbgen.GetPullRequestFetchMetadataParams{
			RepoFullName: repo,
			PrNumber:     int32(number),
		},
	)
	if err != nil {
		return FetchMetadata{}, err
	}
	return FetchMetadata{
		NodeID:         row.NodeID,
		ETag:           row.Etag,
		StackNumber:    intPointer(row.StackNumber),
		StackPosition:  intPointer(row.StackPosition),
		HeadSHA:        row.HeadSha,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.RepoFullName,
	}, nil
}

func (w *EntityWriter) StackMetadata(
	ctx context.Context,
	repo string,
	number int,
) (FetchMetadata, error) {
	row, err := dbgen.New(w.pool).GetStackFetchMetadata(
		ctx,
		dbgen.GetStackFetchMetadataParams{
			RepoFullName: repo,
			StackNumber:  int32(number),
		},
	)
	if err != nil {
		return FetchMetadata{}, err
	}
	return FetchMetadata{
		ETag:           row.Etag,
		HeadSHA:        row.HeadSha,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.RepoFullName,
	}, nil
}

func (w *EntityWriter) RepoRulesMetadata(
	ctx context.Context,
	repo string,
) (FetchMetadata, int64, error) {
	row, err := dbgen.New(w.pool).GetRepoRulesFetchMetadata(ctx, repo)
	if err != nil {
		return FetchMetadata{}, 0, err
	}
	return FetchMetadata{
		ETag:           row.Etag,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.FullName,
	}, row.RepoID, nil
}

func (w *EntityWriter) Repository(
	ctx context.Context,
	fullName string,
) (RepositoryRecord, error) {
	repo, err := dbgen.New(w.pool).GetRepoByFullName(ctx, fullName)
	if err != nil {
		return RepositoryRecord{}, err
	}
	return repositoryFromRow(repo), nil
}

func (w *EntityWriter) ApplyRepository(
	ctx context.Context,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (bool, error) {
	if observedAt.IsZero() {
		observedAt = w.now()
	}
	key := RepositoryEntityKey(repository.InstallationID, repository.GitHubID)
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin repository write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return false, fmt.Errorf("lock repository: %w", err)
	}
	_, applied, err := w.applyRepositoryTx(
		ctx, queries, repository, source, etag, observedAt,
	)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit repository write: %w", err)
	}
	return applied, nil
}

func (w *EntityWriter) ApplyRepositoryObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (bool, error) {
	if observedAt.IsZero() {
		observedAt = w.now()
	}
	wantKey := RepositoryEntityKey(repository.InstallationID, repository.GitHubID)
	if err := requireObservation(observation, wantKey); err != nil {
		return false, err
	}
	tx, err := observation.begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin observed repository write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, applied, err := w.applyRepositoryTx(
		ctx, dbgen.New(tx), repository, source, etag, observedAt,
	)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit observed repository write: %w", err)
	}
	return applied, nil
}

func (w *EntityWriter) applyRepositoryTx(
	ctx context.Context,
	queries *dbgen.Queries,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (dbgen.Repo, bool, error) {
	if err := validateRepository(repository, source); err != nil {
		return dbgen.Repo{}, false, err
	}
	row, err := queries.UpsertRepositoryWriteIfNewer(
		ctx,
		dbgen.UpsertRepositoryWriteIfNewerParams{
			InstallationID: repository.InstallationID,
			OrgID:          repository.OrgID,
			GhID:           repository.GitHubID,
			NodeID:         repository.NodeID,
			Owner:          repository.Owner,
			Name:           repository.Name,
			FullName:       repository.FullName,
			DefaultBranch:  repository.DefaultBranch,
			Archived:       repository.Archived,
			GhUpdatedAt:    timestamp(repository.GitHubUpdatedAt),
			HeadSha:        repository.DefaultHeadSHA,
			SyncedAt:       timestamp(observedAt),
			LastCheckedAt:  timestamp(observedAt),
			Etag:           etag,
			SyncSource:     string(source),
		},
	)
	applied := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		row, err = queries.GetRepoByGitHubID(ctx, repository.GitHubID)
	}
	if err != nil {
		return dbgen.Repo{}, false, fmt.Errorf("upsert repository: %w", err)
	}
	if err := queries.TouchRepositoryCheckedAt(
		ctx,
		dbgen.TouchRepositoryCheckedAtParams{
			CheckedAt: timestamp(observedAt),
			GhID:      repository.GitHubID,
			Etag:      etag,
		},
	); err != nil {
		return dbgen.Repo{}, false, fmt.Errorf("touch repository: %w", err)
	}
	if err := queries.UpsertRepositoryAlias(
		ctx,
		dbgen.UpsertRepositoryAliasParams{
			FullName:   repository.FullName,
			RepoID:     row.ID,
			ObservedAt: timestamp(observedAt),
		},
	); err != nil {
		return dbgen.Repo{}, false, fmt.Errorf("upsert repository alias: %w", err)
	}
	if !applied {
		return row, false, nil
	}
	scopes, err := queries.ListRepositoryDerivationScopes(ctx, row.ID)
	if err != nil {
		return dbgen.Repo{}, false, fmt.Errorf("resolve repository scopes: %w", err)
	}
	if err := markAndEmit(
		ctx,
		queries,
		scopes,
		"repository.changed",
		RepositoryEntityKey(row.InstallationID, row.GhID),
		observedAt,
	); err != nil {
		return dbgen.Repo{}, false, err
	}
	return row, true, nil
}

func (w *EntityWriter) TouchPullRequest(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	number int,
	checkedAt time.Time,
	etag string,
) error {
	key := PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
	if err := requireObservation(observation, key); err != nil {
		return err
	}
	tx, err := observation.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	repo, err := dbgen.New(tx).GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		return fmt.Errorf("find PR repository: %w", err)
	}
	if err := dbgen.New(tx).TouchPullRequestCheckedAt(
		ctx,
		dbgen.TouchPullRequestCheckedAtParams{
			CheckedAt: timestamp(checkedAt),
			RepoID:    repo.ID,
			PrNumber:  int32(number),
			Etag:      etag,
		},
	); err != nil {
		return fmt.Errorf("touch PR checked_at: %w", err)
	}
	return tx.Commit(ctx)
}

func (w *EntityWriter) ApplyPullRequest(
	ctx context.Context,
	pull PullRequestRecord,
) (ApplyPullRequestResult, error) {
	return w.applyPullRequest(ctx, nil, pull, nil)
}

func (w *EntityWriter) ApplyPullRequestObserved(
	ctx context.Context,
	observation *Observation,
	pull PullRequestRecord,
	hook PullRequestHook,
) (ApplyPullRequestResult, error) {
	return w.applyPullRequest(ctx, observation, pull, hook)
}

func (w *EntityWriter) applyPullRequest(
	ctx context.Context,
	observation *Observation,
	pull PullRequestRecord,
	hook PullRequestHook,
) (ApplyPullRequestResult, error) {
	if err := validatePullRequest(pull); err != nil {
		return ApplyPullRequestResult{}, err
	}
	if observation == nil {
		if _, err := w.ApplyRepository(
			ctx,
			pull.Repository,
			pull.Source,
			pull.Repository.ETag,
			pull.SyncedAt,
		); err != nil {
			return ApplyPullRequestResult{}, err
		}
	}
	key := PullRequestEntityKey(
		pull.Repository.InstallationID,
		pull.Repository.GitHubID,
		pull.Number,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("begin PR write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, pull.Repository.GitHubID)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("find PR repository: %w", err)
	}
	old, oldErr := queries.GetPullRequestByIdentity(
		ctx,
		dbgen.GetPullRequestByIdentityParams{
			RepoGhID: pull.Repository.GitHubID,
			PrNumber: int32(pull.Number),
		},
	)
	if oldErr != nil && !errors.Is(oldErr, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, fmt.Errorf("read prior PR: %w", oldErr)
	}
	result := ApplyPullRequestResult{
		OldStackNumber: stackPointerFromPR(oldErr, old.StackNumber),
		OldHeadSHA:     old.HeadSha,
	}
	row, upsertErr := queries.UpsertPullRequestWriteIfNewer(
		ctx,
		dbgen.UpsertPullRequestWriteIfNewerParams{
			RepoID:          repo.ID,
			GhID:            nullableInt8(pull.GitHubID),
			NodeID:          pull.NodeID,
			PrNumber:        int32(pull.Number),
			Title:           pull.Title,
			State:           strings.ToLower(pull.State),
			Draft:           pull.Draft,
			AuthorLogin:     pull.AuthorLogin,
			HeadRef:         pull.HeadRef,
			HeadSha:         pull.HeadSHA,
			BaseRef:         pull.BaseRef,
			BaseSha:         pull.BaseSHA,
			ReviewDecision:  pull.ReviewDecision,
			MergeableState:  pull.MergeableState,
			StackNumber:     nullableInt4(pull.StackNumber),
			StackPosition:   nullableInt4(pull.StackPosition),
			MembershipKnown: pull.MembershipKnown,
			GhUpdatedAt:     timestamp(pull.GitHubUpdatedAt),
			SyncedAt:        timestamp(pull.SyncedAt),
			LastCheckedAt:   timestamp(pull.SyncedAt),
			Etag:            pull.ETag,
			SyncSource:      string(pull.Source),
		},
	)
	if upsertErr != nil && !errors.Is(upsertErr, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, fmt.Errorf("upsert PR: %w", upsertErr)
	}
	result.DomainChanged = upsertErr == nil
	if errors.Is(upsertErr, pgx.ErrNoRows) {
		current, getErr := queries.GetPullRequestByIdentity(
			ctx,
			dbgen.GetPullRequestByIdentityParams{
				RepoGhID: pull.Repository.GitHubID,
				PrNumber: int32(pull.Number),
			},
		)
		if getErr != nil {
			return ApplyPullRequestResult{}, fmt.Errorf("get discarded PR: %w", getErr)
		}
		row = dbgen.PullRequest{
			ID:             current.ID,
			RepoID:         current.RepoID,
			GhID:           current.GhID,
			NodeID:         current.NodeID,
			Number:         current.Number,
			Title:          current.Title,
			State:          current.State,
			Draft:          current.Draft,
			AuthorLogin:    current.AuthorLogin,
			HeadRef:        current.HeadRef,
			HeadSha:        current.HeadSha,
			BaseRef:        current.BaseRef,
			BaseSha:        current.BaseSha,
			ReviewDecision: current.ReviewDecision,
			MergeableState: current.MergeableState,
			StackNumber:    current.StackNumber,
			StackPosition:  current.StackPosition,
			GhUpdatedAt:    current.GhUpdatedAt,
			SyncedAt:       current.SyncedAt,
			Etag:           current.Etag,
			SyncSource:     current.SyncSource,
			TombstonedAt:   current.TombstonedAt,
			LastCheckedAt:  current.LastCheckedAt,
		}
	}
	result.NewStackNumber = intPointer(row.StackNumber)
	result.NewHeadSHA = row.HeadSha
	if err := queries.TouchPullRequestCheckedAt(
		ctx,
		dbgen.TouchPullRequestCheckedAtParams{
			CheckedAt: timestamp(pull.SyncedAt),
			RepoID:    repo.ID,
			PrNumber:  int32(pull.Number),
			Etag:      pull.ETag,
		},
	); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("touch PR: %w", err)
	}

	threadsChanged := false
	if pull.ThreadsKnown {
		threads, err := encodeReviewThreads(pull.ReviewThreads)
		if err != nil {
			return ApplyPullRequestResult{}, err
		}
		changed, err := queries.ReplaceReviewThreads(
			ctx,
			dbgen.ReplaceReviewThreadsParams{
				Threads:       threads,
				RepoID:        repo.ID,
				PrNumber:      int32(pull.Number),
				HeadSha:       row.HeadSha,
				SyncedAt:      timestamp(pull.SyncedAt),
				LastCheckedAt: timestamp(pull.SyncedAt),
				Etag:          pull.ETag,
				SyncSource:    string(pull.Source),
			},
		)
		if err != nil {
			return ApplyPullRequestResult{}, fmt.Errorf("replace review threads: %w", err)
		}
		threadsChanged = len(changed) > 0
		if err := queries.TouchReviewThreadsCheckedAt(
			ctx,
			dbgen.TouchReviewThreadsCheckedAtParams{
				CheckedAt: timestamp(pull.SyncedAt),
				RepoID:    repo.ID,
				PrNumber:  int32(pull.Number),
			},
		); err != nil {
			return ApplyPullRequestResult{}, fmt.Errorf("touch review threads: %w", err)
		}
	}
	result.Applied = result.DomainChanged || threadsChanged
	if result.Applied {
		scopes := uniqueStrings(
			derivationScope(
				pull.Repository, pull.Number, result.OldStackNumber,
			),
			derivationScope(
				pull.Repository, pull.Number, result.NewStackNumber,
			),
		)
		if err := markAndEmit(
			ctx, queries, scopes, "pull_request.changed", key, pull.SyncedAt,
		); err != nil {
			return ApplyPullRequestResult{}, err
		}
	}
	if hook != nil {
		if txHook := hook(result); txHook != nil {
			if err := txHook(ctx, tx); err != nil {
				return ApplyPullRequestResult{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("commit PR write: %w", err)
	}
	return result, nil
}

// ApplyPullRequestBatch preserves independent outcomes. Transport errors are
// handled by the caller; one poisoned entity never discards healthy siblings.
func (w *EntityWriter) ApplyPullRequestBatch(
	ctx context.Context,
	applies []PullRequestApply,
) map[string]PullRequestApplyOutcome {
	sorted := append([]PullRequestApply(nil), applies...)
	sort.Slice(sorted, func(i, j int) bool {
		return PullRequestEntityKey(
			sorted[i].Record.Repository.InstallationID,
			sorted[i].Record.Repository.GitHubID,
			sorted[i].Record.Number,
		) < PullRequestEntityKey(
			sorted[j].Record.Repository.InstallationID,
			sorted[j].Record.Repository.GitHubID,
			sorted[j].Record.Number,
		)
	})
	outcomes := make(map[string]PullRequestApplyOutcome, len(sorted))
	for _, apply := range sorted {
		key := PullRequestEntityKey(
			apply.Record.Repository.InstallationID,
			apply.Record.Repository.GitHubID,
			apply.Record.Number,
		)
		result, err := w.applyPullRequest(
			ctx, apply.Observation, apply.Record, apply.Hook,
		)
		outcomes[key] = PullRequestApplyOutcome{Result: result, Err: err}
	}
	return outcomes
}

func (w *EntityWriter) TombstonePullRequestObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	number int,
	source SyncSource,
	at time.Time,
	hook PullRequestHook,
) (ApplyPullRequestResult, error) {
	if at.IsZero() {
		at = w.now()
	}
	key := PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("begin PR tombstone: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("find tombstone repo: %w", err)
	}
	old, err := queries.GetPullRequestByIdentity(
		ctx,
		dbgen.GetPullRequestByIdentityParams{
			RepoGhID: repository.GitHubID,
			PrNumber: int32(number),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, tx.Commit(ctx)
	}
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("read tombstoned PR: %w", err)
	}
	result := ApplyPullRequestResult{
		OldStackNumber: intPointer(old.StackNumber),
		OldHeadSHA:     old.HeadSha,
	}
	row, err := queries.TombstonePullRequest(
		ctx,
		dbgen.TombstonePullRequestParams{
			TombstonedAt: timestamp(at),
			SyncedAt:     timestamp(at),
			SyncSource:   string(source),
			RepoID:       repo.ID,
			PrNumber:     int32(number),
		},
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, fmt.Errorf("tombstone PR: %w", err)
	}
	if err == nil {
		result.Applied = true
		result.DomainChanged = true
		result.NewHeadSHA = row.HeadSha
		if err := markAndEmit(
			ctx,
			queries,
			[]string{derivationScope(
				repository, number, result.OldStackNumber,
			)},
			"pull_request.tombstoned",
			key,
			at,
		); err != nil {
			return ApplyPullRequestResult{}, err
		}
	}
	if hook != nil {
		if txHook := hook(result); txHook != nil {
			if err := txHook(ctx, tx); err != nil {
				return ApplyPullRequestResult{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("commit PR tombstone: %w", err)
	}
	return result, nil
}

func (w *EntityWriter) ApplyStack(
	ctx context.Context,
	stack StackRecord,
) (ApplyStackResult, error) {
	return w.applyStack(ctx, nil, stack, nil)
}

func (w *EntityWriter) ApplyStackObserved(
	ctx context.Context,
	observation *Observation,
	stack StackRecord,
	hook StackHook,
) (ApplyStackResult, error) {
	return w.applyStack(ctx, observation, stack, hook)
}

func (w *EntityWriter) applyStack(
	ctx context.Context,
	observation *Observation,
	stack StackRecord,
	hook StackHook,
) (ApplyStackResult, error) {
	if err := validateStack(stack); err != nil {
		return ApplyStackResult{}, err
	}
	if observation == nil {
		if _, err := w.ApplyRepository(
			ctx,
			stack.Repository,
			stack.Source,
			stack.Repository.ETag,
			stack.SyncedAt,
		); err != nil {
			return ApplyStackResult{}, err
		}
	}
	key := StackEntityKey(
		stack.Repository.InstallationID,
		stack.Repository.GitHubID,
		stack.Number,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("begin stack write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, stack.Repository.GitHubID)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("find stack repository: %w", err)
	}
	var oldEntries []StackEntry
	old, oldErr := queries.GetStackByIdentity(
		ctx,
		dbgen.GetStackByIdentityParams{
			RepoGhID:    stack.Repository.GitHubID,
			StackNumber: int32(stack.Number),
		},
	)
	if oldErr == nil {
		if err := json.Unmarshal(old.Entries, &oldEntries); err != nil {
			return ApplyStackResult{}, fmt.Errorf("decode cached stack entries: %w", err)
		}
	} else if !errors.Is(oldErr, pgx.ErrNoRows) {
		return ApplyStackResult{}, fmt.Errorf("read prior stack: %w", oldErr)
	}
	entries, err := json.Marshal(stack.Entries)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("encode stack entries: %w", err)
	}
	_, upsertErr := queries.UpsertStackWriteIfNewer(
		ctx,
		dbgen.UpsertStackWriteIfNewerParams{
			RepoID:        repo.ID,
			GhID:          nullableInt8(stack.GitHubID),
			NodeID:        stack.NodeID,
			StackNumber:   int32(stack.Number),
			BaseRef:       stack.BaseRef,
			BaseSha:       stack.BaseSHA,
			Open:          stack.Open,
			Entries:       entries,
			GhUpdatedAt:   timestamp(stack.GitHubUpdatedAt),
			HeadSha:       stackHeadSHA(stack.Entries, stack.BaseSHA),
			SyncedAt:      timestamp(stack.SyncedAt),
			LastCheckedAt: timestamp(stack.SyncedAt),
			Etag:          stack.ETag,
			SyncSource:    string(stack.Source),
		},
	)
	if upsertErr != nil && !errors.Is(upsertErr, pgx.ErrNoRows) {
		return ApplyStackResult{}, fmt.Errorf("upsert stack: %w", upsertErr)
	}
	if err := queries.TouchStackCheckedAt(
		ctx,
		dbgen.TouchStackCheckedAtParams{
			CheckedAt:   timestamp(stack.SyncedAt),
			RepoID:      repo.ID,
			StackNumber: int32(stack.Number),
			Etag:        stack.ETag,
		},
	); err != nil {
		return ApplyStackResult{}, fmt.Errorf("touch stack: %w", err)
	}
	result := ApplyStackResult{}
	if upsertErr == nil {
		result = stackMembershipDiff(oldEntries, stack.Entries)
		result.Applied = true
		candidates := uniqueInts(append(
			append(append([]int(nil), result.JoinedPRs...), result.LeftPRs...),
			result.MovedPRs...,
		)...)
		if len(candidates) > 0 {
			numbers := make([]int32, 0, len(candidates))
			for _, number := range candidates {
				numbers = append(numbers, int32(number))
			}
			memberships, err := queries.ListCachedPRMemberships(
				ctx,
				dbgen.ListCachedPRMembershipsParams{
					RepoID:    repo.ID,
					PrNumbers: numbers,
				},
			)
			if err != nil {
				return ApplyStackResult{}, fmt.Errorf(
					"read prior PR memberships: %w", err,
				)
			}
			result.PriorStackByPR = make(map[int]int)
			for _, membership := range memberships {
				if membership.StackNumber.Valid &&
					int(membership.StackNumber.Int32) != stack.Number {
					result.PriorStackByPR[int(membership.Number)] =
						int(membership.StackNumber.Int32)
				}
			}
		}
		if err := markAndEmit(
			ctx, queries, []string{key}, "stack.changed", key, stack.SyncedAt,
		); err != nil {
			return ApplyStackResult{}, err
		}
	}
	if hook != nil {
		if txHook := hook(result); txHook != nil {
			if err := txHook(ctx, tx); err != nil {
				return ApplyStackResult{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyStackResult{}, fmt.Errorf("commit stack write: %w", err)
	}
	return result, nil
}

func (w *EntityWriter) TouchStack(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	number int,
	checkedAt time.Time,
	etag string,
) error {
	key := StackEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
	if err := requireObservation(observation, key); err != nil {
		return err
	}
	tx, err := observation.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	repo, err := dbgen.New(tx).GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		return err
	}
	if err := dbgen.New(tx).TouchStackCheckedAt(
		ctx,
		dbgen.TouchStackCheckedAtParams{
			CheckedAt:   timestamp(checkedAt),
			RepoID:      repo.ID,
			StackNumber: int32(number),
			Etag:        etag,
		},
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *EntityWriter) TombstoneStackObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	number int,
	source SyncSource,
	at time.Time,
	hook StackHook,
) (ApplyStackResult, error) {
	if at.IsZero() {
		at = w.now()
	}
	key := StackEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("begin stack tombstone: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("find tombstone repo: %w", err)
	}
	old, err := queries.GetStackByIdentity(
		ctx,
		dbgen.GetStackByIdentityParams{
			RepoGhID:    repository.GitHubID,
			StackNumber: int32(number),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyStackResult{}, tx.Commit(ctx)
	}
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("read tombstoned stack: %w", err)
	}
	var entries []StackEntry
	if err := json.Unmarshal(old.Entries, &entries); err != nil {
		return ApplyStackResult{}, fmt.Errorf(
			"decode tombstoned stack entries: %w", err,
		)
	}
	_, tombstoneErr := queries.TombstoneStack(
		ctx,
		dbgen.TombstoneStackParams{
			TombstonedAt: timestamp(at),
			SyncedAt:     timestamp(at),
			SyncSource:   string(source),
			RepoID:       repo.ID,
			StackNumber:  int32(number),
		},
	)
	if tombstoneErr != nil && !errors.Is(tombstoneErr, pgx.ErrNoRows) {
		return ApplyStackResult{}, fmt.Errorf("tombstone stack: %w", tombstoneErr)
	}
	result := ApplyStackResult{}
	if tombstoneErr == nil {
		result = ApplyStackResult{
			Applied:  true,
			LeftPRs:  entryNumbers(entries),
			MovedPRs: nil,
		}
		if err := markAndEmit(
			ctx, queries, []string{key}, "stack.tombstoned", key, at,
		); err != nil {
			return ApplyStackResult{}, err
		}
	}
	if hook != nil {
		if txHook := hook(result); txHook != nil {
			if err := txHook(ctx, tx); err != nil {
				return ApplyStackResult{}, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyStackResult{}, fmt.Errorf("commit stack tombstone: %w", err)
	}
	return result, nil
}

func (w *EntityWriter) ApplyChecksObserved(
	ctx context.Context,
	observation *Observation,
	checks ChecksRecord,
) (bool, error) {
	if !checks.Source.Valid() || checks.Repository.GitHubID <= 0 ||
		checks.HeadSHA == "" {
		return false, fmt.Errorf("invalid checks record")
	}
	if checks.SyncedAt.IsZero() {
		checks.SyncedAt = w.now()
	}
	key := ChecksEntityKey(
		checks.Repository.InstallationID,
		checks.Repository.GitHubID,
		checks.HeadSHA,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return false, fmt.Errorf("begin checks write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, checks.Repository.GitHubID)
	if err != nil {
		return false, fmt.Errorf("find checks repo: %w", err)
	}
	for index := range checks.Runs {
		if checks.Runs[index].SemanticVersion == "" {
			checks.Runs[index].SemanticVersion =
				checkSemanticVersion(checks.Runs[index])
		}
	}
	encoded, err := json.Marshal(checks.Runs)
	if err != nil {
		return false, fmt.Errorf("encode check runs: %w", err)
	}
	changed, err := queries.ReplaceCheckRuns(
		ctx,
		dbgen.ReplaceCheckRunsParams{
			CheckRuns:     encoded,
			RepoID:        repo.ID,
			HeadSha:       checks.HeadSHA,
			SyncedAt:      timestamp(checks.SyncedAt),
			LastCheckedAt: timestamp(checks.SyncedAt),
			Etag:          checks.ETag,
			SyncSource:    string(checks.Source),
		},
	)
	if err != nil {
		return false, fmt.Errorf("replace check runs: %w", err)
	}
	if err := queries.TouchCheckRunsCheckedAt(
		ctx,
		dbgen.TouchCheckRunsCheckedAtParams{
			CheckedAt: timestamp(checks.SyncedAt),
			RepoID:    repo.ID,
			HeadSha:   checks.HeadSHA,
		},
	); err != nil {
		return false, fmt.Errorf("touch check runs: %w", err)
	}
	if err := queries.AppendAcceptedCheckHistory(
		ctx,
		dbgen.AppendAcceptedCheckHistoryParams{
			RepoID:     repo.ID,
			HeadSha:    checks.HeadSHA,
			SyncedAt:   timestamp(checks.SyncedAt),
			Etag:       checks.ETag,
			SyncSource: string(checks.Source),
			CheckRuns:  encoded,
		},
	); err != nil {
		return false, fmt.Errorf("append accepted check history: %w", err)
	}
	if len(changed) > 0 {
		scopes, err := queries.ListPRScopesByHeadSHA(
			ctx,
			dbgen.ListPRScopesByHeadSHAParams{
				RepoFullName: checks.Repository.FullName,
				HeadSha:      checks.HeadSHA,
			},
		)
		if err != nil {
			return false, fmt.Errorf("resolve check scopes: %w", err)
		}
		scopeKeys := make([]string, 0, len(scopes))
		for _, scope := range scopes {
			repository := RepositoryRecord{
				InstallationID: scope.InstallationID,
				GitHubID:       scope.RepoGhID,
			}
			scopeKeys = append(
				scopeKeys,
				derivationScope(
					repository,
					int(scope.Number),
					intPointer(scope.StackNumber),
				),
			)
		}
		if err := markAndEmit(
			ctx,
			queries,
			uniqueStrings(scopeKeys...),
			"checks.changed",
			key,
			checks.SyncedAt,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit checks write: %w", err)
	}
	return len(changed) > 0, nil
}

func (w *EntityWriter) ApplyRepoRulesObserved(
	ctx context.Context,
	observation *Observation,
	rules RepoRulesRecord,
) (bool, error) {
	if !rules.Source.Valid() || rules.Repository.GitHubID <= 0 ||
		rules.SyncedAt.IsZero() {
		return false, fmt.Errorf("invalid repository rules record")
	}
	key := RepoRulesEntityKey(
		rules.Repository.InstallationID, rules.Repository.GitHubID,
	)
	tx, err := w.beginEntityTx(ctx, observation, key)
	if err != nil {
		return false, fmt.Errorf("begin repository rules write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, rules.Repository.GitHubID)
	if err != nil {
		return false, fmt.Errorf("find repository rules repo: %w", err)
	}
	encoded, err := json.Marshal(rules.Rules)
	if err != nil {
		return false, fmt.Errorf("encode repository rules: %w", err)
	}
	changed, err := queries.ReplaceRepoRules(
		ctx,
		dbgen.ReplaceRepoRulesParams{
			Rules:         encoded,
			RepoID:        repo.ID,
			SyncedAt:      timestamp(rules.SyncedAt),
			LastCheckedAt: timestamp(rules.SyncedAt),
			Etag:          rules.ETag,
			SyncSource:    string(rules.Source),
		},
	)
	if err != nil {
		return false, fmt.Errorf("replace repository rules: %w", err)
	}
	if err := queries.TouchRepoRulesCheckedAt(
		ctx,
		dbgen.TouchRepoRulesCheckedAtParams{
			CheckedAt: timestamp(rules.SyncedAt),
			RepoID:    repo.ID,
		},
	); err != nil {
		return false, fmt.Errorf("touch repository rules: %w", err)
	}
	if err := queries.UpsertRepoRuleSyncState(
		ctx,
		dbgen.UpsertRepoRuleSyncStateParams{
			RepoID:    repo.ID,
			Etag:      rules.ETag,
			CheckedAt: timestamp(rules.SyncedAt),
		},
	); err != nil {
		return false, fmt.Errorf("update repository rules metadata: %w", err)
	}
	if len(changed) > 0 {
		scopes, err := queries.ListRepositoryDerivationScopes(ctx, repo.ID)
		if err != nil {
			return false, fmt.Errorf("resolve repository rule scopes: %w", err)
		}
		if err := markAndEmit(
			ctx,
			queries,
			scopes,
			"repo_rules.changed",
			key,
			rules.SyncedAt,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit repository rules: %w", err)
	}
	return len(changed) > 0, nil
}

func (w *EntityWriter) TouchRepoRules(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	checkedAt time.Time,
	etag string,
) error {
	key := RepoRulesEntityKey(
		repository.InstallationID, repository.GitHubID,
	)
	if err := requireObservation(observation, key); err != nil {
		return err
	}
	tx, err := observation.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	repo, err := queries.GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		return err
	}
	if err := queries.TouchRepoRulesCheckedAt(
		ctx,
		dbgen.TouchRepoRulesCheckedAtParams{
			CheckedAt: timestamp(checkedAt),
			RepoID:    repo.ID,
		},
	); err != nil {
		return err
	}
	if err := queries.UpsertRepoRuleSyncState(
		ctx,
		dbgen.UpsertRepoRuleSyncStateParams{
			RepoID:    repo.ID,
			Etag:      etag,
			CheckedAt: timestamp(checkedAt),
		},
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *EntityWriter) BranchTargets(
	ctx context.Context,
	repoFullName string,
	branch string,
) ([]string, error) {
	queries := dbgen.New(w.pool)
	prs, err := queries.ListPRsAffectedByBranch(
		ctx,
		dbgen.ListPRsAffectedByBranchParams{
			RepoFullName: repoFullName,
			Branch:       branch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list branch PRs: %w", err)
	}
	stacks, err := queries.ListStacksAffectedByBranch(
		ctx,
		dbgen.ListStacksAffectedByBranchParams{
			RepoFullName: repoFullName,
			Branch:       branch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list branch stacks: %w", err)
	}
	targets := make([]string, 0, len(prs)+len(stacks))
	seenStacks := make(map[int]struct{}, len(stacks))
	for _, number := range stacks {
		seenStacks[int(number)] = struct{}{}
		targets = append(
			targets,
			fmt.Sprintf("stack:%s:%d", repoFullName, number),
		)
	}
	for _, pr := range prs {
		if pr.StackNumber.Valid {
			number := int(pr.StackNumber.Int32)
			if _, seen := seenStacks[number]; !seen {
				seenStacks[number] = struct{}{}
				targets = append(
					targets,
					fmt.Sprintf("stack:%s:%d", repoFullName, number),
				)
			}
			continue
		}
		targets = append(
			targets,
			fmt.Sprintf("pr:%s:%d", repoFullName, pr.Number),
		)
	}
	sort.Strings(targets)
	return targets, nil
}

func (w *EntityWriter) beginEntityTx(
	ctx context.Context,
	observation *Observation,
	key string,
) (pgx.Tx, error) {
	if observation != nil {
		if err := requireObservation(observation, key); err != nil {
			return nil, err
		}
		return observation.begin(ctx)
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := dbgen.New(tx).AcquireEntityAdvisoryLock(ctx, key); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("lock %s: %w", key, err)
	}
	return tx, nil
}

func requireObservation(observation *Observation, wantKey string) error {
	if observation == nil {
		return fmt.Errorf("observation for %s is required", wantKey)
	}
	if observation.Key() != wantKey {
		return fmt.Errorf(
			"observation key %q does not match %q",
			observation.Key(),
			wantKey,
		)
	}
	return nil
}

func markAndEmit(
	ctx context.Context,
	queries *dbgen.Queries,
	scopes []string,
	kind string,
	entityKey string,
	at time.Time,
) error {
	if len(scopes) > 0 {
		if err := queries.MarkDerivationDirty(
			ctx,
			dbgen.MarkDerivationDirtyParams{
				MarkedAt:  timestamp(at),
				ScopeKeys: scopes,
			},
		); err != nil {
			return fmt.Errorf("mark derivation dirty: %w", err)
		}
	}
	payload := []byte(`{"version":1}`)
	if _, err := queries.InsertChangeEvent(
		ctx,
		dbgen.InsertChangeEventParams{
			Stream:     "entities",
			Kind:       kind,
			EntityKey:  entityKey,
			OccurredAt: timestamp(at),
			Payload:    payload,
		},
	); err != nil {
		return fmt.Errorf("insert entity change event: %w", err)
	}
	return nil
}

func validateRepository(repository RepositoryRecord, source SyncSource) error {
	if !source.Valid() || repository.InstallationID <= 0 ||
		repository.OrgID <= 0 || repository.GitHubID <= 0 ||
		repository.Owner == "" || repository.Name == "" ||
		repository.FullName != repository.Owner+"/"+repository.Name ||
		repository.GitHubUpdatedAt.IsZero() {
		return fmt.Errorf("invalid repository record")
	}
	return nil
}

func validatePullRequest(pull PullRequestRecord) error {
	if err := validateRepository(pull.Repository, pull.Source); err != nil {
		return err
	}
	if pull.Number <= 0 || pull.GitHubID <= 0 || pull.NodeID == "" ||
		pull.HeadSHA == "" || pull.GitHubUpdatedAt.IsZero() ||
		pull.SyncedAt.IsZero() {
		return fmt.Errorf("invalid pull request record")
	}
	if pull.MembershipKnown &&
		((pull.StackNumber == nil) != (pull.StackPosition == nil)) {
		return fmt.Errorf("PR stack number and position must both be set or nil")
	}
	return nil
}

func validateStack(stack StackRecord) error {
	if err := validateRepository(stack.Repository, stack.Source); err != nil {
		return err
	}
	if stack.Number <= 0 || stack.GitHubID <= 0 ||
		stack.GitHubUpdatedAt.IsZero() || stack.SyncedAt.IsZero() {
		return fmt.Errorf("invalid stack record")
	}
	return nil
}

func repositoryFromRow(repo dbgen.Repo) RepositoryRecord {
	var updated time.Time
	if repo.GhUpdatedAt.Valid {
		updated = repo.GhUpdatedAt.Time
	}
	return RepositoryRecord{
		InstallationID:  repo.InstallationID,
		OrgID:           repo.OrgID,
		GitHubID:        repo.GhID,
		NodeID:          repo.NodeID,
		Owner:           repo.Owner,
		Name:            repo.Name,
		FullName:        repo.FullName,
		DefaultBranch:   repo.DefaultBranch,
		DefaultHeadSHA:  repo.HeadSha,
		Archived:        repo.Archived,
		GitHubUpdatedAt: updated,
		ETag:            repo.Etag,
		LastCheckedAt:   repo.LastCheckedAt.Time,
	}
}

func encodeReviewThreads(threads []ReviewThreadRecord) ([]byte, error) {
	type encodedThread struct {
		ID              string                `json:"id"`
		IsResolved      bool                  `json:"is_resolved"`
		IsOutdated      bool                  `json:"is_outdated"`
		Path            string                `json:"path"`
		Line            *int                  `json:"line"`
		Comments        []ReviewCommentRecord `json:"comments"`
		GitHubUpdatedAt time.Time             `json:"gh_updated_at"`
	}
	encoded := make([]encodedThread, 0, len(threads))
	for _, thread := range threads {
		encoded = append(encoded, encodedThread{
			ID:              thread.ID,
			IsResolved:      thread.IsResolved,
			IsOutdated:      thread.IsOutdated,
			Path:            thread.Path,
			Line:            thread.Line,
			Comments:        thread.Comments,
			GitHubUpdatedAt: thread.GitHubUpdatedAt,
		})
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode review threads: %w", err)
	}
	return value, nil
}

func stackMembershipDiff(
	oldEntries []StackEntry,
	newEntries []StackEntry,
) ApplyStackResult {
	oldPositions := make(map[int]int, len(oldEntries))
	newPositions := make(map[int]int, len(newEntries))
	for index, entry := range oldEntries {
		oldPositions[entry.Number] = index + 1
	}
	for index, entry := range newEntries {
		newPositions[entry.Number] = index + 1
	}
	var result ApplyStackResult
	for number, newPosition := range newPositions {
		oldPosition, existed := oldPositions[number]
		if !existed {
			result.JoinedPRs = append(result.JoinedPRs, number)
		} else if oldPosition != newPosition {
			result.MovedPRs = append(result.MovedPRs, number)
		}
	}
	for number := range oldPositions {
		if _, remains := newPositions[number]; !remains {
			result.LeftPRs = append(result.LeftPRs, number)
		}
	}
	sort.Ints(result.JoinedPRs)
	sort.Ints(result.LeftPRs)
	sort.Ints(result.MovedPRs)
	return result
}

func entryNumbers(entries []StackEntry) []int {
	numbers := make([]int, 0, len(entries))
	for _, entry := range entries {
		numbers = append(numbers, entry.Number)
	}
	sort.Ints(numbers)
	return numbers
}

func stackHeadSHA(entries []StackEntry, fallback string) string {
	if len(entries) == 0 {
		return fallback
	}
	return entries[len(entries)-1].HeadSHA
}

func checkSemanticVersion(run CheckRunRecord) string {
	type semanticCheck struct {
		Status      string     `json:"status"`
		Conclusion  string     `json:"conclusion"`
		DetailsURL  string     `json:"details_url"`
		AppSlug     string     `json:"app_slug"`
		StartedAt   *time.Time `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at"`
	}
	encoded, _ := json.Marshal(semanticCheck{
		Status:      run.Status,
		Conclusion:  run.Conclusion,
		DetailsURL:  run.DetailsURL,
		AppSlug:     run.AppSlug,
		StartedAt:   run.StartedAt,
		CompletedAt: run.CompletedAt,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func RepositoryEntityKey(installationID, repositoryGitHubID int64) string {
	return fmt.Sprintf("repo:%d:%d", installationID, repositoryGitHubID)
}

func RepositoryDiscoveryKey(installationID int64, fullName string) string {
	return fmt.Sprintf("repo-discovery:%d:%s", installationID, fullName)
}

func PullRequestEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return fmt.Sprintf(
		"pr:%d:%d:%d",
		installationID,
		repositoryGitHubID,
		number,
	)
}

func StackEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return fmt.Sprintf(
		"stack:%d:%d:%d",
		installationID,
		repositoryGitHubID,
		number,
	)
}

func ChecksEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	sha string,
) string {
	return fmt.Sprintf(
		"checks:%d:%d:%s",
		installationID,
		repositoryGitHubID,
		sha,
	)
}

func RepoRulesEntityKey(
	installationID int64,
	repositoryGitHubID int64,
) string {
	return fmt.Sprintf(
		"repo_rules:%d:%d",
		installationID,
		repositoryGitHubID,
	)
}

func derivationScope(
	repository RepositoryRecord,
	number int,
	stackNumber *int,
) string {
	if stackNumber != nil {
		return StackEntityKey(
			repository.InstallationID, repository.GitHubID, *stackNumber,
		)
	}
	return PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
}

func uniqueStrings(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func uniqueInts(values ...int) []int {
	seen := make(map[int]struct{}, len(values))
	result := make([]int, 0, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Ints(result)
	return result
}

func intPointer(value pgtype.Int4) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int32)
	return &result
}

func stackPointerFromPR(err error, value pgtype.Int4) *int {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return intPointer(value)
}

func nullableInt4(value *int) pgtype.Int4 {
	if value == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*value), Valid: true}
}

func nullableInt8(value int64) pgtype.Int8 {
	return pgtype.Int8{Int64: value, Valid: value > 0}
}

func timestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: !value.IsZero()}
}
