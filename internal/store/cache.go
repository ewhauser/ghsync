package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	GitHubUpdatedAt time.Time       `json:"gh_updated_at"`
	Observed        json.RawMessage `json:"observed"`
}

type ChecksRecord struct {
	RepoFullName string
	HeadSHA      string
	Runs         []CheckRunRecord
	ETag         string
	SyncedAt     time.Time
	Source       SyncSource
}

type ApplyPullRequestResult struct {
	Applied        bool
	OldStackNumber *int
	NewStackNumber *int
	OldHeadSHA     string
	NewHeadSHA     string
}

type ApplyStackResult struct {
	Applied        bool
	JoinedPRs      []int
	LeftPRs        []int
	PriorStackByPR map[int]int
}

type FetchMetadata struct {
	NodeID        string
	ETag          string
	StackNumber   *int
	StackPosition *int
	HeadSHA       string
}

// EntityWriter owns C-C1..C-C5. Network fetches never enter this package;
// callers hand it an observed authoritative version.
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
		NodeID:        row.NodeID,
		ETag:          row.Etag,
		StackNumber:   intPointer(row.StackNumber),
		StackPosition: intPointer(row.StackPosition),
		HeadSHA:       row.HeadSha,
	}, nil
}

func (w *EntityWriter) StackETag(
	ctx context.Context,
	repo string,
	number int,
) (string, error) {
	row, err := dbgen.New(w.pool).GetStackFetchMetadata(
		ctx,
		dbgen.GetStackFetchMetadataParams{
			RepoFullName: repo,
			StackNumber:  int32(number),
		},
	)
	if err != nil {
		return "", err
	}
	return row.Etag, nil
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
	syncedAt time.Time,
) (bool, error) {
	if syncedAt.IsZero() {
		syncedAt = w.now()
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin repository write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	if err := queries.AcquireEntityAdvisoryLock(ctx, "repo:"+repository.FullName); err != nil {
		return false, fmt.Errorf("lock repository: %w", err)
	}
	_, applied, err := upsertRepository(
		ctx,
		queries,
		repository,
		source,
		etag,
		syncedAt,
	)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit repository write: %w", err)
	}
	return applied, nil
}

func (w *EntityWriter) ApplyPullRequest(
	ctx context.Context,
	pull PullRequestRecord,
) (ApplyPullRequestResult, error) {
	if err := validatePullRequest(pull); err != nil {
		return ApplyPullRequestResult{}, err
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("begin PR write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	key := pullRequestKey(pull.Repository.FullName, pull.Number)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("lock %s: %w", key, err)
	}
	repo, _, err := upsertRepository(
		ctx,
		queries,
		pull.Repository,
		pull.Source,
		"",
		pull.SyncedAt,
	)
	if err != nil {
		return ApplyPullRequestResult{}, err
	}

	old, oldErr := queries.GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: pull.Repository.FullName,
			PrNumber:     int32(pull.Number),
		},
	)
	if oldErr != nil && !errors.Is(oldErr, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, fmt.Errorf("read prior PR: %w", oldErr)
	}
	result := ApplyPullRequestResult{
		OldStackNumber: stackPointerFromPR(oldErr, old.StackNumber),
		OldHeadSHA:     old.HeadSha,
	}
	row, err := queries.UpsertPullRequestWriteIfNewer(
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
			Etag:            pull.ETag,
			SyncSource:      string(pull.Source),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return ApplyPullRequestResult{}, fmt.Errorf("commit discarded PR write: %w", err)
		}
		return result, nil
	}
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("upsert PR: %w", err)
	}
	result.Applied = true
	result.NewStackNumber = intPointer(row.StackNumber)
	result.NewHeadSHA = row.HeadSha

	if pull.ThreadsKnown {
		threads, err := encodeReviewThreads(pull.ReviewThreads)
		if err != nil {
			return ApplyPullRequestResult{}, err
		}
		if _, err := queries.ReplaceReviewThreads(
			ctx,
			dbgen.ReplaceReviewThreadsParams{
				Threads:    threads,
				RepoID:     repo.ID,
				PrNumber:   int32(pull.Number),
				HeadSha:    row.HeadSha,
				SyncedAt:   timestamp(pull.SyncedAt),
				Etag:       pull.ETag,
				SyncSource: string(pull.Source),
			},
		); err != nil {
			return ApplyPullRequestResult{}, fmt.Errorf("replace review threads: %w", err)
		}
	}
	scopes := uniqueStrings(
		derivationScope(pull.Repository.FullName, pull.Number, result.OldStackNumber),
		derivationScope(pull.Repository.FullName, pull.Number, result.NewStackNumber),
	)
	if err := markAndEmit(
		ctx,
		queries,
		scopes,
		"pull_request.changed",
		key,
		pull.SyncedAt,
	); err != nil {
		return ApplyPullRequestResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("commit PR write: %w", err)
	}
	return result, nil
}

// ApplyPullRequestBatch is the shared coordinator's C-P4 apply seam. Sorting
// here makes the real advisory-lock acquisition order deterministic even when
// GraphQL returns nodes in an arbitrary order.
func (w *EntityWriter) ApplyPullRequestBatch(
	ctx context.Context,
	pulls []PullRequestRecord,
) (map[string]ApplyPullRequestResult, error) {
	sorted := append([]PullRequestRecord(nil), pulls...)
	sort.Slice(sorted, func(i, j int) bool {
		return pullRequestKey(sorted[i].Repository.FullName, sorted[i].Number) <
			pullRequestKey(sorted[j].Repository.FullName, sorted[j].Number)
	})
	results := make(map[string]ApplyPullRequestResult, len(sorted))
	for _, pull := range sorted {
		result, err := w.ApplyPullRequest(ctx, pull)
		if err != nil {
			return nil, err
		}
		results[pullRequestKey(pull.Repository.FullName, pull.Number)] = result
	}
	return results, nil
}

func (w *EntityWriter) TombstonePullRequest(
	ctx context.Context,
	repoFullName string,
	number int,
	source SyncSource,
	at time.Time,
) (ApplyPullRequestResult, error) {
	if at.IsZero() {
		at = w.now()
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("begin PR tombstone: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	key := pullRequestKey(repoFullName, number)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("lock %s: %w", key, err)
	}
	repo, err := queries.GetRepoByFullName(ctx, repoFullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyPullRequestResult{}, tx.Commit(ctx)
	}
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("find tombstone repo: %w", err)
	}
	old, err := queries.GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repoFullName,
			PrNumber:     int32(number),
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
	if errors.Is(err, pgx.ErrNoRows) {
		return result, tx.Commit(ctx)
	}
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("tombstone PR: %w", err)
	}
	result.Applied = true
	result.NewHeadSHA = row.HeadSha
	if err := markAndEmit(
		ctx,
		queries,
		[]string{derivationScope(repoFullName, number, result.OldStackNumber)},
		"pull_request.tombstoned",
		key,
		at,
	); err != nil {
		return ApplyPullRequestResult{}, err
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
	if err := validateStack(stack); err != nil {
		return ApplyStackResult{}, err
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("begin stack write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	key := stackKey(stack.Repository.FullName, stack.Number)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return ApplyStackResult{}, fmt.Errorf("lock %s: %w", key, err)
	}
	repo, _, err := upsertRepository(
		ctx,
		queries,
		stack.Repository,
		stack.Source,
		"",
		stack.SyncedAt,
	)
	if err != nil {
		return ApplyStackResult{}, err
	}
	var oldEntries []StackEntry
	old, oldErr := queries.GetStackByKey(
		ctx,
		dbgen.GetStackByKeyParams{
			RepoFullName: stack.Repository.FullName,
			StackNumber:  int32(stack.Number),
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
	row, err := queries.UpsertStackWriteIfNewer(
		ctx,
		dbgen.UpsertStackWriteIfNewerParams{
			RepoID:      repo.ID,
			GhID:        nullableInt8(stack.GitHubID),
			NodeID:      stack.NodeID,
			StackNumber: int32(stack.Number),
			BaseRef:     stack.BaseRef,
			BaseSha:     stack.BaseSHA,
			Open:        stack.Open,
			Entries:     entries,
			GhUpdatedAt: timestamp(stack.GitHubUpdatedAt),
			HeadSha:     stackHeadSHA(stack.Entries, stack.BaseSHA),
			SyncedAt:    timestamp(stack.SyncedAt),
			Etag:        stack.ETag,
			SyncSource:  string(stack.Source),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return ApplyStackResult{}, fmt.Errorf("commit discarded stack write: %w", err)
		}
		return ApplyStackResult{}, nil
	}
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("upsert stack: %w", err)
	}
	result := stackMembershipDiff(oldEntries, stack.Entries)
	result.Applied = true
	candidates := append(append([]int(nil), result.JoinedPRs...), result.LeftPRs...)
	if len(candidates) > 0 {
		numbers := make([]int32, 0, len(candidates))
		for _, number := range candidates {
			numbers = append(numbers, int32(number))
		}
		memberships, err := queries.ListCachedPRMemberships(
			ctx,
			dbgen.ListCachedPRMembershipsParams{
				RepoFullName: stack.Repository.FullName,
				PrNumbers:    numbers,
			},
		)
		if err != nil {
			return ApplyStackResult{}, fmt.Errorf("read prior PR memberships: %w", err)
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
		ctx,
		queries,
		[]string{key},
		"stack.changed",
		key,
		stack.SyncedAt,
	); err != nil {
		return ApplyStackResult{}, err
	}
	_ = row
	if err := tx.Commit(ctx); err != nil {
		return ApplyStackResult{}, fmt.Errorf("commit stack write: %w", err)
	}
	return result, nil
}

func (w *EntityWriter) TombstoneStack(
	ctx context.Context,
	repoFullName string,
	number int,
	source SyncSource,
	at time.Time,
) (ApplyStackResult, error) {
	if at.IsZero() {
		at = w.now()
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("begin stack tombstone: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	key := stackKey(repoFullName, number)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return ApplyStackResult{}, fmt.Errorf("lock %s: %w", key, err)
	}
	repo, err := queries.GetRepoByFullName(ctx, repoFullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyStackResult{}, tx.Commit(ctx)
	}
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("find tombstone repo: %w", err)
	}
	old, err := queries.GetStackByKey(
		ctx,
		dbgen.GetStackByKeyParams{
			RepoFullName: repoFullName,
			StackNumber:  int32(number),
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
		return ApplyStackResult{}, fmt.Errorf("decode tombstoned stack entries: %w", err)
	}
	if _, err := queries.TombstoneStack(
		ctx,
		dbgen.TombstoneStackParams{
			TombstonedAt: timestamp(at),
			SyncedAt:     timestamp(at),
			SyncSource:   string(source),
			RepoID:       repo.ID,
			StackNumber:  int32(number),
		},
	); errors.Is(err, pgx.ErrNoRows) {
		return ApplyStackResult{}, tx.Commit(ctx)
	} else if err != nil {
		return ApplyStackResult{}, fmt.Errorf("tombstone stack: %w", err)
	}
	result := ApplyStackResult{Applied: true, LeftPRs: entryNumbers(entries)}
	if err := markAndEmit(
		ctx,
		queries,
		[]string{key},
		"stack.tombstoned",
		key,
		at,
	); err != nil {
		return ApplyStackResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ApplyStackResult{}, fmt.Errorf("commit stack tombstone: %w", err)
	}
	return result, nil
}

func (w *EntityWriter) ApplyChecks(
	ctx context.Context,
	checks ChecksRecord,
) (bool, error) {
	if !checks.Source.Valid() || checks.RepoFullName == "" || checks.HeadSHA == "" {
		return false, fmt.Errorf("invalid checks record")
	}
	if checks.SyncedAt.IsZero() {
		checks.SyncedAt = w.now()
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin checks write: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	key := checksKey(checks.RepoFullName, checks.HeadSHA)
	if err := queries.AcquireEntityAdvisoryLock(ctx, key); err != nil {
		return false, fmt.Errorf("lock %s: %w", key, err)
	}
	repo, err := queries.GetRepoByFullName(ctx, checks.RepoFullName)
	if err != nil {
		return false, fmt.Errorf("find checks repo: %w", err)
	}
	encoded, err := json.Marshal(checks.Runs)
	if err != nil {
		return false, fmt.Errorf("encode check runs: %w", err)
	}
	changed, err := queries.ReplaceCheckRuns(
		ctx,
		dbgen.ReplaceCheckRunsParams{
			CheckRuns:  encoded,
			RepoID:     repo.ID,
			HeadSha:    checks.HeadSHA,
			SyncedAt:   timestamp(checks.SyncedAt),
			Etag:       checks.ETag,
			SyncSource: string(checks.Source),
		},
	)
	if err != nil {
		return false, fmt.Errorf("replace check runs: %w", err)
	}
	if err := queries.AppendCheckHistory(
		ctx,
		dbgen.AppendCheckHistoryParams{
			RepoID:     repo.ID,
			HeadSha:    checks.HeadSHA,
			SyncedAt:   timestamp(checks.SyncedAt),
			Etag:       checks.ETag,
			SyncSource: string(checks.Source),
			CheckRuns:  encoded,
		},
	); err != nil {
		return false, fmt.Errorf("append check history: %w", err)
	}
	scopes, err := queries.ListPRScopesByHeadSHA(
		ctx,
		dbgen.ListPRScopesByHeadSHAParams{
			RepoFullName: checks.RepoFullName,
			HeadSha:      checks.HeadSHA,
		},
	)
	if err != nil {
		return false, fmt.Errorf("resolve check derivation scopes: %w", err)
	}
	scopeKeys := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		scopeKeys = append(
			scopeKeys,
			derivationScope(
				checks.RepoFullName,
				int(scope.Number),
				intPointer(scope.StackNumber),
			),
		)
	}
	if len(changed) > 0 {
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
		targets = append(targets, stackKey(repoFullName, int(number)))
	}
	for _, pr := range prs {
		if pr.StackNumber.Valid {
			number := int(pr.StackNumber.Int32)
			if _, seen := seenStacks[number]; !seen {
				seenStacks[number] = struct{}{}
				targets = append(targets, stackKey(repoFullName, number))
			}
			continue
		}
		targets = append(targets, pullRequestKey(repoFullName, int(pr.Number)))
	}
	sort.Strings(targets)
	return targets, nil
}

func upsertRepository(
	ctx context.Context,
	queries *dbgen.Queries,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	syncedAt time.Time,
) (dbgen.Repo, bool, error) {
	if err := validateRepository(repository, source); err != nil {
		return dbgen.Repo{}, false, err
	}
	existing, existingErr := queries.GetRepoByFullName(
		ctx,
		repository.FullName,
	)
	if existingErr == nil &&
		existing.GhUpdatedAt.Valid &&
		!existing.TombstonedAt.Valid &&
		!repository.GitHubUpdatedAt.After(existing.GhUpdatedAt.Time) {
		return existing, false, nil
	}
	if existingErr != nil && !errors.Is(existingErr, pgx.ErrNoRows) {
		return dbgen.Repo{}, false, fmt.Errorf(
			"read repository version: %w",
			existingErr,
		)
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
			SyncedAt:       timestamp(syncedAt),
			Etag:           etag,
			SyncSource:     string(source),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := queries.GetRepoByFullName(ctx, repository.FullName)
		if getErr != nil {
			return dbgen.Repo{}, false, fmt.Errorf("get discarded repository: %w", getErr)
		}
		return existing, false, nil
	}
	if err != nil {
		return dbgen.Repo{}, false, fmt.Errorf("upsert repository: %w", err)
	}
	if err := emit(
		ctx,
		queries,
		"repository.changed",
		"repo:"+repository.FullName,
		syncedAt,
	); err != nil {
		return dbgen.Repo{}, false, err
	}
	return row, true, nil
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
	return emit(ctx, queries, kind, entityKey, at)
}

func emit(
	ctx context.Context,
	queries *dbgen.Queries,
	kind string,
	entityKey string,
	at time.Time,
) error {
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

func stackMembershipDiff(oldEntries, newEntries []StackEntry) ApplyStackResult {
	oldSet := make(map[int]struct{}, len(oldEntries))
	newSet := make(map[int]struct{}, len(newEntries))
	for _, entry := range oldEntries {
		oldSet[entry.Number] = struct{}{}
	}
	for _, entry := range newEntries {
		newSet[entry.Number] = struct{}{}
	}
	var result ApplyStackResult
	for number := range newSet {
		if _, existed := oldSet[number]; !existed {
			result.JoinedPRs = append(result.JoinedPRs, number)
		}
	}
	for number := range oldSet {
		if _, remains := newSet[number]; !remains {
			result.LeftPRs = append(result.LeftPRs, number)
		}
	}
	sort.Ints(result.JoinedPRs)
	sort.Ints(result.LeftPRs)
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

func derivationScope(repo string, number int, stackNumber *int) string {
	if stackNumber != nil {
		return stackKey(repo, *stackNumber)
	}
	return pullRequestKey(repo, number)
}

func pullRequestKey(repo string, number int) string {
	return fmt.Sprintf("pr:%s:%d", repo, number)
}

func stackKey(repo string, number int) string {
	return fmt.Sprintf("stack:%s:%d", repo, number)
}

func checksKey(repo, sha string) string {
	return "checks:" + repo + ":" + sha
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
