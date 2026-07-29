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

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// PullRequestMetadata returns conditional-fetch metadata for one pull request.
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
		return FetchMetadata{}, fmt.Errorf("get PR fetch metadata: %w", err)
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

// TouchPullRequest records a successful unchanged pull-request observation.
func (w *EntityWriter) TouchPullRequest(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord, //nolint:gocritic // public writer API snapshots caller-owned record values
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
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, queries := entity.ctx, entity.queries
		checkedAt = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(
			ctx,
			repository.GitHubID,
		)
		if err != nil {
			return fmt.Errorf("find PR repository: %w", err)
		}
		if err := queries.TouchPullRequestCheckedAt(
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
		return nil
	})
	if err != nil {
		return fmt.Errorf("touch PR: %w", err)
	}
	return nil
}

// ApplyPullRequest conditionally applies a direct pull-request observation.
func (w *EntityWriter) ApplyPullRequest(
	ctx context.Context,
	pull PullRequestRecord, //nolint:gocritic // public writer API snapshots caller-owned record values
) (ApplyPullRequestResult, error) {
	return w.applyPullRequest(ctx, nil, &pull, nil)
}

// ApplyPullRequestObserved conditionally applies a pull request while holding
// its observation lock.
func (w *EntityWriter) ApplyPullRequestObserved(
	ctx context.Context,
	observation *Observation,
	pull PullRequestRecord, //nolint:gocritic // public writer API snapshots caller-owned record values
	hook PullRequestHook,
) (ApplyPullRequestResult, error) {
	key := PullRequestEntityKey(
		pull.Repository.InstallationID,
		pull.Repository.GitHubID,
		pull.Number,
	)
	if err := requireObservation(observation, key); err != nil {
		return ApplyPullRequestResult{}, err
	}
	return w.applyPullRequest(ctx, observation, &pull, hook)
}

func (w *EntityWriter) applyPullRequest(
	ctx context.Context,
	observation *Observation,
	pull *PullRequestRecord,
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
			return ApplyPullRequestResult{}, fmt.Errorf(
				"apply PR repository: %w",
				err,
			)
		}
	}
	key := PullRequestEntityKey(
		pull.Repository.InstallationID,
		pull.Repository.GitHubID,
		pull.Number,
	)
	var result ApplyPullRequestResult
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, tx, queries := entity.ctx, entity.tx, entity.queries
		pull.SyncedAt = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(ctx, pull.Repository.GitHubID)
		if err != nil {
			return fmt.Errorf("find PR repository: %w", err)
		}
		old, oldErr := queries.GetPullRequestByIdentity(
			ctx,
			dbgen.GetPullRequestByIdentityParams{
				RepoGhID: pull.Repository.GitHubID,
				PrNumber: int32(pull.Number),
			},
		)
		if oldErr != nil && !errors.Is(oldErr, pgx.ErrNoRows) {
			return fmt.Errorf("read prior PR: %w", oldErr)
		}
		result = ApplyPullRequestResult{
			OldStackNumber: stackPointerFromPR(oldErr, old.StackNumber),
			OldHeadSHA:     old.HeadSha,
		}
		row, upsertErr := queries.UpsertPullRequestWriteIfNewer(
			ctx,
			dbgen.UpsertPullRequestWriteIfNewerParams{
				RepoID:               repo.ID,
				GhID:                 nullableInt8(pull.GitHubID),
				NodeID:               pull.NodeID,
				PrNumber:             int32(pull.Number),
				Title:                pull.Title,
				State:                strings.ToLower(pull.State),
				Draft:                pull.Draft,
				AuthorLogin:          pull.AuthorLogin,
				HeadRef:              pull.HeadRef,
				HeadSha:              pull.HeadSHA,
				BaseRef:              pull.BaseRef,
				BaseSha:              pull.BaseSHA,
				ReviewDecision:       pull.ReviewDecision,
				MergeableState:       pull.MergeableState,
				StackNumber:          nullableInt4(pull.StackNumber),
				StackPosition:        nullableInt4(pull.StackPosition),
				MembershipKnown:      pull.MembershipKnown,
				GhUpdatedAt:          timestamp(pull.GitHubUpdatedAt),
				SyncedAt:             timestamp(pull.SyncedAt),
				LastCheckedAt:        timestamp(pull.SyncedAt),
				Etag:                 pull.ETag,
				SyncSource:           string(pull.Source),
				DisplayWindowSeconds: int32(displayWindow / time.Second),
			},
		)
		if upsertErr != nil && !errors.Is(upsertErr, pgx.ErrNoRows) {
			return fmt.Errorf("upsert PR: %w", upsertErr)
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
				return fmt.Errorf("get discarded PR: %w", getErr)
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
			return fmt.Errorf("touch PR: %w", err)
		}

		threadsChanged := false
		if pull.ThreadsKnown {
			threads, err := encodeReviewThreads(pull.ReviewThreads)
			if err != nil {
				return err
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
				return fmt.Errorf("replace review threads: %w", err)
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
				return fmt.Errorf("touch review threads: %w", err)
			}
		}
		result.Applied = result.DomainChanged || threadsChanged
		if result.Applied {
			scopes := uniqueStrings(
				derivationScope(
					&pull.Repository, pull.Number, result.OldStackNumber,
				),
				derivationScope(
					&pull.Repository, pull.Number, result.NewStackNumber,
				),
			)
			if err := w.markAndEmit(
				ctx, queries, scopes, outbox.PullRequestChangedKind, key, pull.SyncedAt,
			); err != nil {
				return err
			}
		}
		if hook != nil {
			if txHook := hook(result); txHook != nil {
				if err := txHook(ctx, tx); err != nil {
					return fmt.Errorf(
						"run PR transaction hook: %w",
						err,
					)
				}
			}
		}

		return nil
	})
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("apply PR: %w", err)
	}
	w.observer.CacheWrite(
		ctx,
		"pull_request",
		result.DomainChanged,
		false,
	)
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
	for index := range sorted {
		apply := &sorted[index]
		key := PullRequestEntityKey(
			apply.Record.Repository.InstallationID,
			apply.Record.Repository.GitHubID,
			apply.Record.Number,
		)
		applyCtx := apply.Context //nolint:contextcheck // each batch item preserves its caller's values and cancellation
		if applyCtx == nil {
			applyCtx = ctx
		}
		result, err := w.applyPullRequest(
			applyCtx, apply.Observation, &apply.Record, apply.Hook,
		)
		outcomes[key] = PullRequestApplyOutcome{Result: result, Err: err}
	}
	return outcomes
}

// TombstonePullRequestObserved conditionally tombstones a pull request while
// holding its observation lock.
func (w *EntityWriter) TombstonePullRequestObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord, //nolint:gocritic // public writer API snapshots caller-owned record values
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
	if err := requireObservation(observation, key); err != nil {
		return ApplyPullRequestResult{}, err
	}
	var result ApplyPullRequestResult
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, tx, queries := entity.ctx, entity.tx, entity.queries
		at = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(ctx, repository.GitHubID)
		if err != nil {
			return fmt.Errorf("find tombstone repo: %w", err)
		}
		old, err := queries.GetPullRequestByIdentity(
			ctx,
			dbgen.GetPullRequestByIdentityParams{
				RepoGhID: repository.GitHubID,
				PrNumber: int32(number),
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tombstoned PR: %w", err)
		}
		result = ApplyPullRequestResult{
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
			return fmt.Errorf("tombstone PR: %w", err)
		}
		if err == nil {
			result.Applied = true
			result.DomainChanged = true
			result.NewHeadSHA = row.HeadSha
			if err := w.markAndEmit(
				ctx,
				queries,
				[]string{derivationScope(
					&repository, number, result.OldStackNumber,
				)},
				outbox.PullRequestTombstonedKind,
				key,
				at,
			); err != nil {
				return err
			}
		}
		if hook != nil {
			if txHook := hook(result); txHook != nil {
				if err := txHook(ctx, tx); err != nil {
					return fmt.Errorf(
						"run PR tombstone hook: %w",
						err,
					)
				}
			}
		}

		return nil
	})
	if err != nil {
		return ApplyPullRequestResult{}, fmt.Errorf("tombstone PR: %w", err)
	}
	w.observer.CacheWrite(ctx, "pull_request", result.Applied, true)
	return result, nil
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
		encoded = append(encoded, encodedThread(thread))
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode review threads: %w", err)
	}
	return value, nil
}
