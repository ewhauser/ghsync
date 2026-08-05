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
	var githubUpdatedAt time.Time
	if row.GhUpdatedAt.Valid {
		githubUpdatedAt = row.GhUpdatedAt.Time
	}
	metadata := FetchMetadata{
		NodeID:                 row.NodeID,
		ETag:                   row.Etag,
		Title:                  row.Title,
		State:                  row.State,
		Draft:                  row.Draft,
		AuthorLogin:            row.AuthorLogin,
		HeadRef:                row.HeadRef,
		BaseRef:                row.BaseRef,
		ReviewDecision:         row.ReviewDecision,
		MergeableState:         row.MergeableState,
		StackNumber:            intPointer(row.StackNumber),
		StackPosition:          intPointer(row.StackPosition),
		GitHubUpdatedAt:        githubUpdatedAt,
		BaseSHA:                row.BaseSha,
		HeadSHA:                row.HeadSha,
		RepoGitHubID:           row.RepoGhID,
		InstallationID:         row.InstallationID,
		RepoFullName:           row.RepoFullName,
		ForceCodeownersRefresh: row.ForceCodeownersRefresh,
	}
	if row.CodeownersState != "" {
		metadata.Codeowners = &CodeownersSourceRecord{
			Ref:     row.CodeownersRef,
			SHA:     row.CodeownersSha,
			Path:    row.CodeownersPath,
			State:   row.CodeownersState,
			Content: row.CodeownersSource,
			Hash:    row.CodeownersHash,
			ETag:    row.CodeownersEtag,
		}
	}
	return metadata, nil
}

// PullRequestChangeMetadata returns the changed-files validator and cached
// rename supplements. The query completes before callers perform REST I/O
// (C-C6).
func (w *EntityWriter) PullRequestChangeMetadata(
	ctx context.Context,
	repo string,
	number int,
) (PullRequestChangeFetchMetadata, error) {
	rows, err := dbgen.New(w.pool).ListPullRequestChangeFetchMetadata(
		ctx,
		dbgen.ListPullRequestChangeFetchMetadataParams{
			RepoFullName: repo,
			PrNumber:     int32(number),
		},
	)
	if err != nil {
		return PullRequestChangeFetchMetadata{}, fmt.Errorf(
			"list PR change fetch metadata: %w",
			err,
		)
	}
	metadata := PullRequestChangeFetchMetadata{
		PreviousPaths: make(map[string]string),
	}
	for _, row := range rows {
		metadata.ETag = row.Etag
		if row.Path != "" && row.PreviousPath != "" {
			metadata.PreviousPaths[row.Path] = row.PreviousPath
		}
	}
	return metadata, nil
}

// TouchPullRequest records a successful unchanged pull-request metadata
// observation. The supplied validator must still belong to the current row;
// GraphQL participation and changed-file clocks are owned by their own fetches.
func (w *EntityWriter) TouchPullRequest(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord, //nolint:gocritic // public writer API snapshots caller-owned record values
	number int,
	checkedAt time.Time,
	etag string,
) (bool, error) {
	key := PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
	if err := requireObservation(observation, key); err != nil {
		return false, err
	}
	confirmed := false
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
		rows, err := queries.ConfirmPullRequestMetadataCheckedAt(
			ctx,
			dbgen.ConfirmPullRequestMetadataCheckedAtParams{
				CheckedAt: timestamp(checkedAt),
				RepoID:    repo.ID,
				PrNumber:  int32(number),
				Etag:      etag,
			},
		)
		if err != nil {
			return fmt.Errorf("touch PR checked_at: %w", err)
		}
		confirmed = rows == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("touch PR: %w", err)
	}
	return confirmed, nil
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
		if upsertErr == nil {
			result.StackStateChanged = errors.Is(oldErr, pgx.ErrNoRows) &&
				row.StackNumber.Valid
			if oldErr == nil {
				result.StackStateChanged =
					(old.StackNumber.Valid || row.StackNumber.Valid) &&
						(old.StackNumber != row.StackNumber ||
							old.StackPosition != row.StackPosition ||
							old.State != row.State ||
							old.Draft != row.Draft ||
							old.HeadRef != row.HeadRef ||
							old.HeadSha != row.HeadSha ||
							old.BaseRef != row.BaseRef ||
							old.BaseSha != row.BaseSha)
			}
		}
		if pull.StackSummary != nil {
			matches, err := pullStackSummaryMatches(
				ctx,
				queries,
				pull.Repository.GitHubID,
				pull.Number,
				pull.StackSummary,
			)
			if err != nil {
				return err
			}
			result.StackStateChanged =
				result.StackStateChanged || !matches
		}
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

		if pull.ReviewRequestsKnown {
			requests, err := encodeReviewRequests(pull.ReviewRequests)
			if err != nil {
				return err
			}
			changed, err := queries.ReplacePullRequestReviewRequests(
				ctx,
				dbgen.ReplacePullRequestReviewRequestsParams{
					ReviewRequests: requests,
					RepoID:         repo.ID,
					PrNumber:       int32(pull.Number),
					FirstSeenAt:    timestamp(pull.SyncedAt),
					GhUpdatedAt:    timestamp(pull.GitHubUpdatedAt),
					HeadSha:        row.HeadSha,
					SyncedAt:       timestamp(pull.SyncedAt),
					LastCheckedAt:  timestamp(pull.SyncedAt),
					Etag:           pull.ETag,
					SyncSource:     string(pull.Source),
				},
			)
			if err != nil {
				return fmt.Errorf("replace PR review requests: %w", err)
			}
			result.ReviewRequestsChanged = len(changed) > 0
			if err := queries.TouchPullRequestReviewRequestsCheckedAt(
				ctx,
				dbgen.TouchPullRequestReviewRequestsCheckedAtParams{
					CheckedAt:   timestamp(pull.SyncedAt),
					Etag:        pull.ETag,
					RepoID:      repo.ID,
					PrNumber:    int32(pull.Number),
					GhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
				},
			); err != nil {
				return fmt.Errorf("touch PR review requests: %w", err)
			}
		}

		if pull.ReviewsKnown {
			reviews, err := encodePullRequestReviews(pull.Reviews)
			if err != nil {
				return err
			}
			changed, err := queries.ReplacePullRequestReviews(
				ctx,
				dbgen.ReplacePullRequestReviewsParams{
					Reviews:           reviews,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
					HeadSha:           row.HeadSha,
					SyncedAt:          timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					SyncSource:        string(pull.Source),
					LastCheckedAt:     timestamp(pull.SyncedAt),
				},
			)
			if err != nil {
				return fmt.Errorf("replace PR reviews: %w", err)
			}
			result.ReviewsChanged = len(changed) > 0
			if err := queries.TouchPullRequestReviewsCheckedAt(
				ctx,
				dbgen.TouchPullRequestReviewsCheckedAtParams{
					CheckedAt:         timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
				},
			); err != nil {
				return fmt.Errorf("touch PR reviews: %w", err)
			}
		}

		if pull.CommentsKnown {
			comments, err := encodePullRequestComments(pull.Comments)
			if err != nil {
				return err
			}
			changed, err := queries.ReplacePullRequestComments(
				ctx,
				dbgen.ReplacePullRequestCommentsParams{
					Comments:          comments,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
					HeadSha:           row.HeadSha,
					SyncedAt:          timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					SyncSource:        string(pull.Source),
					LastCheckedAt:     timestamp(pull.SyncedAt),
				},
			)
			if err != nil {
				return fmt.Errorf("replace PR comments: %w", err)
			}
			result.CommentsChanged = len(changed) > 0
			if err := queries.TouchPullRequestCommentsCheckedAt(
				ctx,
				dbgen.TouchPullRequestCommentsCheckedAtParams{
					CheckedAt:         timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
				},
			); err != nil {
				return fmt.Errorf("touch PR comments: %w", err)
			}
		}

		if pull.ChangeInputsKnown {
			snapshot := pull.ChangeSnapshot
			changedCount, err := queries.UpsertPullRequestChangeSnapshot(
				ctx,
				dbgen.UpsertPullRequestChangeSnapshotParams{
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					HeadSha:           snapshot.HeadSHA,
					BaseSha:           snapshot.BaseSHA,
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
					FilesTotalCount:   int32(snapshot.FilesTotalCount),
					FilesTruncated:    snapshot.FilesTruncated,
					CodeownersRef:     snapshot.CodeownersRef,
					CodeownersSha:     snapshot.CodeownersSHA,
					CodeownersPath:    nullableText(snapshot.CodeownersPath),
					CodeownersState:   snapshot.CodeownersState,
					CodeownersSource: optionalText(
						snapshot.CodeownersSource,
						snapshot.CodeownersState == "present",
					),
					CodeownersHash: snapshot.CodeownersHash,
					SyncedAt:       timestamp(pull.SyncedAt),
					CodeownersEtag: snapshot.CodeownersETag,
					FilesEtag:      snapshot.ETag,
					SyncSource:     string(pull.Source),
					LastCheckedAt:  timestamp(pull.SyncedAt),
				},
			)
			if err != nil {
				return fmt.Errorf("upsert PR change snapshot: %w", err)
			}
			files, err := encodeChangedFiles(snapshot.Files)
			if err != nil {
				return err
			}
			changedFiles, err := queries.ReplacePullRequestChangedFiles(
				ctx,
				dbgen.ReplacePullRequestChangedFilesParams{
					ChangedFiles:      files,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					BaseSha:           snapshot.BaseSHA,
					HeadSha:           snapshot.HeadSHA,
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
					SyncedAt:          timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					SyncSource:        string(pull.Source),
					LastCheckedAt:     timestamp(pull.SyncedAt),
				},
			)
			if err != nil {
				return fmt.Errorf("replace PR changed files: %w", err)
			}
			owners, err := encodeFileOwners(snapshot.Owners)
			if err != nil {
				return err
			}
			changedOwners, err := queries.ReplacePullRequestFileOwners(
				ctx,
				dbgen.ReplacePullRequestFileOwnersParams{
					FileOwners:        owners,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					BaseSha:           snapshot.BaseSHA,
					HeadSha:           snapshot.HeadSHA,
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
					SyncedAt:          timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					SyncSource:        string(pull.Source),
					LastCheckedAt:     timestamp(pull.SyncedAt),
				},
			)
			if err != nil {
				return fmt.Errorf("replace PR file owners: %w", err)
			}
			result.ChangeInputsChanged = changedCount > 0 ||
				len(changedFiles) > 0 || len(changedOwners) > 0
			changeTouch := dbgen.TouchPullRequestChangeInputsCheckedAtParams{
				CheckedAt:         timestamp(pull.SyncedAt),
				CodeownersEtag:    snapshot.CodeownersETag,
				FilesEtag:         snapshot.ETag,
				RepoID:            repo.ID,
				PrNumber:          int32(pull.Number),
				ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
			}
			if err := queries.TouchPullRequestChangeInputsCheckedAt(
				ctx, changeTouch,
			); err != nil {
				return fmt.Errorf("touch PR change snapshot: %w", err)
			}
			if err := queries.TouchPullRequestChangedFilesCheckedAt(
				ctx,
				dbgen.TouchPullRequestChangedFilesCheckedAtParams{
					CheckedAt:         timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
				},
			); err != nil {
				return fmt.Errorf("touch PR changed files: %w", err)
			}
			if err := queries.TouchPullRequestFileOwnersCheckedAt(
				ctx,
				dbgen.TouchPullRequestFileOwnersCheckedAtParams{
					CheckedAt:         timestamp(pull.SyncedAt),
					Etag:              pull.ETag,
					RepoID:            repo.ID,
					PrNumber:          int32(pull.Number),
					ParentGhUpdatedAt: timestamp(pull.GitHubUpdatedAt),
				},
			); err != nil {
				return fmt.Errorf("touch PR file owners: %w", err)
			}
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
		result.Applied = result.DomainChanged || threadsChanged ||
			result.ReviewRequestsChanged || result.ReviewsChanged ||
			result.CommentsChanged || result.ChangeInputsChanged
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

func pullStackSummaryMatches(
	ctx context.Context,
	queries *dbgen.Queries,
	repoGitHubID int64,
	prNumber int,
	summary *StackSummaryRecord,
) (bool, error) {
	// GitHub can retain a PR's historical ordinal after the current stack
	// shrinks. That is valid membership truth, but it cannot identify a member
	// in the current ordered entries, so retain the authoritative stack fetch.
	if summary.Position > summary.Size {
		return false, nil
	}
	// An unknown SHA cannot prove that the summary and cached stack describe
	// the same base commit. Fail open so the authoritative stack fetch runs.
	if summary.BaseSHA == "" {
		return false, nil
	}
	stack, err := queries.GetStackByIdentity(
		ctx,
		dbgen.GetStackByIdentityParams{
			RepoGhID:    repoGitHubID,
			StackNumber: int32(summary.Number),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read PR stack summary target: %w", err)
	}
	if stack.TombstonedAt.Valid ||
		!stack.GhID.Valid ||
		stack.GhID.Int64 != summary.GitHubID ||
		stack.BaseRef != summary.BaseRef ||
		stack.BaseSha == "" ||
		stack.BaseSha != summary.BaseSHA {
		return false, nil
	}
	var entries []struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(stack.Entries, &entries); err != nil {
		return false, fmt.Errorf("decode PR stack summary target: %w", err)
	}
	return len(entries) == summary.Size &&
		summary.Position <= len(entries) &&
		entries[summary.Position-1].Number == prNumber, nil
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
			result.StackStateChanged = result.OldStackNumber != nil
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
			if _, err := queries.TombstonePullRequestReviewRequests(
				ctx,
				dbgen.TombstonePullRequestReviewRequestsParams{
					TombstonedAt: timestamp(at),
					SyncSource:   string(source),
					RepoID:       repo.ID,
					PrNumber:     int32(number),
				},
			); err != nil {
				return fmt.Errorf("tombstone PR review requests: %w", err)
			}
			if _, err := queries.TombstonePullRequestReviews(
				ctx,
				dbgen.TombstonePullRequestReviewsParams{
					TombstonedAt: timestamp(at),
					SyncSource:   string(source),
					RepoID:       repo.ID,
					PrNumber:     int32(number),
				},
			); err != nil {
				return fmt.Errorf("tombstone PR reviews: %w", err)
			}
			if _, err := queries.TombstonePullRequestComments(
				ctx,
				dbgen.TombstonePullRequestCommentsParams{
					TombstonedAt: timestamp(at),
					SyncSource:   string(source),
					RepoID:       repo.ID,
					PrNumber:     int32(number),
				},
			); err != nil {
				return fmt.Errorf("tombstone PR comments: %w", err)
			}
			changeTombstone := dbgen.TombstonePullRequestChangeSnapshotParams{
				TombstonedAt: timestamp(at),
				SyncSource:   string(source),
				RepoID:       repo.ID,
				PrNumber:     int32(number),
			}
			if _, err := queries.TombstonePullRequestFileOwners(
				ctx,
				dbgen.TombstonePullRequestFileOwnersParams(changeTombstone),
			); err != nil {
				return fmt.Errorf("tombstone PR file owners: %w", err)
			}
			if _, err := queries.TombstonePullRequestChangedFiles(
				ctx,
				dbgen.TombstonePullRequestChangedFilesParams(changeTombstone),
			); err != nil {
				return fmt.Errorf("tombstone PR changed files: %w", err)
			}
			if _, err := queries.TombstonePullRequestChangeSnapshot(
				ctx, changeTombstone,
			); err != nil {
				return fmt.Errorf("tombstone PR change snapshot: %w", err)
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

func encodeReviewRequests(requests []ReviewRequestRecord) ([]byte, error) {
	type encodedRequest struct {
		Kind        ReviewRequestKind `json:"kind"`
		GitHubID    int64             `json:"gh_id"`
		NodeID      string            `json:"node_id"`
		Login       string            `json:"login"`
		RequestedAt *time.Time        `json:"requested_at"`
	}
	encoded := make([]encodedRequest, 0, len(requests))
	for _, request := range requests {
		encoded = append(encoded, encodedRequest(request))
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode PR review requests: %w", err)
	}
	return value, nil
}

func encodePullRequestReviews(
	reviews []PullRequestReviewRecord,
) ([]byte, error) {
	type encodedReview struct {
		GitHubID        int64      `json:"gh_id"`
		NodeID          string     `json:"node_id"`
		AuthorKind      string     `json:"author_kind"`
		AuthorNodeID    string     `json:"author_node_id"`
		AuthorLogin     string     `json:"author_login"`
		State           string     `json:"state"`
		SubmittedAt     *time.Time `json:"submitted_at"`
		CommitOID       string     `json:"commit_oid"`
		GitHubUpdatedAt time.Time  `json:"gh_updated_at"`
	}
	encoded := make([]encodedReview, 0, len(reviews))
	for index := range reviews {
		review := reviews[index]
		encoded = append(encoded, encodedReview{
			GitHubID:        review.GitHubID,
			NodeID:          review.NodeID,
			AuthorKind:      review.AuthorKind,
			AuthorNodeID:    review.AuthorNodeID,
			AuthorLogin:     review.AuthorLogin,
			State:           strings.ToLower(review.State),
			SubmittedAt:     review.SubmittedAt,
			CommitOID:       review.CommitOID,
			GitHubUpdatedAt: review.GitHubUpdatedAt,
		})
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode PR reviews: %w", err)
	}
	return value, nil
}

func encodePullRequestComments(
	comments []PullRequestCommentRecord,
) ([]byte, error) {
	type encodedComment struct {
		GitHubID        int64     `json:"gh_id"`
		NodeID          string    `json:"node_id"`
		AuthorKind      string    `json:"author_kind"`
		AuthorNodeID    string    `json:"author_node_id"`
		AuthorLogin     string    `json:"author_login"`
		CreatedAt       time.Time `json:"created_at"`
		GitHubUpdatedAt time.Time `json:"gh_updated_at"`
	}
	encoded := make([]encodedComment, 0, len(comments))
	for _, comment := range comments {
		encoded = append(encoded, encodedComment(comment))
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode PR comments: %w", err)
	}
	return value, nil
}

func encodeChangedFiles(files []ChangedFileRecord) ([]byte, error) {
	type encodedFile struct {
		Path         string `json:"path"`
		PreviousPath string `json:"previous_path"`
		ChangeType   string `json:"change_type"`
	}
	encoded := make([]encodedFile, 0, len(files))
	for _, file := range files {
		encoded = append(encoded, encodedFile{
			Path: file.Path, PreviousPath: file.PreviousPath,
			ChangeType: strings.ToLower(file.ChangeType),
		})
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode PR changed files: %w", err)
	}
	return value, nil
}

func encodeFileOwners(owners []FileOwnerRecord) ([]byte, error) {
	type encodedOwner struct {
		Path            string `json:"path"`
		OwnerToken      string `json:"owner_token"`
		OwnerType       string `json:"owner_type"`
		OwnerName       string `json:"owner_name"`
		ResolutionState string `json:"resolution_state"`
		OwnerGitHubID   int64  `json:"owner_gh_id"`
		OwnerNodeID     string `json:"owner_node_id"`
		OwnerLogin      string `json:"owner_login"`
		SourcePattern   string `json:"source_pattern"`
		SourceLine      int    `json:"source_line"`
	}
	encoded := make([]encodedOwner, 0, len(owners))
	for index := range owners {
		encoded = append(encoded, encodedOwner(owners[index]))
	}
	value, err := json.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("encode PR file owners: %w", err)
	}
	return value, nil
}
