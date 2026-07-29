package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/store/dbgen"
)

// StackMetadata returns conditional-fetch metadata for one stack.
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
		return FetchMetadata{}, fmt.Errorf("get stack fetch metadata: %w", err)
	}
	return FetchMetadata{
		ETag:           row.Etag,
		HeadSHA:        row.HeadSha,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.RepoFullName,
	}, nil
}

// ApplyStack conditionally applies a direct stack observation.
func (w *EntityWriter) ApplyStack(
	ctx context.Context,
	stack StackRecord,
) (ApplyStackResult, error) {
	return w.applyStack(ctx, nil, stack, nil)
}

// ApplyStackObserved conditionally applies a stack while holding its
// observation lock.
func (w *EntityWriter) ApplyStackObserved(
	ctx context.Context,
	observation *Observation,
	stack StackRecord,
	hook StackHook,
) (ApplyStackResult, error) {
	key := StackEntityKey(
		stack.Repository.InstallationID,
		stack.Repository.GitHubID,
		stack.Number,
	)
	if err := requireObservation(observation, key); err != nil {
		return ApplyStackResult{}, err
	}
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
			return ApplyStackResult{}, fmt.Errorf(
				"apply stack repository: %w",
				err,
			)
		}
	}
	key := StackEntityKey(
		stack.Repository.InstallationID,
		stack.Repository.GitHubID,
		stack.Number,
	)
	var result ApplyStackResult
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, tx, queries := entity.ctx, entity.tx, entity.queries
		stack.SyncedAt = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(ctx, stack.Repository.GitHubID)
		if err != nil {
			return fmt.Errorf("find stack repository: %w", err)
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
				return fmt.Errorf("decode cached stack entries: %w", err)
			}
		} else if !errors.Is(oldErr, pgx.ErrNoRows) {
			return fmt.Errorf("read prior stack: %w", oldErr)
		}
		entries, err := json.Marshal(stack.Entries)
		if err != nil {
			return fmt.Errorf("encode stack entries: %w", err)
		}
		_, upsertErr := queries.UpsertStackWriteIfNewer(
			ctx,
			dbgen.UpsertStackWriteIfNewerParams{
				RepoID:               repo.ID,
				GhID:                 nullableInt8(stack.GitHubID),
				NodeID:               stack.NodeID,
				StackNumber:          int32(stack.Number),
				BaseRef:              stack.BaseRef,
				BaseSha:              stack.BaseSHA,
				Open:                 stack.Open,
				Entries:              entries,
				GhUpdatedAt:          timestamp(stack.GitHubUpdatedAt),
				HeadSha:              stackHeadSHA(stack.Entries, stack.BaseSHA),
				SyncedAt:             timestamp(stack.SyncedAt),
				LastCheckedAt:        timestamp(stack.SyncedAt),
				Etag:                 stack.ETag,
				SyncSource:           string(stack.Source),
				DisplayWindowSeconds: int32(displayWindow / time.Second),
			},
		)
		if upsertErr != nil && !errors.Is(upsertErr, pgx.ErrNoRows) {
			return fmt.Errorf("upsert stack: %w", upsertErr)
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
			return fmt.Errorf("touch stack: %w", err)
		}
		result = ApplyStackResult{}
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
					return fmt.Errorf(
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
			if err := w.markAndEmit(
				ctx, queries, []string{key}, outbox.StackChangedKind, key, stack.SyncedAt,
			); err != nil {
				return err
			}
		}
		if hook != nil {
			if txHook := hook(result); txHook != nil {
				if err := txHook(ctx, tx); err != nil {
					return fmt.Errorf(
						"run stack transaction hook: %w",
						err,
					)
				}
			}
		}

		return nil
	})
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("apply stack: %w", err)
	}
	w.observer.CacheWrite(ctx, "stack", result.Applied, false)
	return result, nil
}

// TouchStack records a successful unchanged stack observation.
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
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, queries := entity.ctx, entity.queries
		checkedAt = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(
			ctx,
			repository.GitHubID,
		)
		if err != nil {
			return fmt.Errorf("find stack repository: %w", err)
		}
		if err := queries.TouchStackCheckedAt(
			ctx,
			dbgen.TouchStackCheckedAtParams{
				CheckedAt:   timestamp(checkedAt),
				RepoID:      repo.ID,
				StackNumber: int32(number),
				Etag:        etag,
			},
		); err != nil {
			return fmt.Errorf("touch stack checked_at: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("touch stack: %w", err)
	}
	return nil
}

// TombstoneStackObserved conditionally tombstones a stack while holding its
// observation lock.
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
	if err := requireObservation(observation, key); err != nil {
		return ApplyStackResult{}, err
	}
	var result ApplyStackResult
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, tx, queries := entity.ctx, entity.tx, entity.queries
		at = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(ctx, repository.GitHubID)
		if err != nil {
			return fmt.Errorf("find tombstone repo: %w", err)
		}
		old, err := queries.GetStackByIdentity(
			ctx,
			dbgen.GetStackByIdentityParams{
				RepoGhID:    repository.GitHubID,
				StackNumber: int32(number),
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tombstoned stack: %w", err)
		}
		var entries []StackEntry
		if err := json.Unmarshal(old.Entries, &entries); err != nil {
			return fmt.Errorf(
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
			return fmt.Errorf("tombstone stack: %w", tombstoneErr)
		}
		result = ApplyStackResult{}
		if tombstoneErr == nil {
			result = ApplyStackResult{
				Applied:  true,
				LeftPRs:  entryNumbers(entries),
				MovedPRs: nil,
			}
			if err := w.markAndEmit(
				ctx, queries, []string{key}, outbox.StackTombstonedKind, key, at,
			); err != nil {
				return err
			}
		}
		if hook != nil {
			if txHook := hook(result); txHook != nil {
				if err := txHook(ctx, tx); err != nil {
					return fmt.Errorf(
						"run stack tombstone hook: %w",
						err,
					)
				}
			}
		}

		return nil
	})
	if err != nil {
		return ApplyStackResult{}, fmt.Errorf("tombstone stack: %w", err)
	}
	w.observer.CacheWrite(ctx, "stack", result.Applied, true)
	return result, nil
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
