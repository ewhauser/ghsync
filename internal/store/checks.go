package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/store/dbgen"
)

// ChecksMetadata returns conditional-fetch metadata for one head SHA.
func (w *EntityWriter) ChecksMetadata(
	ctx context.Context,
	repo string,
	headSHA string,
) (FetchMetadata, error) {
	row, err := dbgen.New(w.pool).GetCheckRunsFetchMetadata(
		ctx,
		dbgen.GetCheckRunsFetchMetadataParams{
			HeadSha:      headSHA,
			RepoFullName: repo,
		},
	)
	if err != nil {
		return FetchMetadata{}, fmt.Errorf("get checks fetch metadata: %w", err)
	}
	return FetchMetadata{
		ETag:           row.Etag,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.RepoFullName,
	}, nil
}

// ApplyChecksObserved conditionally replaces a head SHA's check runs while
// holding its observation lock.
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
	if err := requireObservation(observation, key); err != nil {
		return false, err
	}
	var applied bool
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, queries := entity.ctx, entity.queries
		checks.SyncedAt = entity.databaseTime
		repo, err := queries.GetRepoByGitHubID(ctx, checks.Repository.GitHubID)
		if err != nil {
			return fmt.Errorf("find checks repo: %w", err)
		}
		for index := range checks.Runs {
			if checks.Runs[index].SemanticVersion == "" {
				checks.Runs[index].SemanticVersion =
					checkSemanticVersion(checks.Runs[index])
			}
		}
		encoded, err := json.Marshal(checks.Runs)
		if err != nil {
			return fmt.Errorf("encode check runs: %w", err)
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
			return fmt.Errorf("replace check runs: %w", err)
		}
		if err := queries.TouchCheckRunsCheckedAt(
			ctx,
			dbgen.TouchCheckRunsCheckedAtParams{
				CheckedAt: timestamp(checks.SyncedAt),
				Etag:      checks.ETag,
				RepoID:    repo.ID,
				HeadSha:   checks.HeadSHA,
			},
		); err != nil {
			return fmt.Errorf("touch check runs: %w", err)
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
			return fmt.Errorf("append accepted check history: %w", err)
		}
		if len(changed) > 0 {
			scopes, err := queries.ListPRScopesByHeadSHA(
				ctx,
				dbgen.ListPRScopesByHeadSHAParams{
					RepoGhID: checks.Repository.GitHubID,
					HeadSha:  checks.HeadSHA,
				},
			)
			if err != nil {
				return fmt.Errorf("resolve check scopes: %w", err)
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
			if err := w.markAndEmit(
				ctx,
				queries,
				uniqueStrings(scopeKeys...),
				outbox.ChecksChangedKind,
				key,
				checks.SyncedAt,
			); err != nil {
				return err
			}
		}

		applied = len(changed) > 0
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("apply checks: %w", err)
	}
	w.observer.CacheWrite(ctx, "checks", applied, false)
	return applied, nil
}

// TouchChecks records a successful unchanged check-runs observation.
func (w *EntityWriter) TouchChecks(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	headSHA string,
	checkedAt time.Time,
	etag string,
) error {
	key := ChecksEntityKey(
		repository.InstallationID, repository.GitHubID, headSHA,
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
			return fmt.Errorf("find checks repository: %w", err)
		}
		if err := queries.TouchCheckRunsCheckedAt(
			ctx,
			dbgen.TouchCheckRunsCheckedAtParams{
				CheckedAt: timestamp(checkedAt),
				Etag:      etag,
				RepoID:    repo.ID,
				HeadSha:   headSHA,
			},
		); err != nil {
			return fmt.Errorf("touch checks checked_at: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("touch checks: %w", err)
	}
	return nil
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
