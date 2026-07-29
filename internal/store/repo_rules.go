package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/store/dbgen"
)

func (w *EntityWriter) RepoRulesMetadata(
	ctx context.Context,
	repo string,
) (FetchMetadata, int64, error) {
	row, err := dbgen.New(w.pool).GetRepoRulesFetchMetadata(ctx, repo)
	if err != nil {
		return FetchMetadata{}, 0, fmt.Errorf(
			"get repository rules fetch metadata: %w",
			err,
		)
	}
	return FetchMetadata{
		ETag:           row.Etag,
		RepoGitHubID:   row.RepoGhID,
		InstallationID: row.InstallationID,
		RepoFullName:   row.FullName,
	}, row.RepoID, nil
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
	if err := requireObservation(observation, key); err != nil {
		return false, err
	}
	var applied bool
	err := w.withEntityTx(
		ctx,
		observation,
		key,
		func(
			ctx context.Context,
			_ pgx.Tx,
			queries *dbgen.Queries,
			databaseTime time.Time,
		) error {
			rules.SyncedAt = databaseTime
			repo, err := queries.GetRepoByGitHubID(ctx, rules.Repository.GitHubID)
			if err != nil {
				return fmt.Errorf("find repository rules repo: %w", err)
			}
			encoded, err := json.Marshal(rules.Rules)
			if err != nil {
				return fmt.Errorf("encode repository rules: %w", err)
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
				return fmt.Errorf("replace repository rules: %w", err)
			}
			if err := queries.TouchRepoRulesCheckedAt(
				ctx,
				dbgen.TouchRepoRulesCheckedAtParams{
					CheckedAt: timestamp(rules.SyncedAt),
					RepoID:    repo.ID,
				},
			); err != nil {
				return fmt.Errorf("touch repository rules: %w", err)
			}
			if err := queries.UpsertRepoRuleSyncState(
				ctx,
				dbgen.UpsertRepoRuleSyncStateParams{
					RepoID:    repo.ID,
					Etag:      rules.ETag,
					CheckedAt: timestamp(rules.SyncedAt),
				},
			); err != nil {
				return fmt.Errorf("update repository rules metadata: %w", err)
			}
			if len(changed) > 0 {
				scopes, err := queries.ListRepositoryDerivationScopes(ctx, repo.ID)
				if err != nil {
					return fmt.Errorf("resolve repository rule scopes: %w", err)
				}
				if err := w.markAndEmit(
					ctx,
					queries,
					scopes,
					outbox.RepoRulesChangedKind,
					key,
					rules.SyncedAt,
				); err != nil {
					return err
				}
			}

			applied = len(changed) > 0
			return nil
		},
	)
	if err != nil {
		return false, fmt.Errorf("apply repository rules: %w", err)
	}
	w.observer.CacheWrite(ctx, "repo_rules", applied, false)
	return applied, nil
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
	err := w.withEntityTx(
		ctx,
		observation,
		key,
		func(
			ctx context.Context,
			_ pgx.Tx,
			queries *dbgen.Queries,
			databaseTime time.Time,
		) error {
			checkedAt = databaseTime
			repo, err := queries.GetRepoByGitHubID(
				ctx,
				repository.GitHubID,
			)
			if err != nil {
				return fmt.Errorf(
					"find repository rules repository: %w",
					err,
				)
			}
			if err := queries.TouchRepoRulesCheckedAt(
				ctx,
				dbgen.TouchRepoRulesCheckedAtParams{
					CheckedAt: timestamp(checkedAt),
					RepoID:    repo.ID,
				},
			); err != nil {
				return fmt.Errorf(
					"touch repository rules checked_at: %w",
					err,
				)
			}
			if err := queries.UpsertRepoRuleSyncState(
				ctx,
				dbgen.UpsertRepoRuleSyncStateParams{
					RepoID:    repo.ID,
					Etag:      etag,
					CheckedAt: timestamp(checkedAt),
				},
			); err != nil {
				return fmt.Errorf(
					"update repository rules metadata: %w",
					err,
				)
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("touch repository rules: %w", err)
	}
	return nil
}
