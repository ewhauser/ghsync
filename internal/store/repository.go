package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// Repository resolves a current repository record by full name or alias.
func (w *EntityWriter) Repository(
	ctx context.Context,
	fullName string,
) (RepositoryRecord, error) {
	repo, err := dbgen.New(w.pool).GetRepoByFullName(ctx, fullName)
	if err != nil {
		return RepositoryRecord{}, fmt.Errorf("get repository by full name: %w", err)
	}
	return repositoryFromRow(repo), nil
}

// ApplyRepository conditionally applies a direct repository observation.
func (w *EntityWriter) ApplyRepository(
	ctx context.Context,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (bool, error) {
	return w.applyRepository(ctx, nil, repository, source, etag, observedAt)
}

// ApplyRepositoryObserved conditionally applies a repository while holding its
// observation lock.
func (w *EntityWriter) ApplyRepositoryObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (bool, error) {
	wantKey := RepositoryEntityKey(repository.InstallationID, repository.GitHubID)
	if err := requireObservation(observation, wantKey); err != nil {
		return false, err
	}
	return w.applyRepository(
		ctx,
		observation,
		repository,
		source,
		etag,
		observedAt,
	)
}

func (w *EntityWriter) applyRepository(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	source SyncSource,
	etag string,
	observedAt time.Time,
) (bool, error) {
	key := RepositoryEntityKey(repository.InstallationID, repository.GitHubID)
	var applied bool
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, queries := entity.ctx, entity.queries
		observedAt = entity.databaseTime
		var err error
		_, applied, err = w.applyRepositoryTx(
			ctx,
			queries,
			repository,
			source,
			etag,
			observedAt,
		)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("apply repository: %w", err)
	}
	w.observer.CacheWrite(ctx, "repository", applied, false)
	return applied, nil
}

// TombstoneRepositoryObserved applies C-R3's verified repository
// disappearance through the same lock/dirty/outbox transaction as every other
// authoritative cache mutation. Child mirrors remain retained history; live
// readers and future sweeps exclude them through the repository tombstone.
func (w *EntityWriter) TombstoneRepositoryObserved(
	ctx context.Context,
	observation *Observation,
	repository RepositoryRecord,
	source SyncSource,
	at time.Time,
) (bool, error) {
	if !source.Valid() {
		return false, fmt.Errorf("invalid repository tombstone source")
	}
	if at.IsZero() {
		at = w.now()
	}
	key := RepositoryEntityKey(
		repository.InstallationID,
		repository.GitHubID,
	)
	if err := requireObservation(observation, key); err != nil {
		return false, err
	}
	var applied bool
	err := w.withEntityTx(ctx, observation, key, func(entity entityTx) error {
		ctx, queries := entity.ctx, entity.queries
		at = entity.databaseTime
		current, err := queries.GetRepoByGitHubID(
			ctx,
			repository.GitHubID,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tombstoned repository: %w", err)
		}
		scopes, err := queries.ListRepositoryDerivationScopes(
			ctx,
			current.ID,
		)
		if err != nil {
			return fmt.Errorf(
				"resolve repository tombstone scopes: %w",
				err,
			)
		}
		_, tombstoneErr := queries.TombstoneRepository(
			ctx,
			dbgen.TombstoneRepositoryParams{
				TombstonedAt: timestamp(at),
				SyncedAt:     timestamp(at),
				SyncSource:   string(source),
				GhID:         repository.GitHubID,
			},
		)
		if tombstoneErr != nil &&
			!errors.Is(tombstoneErr, pgx.ErrNoRows) {
			return fmt.Errorf(
				"tombstone repository: %w",
				tombstoneErr,
			)
		}
		applied = tombstoneErr == nil
		if !applied {
			return nil
		}
		return w.markAndEmit(
			ctx,
			queries,
			scopes,
			outbox.RepositoryTombstonedKind,
			key,
			at,
		)
	})
	if err != nil {
		return false, fmt.Errorf("tombstone repository: %w", err)
	}
	w.observer.CacheWrite(ctx, "repository", applied, true)
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
	if err := w.markAndEmit(
		ctx,
		queries,
		scopes,
		outbox.RepositoryChangedKind,
		RepositoryEntityKey(row.InstallationID, row.GhID),
		observedAt,
	); err != nil {
		return dbgen.Repo{}, false, err
	}
	return row, true, nil
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
