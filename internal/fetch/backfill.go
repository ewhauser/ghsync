package fetch

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

func (h *Handler) BackfillRepoPage(
	ctx context.Context,
	args queue.BackfillRepoPageArgs,
) error {
	cursor, err := dbgen.New(h.pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: args.InstallationID,
			RepoFullName:   args.RepoFullName,
		},
	)
	if err != nil {
		return fmt.Errorf("read backfill cursor: %w", err)
	}
	if cursor.Phase != args.Phase || int(cursor.Page) != args.Page ||
		cursor.Phase == "done" {
		return nil
	}
	owner, repoName, err := splitRepo(args.RepoFullName)
	if err != nil {
		return err
	}

	switch args.Phase {
	case "repository":
		repository, response, err := h.rest.GetRepository(
			ctx,
			budget.Interactive,
			owner,
			repoName,
			"",
		)
		if err != nil {
			return fmt.Errorf("backfill repository: %w", err)
		}
		record := repositoryRecordFromREST(
			repository,
			h.installationID,
			h.orgID,
		)
		if _, err := h.writer.ApplyRepository(
			ctx,
			record,
			store.SyncSourceBackfill,
			response.ETag,
			time.Now(),
		); err != nil {
			return err
		}
		return h.advanceBackfill(ctx, args, "stacks", 1, nil)

	case "stacks":
		stacks, response, err := h.rest.ListStacks(
			ctx,
			budget.Interactive,
			owner,
			repoName,
			gh.ListStacksOptions{
				PerPage: h.backfillPageSize,
				Page:    args.Page,
			},
			"",
		)
		if err != nil {
			return fmt.Errorf("backfill stacks page %d: %w", args.Page, err)
		}
		specs := make([]queue.RefreshSpec, 0, len(stacks))
		for _, stack := range stacks {
			specs = append(specs, queue.RefreshSpec{
				Kind: queue.KindRefreshStack,
				Key: fmt.Sprintf(
					"stack:%s:%d",
					args.RepoFullName,
					stack.Number,
				),
			})
		}
		if response.NextPage != 0 {
			return h.advanceBackfill(
				ctx,
				args,
				"stacks",
				response.NextPage,
				specs,
			)
		}
		return h.advanceBackfill(ctx, args, "pull_requests", 1, specs)

	case "pull_requests":
		pulls, response, err := h.rest.ListPulls(
			ctx,
			budget.Interactive,
			owner,
			repoName,
			gh.ListPullsOptions{
				State:     "open",
				Sort:      "updated",
				Direction: "asc",
				PerPage:   h.backfillPageSize,
				Page:      args.Page,
			},
			"",
		)
		if err != nil {
			return fmt.Errorf("backfill pull requests page %d: %w", args.Page, err)
		}
		repository, err := h.ensureRepository(
			ctx,
			budget.Interactive,
			store.SyncSourceBackfill,
			args.RepoFullName,
		)
		if err != nil {
			return err
		}
		if len(pulls) > 0 {
			// The list response is itself authoritative. Applying its shallow
			// rows seeds node IDs so the detail jobs gang through nodes().
			if _, err := h.writer.ApplyPullRequestBatch(
				ctx,
				pullRecordsFromList(
					repository,
					pulls,
					response.ETag,
					store.SyncSourceBackfill,
					time.Now(),
				),
			); err != nil {
				return fmt.Errorf("apply backfill PR page: %w", err)
			}
		}
		specs := make([]queue.RefreshSpec, 0, len(pulls))
		for _, pull := range pulls {
			specs = append(specs, queue.RefreshSpec{
				Kind: queue.KindRefreshPR,
				Key: fmt.Sprintf(
					"pr:%s:%d",
					args.RepoFullName,
					pull.GetNumber(),
				),
			})
		}
		if response.NextPage != 0 {
			return h.advanceBackfill(
				ctx,
				args,
				"pull_requests",
				response.NextPage,
				specs,
			)
		}
		return h.advanceBackfill(ctx, args, "done", 1, specs)

	default:
		return fmt.Errorf("unsupported backfill phase %q", args.Phase)
	}
}

func (h *Handler) advanceBackfill(
	ctx context.Context,
	args queue.BackfillRepoPageArgs,
	nextPhase string,
	nextPage int,
	specs []queue.RefreshSpec,
) error {
	client := h.riverClient(ctx)
	if client == nil {
		return fmt.Errorf("River client missing from backfill context")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin backfill advance: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	cursor, err := queries.GetBackfillCursorForUpdate(
		ctx,
		dbgen.GetBackfillCursorForUpdateParams{
			InstallationID: args.InstallationID,
			RepoFullName:   args.RepoFullName,
		},
	)
	if err != nil {
		return fmt.Errorf("lock backfill cursor: %w", err)
	}
	if cursor.Phase != args.Phase || int(cursor.Page) != args.Page {
		return tx.Commit(ctx)
	}
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		specs,
		queue.QueueInteractive,
	); err != nil {
		return err
	}
	if _, err := queries.AdvanceBackfillCursor(
		ctx,
		dbgen.AdvanceBackfillCursorParams{
			NextPhase:      nextPhase,
			NextPage:       int32(nextPage),
			InstallationID: args.InstallationID,
			RepoFullName:   args.RepoFullName,
			ExpectedPhase:  args.Phase,
			ExpectedPage:   int32(args.Page),
		},
	); err != nil {
		return fmt.Errorf("advance backfill cursor: %w", err)
	}
	if nextPhase != "done" {
		if _, err := client.InsertTx(
			ctx,
			tx,
			queue.NewBackfillRepoPageArgs(
				args.InstallationID,
				args.RepoFullName,
				nextPhase,
				nextPage,
			),
			queue.NewBackfillInsertOpts(),
		); err != nil {
			return fmt.Errorf("insert next backfill page: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit backfill advance: %w", err)
	}
	return nil
}

func (h *Handler) riverClient(ctx context.Context) *river.Client[pgx.Tx] {
	if client, _ := river.ClientFromContextSafely[pgx.Tx](ctx); client != nil {
		return client
	}
	h.riverMu.RLock()
	defer h.riverMu.RUnlock()
	return h.river
}

// StartBackfill is the idempotent CLI kickoff. A restart inserts the cursor's
// current expected job; River uniqueness and the cursor predicate make this
// safe while a worker is running or after a crash.
func StartBackfill(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	installationID int64,
	repoFullName string,
) (dbgen.BackfillCursor, error) {
	if pool == nil || client == nil || installationID <= 0 {
		return dbgen.BackfillCursor{}, fmt.Errorf("invalid backfill kickoff")
	}
	if _, _, err := splitRepo(repoFullName); err != nil {
		return dbgen.BackfillCursor{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbgen.BackfillCursor{}, fmt.Errorf("begin backfill kickoff: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	cursor, err := dbgen.New(tx).EnsureBackfillCursor(
		ctx,
		dbgen.EnsureBackfillCursorParams{
			InstallationID: installationID,
			RepoFullName:   repoFullName,
		},
	)
	if err != nil {
		return dbgen.BackfillCursor{}, fmt.Errorf("ensure backfill cursor: %w", err)
	}
	if cursor.Phase != "done" {
		if _, err := client.InsertTx(
			ctx,
			tx,
			queue.NewBackfillRepoPageArgs(
				installationID,
				repoFullName,
				cursor.Phase,
				int(cursor.Page),
			),
			queue.NewBackfillInsertOpts(),
		); err != nil {
			return dbgen.BackfillCursor{}, fmt.Errorf("insert backfill kickoff: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.BackfillCursor{}, fmt.Errorf("commit backfill kickoff: %w", err)
	}
	return cursor, nil
}
