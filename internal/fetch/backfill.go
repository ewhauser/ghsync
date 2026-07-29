package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const backfillPollInterval = 100 * time.Millisecond

func (h *Handler) BackfillInstallationPage(
	ctx context.Context,
	args queue.BackfillInstallationPageArgs,
) error {
	cursor, err := dbgen.New(h.pool).GetInstallationBackfillCursor(
		ctx,
		args.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("read installation backfill cursor: %w", err)
	}
	if cursor.Phase != args.Phase || int(cursor.Page) != args.Page ||
		cursor.Phase == "done" {
		return nil
	}
	switch cursor.Phase {
	case "repositories":
		class := backfillClass(cursor.QueueName)
		repositories, response, err := h.rest.ListInstallationRepositories(
			ctx,
			class,
			gh.ListRepositoriesOptions{
				PerPage: h.backfillPageSize,
				Page:    args.Page,
			},
			"",
		)
		if err != nil {
			return fmt.Errorf(
				"backfill installation repositories page %d: %w",
				args.Page,
				err,
			)
		}
		nextPhase := "waiting"
		nextPage := 1
		nextQueue := cursor.QueueName
		if response.NextPage != 0 {
			nextPhase = "repositories"
			nextPage = response.NextPage
			nextQueue = queue.QueueSweep
		}
		return h.advanceInstallationBackfill(
			ctx,
			args,
			repositories,
			nextPhase,
			nextPage,
			nextQueue,
		)
	case "waiting":
		pending, err := dbgen.New(h.pool).CountPendingInstallationRepos(
			ctx,
			args.InstallationID,
		)
		if err != nil {
			return fmt.Errorf("count pending repository backfills: %w", err)
		}
		if pending > 0 {
			return river.JobSnooze(backfillPollInterval)
		}
		return h.advanceInstallationBackfill(
			ctx,
			args,
			nil,
			"done",
			1,
			queue.QueueSweep,
		)
	default:
		return fmt.Errorf(
			"unsupported installation backfill phase %q",
			cursor.Phase,
		)
	}
}

func (h *Handler) advanceInstallationBackfill(
	ctx context.Context,
	args queue.BackfillInstallationPageArgs,
	repositories []gh.Repository,
	nextPhase string,
	nextPage int,
	nextQueue string,
) error {
	client := h.riverClient(ctx)
	if client == nil {
		return fmt.Errorf("river client missing from installation backfill")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin installation backfill advance: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	cursor, err := queries.GetInstallationBackfillCursorForUpdate(
		ctx,
		args.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("lock installation backfill cursor: %w", err)
	}
	if cursor.Phase != args.Phase || int(cursor.Page) != args.Page {
		return tx.Commit(ctx)
	}
	for _, repository := range repositories {
		repoCursor, err := queries.EnsureBackfillCursor(
			ctx,
			dbgen.EnsureBackfillCursorParams{
				InstallationID: args.InstallationID,
				RepoFullName:   repository.FullName,
			},
		)
		if err != nil {
			return fmt.Errorf("ensure repository backfill cursor: %w", err)
		}
		if repoCursor.Phase == "done" {
			continue
		}
		if _, err := client.InsertTx(
			ctx,
			tx,
			queue.NewBackfillRepoPageArgs(
				args.InstallationID,
				repository.FullName,
				repoCursor.Phase,
				int(repoCursor.Page),
			),
			queue.NewBackfillInsertOptsForQueue(repoCursor.QueueName),
		); err != nil {
			return fmt.Errorf("insert repository backfill kickoff: %w", err)
		}
	}
	if _, err := queries.AdvanceInstallationBackfillCursor(
		ctx,
		dbgen.AdvanceInstallationBackfillCursorParams{
			NextPhase:      nextPhase,
			NextPage:       int32(nextPage),
			QueueName:      nextQueue,
			InstallationID: args.InstallationID,
			ExpectedPhase:  args.Phase,
			ExpectedPage:   int32(args.Page),
		},
	); err != nil {
		return fmt.Errorf("advance installation backfill cursor: %w", err)
	}
	if nextPhase != "done" {
		if _, err := client.InsertTx(
			ctx,
			tx,
			queue.NewBackfillInstallationPageArgs(
				args.InstallationID,
				nextPhase,
				nextPage,
			),
			queue.NewBackfillInsertOptsForQueue(nextQueue),
		); err != nil {
			return fmt.Errorf("insert next installation backfill page: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit installation backfill advance: %w", err)
	}
	return nil
}

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
	class := backfillClass(cursor.QueueName)
	source := store.SyncSourceBackfill
	if cursor.QueueName == queue.QueueSweep {
		source = store.SyncSourceReconcile
	}
	owner, repoName, err := repoutil.Split(args.RepoFullName)
	if err != nil {
		return err
	}

	switch args.Phase {
	case "repository":
		if err := h.RefreshRepository(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewRefreshRepositoryArgs(
					"repo:" + args.RepoFullName + ":metadata",
				).RefreshArgs,
				Queue: cursor.QueueName,
			},
		); err != nil {
			return fmt.Errorf("backfill repository: %w", err)
		}
		return h.advanceBackfill(
			ctx,
			args,
			"stacks",
			1,
			cursor.QueueName,
			[]queue.RefreshSpec{{
				Kind: queue.KindRefreshRepoRules,
				Key:  "repo_rules:" + args.RepoFullName + ":rules",
			}},
		)

	case "stacks":
		stacks, response, err := h.rest.ListStacks(
			ctx,
			class,
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
				queue.QueueSweep,
				specs,
			)
		}
		return h.advanceBackfill(
			ctx,
			args,
			"pull_requests",
			1,
			cursor.QueueName,
			specs,
		)

	case "pull_requests":
		page, _, _, err := decodePullBackfillCursor(args.Page)
		if err != nil {
			return err
		}
		pulls, response, err := h.rest.ListPulls(
			ctx,
			class,
			owner,
			repoName,
			gh.ListPullsOptions{
				State:     "open",
				Sort:      "created",
				Direction: "asc",
				PerPage:   h.backfillPageSize,
				Page:      page,
			},
			"",
		)
		if err != nil {
			return fmt.Errorf(
				"backfill pull requests page %d: %w",
				page,
				err,
			)
		}
		repository, err := h.ensureRepository(
			ctx,
			class,
			source,
			args.RepoFullName,
		)
		if err != nil {
			return err
		}
		if len(pulls) > 0 {
			etags := make(map[int]string, len(pulls))
			for _, pull := range pulls {
				etags[pull.GetNumber()] = response.ETag
			}
			records := pullRecordsFromList(
				repository,
				pulls,
				etags,
				source,
				time.Now(),
			)
			applies := make([]store.PullRequestApply, 0, len(records))
			for _, record := range records {
				applies = append(applies, store.PullRequestApply{Record: record})
			}
			for key, outcome := range h.writer.ApplyPullRequestBatch(ctx, applies) {
				if outcome.Err != nil {
					return fmt.Errorf(
						"apply backfill PR snapshot %s: %w",
						key,
						outcome.Err,
					)
				}
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
		return h.advancePullRequestBackfill(
			ctx,
			args,
			response.NextPage,
			specs,
		)

	case "waiting":
		queries := dbgen.New(h.pool)
		if _, err := queries.MarkCompletedBackfillChildren(
			ctx,
			dbgen.MarkCompletedBackfillChildrenParams{
				InstallationID: args.InstallationID,
				RepoFullName:   args.RepoFullName,
			},
		); err != nil {
			return fmt.Errorf("mark completed backfill children: %w", err)
		}
		pending, err := queries.CountPendingBackfillChildren(
			ctx,
			dbgen.CountPendingBackfillChildrenParams{
				InstallationID: args.InstallationID,
				RepoFullName:   args.RepoFullName,
			},
		)
		if err != nil {
			return fmt.Errorf("count pending backfill children: %w", err)
		}
		if pending > 0 {
			return river.JobSnooze(backfillPollInterval)
		}
		return h.advanceBackfill(
			ctx,
			args,
			"done",
			1,
			queue.QueueSweep,
			nil,
		)
	default:
		return fmt.Errorf("unsupported backfill phase %q", args.Phase)
	}
}

// The existing backfill cursor has one positive integer. Pull-request scans
// need the next API page, the number of identifiers added in the current
// overlap pass, and an alternating pass bit. Page stores a reversible encoding
// of that state. The bit prevents a one-page pass from enqueueing identical
// River args while its preceding page-1 job is still running. This keeps crash
// recovery authoritative in backfill_cursors without a process-local seen set.
func encodePullBackfillCursor(
	page int,
	passNewCount int,
	alternatePass bool,
) (int, error) {
	if page <= 0 || passNewCount < 0 {
		return 0, fmt.Errorf(
			"invalid pull request backfill cursor page=%d pass_new_count=%d",
			page,
			passNewCount,
		)
	}
	x := int64(page - 1)
	y := int64(passNewCount)
	diagonal := x + y
	paired := diagonal*(diagonal+1)/2 + y
	encoded := paired*2 + 1
	if alternatePass {
		encoded++
	}
	const maxInt32 = int64(1<<31 - 1)
	if encoded > maxInt32 {
		return 0, fmt.Errorf(
			"pull request backfill cursor exceeds PostgreSQL integer range",
		)
	}
	return int(encoded), nil
}

func decodePullBackfillCursor(encoded int) (int, int, bool, error) {
	if encoded <= 0 {
		return 0, 0, false, fmt.Errorf(
			"invalid pull request backfill cursor %d",
			encoded,
		)
	}
	raw := int64(encoded - 1)
	alternatePass := raw%2 == 1
	paired := raw / 2
	diagonal := int64(
		(math.Sqrt(float64(8*paired+1)) - 1) / 2,
	)
	for diagonal*(diagonal+1)/2 > paired {
		diagonal--
	}
	for (diagonal+1)*(diagonal+2)/2 <= paired {
		diagonal++
	}
	offset := paired - diagonal*(diagonal+1)/2
	page := int(diagonal-offset) + 1
	passNewCount := int(offset)
	roundTrip, err := encodePullBackfillCursor(
		page,
		passNewCount,
		alternatePass,
	)
	if err != nil || roundTrip != encoded {
		return 0, 0, false, fmt.Errorf(
			"invalid pull request backfill cursor %d",
			encoded,
		)
	}
	return page, passNewCount, alternatePass, nil
}

func (h *Handler) advancePullRequestBackfill(
	ctx context.Context,
	args queue.BackfillRepoPageArgs,
	nextAPIPage int,
	specs []queue.RefreshSpec,
) error {
	client := h.riverClient(ctx)
	if client == nil {
		return fmt.Errorf("river client missing from backfill context")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin pull request backfill advance: %w", err)
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
		return fmt.Errorf("lock pull request backfill cursor: %w", err)
	}
	if cursor.Phase != args.Phase || int(cursor.Page) != args.Page {
		return tx.Commit(ctx)
	}
	_, passNewCount, alternatePass, err := decodePullBackfillCursor(
		int(cursor.Page),
	)
	if err != nil {
		return err
	}
	unseen, err := unseenBackfillSpecs(ctx, tx, args, specs)
	if err != nil {
		return err
	}
	passNewCount += len(unseen)
	generations, err := queue.InsertRefreshesTxReturning(
		ctx,
		tx,
		client,
		unseen,
		cursor.QueueName,
	)
	if err != nil {
		return err
	}
	if err := persistBackfillChildren(ctx, queries, args, generations); err != nil {
		return err
	}

	nextPhase := "pull_requests"
	nextQueue := queue.QueueSweep
	nextCursor := 1
	switch {
	case nextAPIPage != 0:
		nextCursor, err = encodePullBackfillCursor(
			nextAPIPage,
			passNewCount,
			alternatePass,
		)
	case passNewCount > 0:
		nextCursor, err = encodePullBackfillCursor(
			1,
			0,
			!alternatePass,
		)
	default:
		nextPhase = "waiting"
	}
	if err != nil {
		return err
	}
	if _, err := queries.AdvanceBackfillCursor(
		ctx,
		dbgen.AdvanceBackfillCursorParams{
			NextPhase:      nextPhase,
			NextPage:       int32(nextCursor),
			QueueName:      nextQueue,
			InstallationID: args.InstallationID,
			RepoFullName:   args.RepoFullName,
			ExpectedPhase:  args.Phase,
			ExpectedPage:   int32(args.Page),
		},
	); err != nil {
		return fmt.Errorf("advance pull request backfill cursor: %w", err)
	}
	if _, err := client.InsertTx(
		ctx,
		tx,
		queue.NewBackfillRepoPageArgs(
			args.InstallationID,
			args.RepoFullName,
			nextPhase,
			nextCursor,
		),
		queue.NewBackfillInsertOptsForQueue(nextQueue),
	); err != nil {
		return fmt.Errorf("insert next pull request backfill page: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit pull request backfill advance: %w", err)
	}
	return nil
}

func unseenBackfillSpecs(
	ctx context.Context,
	tx pgx.Tx,
	args queue.BackfillRepoPageArgs,
	specs []queue.RefreshSpec,
) ([]queue.RefreshSpec, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(specs))
	for _, spec := range specs {
		keys = append(keys, spec.Key)
	}
	rows, err := tx.Query(ctx, `
		SELECT refresh_key
		FROM backfill_children
		WHERE installation_id = $1
		  AND repo_full_name = $2
		  AND kind = $3
		  AND refresh_key = ANY($4::text[])
	`, args.InstallationID, args.RepoFullName, queue.KindRefreshPR, keys)
	if err != nil {
		return nil, fmt.Errorf("read seen pull request backfill keys: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{}, len(specs))
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan seen pull request backfill key: %w", err)
		}
		seen[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read seen pull request backfill keys: %w", err)
	}
	unseen := make([]queue.RefreshSpec, 0, len(specs))
	for _, spec := range specs {
		if _, exists := seen[spec.Key]; !exists {
			unseen = append(unseen, spec)
		}
	}
	return unseen, nil
}

func persistBackfillChildren(
	ctx context.Context,
	queries *dbgen.Queries,
	args queue.BackfillRepoPageArgs,
	generations []queue.RefreshGeneration,
) error {
	if len(generations) == 0 {
		return nil
	}
	type child struct {
		Kind             string `json:"kind"`
		RefreshKey       string `json:"refresh_key"`
		TargetGeneration int64  `json:"target_generation"`
	}
	children := make([]child, 0, len(generations))
	for _, generation := range generations {
		children = append(children, child{
			Kind:             generation.Spec.Kind,
			RefreshKey:       generation.Spec.Key,
			TargetGeneration: generation.Generation,
		})
	}
	encoded, err := json.Marshal(children)
	if err != nil {
		return fmt.Errorf("encode backfill children: %w", err)
	}
	if err := queries.UpsertBackfillChildren(
		ctx,
		dbgen.UpsertBackfillChildrenParams{
			Children:       encoded,
			InstallationID: args.InstallationID,
			RepoFullName:   args.RepoFullName,
		},
	); err != nil {
		return fmt.Errorf("persist backfill children: %w", err)
	}
	return nil
}

func (h *Handler) advanceBackfill(
	ctx context.Context,
	args queue.BackfillRepoPageArgs,
	nextPhase string,
	nextPage int,
	nextQueue string,
	specs []queue.RefreshSpec,
) error {
	client := h.riverClient(ctx)
	if client == nil {
		return fmt.Errorf("river client missing from backfill context")
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
	generations, err := queue.InsertRefreshesTxReturning(
		ctx,
		tx,
		client,
		specs,
		nextQueue,
	)
	if err != nil {
		return err
	}
	if len(generations) > 0 {
		type child struct {
			Kind             string `json:"kind"`
			RefreshKey       string `json:"refresh_key"`
			TargetGeneration int64  `json:"target_generation"`
		}
		children := make([]child, 0, len(generations))
		for _, generation := range generations {
			children = append(children, child{
				Kind:             generation.Spec.Kind,
				RefreshKey:       generation.Spec.Key,
				TargetGeneration: generation.Generation,
			})
		}
		encoded, err := json.Marshal(children)
		if err != nil {
			return fmt.Errorf("encode backfill children: %w", err)
		}
		if err := queries.UpsertBackfillChildren(
			ctx,
			dbgen.UpsertBackfillChildrenParams{
				Children:       encoded,
				InstallationID: args.InstallationID,
				RepoFullName:   args.RepoFullName,
			},
		); err != nil {
			return fmt.Errorf("persist backfill children: %w", err)
		}
	}
	if _, err := queries.AdvanceBackfillCursor(
		ctx,
		dbgen.AdvanceBackfillCursorParams{
			NextPhase:      nextPhase,
			NextPage:       int32(nextPage),
			QueueName:      nextQueue,
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
			queue.NewBackfillInsertOptsForQueue(nextQueue),
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

func StartInstallationBackfill(
	ctx context.Context,
	pool *pgxpool.Pool,
	client *river.Client[pgx.Tx],
	installationID int64,
) (dbgen.InstallationBackfillCursor, error) {
	if pool == nil || client == nil || installationID <= 0 {
		return dbgen.InstallationBackfillCursor{}, fmt.Errorf(
			"invalid installation backfill kickoff",
		)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbgen.InstallationBackfillCursor{}, fmt.Errorf(
			"begin installation backfill kickoff: %w",
			err,
		)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	cursor, err := dbgen.New(tx).EnsureInstallationBackfillCursor(
		ctx,
		installationID,
	)
	if err != nil {
		return dbgen.InstallationBackfillCursor{}, fmt.Errorf(
			"ensure installation backfill cursor: %w",
			err,
		)
	}
	if cursor.Phase != "done" {
		if _, err := client.InsertTx(
			ctx,
			tx,
			queue.NewBackfillInstallationPageArgs(
				installationID,
				cursor.Phase,
				int(cursor.Page),
			),
			queue.NewBackfillInsertOptsForQueue(cursor.QueueName),
		); err != nil {
			return dbgen.InstallationBackfillCursor{}, fmt.Errorf(
				"insert installation backfill kickoff: %w",
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.InstallationBackfillCursor{}, fmt.Errorf(
			"commit installation backfill kickoff: %w",
			err,
		)
	}
	return cursor, nil
}

// StartBackfill remains a focused test/ops seam; production CLI onboarding
// uses StartInstallationBackfill so C-O5 begins at installation enumeration.
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
	if _, _, err := repoutil.Split(repoFullName); err != nil {
		return dbgen.BackfillCursor{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return dbgen.BackfillCursor{}, fmt.Errorf(
			"begin backfill kickoff: %w",
			err,
		)
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
		return dbgen.BackfillCursor{}, fmt.Errorf(
			"ensure backfill cursor: %w",
			err,
		)
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
			queue.NewBackfillInsertOptsForQueue(cursor.QueueName),
		); err != nil {
			return dbgen.BackfillCursor{}, fmt.Errorf(
				"insert backfill kickoff: %w",
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.BackfillCursor{}, fmt.Errorf(
			"commit backfill kickoff: %w",
			err,
		)
	}
	return cursor, nil
}

func backfillClass(queueName string) budget.Class {
	if queueName == queue.QueueSweep {
		return budget.Sweep
	}
	return budget.Interactive
}
