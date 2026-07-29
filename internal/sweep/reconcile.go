package sweep

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/opsstate"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func (s *Service) Kickoff(
	ctx context.Context,
	args KickoffArgs,
) error {
	if args.Installation != s.config.InstallationID {
		return nil
	}
	if s.riverClient() == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	switch args.SweepKind {
	case KindStacks:
		if err := s.enqueueStaleStacks(ctx); err != nil {
			return err
		}
		return s.kickoffRepositoryScopes(
			ctx,
			KindStacks,
			scheduleForBound(
				s.config.OpenStackMaxStaleness,
			).Cadence,
		)
	case KindPullRequests:
		if err := s.enqueueStalePullRequests(ctx); err != nil {
			return err
		}
		return s.kickoffRepositoryScopes(
			ctx,
			KindPullRequests,
			scheduleForBound(
				s.config.OpenPRMaxStaleness,
			).Cadence,
		)
	case KindRepoRules:
		return s.enqueueStaleRepoRules(ctx)
	case KindClosed:
		return s.enqueueClosedTracked(ctx)
	case KindRepositories:
		return s.startOrResumeScope(
			ctx,
			KindRepositories,
			"",
			"1",
			scheduleForBound(
				s.config.RepositoryListPeriod,
			).Cadence,
		)
	default:
		return fmt.Errorf("unsupported sweep kind %q", args.SweepKind)
	}
}

func (s *Service) kickoffRepositoryScopes(
	ctx context.Context,
	kind string,
	period time.Duration,
) error {
	queries := dbgen.New(s.pool)
	if _, err := queries.ReapOrphanedRepositorySweepCursors(
		ctx,
		s.config.InstallationID,
	); err != nil {
		return fmt.Errorf("reap orphaned repository sweep cursors: %w", err)
	}
	repositories, err := queries.ListSweepRepositories(
		ctx,
		s.config.InstallationID,
	)
	if err != nil {
		return fmt.Errorf("list sweep repositories: %w", err)
	}
	for _, repository := range repositories {
		if err := s.startOrResumeScope(
			ctx,
			kind,
			repository.FullName,
			"1",
			period,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) startOrResumeScope(
	ctx context.Context,
	kind string,
	scope string,
	firstCursor string,
	period time.Duration,
) error {
	client := s.riverClient()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin sweep cursor kickoff: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	params := dbgen.EnsureSweepCursorParams{
		InstallationID: s.config.InstallationID,
		SweepKind:      kind,
		ScopeKey:       scope,
	}
	if _, err := queries.EnsureSweepCursor(ctx, params); err != nil {
		return fmt.Errorf("ensure sweep cursor: %w", err)
	}
	cursor, err := queries.GetSweepCursorForUpdate(
		ctx,
		dbgen.GetSweepCursorForUpdateParams(params),
	)
	if err != nil {
		return fmt.Errorf("lock sweep cursor: %w", err)
	}
	now := s.config.Now().UTC()
	overrun := false
	elapsed := time.Duration(0)
	if !cursor.StartedAt.Valid || cursor.CompletedAt.Valid {
		cursor, err = queries.StartSweepCursor(
			ctx,
			dbgen.StartSweepCursorParams{
				FirstCursor:    firstCursor,
				StartedAt:      repoutil.Timestamptz(now),
				InstallationID: s.config.InstallationID,
				SweepKind:      kind,
				ScopeKey:       scope,
			},
		)
		if err != nil {
			return fmt.Errorf("start sweep cursor: %w", err)
		}
	} else {
		elapsed = now.Sub(cursor.StartedAt.Time)
		overrun = elapsed >= period
	}
	if _, err := client.InsertTx(
		ctx,
		tx,
		ListPageArgs{
			SweepKind:    kind,
			Installation: s.config.InstallationID,
			ScopeKey:     scope,
			Cursor:       cursor.Cursor,
		},
		listPageInsertOpts(),
	); err != nil {
		return fmt.Errorf("insert sweep page: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sweep cursor kickoff: %w", err)
	}
	if overrun {
		// C-R2: this is the M4 observable hook; M6 maps it to a counter.
		s.config.Observer.SweepOverrun(ctx, kind, scope, elapsed)
	}
	return nil
}

type fetchedPage struct {
	keys         []string
	nextCursor   string
	etag         string
	notModified  bool
	refreshSpecs []queue.RefreshSpec
}

func (s *Service) ReconcilePage(
	ctx context.Context,
	args ListPageArgs,
) error {
	if args.Installation != s.config.InstallationID {
		return nil
	}
	if s.rest == nil {
		return fmt.Errorf("sweep REST client is not configured")
	}
	queries := dbgen.New(s.pool)
	cursorParams := dbgen.GetSweepCursorParams{
		InstallationID: args.Installation,
		SweepKind:      args.SweepKind,
		ScopeKey:       args.ScopeKey,
	}
	cursor, err := queries.GetSweepCursor(ctx, cursorParams)
	if err != nil {
		return fmt.Errorf("read sweep cursor: %w", err)
	}
	if cursor.CompletedAt.Valid || cursor.Cursor != args.Cursor {
		return nil
	}
	pageParams := dbgen.GetSweepPageParams{
		InstallationID: args.Installation,
		SweepKind:      args.SweepKind,
		ScopeKey:       args.ScopeKey,
		Cursor:         args.Cursor,
	}
	cachedPage, pageErr := queries.GetSweepPage(ctx, pageParams)
	if pageErr != nil && !errors.Is(pageErr, pgx.ErrNoRows) {
		return fmt.Errorf("read sweep page validator: %w", pageErr)
	}
	etag := ""
	if pageErr == nil {
		etag = cachedPage.Etag
	}
	page, err := s.fetchPage(ctx, args, etag)
	if err != nil {
		return err
	}
	if page.notModified {
		if pageErr != nil {
			return fmt.Errorf("304 sweep page has no persisted page state")
		}
		if err := json.Unmarshal(cachedPage.EntityKeys, &page.keys); err != nil {
			return fmt.Errorf("decode cached sweep page keys: %w", err)
		}
		page.nextCursor = cachedPage.NextCursor
		page.etag = cachedPage.Etag
	}
	return s.persistPage(ctx, args, page)
}

func (s *Service) fetchPage(
	ctx context.Context,
	args ListPageArgs,
	etag string,
) (fetchedPage, error) {
	switch args.SweepKind {
	case KindRepositories:
		page, err := decimalCursor(args.Cursor)
		if err != nil {
			return fetchedPage{}, err
		}
		repositories, response, err := s.rest.ListInstallationRepositories(
			ctx,
			queueClass(),
			gh.ListRepositoriesOptions{
				PerPage: s.config.PageSize,
				Page:    page,
			},
			etag,
		)
		if err != nil {
			return fetchedPage{}, fmt.Errorf(
				"list installation repositories page %d: %w",
				page,
				err,
			)
		}
		result := fetchedPage{
			etag:        response.ETag,
			notModified: response.NotModified,
			nextCursor:  numericNextCursor(response.NextPage),
		}
		for _, repository := range repositories {
			key := "repo:" + repository.FullName + ":metadata"
			result.keys = append(result.keys, key)
			result.refreshSpecs = append(
				result.refreshSpecs,
				queue.RefreshSpec{
					Kind: queue.KindRefreshRepository,
					Key:  key,
					Deadline: s.config.Now().Add(
						scheduleForBound(
							s.config.RepositoryListPeriod,
						).CompletionHeadroom,
					),
				},
			)
		}
		return result, nil
	case KindStacks:
		page, err := decimalCursor(args.Cursor)
		if err != nil {
			return fetchedPage{}, err
		}
		owner, repo, err := repoutil.Split(args.ScopeKey)
		if err != nil {
			return fetchedPage{}, err
		}
		stacks, response, err := s.rest.ListStacks(
			ctx,
			queueClass(),
			owner,
			repo,
			gh.ListStacksOptions{
				PerPage: s.config.PageSize,
				Page:    page,
			},
			etag,
		)
		if err != nil {
			return fetchedPage{}, fmt.Errorf(
				"list stacks %s page %d: %w",
				args.ScopeKey,
				page,
				err,
			)
		}
		result := fetchedPage{
			etag:        response.ETag,
			notModified: response.NotModified,
			nextCursor:  numericNextCursor(response.NextPage),
		}
		for _, stack := range stacks {
			key := fmt.Sprintf(
				"stack:%s:%d",
				args.ScopeKey,
				stack.Number,
			)
			result.keys = append(result.keys, key)
			result.refreshSpecs = append(
				result.refreshSpecs,
				queue.RefreshSpec{
					Kind: queue.KindRefreshStack,
					Key:  key,
					Deadline: s.config.Now().Add(
						scheduleForBound(
							s.config.OpenStackMaxStaleness,
						).CompletionHeadroom,
					),
				},
			)
		}
		return result, nil
	case KindPullRequests:
		page, err := decimalCursor(args.Cursor)
		if err != nil {
			return fetchedPage{}, err
		}
		owner, repo, err := repoutil.Split(args.ScopeKey)
		if err != nil {
			return fetchedPage{}, err
		}
		pulls, response, err := s.rest.ListPulls(
			ctx,
			queueClass(),
			owner,
			repo,
			gh.ListPullsOptions{
				State:     "open",
				Sort:      "created",
				Direction: "asc",
				PerPage:   s.config.PageSize,
				Page:      page,
			},
			etag,
		)
		if err != nil {
			return fetchedPage{}, fmt.Errorf(
				"list pull requests %s page %d: %w",
				args.ScopeKey,
				page,
				err,
			)
		}
		result := fetchedPage{
			etag:        response.ETag,
			notModified: response.NotModified,
			nextCursor:  numericNextCursor(response.NextPage),
		}
		for _, pull := range pulls {
			key := fmt.Sprintf(
				"pr:%s:%d",
				args.ScopeKey,
				pull.GetNumber(),
			)
			result.keys = append(result.keys, key)
			result.refreshSpecs = append(
				result.refreshSpecs,
				queue.RefreshSpec{
					Kind: queue.KindRefreshPR,
					Key:  key,
					Deadline: s.config.Now().Add(
						scheduleForBound(
							s.config.OpenPRMaxStaleness,
						).CompletionHeadroom,
					),
				},
			)
		}
		return result, nil
	default:
		return fetchedPage{}, fmt.Errorf(
			"unsupported list sweep kind %q",
			args.SweepKind,
		)
	}
}

func (s *Service) persistPage(
	ctx context.Context,
	args ListPageArgs,
	page fetchedPage,
) error {
	client := s.riverClient()
	if client == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	encodedPageKeys, err := json.Marshal(sortedUnique(page.keys))
	if err != nil {
		return fmt.Errorf("encode sweep page keys: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin sweep page commit: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	queries := dbgen.New(tx)
	current, err := queries.GetSweepCursorForUpdate(
		ctx,
		dbgen.GetSweepCursorForUpdateParams{
			InstallationID: args.Installation,
			SweepKind:      args.SweepKind,
			ScopeKey:       args.ScopeKey,
		},
	)
	if err != nil {
		return fmt.Errorf("lock sweep page cursor: %w", err)
	}
	if current.CompletedAt.Valid || current.Cursor != args.Cursor {
		return tx.Commit(ctx)
	}
	now := s.config.Now().UTC()
	newKeys, err := queries.InsertSweepSeenKeys(
		ctx,
		dbgen.InsertSweepSeenKeysParams{
			EntityKeys:     sortedUnique(page.keys),
			InstallationID: args.Installation,
			SweepKind:      args.SweepKind,
			ScopeKey:       args.ScopeKey,
			FirstSeenAt:    repoutil.Timestamptz(now),
		},
	)
	if err != nil {
		return fmt.Errorf("persist sweep seen keys: %w", err)
	}
	passNewCount := int64(current.PassNewCount) + newKeys
	if err := queries.UpsertSweepPage(
		ctx,
		dbgen.UpsertSweepPageParams{
			InstallationID: args.Installation,
			SweepKind:      args.SweepKind,
			ScopeKey:       args.ScopeKey,
			Cursor:         args.Cursor,
			Etag:           page.etag,
			NextCursor:     page.nextCursor,
			EntityKeys:     encodedPageKeys,
			ListSeenAt:     repoutil.Timestamptz(now),
		},
	); err != nil {
		return fmt.Errorf("persist sweep page validator: %w", err)
	}
	specs := append([]queue.RefreshSpec(nil), page.refreshSpecs...)
	completed := false
	if page.nextCursor == "" {
		// PR listings can shrink between numbered pages. Restart from the
		// leading page until a complete overlap pass adds no identifiers.
		if args.SweepKind == KindPullRequests && passNewCount > 0 {
			if _, err := queries.RestartSweepCursorPass(
				ctx,
				dbgen.RestartSweepCursorPassParams{
					FirstCursor:    "1",
					UpdatedAt:      repoutil.Timestamptz(now),
					InstallationID: args.Installation,
					SweepKind:      args.SweepKind,
					ScopeKey:       args.ScopeKey,
					ExpectedCursor: args.Cursor,
				},
			); err != nil {
				return fmt.Errorf("restart overlapping PR sweep: %w", err)
			}
			if _, err := client.InsertTx(
				ctx,
				tx,
				ListPageArgs{
					SweepKind:    args.SweepKind,
					Installation: args.Installation,
					ScopeKey:     args.ScopeKey,
					Cursor:       "1",
				},
				listPageInsertOpts(),
			); err != nil {
				return fmt.Errorf(
					"insert overlapping PR sweep pass: %w",
					err,
				)
			}
		} else {
			missing, err := s.missingVerificationSpecs(
				ctx,
				queries,
				args,
			)
			if err != nil {
				return err
			}
			specs = append(specs, missing...)
			if _, err := queries.CompleteSweepCursor(
				ctx,
				dbgen.CompleteSweepCursorParams{
					CompletedAt:    repoutil.Timestamptz(now),
					InstallationID: args.Installation,
					SweepKind:      args.SweepKind,
					ScopeKey:       args.ScopeKey,
					ExpectedCursor: args.Cursor,
				},
			); err != nil {
				return fmt.Errorf("complete sweep cursor: %w", err)
			}
			completed = true
		}
	} else {
		if _, err := queries.AdvanceSweepCursor(
			ctx,
			dbgen.AdvanceSweepCursorParams{
				NextCursor:     page.nextCursor,
				PassNewCount:   int32(passNewCount),
				UpdatedAt:      repoutil.Timestamptz(now),
				InstallationID: args.Installation,
				SweepKind:      args.SweepKind,
				ScopeKey:       args.ScopeKey,
				ExpectedCursor: args.Cursor,
			},
		); err != nil {
			return fmt.Errorf("advance sweep cursor: %w", err)
		}
		if _, err := client.InsertTx(
			ctx,
			tx,
			ListPageArgs{
				SweepKind:    args.SweepKind,
				Installation: args.Installation,
				ScopeKey:     args.ScopeKey,
				Cursor:       page.nextCursor,
			},
			listPageInsertOpts(),
		); err != nil {
			return fmt.Errorf("insert next sweep page: %w", err)
		}
	}
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		specs,
		queue.QueueSweep,
	); err != nil {
		return err
	}
	if completed {
		if err := opsstate.RecordSuccessN(
			ctx,
			tx,
			args.Installation,
			"sweep",
			args.SweepKind,
			1,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sweep page: %w", err)
	}
	return nil
}

func (s *Service) missingVerificationSpecs(
	ctx context.Context,
	queries *dbgen.Queries,
	args ListPageArgs,
) ([]queue.RefreshSpec, error) {
	var kind string
	var bound time.Duration
	switch args.SweepKind {
	case KindRepositories:
		kind = queue.KindRefreshRepository
		bound = s.config.RepositoryListPeriod
	case KindStacks:
		kind = queue.KindRefreshStack
		bound = s.config.OpenStackMaxStaleness
	case KindPullRequests:
		kind = queue.KindRefreshPR
		bound = s.config.OpenPRMaxStaleness
	default:
		return nil, fmt.Errorf(
			"unsupported disappearance sweep %q",
			args.SweepKind,
		)
	}
	missing, err := queries.ListMissingSweepEntityKeys(
		ctx,
		dbgen.ListMissingSweepEntityKeysParams{
			InstallationID: args.Installation,
			SweepKind:      args.SweepKind,
			ScopeKey:       args.ScopeKey,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list missing sweep entities: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(missing))
	deadline := s.config.Now().Add(
		scheduleForBound(bound).CompletionHeadroom,
	)
	for _, key := range missing {
		// C-R3: listing absence is only a hint. The ordinary entity worker
		// performs the verification fetch, and only a 404 writes a tombstone.
		specs = append(specs, queue.RefreshSpec{
			Kind: kind, Key: key, Deadline: deadline,
		})
	}
	return specs, nil
}

func sortedUnique(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func decimalCursor(cursor string) (int, error) {
	page, err := strconv.Atoi(cursor)
	if err != nil || page <= 0 {
		return 0, fmt.Errorf("invalid decimal sweep cursor %q", cursor)
	}
	return page, nil
}

func numericNextCursor(page int) string {
	if page <= 0 {
		return ""
	}
	return strconv.Itoa(page)
}
