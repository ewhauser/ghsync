// Package sweep implements M4's bounded-staleness reconciliation, resumable
// authoritative listings, disappearance verification, delivery-gap healing,
// and retention work on River's sweep queue.
package sweep

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store/dbgen"
)

const (
	KindRepositories = "repositories"
	KindStacks       = "stacks"
	KindPullRequests = "pull_requests"
	KindRepoRules    = "repo_rules"
	KindClosed       = "closed_tracked"

	jobKindKickoff = "sweep_kickoff"
	jobKindPage    = "sweep_list_page"
	jobKindGapHeal = "sweep_gap_heal"
	jobKindPrune   = "retention_prune"
)

type Config struct {
	InstallationID int64

	OpenStackMaxStaleness time.Duration
	OpenPRMaxStaleness    time.Duration
	RepoRulesMaxStaleness time.Duration
	ClosedMaxStaleness    time.Duration
	RepositoryListPeriod  time.Duration

	PageSize int

	GapHealPeriod time.Duration
	GapWindow     time.Duration
	GapPageSize   int
	GapMaxPages   int

	RetentionPeriod time.Duration
	RetentionAge    time.Duration

	Now      func() time.Time
	Observer Observer
}

func (c Config) validate() error {
	for name, value := range map[string]time.Duration{
		"open stack staleness": c.OpenStackMaxStaleness,
		"open PR staleness":    c.OpenPRMaxStaleness,
		"repo rules staleness": c.RepoRulesMaxStaleness,
		"closed staleness":     c.ClosedMaxStaleness,
		"repository period":    c.RepositoryListPeriod,
		"gap-heal period":      c.GapHealPeriod,
		"gap window":           c.GapWindow,
		"retention period":     c.RetentionPeriod,
		"retention age":        c.RetentionAge,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.PageSize <= 0 || c.PageSize > 100 ||
		c.GapPageSize <= 0 || c.GapPageSize > 100 ||
		c.GapMaxPages <= 0 {
		return fmt.Errorf("sweep page sizes/max pages are invalid")
	}
	return nil
}

type Observer interface {
	SweepOverrun(
		context.Context,
		string,
		string,
		time.Duration,
	)
	GapRedelivery(context.Context, int64, string)
}

type LogObserver struct{}

func (LogObserver) SweepOverrun(
	_ context.Context,
	kind string,
	scope string,
	elapsed time.Duration,
) {
	slog.Warn(
		"C-R2 sweep overran its configured period",
		"sweep_kind", kind,
		"scope", scope,
		"elapsed", elapsed,
	)
}

func (LogObserver) GapRedelivery(
	_ context.Context,
	deliveryID int64,
	guid string,
) {
	slog.Warn(
		"C-R4 webhook delivery gap requested for redelivery",
		"delivery_id", deliveryID,
		"delivery_guid", guid,
	)
}

type noopObserver struct{}

func (noopObserver) SweepOverrun(
	context.Context,
	string,
	string,
	time.Duration,
) {
}
func (noopObserver) GapRedelivery(context.Context, int64, string) {}

type Options struct {
	Pool       *pgxpool.Pool
	REST       *gh.RESTClient
	Deliveries *gh.DeliveriesClient
	Config     Config
}

type Service struct {
	pool       *pgxpool.Pool
	rest       *gh.RESTClient
	deliveries *gh.DeliveriesClient
	config     Config

	riverMu sync.RWMutex
	river   *river.Client[pgx.Tx]
}

func New(options Options) (*Service, error) {
	if options.Pool == nil {
		return nil, fmt.Errorf("sweep service requires Postgres")
	}
	if options.Config.Now == nil {
		options.Config.Now = time.Now
	}
	if options.Config.Observer == nil {
		options.Config.Observer = noopObserver{}
	}
	if err := options.Config.validate(); err != nil {
		return nil, err
	}
	return &Service{
		pool:       options.Pool,
		rest:       options.REST,
		deliveries: options.Deliveries,
		config:     options.Config,
	}, nil
}

func (s *Service) SetRiverClient(client *river.Client[pgx.Tx]) {
	s.riverMu.Lock()
	s.river = client
	s.riverMu.Unlock()
}

func (s *Service) riverClient() *river.Client[pgx.Tx] {
	s.riverMu.RLock()
	defer s.riverMu.RUnlock()
	return s.river
}

type KickoffArgs struct {
	SweepKind    string `json:"sweep_kind"`
	Installation int64  `json:"installation_id"`
}

func (KickoffArgs) Kind() string { return jobKindKickoff }

type ListPageArgs struct {
	SweepKind    string `json:"sweep_kind"`
	Installation int64  `json:"installation_id"`
	ScopeKey     string `json:"scope_key"`
	Cursor       string `json:"cursor"`
}

func (ListPageArgs) Kind() string { return jobKindPage }

type GapHealArgs struct {
	Installation int64 `json:"installation_id"`
}

func (GapHealArgs) Kind() string { return jobKindGapHeal }

type PruneArgs struct{}

func (PruneArgs) Kind() string { return jobKindPrune }

type kickoffWorker struct {
	river.WorkerDefaults[KickoffArgs]
	service *Service
}

func (w *kickoffWorker) Work(
	ctx context.Context,
	job *river.Job[KickoffArgs],
) error {
	return w.service.Kickoff(ctx, job.Args)
}

type listPageWorker struct {
	river.WorkerDefaults[ListPageArgs]
	service *Service
}

func (w *listPageWorker) Work(
	ctx context.Context,
	job *river.Job[ListPageArgs],
) error {
	return w.service.ReconcilePage(ctx, job.Args)
}

type gapHealWorker struct {
	river.WorkerDefaults[GapHealArgs]
	service *Service
}

func (w *gapHealWorker) Work(
	ctx context.Context,
	job *river.Job[GapHealArgs],
) error {
	return w.service.HealDeliveryGaps(ctx, job.Args)
}

type pruneWorker struct {
	river.WorkerDefaults[PruneArgs]
	service *Service
}

func (w *pruneWorker) Work(
	ctx context.Context,
	_ *river.Job[PruneArgs],
) error {
	_, _, err := w.service.Prune(ctx)
	return err
}

func (s *Service) RegisterReconciliationWorkers(
	workers *river.Workers,
) {
	river.AddWorker(workers, &kickoffWorker{service: s})
	river.AddWorker(workers, &listPageWorker{service: s})
	river.AddWorker(workers, &gapHealWorker{service: s})
}

func (s *Service) RegisterPrunerWorker(workers *river.Workers) {
	river.AddWorker(workers, &pruneWorker{service: s})
}

func (s *Service) ReconciliationPeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		s.kickoffPeriodic(KindStacks, s.config.OpenStackMaxStaleness),
		s.kickoffPeriodic(KindPullRequests, s.config.OpenPRMaxStaleness),
		s.kickoffPeriodic(KindRepoRules, s.config.RepoRulesMaxStaleness),
		s.kickoffPeriodic(KindClosed, s.config.ClosedMaxStaleness),
		s.kickoffPeriodic(KindRepositories, s.config.RepositoryListPeriod),
		river.NewPeriodicJob(
			river.PeriodicInterval(s.config.GapHealPeriod),
			func() (river.JobArgs, *river.InsertOpts) {
				return GapHealArgs{
						Installation: s.config.InstallationID,
					},
					periodicInsertOpts()
			},
			&river.PeriodicJobOpts{
				ID:         "frontier_gap_heal",
				RunOnStart: true,
			},
		),
	}
}

func (s *Service) PrunerPeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(s.config.RetentionPeriod),
			func() (river.JobArgs, *river.InsertOpts) {
				return PruneArgs{}, periodicInsertOpts()
			},
			&river.PeriodicJobOpts{
				ID:         "frontier_retention_prune",
				RunOnStart: true,
			},
		),
	}
}

func (s *Service) kickoffPeriodic(
	kind string,
	period time.Duration,
) *river.PeriodicJob {
	return river.NewPeriodicJob(
		river.PeriodicInterval(period),
		func() (river.JobArgs, *river.InsertOpts) {
			return KickoffArgs{
					SweepKind:    kind,
					Installation: s.config.InstallationID,
				},
				periodicInsertOpts()
		},
		&river.PeriodicJobOpts{
			ID:         "frontier_sweep_" + kind,
			RunOnStart: true,
		},
	)
}

func periodicInsertOpts() *river.InsertOpts {
	return &river.InsertOpts{
		Queue:    queue.QueueSweep,
		Priority: 1,
	}
}

func listPageInsertOpts() *river.InsertOpts {
	return &river.InsertOpts{
		Queue:    queue.QueueSweep,
		Priority: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: true,
			ByState: []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			},
		},
	}
}

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
			s.config.OpenStackMaxStaleness,
		)
	case KindPullRequests:
		if err := s.enqueueStalePullRequests(ctx); err != nil {
			return err
		}
		return s.kickoffRepositoryScopes(
			ctx,
			KindPullRequests,
			s.config.OpenPRMaxStaleness,
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
			s.config.RepositoryListPeriod,
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
	repositories, err := dbgen.New(s.pool).ListSweepRepositories(
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
				StartedAt:      timestamptz(now),
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
	return s.persistPage(ctx, args, cursor, page)
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
				},
			)
		}
		return result, nil
	case KindStacks:
		page, err := decimalCursor(args.Cursor)
		if err != nil {
			return fetchedPage{}, err
		}
		owner, repo, err := splitRepo(args.ScopeKey)
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
				},
			)
		}
		return result, nil
	case KindPullRequests:
		page, err := decimalCursor(args.Cursor)
		if err != nil {
			return fetchedPage{}, err
		}
		owner, repo, err := splitRepo(args.ScopeKey)
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
				Sort:      "updated",
				Direction: "desc",
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
	prior dbgen.SweepCursor,
	page fetchedPage,
) error {
	client := s.riverClient()
	if client == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	seen, err := mergeKeys(prior.SeenKeys, page.keys)
	if err != nil {
		return err
	}
	encodedPageKeys, err := json.Marshal(sortedUnique(page.keys))
	if err != nil {
		return fmt.Errorf("encode sweep page keys: %w", err)
	}
	encodedSeen, err := json.Marshal(seen)
	if err != nil {
		return fmt.Errorf("encode sweep seen keys: %w", err)
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
			LastCheckedAt:  timestamptz(now),
		},
	); err != nil {
		return fmt.Errorf("persist sweep page validator: %w", err)
	}
	if page.notModified {
		if err := s.touchValidatedPage(
			ctx,
			queries,
			args,
			page.keys,
			now,
		); err != nil {
			return err
		}
	}
	specs := append([]queue.RefreshSpec(nil), page.refreshSpecs...)
	if page.nextCursor == "" {
		missing, err := s.missingVerificationSpecs(
			ctx,
			queries,
			args,
			seen,
		)
		if err != nil {
			return err
		}
		specs = append(specs, missing...)
		if _, err := queries.CompleteSweepCursor(
			ctx,
			dbgen.CompleteSweepCursorParams{
				SeenKeys:       encodedSeen,
				CompletedAt:    timestamptz(now),
				InstallationID: args.Installation,
				SweepKind:      args.SweepKind,
				ScopeKey:       args.ScopeKey,
				ExpectedCursor: args.Cursor,
			},
		); err != nil {
			return fmt.Errorf("complete sweep cursor: %w", err)
		}
	} else {
		if _, err := queries.AdvanceSweepCursor(
			ctx,
			dbgen.AdvanceSweepCursorParams{
				NextCursor:     page.nextCursor,
				SeenKeys:       encodedSeen,
				UpdatedAt:      timestamptz(now),
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
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sweep page: %w", err)
	}
	return nil
}

func (s *Service) missingVerificationSpecs(
	ctx context.Context,
	queries *dbgen.Queries,
	args ListPageArgs,
	seen []string,
) ([]queue.RefreshSpec, error) {
	seenSet := make(map[string]struct{}, len(seen))
	for _, key := range seen {
		seenSet[key] = struct{}{}
	}
	var cached []string
	var kind string
	switch args.SweepKind {
	case KindRepositories:
		names, err := queries.ListLiveRepositoryNames(
			ctx,
			args.Installation,
		)
		if err != nil {
			return nil, fmt.Errorf("list cached repositories: %w", err)
		}
		for _, name := range names {
			cached = append(cached, "repo:"+name+":metadata")
		}
		kind = queue.KindRefreshRepository
	case KindStacks:
		numbers, err := queries.ListLiveStackNumbers(
			ctx,
			dbgen.ListLiveStackNumbersParams{
				InstallationID: args.Installation,
				RepoFullName:   args.ScopeKey,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("list cached stacks: %w", err)
		}
		for _, number := range numbers {
			cached = append(
				cached,
				fmt.Sprintf("stack:%s:%d", args.ScopeKey, number),
			)
		}
		kind = queue.KindRefreshStack
	case KindPullRequests:
		numbers, err := queries.ListLiveOpenPullRequestNumbers(
			ctx,
			dbgen.ListLiveOpenPullRequestNumbersParams{
				InstallationID: args.Installation,
				RepoFullName:   args.ScopeKey,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("list cached pull requests: %w", err)
		}
		for _, number := range numbers {
			cached = append(
				cached,
				fmt.Sprintf("pr:%s:%d", args.ScopeKey, number),
			)
		}
		kind = queue.KindRefreshPR
	default:
		return nil, fmt.Errorf(
			"unsupported disappearance sweep %q",
			args.SweepKind,
		)
	}
	var specs []queue.RefreshSpec
	for _, key := range cached {
		if _, present := seenSet[key]; present {
			continue
		}
		// C-R3: listing absence is only a hint. The ordinary entity worker
		// performs the verification fetch, and only a 404 writes a tombstone.
		specs = append(specs, queue.RefreshSpec{Kind: kind, Key: key})
	}
	return specs, nil
}

func (s *Service) touchValidatedPage(
	ctx context.Context,
	queries *dbgen.Queries,
	args ListPageArgs,
	keys []string,
	checkedAt time.Time,
) error {
	switch args.SweepKind {
	case KindStacks:
		numbers, err := pageNumbers(keys, "stack:", args.ScopeKey)
		if err != nil {
			return err
		}
		if len(numbers) == 0 {
			return nil
		}
		if _, err := queries.TouchListedStacksCheckedAt(
			ctx,
			dbgen.TouchListedStacksCheckedAtParams{
				CheckedAt:      timestamptz(checkedAt),
				InstallationID: args.Installation,
				RepoFullName:   args.ScopeKey,
				StackNumbers:   numbers,
			},
		); err != nil {
			return fmt.Errorf("touch stack-list validations: %w", err)
		}
	case KindPullRequests:
		numbers, err := pageNumbers(keys, "pr:", args.ScopeKey)
		if err != nil {
			return err
		}
		if len(numbers) == 0 {
			return nil
		}
		if _, err := queries.TouchListedPullRequestsCheckedAt(
			ctx,
			dbgen.TouchListedPullRequestsCheckedAtParams{
				CheckedAt:      timestamptz(checkedAt),
				InstallationID: args.Installation,
				RepoFullName:   args.ScopeKey,
				PrNumbers:      numbers,
			},
		); err != nil {
			return fmt.Errorf("touch PR-list validations: %w", err)
		}
	}
	return nil
}

func pageNumbers(
	keys []string,
	prefix string,
	repo string,
) ([]int32, error) {
	fullPrefix := prefix + repo + ":"
	numbers := make([]int32, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, fullPrefix) {
			return nil, fmt.Errorf("unexpected sweep page key %q", key)
		}
		number, err := strconv.ParseInt(
			strings.TrimPrefix(key, fullPrefix),
			10,
			32,
		)
		if err != nil || number <= 0 {
			return nil, fmt.Errorf("invalid sweep page key %q", key)
		}
		numbers = append(numbers, int32(number))
	}
	return numbers, nil
}

func (s *Service) enqueueStaleStacks(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	open, err := queries.ListStaleOpenStacks(
		ctx,
		dbgen.ListStaleOpenStacksParams{
			InstallationID: s.config.InstallationID,
			StaleBefore: timestamptz(
				s.config.Now().Add(-s.config.OpenStackMaxStaleness),
			),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale open stacks: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(open))
	for _, row := range open {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key: fmt.Sprintf(
				"stack:%s:%d",
				row.RepoFullName,
				row.Number,
			),
		})
	}
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueStalePullRequests(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	open, err := queries.ListStaleOpenPullRequests(
		ctx,
		dbgen.ListStaleOpenPullRequestsParams{
			InstallationID: s.config.InstallationID,
			StaleBefore: timestamptz(
				s.config.Now().Add(-s.config.OpenPRMaxStaleness),
			),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale open pull requests: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(open))
	for _, row := range open {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshPR,
			Key:  fmt.Sprintf("pr:%s:%d", row.RepoFullName, row.Number),
		})
	}
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueStaleRepoRules(ctx context.Context) error {
	names, err := dbgen.New(s.pool).ListStaleRepoRules(
		ctx,
		dbgen.ListStaleRepoRulesParams{
			InstallationID: s.config.InstallationID,
			// C-R1: the hourly periodic job conditionally validates every
			// repository rules collection, so a run cannot miss a row that
			// became due just after the previous scheduler tick.
			StaleBefore: timestamptz(s.config.Now()),
		},
	)
	if err != nil {
		return fmt.Errorf("list stale repository rules: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshRepoRules,
			Key:  "repo_rules:" + name + ":rules",
		})
	}
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueClosedTracked(ctx context.Context) error {
	queries := dbgen.New(s.pool)
	stacks, err := queries.ListStaleClosedStacks(
		ctx,
		dbgen.ListStaleClosedStacksParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    timestamptz(s.config.Now()),
		},
	)
	if err != nil {
		return fmt.Errorf("list closed tracked stacks: %w", err)
	}
	pulls, err := queries.ListStaleClosedPullRequests(
		ctx,
		dbgen.ListStaleClosedPullRequestsParams{
			InstallationID: s.config.InstallationID,
			StaleBefore:    timestamptz(s.config.Now()),
		},
	)
	if err != nil {
		return fmt.Errorf("list closed tracked pull requests: %w", err)
	}
	specs := make([]queue.RefreshSpec, 0, len(stacks)+len(pulls))
	for _, row := range stacks {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key: fmt.Sprintf(
				"stack:%s:%d",
				row.RepoFullName,
				row.Number,
			),
		})
	}
	for _, row := range pulls {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshPR,
			Key:  fmt.Sprintf("pr:%s:%d", row.RepoFullName, row.Number),
		})
	}
	return s.enqueueRefreshes(ctx, specs)
}

func (s *Service) enqueueRefreshes(
	ctx context.Context,
	specs []queue.RefreshSpec,
) error {
	if len(specs) == 0 {
		return nil
	}
	client := s.riverClient()
	if client == nil {
		return fmt.Errorf("sweep River client is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin stale refresh enqueue: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		specs,
		queue.QueueSweep,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit stale refresh enqueue: %w", err)
	}
	return nil
}

func mergeKeys(existing []byte, added []string) ([]string, error) {
	var keys []string
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &keys); err != nil {
			return nil, fmt.Errorf("decode sweep seen keys: %w", err)
		}
	}
	return sortedUnique(append(keys, added...)), nil
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

func splitRepo(fullName string) (string, string, error) {
	owner, repo, ok := strings.Cut(fullName, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", fmt.Errorf("invalid repository name %q", fullName)
	}
	return owner, repo, nil
}

func timestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}
