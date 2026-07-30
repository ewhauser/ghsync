// Package fetch turns durable River pointers into budget-gated authoritative
// GitHub fetches and transactional mirror writes.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/repoutil"
	"github.com/ewhauser/ghsync/internal/store"
)

type Options struct {
	Pool             *pgxpool.Pool
	REST             *gh.RESTClient
	GraphQL          *gh.GraphQLClient
	InstallationID   int64
	OrgID            int64
	BatchWindow      time.Duration
	BackfillPageSize int
	CacheObserver    store.CacheObserver
}

type Handler struct {
	pool             *pgxpool.Pool
	rest             *gh.RESTClient
	graphQL          *gh.GraphQLClient
	writer           *store.EntityWriter
	installationID   int64
	orgID            int64
	backfillPageSize int
	coordinator      *prCoordinator

	riverMu sync.RWMutex
	river   *river.Client[pgx.Tx]
}

func New(options Options) (*Handler, error) {
	if options.Pool == nil || options.REST == nil || options.GraphQL == nil {
		return nil, fmt.Errorf(
			"fetch handler requires Postgres, REST, and GraphQL",
		)
	}
	if options.InstallationID <= 0 || options.OrgID <= 0 {
		return nil, fmt.Errorf("fetch handler IDs must be positive")
	}
	if options.BackfillPageSize <= 0 {
		options.BackfillPageSize = 100
	}
	writer := store.NewEntityWriter(options.Pool, options.CacheObserver)
	handler := &Handler{
		pool:             options.Pool,
		rest:             options.REST,
		graphQL:          options.GraphQL,
		writer:           writer,
		installationID:   options.InstallationID,
		orgID:            options.OrgID,
		backfillPageSize: options.BackfillPageSize,
	}
	handler.coordinator = newPRCoordinator(
		options.GraphQL,
		writer,
		options.InstallationID,
		options.OrgID,
		options.BatchWindow,
	)
	return handler, nil
}

func (h *Handler) SetRiverClient(client *river.Client[pgx.Tx]) {
	h.riverMu.Lock()
	h.river = client
	h.riverMu.Unlock()
}

func (h *Handler) RefreshRepository(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "repo")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	repository, err := h.writer.Repository(ctx, key.Repo)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = h.ensureRepository(ctx, class, source, key.Repo)
		return err
	}
	if err != nil {
		return fmt.Errorf("read repository metadata: %w", err)
	}
	observation, err := h.writer.BeginObservation(
		ctx,
		store.RepositoryEntityKey(
			repository.InstallationID,
			repository.GitHubID,
		),
	)
	if err != nil {
		return err
	}
	defer observation.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	owner, name, err := repoutil.Split(key.Repo)
	if err != nil {
		return err
	}
	startedAt := time.Now()
	fetched, response, err := h.rest.GetRepository(
		ctx,
		class,
		owner,
		name,
		repository.ETag,
	)
	if repoutil.IsNotFound(err) {
		// C-R3: only installation-list reconciliation enqueues this
		// verification fetch; the confirmed 404 is authoritative.
		_, tombstoneErr := h.writer.TombstoneRepositoryObserved(
			ctx,
			observation,
			repository,
			source,
			startedAt,
		)
		return tombstoneErr
	}
	if err != nil {
		return fmt.Errorf("fetch repository %s: %w", key.Repo, err)
	}
	if response.NotModified {
		_, err = h.writer.ApplyRepositoryObserved(
			ctx,
			observation,
			repository,
			source,
			repository.ETag,
			startedAt,
		)
		return err
	}
	record := repositoryRecordFromREST(
		fetched,
		h.installationID,
		h.orgID,
	)
	_, err = h.writer.ApplyRepositoryObserved(
		ctx,
		observation,
		record,
		source,
		response.ETag,
		startedAt,
	)
	return err
}

func (h *Handler) RefreshRepoRules(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "repo_rules")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	repository, err := h.ensureRepository(ctx, class, source, key.Repo)
	if err != nil {
		return err
	}
	metadata, _, err := h.writer.RepoRulesMetadata(ctx, key.Repo)
	if err != nil {
		return fmt.Errorf("read repository rules metadata: %w", err)
	}
	observation, err := h.writer.BeginObservation(
		ctx,
		store.RepoRulesEntityKey(
			repository.InstallationID,
			repository.GitHubID,
		),
	)
	if err != nil {
		return err
	}
	defer observation.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	owner, name, err := repoutil.Split(key.Repo)
	if err != nil {
		return err
	}
	startedAt := time.Now()
	rules, response, err := h.rest.ListRepositoryRules(
		ctx,
		class,
		owner,
		name,
		metadata.ETag,
	)
	if err != nil {
		return fmt.Errorf("fetch repository rules %s: %w", key.Repo, err)
	}
	if response.NotModified {
		return h.writer.TouchRepoRules(
			ctx,
			observation,
			repository,
			startedAt,
			metadata.ETag,
		)
	}
	records := make([]store.RepoRuleRecord, 0, len(rules))
	for _, rule := range rules {
		if rule.ID <= 0 {
			return fmt.Errorf("repository rules response has invalid ID")
		}
		records = append(records, store.RepoRuleRecord{
			Key:             strconv.FormatInt(rule.ID, 10),
			Rule:            rule.Raw,
			GitHubUpdatedAt: rule.UpdatedAt,
			HeadSHA:         repository.DefaultHeadSHA,
		})
	}
	_, err = h.writer.ApplyRepoRulesObserved(
		ctx,
		observation,
		store.RepoRulesRecord{
			Repository: repository,
			Rules:      records,
			ETag:       response.ETag,
			SyncedAt:   startedAt,
			Source:     source,
		},
	)
	return err
}

func (h *Handler) RefreshPR(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "pr")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	metadata, err := h.writer.PullRequestMetadata(
		ctx,
		key.Repo,
		key.Number,
	)
	if err == nil && metadata.NodeID != "" {
		_, err := h.coordinator.submit(
			ctx,
			key,
			metadata.NodeID,
			&metadata,
			class,
			source,
			func(repo string) store.PullRequestHook {
				return h.pullRequestHook(repo, request.Queue)
			},
		)
		if err != nil {
			if errors.Is(err, errGraphQLNodeNotFound) {
				return h.refreshPRREST(
					ctx,
					key,
					class,
					source,
					request.Queue,
				)
			}
			return err
		}
		return nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read PR fetch metadata: %w", err)
	}
	return h.refreshPRREST(ctx, key, class, source, request.Queue)
}

func (h *Handler) ResolveStackMembership(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "pr")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	return h.refreshPRREST(ctx, key, class, source, request.Queue)
}

func (h *Handler) refreshPRREST(
	ctx context.Context,
	key entityKey,
	class budget.Class,
	source store.SyncSource,
	queueName string,
) error {
	repository, err := h.ensureRepository(ctx, class, source, key.Repo)
	if err != nil {
		return err
	}
	observation, err := h.writer.BeginObservation(
		ctx,
		store.PullRequestEntityKey(
			repository.InstallationID,
			repository.GitHubID,
			key.Number,
		),
	)
	if err != nil {
		return err
	}
	defer observation.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	startedAt := time.Now()
	owner, repoName, err := repoutil.Split(key.Repo)
	if err != nil {
		return err
	}
	var etag string
	metadata, metadataErr := h.writer.PullRequestMetadata(
		ctx,
		key.Repo,
		key.Number,
	)
	if metadataErr == nil {
		etag = metadata.ETag
	} else if !errors.Is(metadataErr, pgx.ErrNoRows) {
		return fmt.Errorf("read PR ETag: %w", metadataErr)
	}
	pull, response, err := h.rest.GetPull(
		ctx,
		class,
		owner,
		repoName,
		key.Number,
		etag,
	)
	hook := h.pullRequestHook(
		repository.FullName,
		queueName,
	)
	if repoutil.IsNotFound(err) {
		_, tombstoneErr := h.writer.TombstonePullRequestObserved(
			ctx,
			observation,
			repository,
			key.Number,
			source,
			startedAt,
			hook,
		)
		return tombstoneErr
	}
	if err != nil {
		return fmt.Errorf("fetch PR %s: %w", requestKey(key), err)
	}
	if response.NotModified {
		return h.writer.TouchPullRequest(
			ctx,
			observation,
			repository,
			key.Number,
			startedAt,
			etag,
		)
	}
	record := pullRecordFromREST(
		&repository,
		pull,
		response.ETag,
		source,
		startedAt,
	)
	record.MembershipKnown = true
	_, err = h.writer.ApplyPullRequestObserved(
		ctx,
		observation,
		record,
		hook,
	)
	return err
}

func (h *Handler) RefreshStack(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "stack")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	repository, err := h.ensureRepository(ctx, class, source, key.Repo)
	if err != nil {
		return err
	}
	observation, err := h.writer.BeginObservation(
		ctx,
		store.StackEntityKey(
			repository.InstallationID,
			repository.GitHubID,
			key.Number,
		),
	)
	if err != nil {
		return err
	}
	defer observation.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	startedAt := time.Now()
	owner, repoName, err := repoutil.Split(key.Repo)
	if err != nil {
		return err
	}
	var etag string
	metadata, metadataErr := h.writer.StackMetadata(
		ctx,
		key.Repo,
		key.Number,
	)
	if metadataErr == nil {
		etag = metadata.ETag
	} else if !errors.Is(metadataErr, pgx.ErrNoRows) {
		return fmt.Errorf("read stack ETag: %w", metadataErr)
	}
	stack, response, err := h.rest.GetStack(
		ctx,
		class,
		owner,
		repoName,
		key.Number,
		etag,
	)
	hook := h.stackHook(repository.FullName, request.Queue)
	if repoutil.IsNotFound(err) {
		_, tombstoneErr := h.writer.TombstoneStackObserved(
			ctx,
			observation,
			repository,
			key.Number,
			source,
			startedAt,
			hook,
		)
		return tombstoneErr
	}
	if err != nil {
		return fmt.Errorf("fetch stack %s: %w", requestKey(key), err)
	}
	if response.NotModified {
		return h.writer.TouchStack(
			ctx,
			observation,
			repository,
			key.Number,
			startedAt,
			etag,
		)
	}
	record := stackRecordFromREST(
		&repository,
		stack,
		response.ETag,
		source,
		startedAt,
	)
	_, err = h.writer.ApplyStackObserved(
		ctx,
		observation,
		record,
		hook,
	)
	return err
}

func (h *Handler) RefreshChecks(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "checks")
	if err != nil {
		return err
	}
	class, source, err := classAndSource(request.Queue)
	if err != nil {
		return err
	}
	repository, err := h.ensureRepository(ctx, class, source, key.Repo)
	if err != nil {
		return err
	}
	observation, err := h.writer.BeginObservation(
		ctx,
		store.ChecksEntityKey(
			repository.InstallationID,
			repository.GitHubID,
			key.Value,
		),
	)
	if err != nil {
		return err
	}
	defer observation.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	startedAt := time.Now()
	owner, repoName, err := repoutil.Split(key.Repo)
	if err != nil {
		return err
	}
	metadata, err := h.writer.ChecksMetadata(ctx, key.Repo, key.Value)
	if err != nil {
		return fmt.Errorf("read checks ETag: %w", err)
	}
	var all []gh.CheckRun
	page := 1
	etag := metadata.ETag
	for {
		conditionalETag := ""
		if page == 1 {
			conditionalETag = etag
		}
		runs, response, fetchErr := h.rest.ListCheckRuns(
			ctx,
			class,
			owner,
			repoName,
			key.Value,
			gh.ListCheckRunsOptions{PerPage: 100, Page: page},
			conditionalETag,
		)
		if repoutil.IsNotFound(fetchErr) {
			if page == 1 {
				all = nil
				etag = ""
				break
			}
			return fmt.Errorf(
				"fetch checks %s page %d: listing returned 404 after page 1",
				requestKey(key),
				page,
			)
		}
		if fetchErr != nil {
			return fmt.Errorf(
				"fetch checks %s page %d: %w",
				requestKey(key),
				page,
				fetchErr,
			)
		}
		if response.NotModified {
			if page != 1 {
				return fmt.Errorf(
					"fetch checks %s page %d: unexpected 304",
					requestKey(key),
					page,
				)
			}
			return h.writer.TouchChecks(
				ctx,
				observation,
				repository,
				key.Value,
				startedAt,
				etag,
			)
		}
		if page == 1 {
			etag = response.ETag
		}
		all = append(all, runs...)
		if response.NextPage == 0 {
			break
		}
		page = response.NextPage
	}
	records := make([]store.CheckRunRecord, 0, len(all))
	for index := range all {
		run := &all[index]
		record, err := checkRecordFromREST(run)
		if err != nil {
			return err
		}
		records = append(records, record)
	}
	_, err = h.writer.ApplyChecksObserved(
		ctx,
		observation,
		store.ChecksRecord{
			Repository: repository,
			HeadSHA:    key.Value,
			Runs:       records,
			ETag:       etag,
			SyncedAt:   startedAt,
			Source:     source,
		},
	)
	return err
}

func (h *Handler) RefreshBranch(
	ctx context.Context,
	request queue.RefreshRequest,
) error {
	key, err := parseEntityKey(request.Args.Key, "branch")
	if err != nil {
		return err
	}
	targets, err := h.writer.BranchTargets(ctx, key.Repo, key.Value)
	if err != nil {
		return err
	}
	specs := make([]queue.RefreshSpec, 0, len(targets))
	for _, target := range targets {
		kind := queue.KindRefreshPR
		if strings.HasPrefix(target, "stack:") {
			kind = queue.KindRefreshStack
		}
		specs = append(specs, queue.RefreshSpec{Kind: kind, Key: target})
	}
	return h.enqueue(ctx, specs, request.Queue)
}

func (h *Handler) pullRequestHook(
	repo string,
	queueName string,
) store.PullRequestHook {
	return func(result store.ApplyPullRequestResult) store.TransactionHook {
		specs := pullRequestFollowupSpecs(repo, result)
		return h.insertFollowupsHook(specs, queueName)
	}
}

func pullRequestFollowupSpecs(
	repo string,
	result store.ApplyPullRequestResult,
) []queue.RefreshSpec {
	specs := make([]queue.RefreshSpec, 0, 3)
	if result.NewHeadSHA != "" && result.NewHeadSHA != result.OldHeadSHA {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshChecks,
			Key:  fmt.Sprintf("checks:%s:%s", repo, result.NewHeadSHA),
		})
	}
	if result.StackStateChanged {
		stackNumbers := make(map[int]struct{}, 2)
		if result.OldStackNumber != nil {
			stackNumbers[*result.OldStackNumber] = struct{}{}
		}
		if result.NewStackNumber != nil {
			stackNumbers[*result.NewStackNumber] = struct{}{}
		}
		for stackNumber := range stackNumbers {
			specs = append(specs, queue.RefreshSpec{
				Kind: queue.KindRefreshStack,
				Key:  fmt.Sprintf("stack:%s:%d", repo, stackNumber),
			})
		}
	}
	return specs
}

func (h *Handler) stackHook(
	repo string,
	queueName string,
) store.StackHook {
	return func(result store.ApplyStackResult) store.TransactionHook {
		if !result.Applied {
			return nil
		}
		specs := stackFollowupSpecs(repo, &result)
		return h.insertFollowupsHook(specs, queueName)
	}
}

func stackFollowupSpecs(
	repo string,
	result *store.ApplyStackResult,
) []queue.RefreshSpec {
	specs := make([]queue.RefreshSpec, 0)
	seenPRs := make(map[int]struct{})
	affected := append(
		append(
			append([]int(nil), result.JoinedPRs...),
			result.LeftPRs...,
		),
		result.MovedPRs...,
	)
	for _, number := range affected {
		if _, seen := seenPRs[number]; seen {
			continue
		}
		seenPRs[number] = struct{}{}
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindResolveStackMembership,
			Key:  fmt.Sprintf("pr:%s:%d", repo, number),
		})
	}
	for _, oldStack := range result.PriorStackByPR {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key:  fmt.Sprintf("stack:%s:%d", repo, oldStack),
		})
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Kind != specs[j].Kind {
			return specs[i].Kind < specs[j].Kind
		}
		return specs[i].Key < specs[j].Key
	})
	return specs
}

func (h *Handler) insertFollowupsHook(
	specs []queue.RefreshSpec,
	queueName string,
) store.TransactionHook {
	if len(specs) == 0 {
		return nil
	}
	return func(ctx context.Context, tx pgx.Tx) error {
		client := h.riverClient(ctx)
		if client == nil {
			return fmt.Errorf("river client missing from fetch transaction")
		}
		return queue.InsertRefreshesTx(
			ctx,
			tx,
			client,
			specs,
			queueName,
		)
	}
}

func (h *Handler) enqueue(
	ctx context.Context,
	specs []queue.RefreshSpec,
	queueName string,
) error {
	if len(specs) == 0 {
		return nil
	}
	client := h.riverClient(ctx)
	if client == nil {
		return fmt.Errorf("river client missing from fetch context")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh fan-out: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		specs,
		queueName,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit refresh fan-out: %w", err)
	}
	return nil
}

func (h *Handler) ensureRepository(
	ctx context.Context,
	class budget.Class,
	source store.SyncSource,
	fullName string,
) (store.RepositoryRecord, error) {
	repository, err := h.writer.Repository(ctx, fullName)
	if err == nil {
		return repository, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.RepositoryRecord{}, fmt.Errorf(
			"read repository cache: %w",
			err,
		)
	}
	discovery, err := h.writer.BeginObservation(
		ctx,
		store.RepositoryDiscoveryKey(h.installationID, fullName),
	)
	if err != nil {
		return store.RepositoryRecord{}, err
	}
	defer discovery.CloseContext(ctx) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if repository, err = h.writer.Repository(ctx, fullName); err == nil {
		return repository, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return store.RepositoryRecord{}, err
	}
	owner, repoName, err := repoutil.Split(fullName)
	if err != nil {
		return store.RepositoryRecord{}, err
	}
	fetched, response, err := h.rest.GetRepository(
		ctx,
		class,
		owner,
		repoName,
		"",
	)
	if err != nil {
		return store.RepositoryRecord{}, fmt.Errorf(
			"fetch repository: %w",
			err,
		)
	}
	record := repositoryRecordFromREST(
		fetched,
		h.installationID,
		h.orgID,
	)
	record.ETag = response.ETag
	record.LastCheckedAt = time.Now()
	if _, err := h.writer.ApplyRepository(
		ctx,
		record,
		source,
		response.ETag,
		record.LastCheckedAt,
	); err != nil {
		return store.RepositoryRecord{}, err
	}
	return record, nil
}

func classAndSource(
	queueName string,
) (budget.Class, store.SyncSource, error) {
	switch queueName {
	case queue.QueueInteractive:
		return budget.Interactive, store.SyncSourceInteractive, nil
	case queue.QueueEvent:
		return budget.Event, store.SyncSourceWebhook, nil
	case queue.QueueSweep:
		return budget.Sweep, store.SyncSourceReconcile, nil
	default:
		return "", "", fmt.Errorf("unknown refresh queue %q", queueName)
	}
}

func requestKey(key entityKey) string {
	return key.Kind + ":" + key.Repo + ":" + key.Value
}
