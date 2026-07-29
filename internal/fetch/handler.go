// Package fetch turns durable River pointers into budget-gated authoritative
// GitHub fetches and transactional mirror writes.
package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
)

type Options struct {
	Pool             *pgxpool.Pool
	REST             *gh.RESTClient
	GraphQL          *gh.GraphQLClient
	InstallationID   int64
	OrgID            int64
	BatchWindow      time.Duration
	BackfillPageSize int
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
		return nil, fmt.Errorf("fetch handler requires Postgres, REST, and GraphQL")
	}
	if options.InstallationID <= 0 || options.OrgID <= 0 {
		return nil, fmt.Errorf("fetch handler IDs must be positive")
	}
	if options.BackfillPageSize <= 0 {
		options.BackfillPageSize = 100
	}
	writer := store.NewEntityWriter(options.Pool)
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

// SetRiverClient is the construction-cycle seam: queue workers need Handler,
// while Handler's fan-out paths need the resulting client.
func (h *Handler) SetRiverClient(client *river.Client[pgx.Tx]) {
	h.riverMu.Lock()
	h.river = client
	h.riverMu.Unlock()
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
	metadata, err := h.writer.PullRequestMetadata(ctx, key.Repo, key.Number)
	if err == nil && metadata.NodeID != "" {
		result, err := h.coordinator.submit(
			ctx,
			key,
			metadata.NodeID,
			metadata,
			class,
			source,
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
		return h.enqueuePRFollowups(ctx, key, result, request.Queue)
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
	startedAt := time.Now()
	owner, repoName, err := splitRepo(key.Repo)
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
	if isNotFound(err) {
		result, tombstoneErr := h.writer.TombstonePullRequest(
			ctx,
			key.Repo,
			key.Number,
			source,
			startedAt,
		)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		return h.enqueuePRFollowups(ctx, key, result, queueName)
	}
	if err != nil {
		return fmt.Errorf("fetch PR %s: %w", requestKey(key), err)
	}
	if response.NotModified {
		return nil
	}
	repository, err := h.ensureRepository(
		ctx,
		class,
		source,
		key.Repo,
	)
	if err != nil {
		return err
	}
	record := pullRecordFromREST(
		repository,
		pull,
		response.ETag,
		source,
		startedAt,
	)
	record.MembershipKnown = true
	result, err := h.writer.ApplyPullRequest(ctx, record)
	if err != nil {
		return err
	}
	return h.enqueuePRFollowups(ctx, key, result, queueName)
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
	startedAt := time.Now()
	owner, repoName, err := splitRepo(key.Repo)
	if err != nil {
		return err
	}
	etag, err := h.writer.StackETag(ctx, key.Repo, key.Number)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read stack ETag: %w", err)
	}
	stack, response, err := h.rest.GetStack(
		ctx,
		class,
		owner,
		repoName,
		key.Number,
		etag,
	)
	if isNotFound(err) {
		result, tombstoneErr := h.writer.TombstoneStack(
			ctx,
			key.Repo,
			key.Number,
			source,
			startedAt,
		)
		if tombstoneErr != nil {
			return tombstoneErr
		}
		return h.enqueueStackDiff(ctx, key.Repo, result, request.Queue)
	}
	if err != nil {
		return fmt.Errorf("fetch stack %s: %w", requestKey(key), err)
	}
	if response.NotModified {
		return nil
	}
	repository, err := h.ensureRepository(
		ctx,
		class,
		source,
		key.Repo,
	)
	if err != nil {
		return err
	}
	record := stackRecordFromREST(
		repository,
		stack,
		response.ETag,
		source,
		startedAt,
	)
	result, err := h.writer.ApplyStack(ctx, record)
	if err != nil {
		return err
	}
	return h.enqueueStackDiff(ctx, key.Repo, result, request.Queue)
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
	startedAt := time.Now()
	owner, repoName, err := splitRepo(key.Repo)
	if err != nil {
		return err
	}
	var all []gh.CheckRun
	page := 1
	var etag string
	for {
		runs, response, fetchErr := h.rest.ListCheckRuns(
			ctx,
			class,
			owner,
			repoName,
			key.Value,
			gh.ListCheckRunsOptions{PerPage: 100, Page: page},
			"",
		)
		if isNotFound(fetchErr) {
			all = nil
			break
		}
		if fetchErr != nil {
			return fmt.Errorf("fetch checks %s: %w", requestKey(key), fetchErr)
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
	if _, err := h.ensureRepository(ctx, class, source, key.Repo); err != nil {
		return err
	}
	records := make([]store.CheckRunRecord, 0, len(all))
	for _, run := range all {
		observed, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode check observation: %w", err)
		}
		updated := startedAt
		if run.StartedAt != nil {
			updated = *run.StartedAt
		}
		if run.CompletedAt != nil && run.CompletedAt.After(updated) {
			updated = *run.CompletedAt
		}
		records = append(records, store.CheckRunRecord{
			GitHubID:        run.ID,
			NodeID:          run.NodeID,
			Name:            run.Name,
			Status:          run.Status,
			Conclusion:      run.Conclusion,
			DetailsURL:      run.DetailsURL,
			AppSlug:         run.AppSlug,
			StartedAt:       run.StartedAt,
			CompletedAt:     run.CompletedAt,
			GitHubUpdatedAt: updated,
			Observed:        observed,
		})
	}
	_, err = h.writer.ApplyChecks(ctx, store.ChecksRecord{
		RepoFullName: key.Repo,
		HeadSHA:      key.Value,
		Runs:         records,
		ETag:         etag,
		SyncedAt:     startedAt,
		Source:       source,
	})
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

func (h *Handler) enqueuePRFollowups(
	ctx context.Context,
	key entityKey,
	result store.ApplyPullRequestResult,
	queueName string,
) error {
	if !result.Applied {
		return nil
	}
	specs := make([]queue.RefreshSpec, 0, 3)
	if result.NewHeadSHA != "" && result.NewHeadSHA != result.OldHeadSHA {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshChecks,
			Key:  fmt.Sprintf("checks:%s:%s", key.Repo, result.NewHeadSHA),
		})
	}
	stackNumbers := make(map[int]struct{}, 2)
	if result.OldStackNumber != nil {
		stackNumbers[*result.OldStackNumber] = struct{}{}
	}
	if result.NewStackNumber != nil {
		stackNumbers[*result.NewStackNumber] = struct{}{}
	}
	for number := range stackNumbers {
		specs = append(specs, queue.RefreshSpec{
			Kind: queue.KindRefreshStack,
			Key:  fmt.Sprintf("stack:%s:%d", key.Repo, number),
		})
	}
	return h.enqueue(ctx, specs, queueName)
}

func (h *Handler) enqueueStackDiff(
	ctx context.Context,
	repo string,
	result store.ApplyStackResult,
	queueName string,
) error {
	if !result.Applied {
		return nil
	}
	specs := make([]queue.RefreshSpec, 0)
	seenPRs := make(map[int]struct{})
	for _, number := range append(
		append([]int(nil), result.JoinedPRs...),
		result.LeftPRs...,
	) {
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
	return h.enqueue(ctx, specs, queueName)
}

func (h *Handler) enqueue(
	ctx context.Context,
	specs []queue.RefreshSpec,
	queueName string,
) error {
	if len(specs) == 0 {
		return nil
	}
	client, _ := river.ClientFromContextSafely[pgx.Tx](ctx)
	if client == nil {
		h.riverMu.RLock()
		client = h.river
		h.riverMu.RUnlock()
	}
	if client == nil {
		return fmt.Errorf("River client missing from fetch context")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin refresh fan-out: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
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
		return store.RepositoryRecord{}, fmt.Errorf("read repository cache: %w", err)
	}
	owner, repoName, err := splitRepo(fullName)
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
		return store.RepositoryRecord{}, fmt.Errorf("fetch repository: %w", err)
	}
	record := repositoryRecordFromREST(
		fetched,
		h.installationID,
		h.orgID,
	)
	if _, err := h.writer.ApplyRepository(
		ctx,
		record,
		source,
		response.ETag,
		time.Now(),
	); err != nil {
		return store.RepositoryRecord{}, err
	}
	return record, nil
}

func classAndSource(queueName string) (budget.Class, store.SyncSource, error) {
	switch queueName {
	case queue.QueueInteractive:
		return budget.Interactive, store.SyncSourceBackfill, nil
	case queue.QueueEvent:
		return budget.Event, store.SyncSourceWebhook, nil
	case queue.QueueSweep:
		return budget.Sweep, store.SyncSourceReconcile, nil
	default:
		return "", "", fmt.Errorf("unknown refresh queue %q", queueName)
	}
}

func isNotFound(err error) bool {
	var httpError *gh.HTTPError
	return errors.As(err, &httpError) &&
		httpError.StatusCode == http.StatusNotFound
}

func requestKey(key entityKey) string {
	return key.Kind + ":" + key.Repo + ":" + key.Value
}
