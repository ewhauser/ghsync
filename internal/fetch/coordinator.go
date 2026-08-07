package fetch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/changeinputs"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
)

var errGraphQLNodeNotFound = errors.New("GraphQL PR node was not found")

const (
	defaultBatchWindow = 5 * time.Millisecond
	defaultBatchSize   = gh.MaxPullRequestBatch
)

type pullBatchItem struct {
	ctx                   context.Context //nolint:containedctx // queued work must retain each caller's values until the batch flushes
	key                   entityKey
	nodeID                string
	metadata              store.FetchMetadata
	source                store.SyncSource
	class                 budget.Class
	startedAt             time.Time
	hook                  func(string) store.PullRequestHook
	result                chan pullBatchResult
	repositoryGeneration  int64
	repositoryObservation *store.Observation
}

type pullBatchResult struct {
	applied store.ApplyPullRequestResult
	err     error
}

type pullApplyContext struct {
	context.Context                 //nolint:containedctx // batch cancellation is combined with per-item values
	values          context.Context //nolint:containedctx // per-item values must survive shared batch transport
}

type skipRepositoryObservationContextKey struct{}

// withoutRepositoryObservation keeps branch-page PR reconciliation scoped to
// its PR target. The push transaction already established the repository's
// branch SHA; replaying the GraphQL node's incidental parent representation
// could otherwise overwrite a newer local branch generation.
func withoutRepositoryObservation(ctx context.Context) context.Context {
	return context.WithValue(
		ctx,
		skipRepositoryObservationContextKey{},
		true,
	)
}

func skipsRepositoryObservation(ctx context.Context) bool {
	skips, _ := ctx.Value(skipRepositoryObservationContextKey{}).(bool)
	return skips
}

func (c pullApplyContext) Value(key any) any {
	// Cancellation and the transport deadline come from the shared batch
	// context, but values are entity-local. Falling back to the first item's
	// context when this item had no value leaked branch/generation fences into
	// unrelated direct refreshes in the same GraphQL batch.
	return c.values.Value(key)
}

type pullBatchKey struct {
	class  budget.Class
	source store.SyncSource
}

type pendingPullBatch struct {
	items  []*pullBatchItem
	timer  *time.Timer
	closed bool
}

// prCoordinator gangs GraphQL transport while preserving one independently
// locked transaction and one result per entity.
type prCoordinator struct {
	mu             sync.Mutex
	batches        map[pullBatchKey]*pendingPullBatch
	window         time.Duration
	max            int
	graphQL        *gh.GraphQLClient
	rest           *gh.RESTClient
	codeowners     *changeinputs.SourceResolver
	writer         *store.EntityWriter
	installationID int64
	orgID          int64
}

func newPRCoordinator(
	graphQL *gh.GraphQLClient,
	rest *gh.RESTClient,
	codeowners *changeinputs.SourceResolver,
	writer *store.EntityWriter,
	installationID int64,
	orgID int64,
	window time.Duration,
) *prCoordinator {
	if window <= 0 {
		window = defaultBatchWindow
	}
	return &prCoordinator{
		batches:        make(map[pullBatchKey]*pendingPullBatch),
		window:         window,
		max:            defaultBatchSize,
		graphQL:        graphQL,
		rest:           rest,
		codeowners:     codeowners,
		writer:         writer,
		installationID: installationID,
		orgID:          orgID,
	}
}

func (c *prCoordinator) submit(
	ctx context.Context,
	key entityKey,
	nodeID string,
	metadata *store.FetchMetadata,
	class budget.Class,
	source store.SyncSource,
	hook func(string) store.PullRequestHook,
) (store.ApplyPullRequestResult, error) {
	item := &pullBatchItem{
		ctx:       ctx,
		key:       key,
		nodeID:    nodeID,
		metadata:  *metadata,
		class:     class,
		source:    source,
		startedAt: time.Now(),
		hook:      hook,
		result:    make(chan pullBatchResult, 1),
	}
	batchKey := pullBatchKey{class: class, source: source}
	c.mu.Lock()
	batch := c.batches[batchKey]
	if batch == nil {
		batch = &pendingPullBatch{}
		c.batches[batchKey] = batch
		batch.timer = time.AfterFunc(c.window, func() { //nolint:contextcheck // the batch owns queued caller contexts
			c.flush(batchKey, batch)
		})
	}
	batch.items = append(batch.items, item)
	if len(batch.items) == c.max {
		delete(c.batches, batchKey)
		batch.closed = true
		batch.timer.Stop()
		go c.execute(batch) //nolint:contextcheck // the batch owns queued caller contexts
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return store.ApplyPullRequestResult{}, ctx.Err()
	case result := <-item.result:
		return result.applied, result.err
	}
}

func (c *prCoordinator) flush(key pullBatchKey, batch *pendingPullBatch) {
	c.mu.Lock()
	if batch.closed || c.batches[key] != batch {
		c.mu.Unlock()
		return
	}
	delete(c.batches, key)
	batch.closed = true
	c.mu.Unlock()
	c.execute(batch)
}

func (c *prCoordinator) execute(batch *pendingPullBatch) {
	sort.Slice(batch.items, func(i, j int) bool {
		return immutablePRKey(batch.items[i]) < immutablePRKey(batch.items[j])
	})
	callCtx, cancel := context.WithTimeout(
		context.WithoutCancel(batch.items[0].ctx),
		30*time.Second,
	)
	defer cancel()

	var observations []*store.Observation
	closeObservations := func() {
		for _, v := range slices.Backward(observations) {
			closeObservation(callCtx, v)
		}
	}
	defer closeObservations()

	itemObservations := make(map[*pullBatchItem]*store.Observation, len(batch.items))
	active := make([]*pullBatchItem, 0, len(batch.items))
	results := make(map[*pullBatchItem]pullBatchResult, len(batch.items))
	for _, item := range batch.items {
		observation, err := c.writer.BeginObservation(
			callCtx,
			store.PullRequestEntityKey(
				item.metadata.InstallationID,
				item.metadata.RepoGitHubID,
				item.key.Number,
			),
		)
		if err != nil {
			results[item] = pullBatchResult{err: err}
			continue
		}
		itemObservations[item] = observation
		observations = append(observations, observation)
		metadata, err := c.writer.PullRequestMetadata(
			callCtx,
			item.key.Repo,
			item.key.Number,
		)
		if err != nil {
			results[item] = pullBatchResult{err: err}
			continue
		}
		item.metadata = metadata
		item.nodeID = metadata.NodeID
		repositoryKey := "repo:" + metadata.RepoFullName + ":metadata"
		repositoryGeneration, err := c.writer.RefreshGeneration(
			callCtx,
			queue.KindRefreshRepository,
			repositoryKey,
		)
		if err != nil {
			results[item] = pullBatchResult{err: err}
			continue
		}
		item.repositoryGeneration = repositoryGeneration
		repositoryObservation, err := c.writer.BeginObservation(
			callCtx,
			store.RepositoryEntityKey(
				item.metadata.InstallationID,
				item.metadata.RepoGitHubID,
			),
		)
		if err != nil {
			results[item] = pullBatchResult{err: err}
			continue
		}
		item.repositoryObservation = repositoryObservation
		active = append(active, item)
	}
	if len(active) == 0 {
		c.finishAll(batch, results, nil)
		return
	}

	ids := make([]string, 0, len(active))
	for _, item := range active {
		ids = append(ids, item.nodeID)
	}
	nodes, nodeErrors := c.fetchPullRequestNodes(callCtx, active, ids)

	type preparedPull struct {
		item   *pullBatchItem
		record store.PullRequestRecord
	}
	prepared := make([]preparedPull, 0, len(active))
	repositoryCandidates := make(map[int64]preparedPull)
	for index, node := range nodes {
		item := active[index]
		if nodeErr := nodeErrors[index]; nodeErr != nil {
			results[item] = pullBatchResult{err: nodeErr}
			continue
		}
		if node == nil {
			results[item] = pullBatchResult{
				err: fmt.Errorf(
					"%w: %q",
					errGraphQLNodeNotFound,
					ids[index],
				),
			}
			continue
		}
		fenceETag := matchingPullETag(&item.metadata, node)
		item.metadata.ETag = fenceETag
		record := pullRecordFromNode(
			node,
			item,
			c.installationID,
			c.orgID,
		)
		snapshot, fence, err := changeinputs.HydrateFromMirrorConditional(
			callCtx,
			c.rest,
			c.codeowners,
			c.writer,
			item.class,
			record.Repository.GitHubID,
			record.Repository.Owner,
			record.Repository.Name,
			record.Number,
			node,
			item.metadata.Codeowners,
			item.metadata.ForceCodeownersRefresh,
			fenceETag,
		)
		if err != nil {
			results[item] = pullBatchResult{
				err: fmt.Errorf("hydrate PR change inputs: %w", err),
			}
			continue
		}
		applyPullFence(&record, fence)
		record.ChangeSnapshot = snapshot
		record.ChangeInputsKnown = true
		repoID := record.Repository.GitHubID
		candidate := preparedPull{item: item, record: record}
		prepared = append(prepared, candidate)
		if _, exists := repositoryCandidates[repoID]; !exists && !skipsRepositoryObservation(item.ctx) {
			repositoryCandidates[repoID] = candidate
		}
	}

	// Every remote fetch and hydration request is complete before repository
	// apply. PR observations above are resource-free version tokens; repository
	// and PR compare-and-write transactions each take only their own entity's
	// transaction-scoped lock (C-C6).
	repositoryFailures := make(map[int64]error)
	repoIDs := make([]int64, 0, len(repositoryCandidates))
	for repoID := range repositoryCandidates {
		repoIDs = append(repoIDs, repoID)
	}
	slices.Sort(repoIDs)
	for _, repoID := range repoIDs {
		candidate := repositoryCandidates[repoID]
		err := func() error {
			repositoryCtx := context.Context(pullApplyContext{
				Context: callCtx,
				values:  candidate.item.ctx,
			})
			repositoryCtx = store.WithRefreshGenerationFence(
				repositoryCtx,
				store.RefreshGenerationFence{
					Kind: queue.KindRefreshRepository,
					RefreshKey: "repo:" +
						candidate.item.metadata.RepoFullName + ":metadata",
					Generation: candidate.item.repositoryGeneration,
				},
			)
			// pullApplyContext embeds callCtx, so cancellation and deadlines
			// are inherited; only Values are item-local. contextcheck cannot
			// see through the custom wrapper type.
			defer closeObservation( //nolint:contextcheck // pullApplyContext embeds callCtx
				repositoryCtx,
				candidate.item.repositoryObservation,
			)
			_, err := c.writer.ApplyRepositoryObserved( //nolint:contextcheck // pullApplyContext embeds callCtx
				repositoryCtx,
				candidate.item.repositoryObservation,
				candidate.record.Repository,
				candidate.item.source,
				"",
				candidate.item.startedAt,
			)
			return err
		}()
		if errors.Is(err, store.ErrRefreshGenerationSuperseded) ||
			errors.Is(err, store.ErrObservationSuperseded) {
			// A newer repository/default-branch generation supersedes only the
			// incidental parent representation. The independently fenced PR
			// response remains authoritative and must still be applied.
			continue
		}
		if err != nil {
			repositoryFailures[repoID] = err
		}
	}

	applies := make([]store.PullRequestApply, 0, len(prepared))
	applyItems := make(map[string]*pullBatchItem, len(prepared))
	for index := range prepared {
		candidate := &prepared[index]
		item := candidate.item
		record := &candidate.record
		if repoErr := repositoryFailures[record.Repository.GitHubID]; repoErr != nil {
			results[item] = pullBatchResult{err: repoErr}
			continue
		}
		key := store.PullRequestEntityKey(
			record.Repository.InstallationID,
			record.Repository.GitHubID,
			record.Number,
		)
		applies = append(applies, store.PullRequestApply{
			Context: pullApplyContext{
				Context: callCtx,
				values:  item.ctx,
			},
			Record:      *record,
			Observation: itemObservations[item],
			Hook:        item.hook(record.Repository.FullName),
		})
		applyItems[key] = item
	}
	outcomes := c.writer.ApplyPullRequestBatch(callCtx, applies)
	for key, outcome := range outcomes {
		item := applyItems[key]
		results[item] = pullBatchResult{
			applied: outcome.Result,
			err:     outcome.Err,
		}
	}
	c.finishAll(batch, results, nil)
}

// matchingPullETag returns a validator only while it is still paired with the
// exact REST metadata version stored on the row. A later GraphQL observation
// with a new parent version invalidates the old association even if its
// base/head pair happens to be unchanged (C-C2).
func matchingPullETag(
	metadata *store.FetchMetadata,
	node *gh.PullRequestNode,
) string {
	if metadata == nil || node == nil || metadata.ETag == "" ||
		!node.UpdatedAt.Equal(metadata.GitHubUpdatedAt) ||
		node.Title != metadata.Title ||
		!strings.EqualFold(node.State, metadata.State) ||
		node.IsDraft != metadata.Draft ||
		node.Author.Login != metadata.AuthorLogin ||
		node.HeadRefName != metadata.HeadRef ||
		node.BaseRefName != metadata.BaseRef ||
		node.ReviewDecision != metadata.ReviewDecision ||
		node.Mergeable != metadata.MergeableState ||
		node.BaseRefOID != metadata.BaseSHA ||
		node.HeadRefOID != metadata.HeadSHA {
		return ""
	}
	return metadata.ETag
}

func (c *prCoordinator) fetchPullRequestNodes(
	ctx context.Context,
	items []*pullBatchItem,
	ids []string,
) ([]*gh.PullRequestNode, map[int]error) {
	nodes, _, batchErr := c.graphQL.BatchPullRequests(
		ctx,
		items[0].class,
		ids,
	)
	if batchErr == nil {
		return nodes, nil
	}
	nodes = make([]*gh.PullRequestNode, len(items))
	nodeErrors := make(map[int]error, len(items))
	if len(items) == 1 {
		nodeErrors[0] = batchErr
		return nodes, nodeErrors
	}
	// BatchPullRequests completes review-thread connections after the nodes()
	// response. If one connection fails, retrying each node independently
	// recovers already returned healthy nodes and confines a persistent
	// completion failure to its owning PR.
	for index, item := range items {
		isolated, _, err := c.graphQL.BatchPullRequests(
			ctx,
			item.class,
			[]string{ids[index]},
		)
		if err != nil {
			nodeErrors[index] = fmt.Errorf(
				"GraphQL PR batch failed (%w); isolate node %q: %w",
				batchErr,
				ids[index],
				err,
			)
			continue
		}
		nodes[index] = isolated[0]
	}
	return nodes, nodeErrors
}

func (c *prCoordinator) finishAll(
	batch *pendingPullBatch,
	results map[*pullBatchItem]pullBatchResult,
	batchErr error,
) {
	for _, item := range batch.items {
		result, ok := results[item]
		if batchErr != nil {
			result = pullBatchResult{err: batchErr}
		} else if !ok {
			result = pullBatchResult{
				err: fmt.Errorf(
					"PR batch omitted result for %s",
					requestKey(item.key),
				),
			}
		}
		item.result <- result
	}
}

func immutablePRKey(item *pullBatchItem) string {
	return store.PullRequestEntityKey(
		item.metadata.InstallationID,
		item.metadata.RepoGitHubID,
		item.key.Number,
	)
}

func SortedPullRequestKeys(records []store.PullRequestRecord) []string {
	keys := make([]string, 0, len(records))
	for index := range records {
		record := &records[index]
		keys = append(keys, store.PullRequestEntityKey(
			record.Repository.InstallationID,
			record.Repository.GitHubID,
			record.Number,
		))
	}
	sort.Strings(keys)
	return keys
}
