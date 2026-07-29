package fetch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store"
)

var errGraphQLNodeNotFound = errors.New("GraphQL PR node was not found")

const (
	defaultBatchWindow = 5 * time.Millisecond
	defaultBatchSize   = gh.MaxPullRequestBatch
)

type pullBatchWriter interface {
	ApplyPullRequestBatch(
		context.Context,
		[]store.PullRequestRecord,
	) (map[string]store.ApplyPullRequestResult, error)
}

type pullBatchItem struct {
	ctx       context.Context
	key       entityKey
	nodeID    string
	metadata  store.FetchMetadata
	source    store.SyncSource
	class     budget.Class
	startedAt time.Time
	result    chan pullBatchResult
}

type pullBatchResult struct {
	applied store.ApplyPullRequestResult
	err     error
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

// prCoordinator implements C-P4 as a shared worker coordinator. River claims
// individual jobs; calls arriving in a short window gang into nodes(ids:).
// This avoids teaching River about multi-job acknowledgement while retaining
// one generation recheck per durable job.
type prCoordinator struct {
	mu             sync.Mutex
	batches        map[pullBatchKey]*pendingPullBatch
	window         time.Duration
	max            int
	graphQL        *gh.GraphQLClient
	writer         pullBatchWriter
	installationID int64
	orgID          int64
}

func newPRCoordinator(
	graphQL *gh.GraphQLClient,
	writer pullBatchWriter,
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
		writer:         writer,
		installationID: installationID,
		orgID:          orgID,
	}
}

func (c *prCoordinator) submit(
	ctx context.Context,
	key entityKey,
	nodeID string,
	metadata store.FetchMetadata,
	class budget.Class,
	source store.SyncSource,
) (store.ApplyPullRequestResult, error) {
	item := &pullBatchItem{
		ctx:       ctx,
		key:       key,
		nodeID:    nodeID,
		metadata:  metadata,
		class:     class,
		source:    source,
		startedAt: time.Now(),
		result:    make(chan pullBatchResult, 1),
	}
	batchKey := pullBatchKey{class: class, source: source}
	c.mu.Lock()
	batch := c.batches[batchKey]
	if batch == nil {
		batch = &pendingPullBatch{}
		c.batches[batchKey] = batch
		batch.timer = time.AfterFunc(c.window, func() {
			c.flush(batchKey, batch)
		})
	}
	batch.items = append(batch.items, item)
	if len(batch.items) == c.max {
		delete(c.batches, batchKey)
		batch.closed = true
		batch.timer.Stop()
		go c.execute(batch)
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
		return batch.items[i].key.Repo+":"+batch.items[i].key.Value <
			batch.items[j].key.Repo+":"+batch.items[j].key.Value
	})
	ids := make([]string, 0, len(batch.items))
	for _, item := range batch.items {
		ids = append(ids, item.nodeID)
	}
	callCtx, cancel := context.WithTimeout(
		context.WithoutCancel(batch.items[0].ctx),
		30*time.Second,
	)
	defer cancel()
	nodes, _, err := c.graphQL.BatchPullRequests(
		callCtx,
		batch.items[0].class,
		ids,
	)
	if err != nil {
		c.finishAll(batch, nil, err)
		return
	}
	pulls := make([]store.PullRequestRecord, 0, len(nodes))
	for index, node := range nodes {
		if node == nil {
			c.finishAll(
				batch,
				nil,
				fmt.Errorf("%w: %q", errGraphQLNodeNotFound, ids[index]),
			)
			return
		}
		pulls = append(
			pulls,
			pullRecordFromNode(
				node,
				batch.items[index],
				c.installationID,
				c.orgID,
			),
		)
	}
	results, err := c.writer.ApplyPullRequestBatch(callCtx, pulls)
	c.finishAll(batch, results, err)
}

func (c *prCoordinator) finishAll(
	batch *pendingPullBatch,
	results map[string]store.ApplyPullRequestResult,
	err error,
) {
	for _, item := range batch.items {
		result := pullBatchResult{err: err}
		if err == nil {
			entityKey := fmt.Sprintf(
				"pr:%s:%d",
				item.key.Repo,
				item.key.Number,
			)
			result.applied = results[entityKey]
		}
		item.result <- result
	}
}

// SortedPullRequestKeys documents and unit-tests the lock acquisition order
// independently of Postgres (C-P4).
func SortedPullRequestKeys(records []store.PullRequestRecord) []string {
	keys := make([]string, 0, len(records))
	for _, record := range records {
		keys = append(
			keys,
			fmt.Sprintf("pr:%s:%d", record.Repository.FullName, record.Number),
		)
	}
	sort.Strings(keys)
	return keys
}
