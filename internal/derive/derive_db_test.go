package derive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/stream"
	"github.com/acme/frontier/internal/testdb"
)

var deriveTestID atomic.Int64

func TestNoopDeriverEndToEnd(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "noop")
	service, err := New(Options{
		Pool:           pool,
		Deriver:        NoopDeriver{},
		DirtyCap:       10_000,
		InstallationID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := service.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed < 1 {
		t.Fatalf("claimed dirty scopes = %d", claimed)
	}
	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM derivation_dirty WHERE scope_key = $1
	`, scope).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 0 {
		t.Fatalf("NoopDeriver left %d dirty rows for %s", dirty, scope)
	}
	var items int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM work_items WHERE identity_key = $1
	`, PullRequestIdentity(repositoryID, 42)).Scan(&items); err != nil {
		t.Fatal(err)
	}
	if items != 0 {
		t.Fatalf("NoopDeriver wrote %d work items", items)
	}

	if claimed, err := service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	} else if claimed != 0 {
		t.Fatalf("idle pass claimed %d scopes, want 0", claimed)
	}
	var successes, samples int64
	if err := pool.QueryRow(ctx, `
		SELECT success_count, sample_count
		FROM operation_heartbeats
		WHERE installation_id = 1
		  AND component = 'deriver'
		  AND operation = 'dirty_sets'
	`).Scan(&successes, &samples); err != nil {
		t.Fatal(err)
	}
	if successes != 2 || samples < 1 {
		t.Fatalf(
			"deriver heartbeat successes/samples = %d/%d, want 2/at least 1",
			successes,
			samples,
		)
	}
}

func TestRunReconnectsAfterListenerBackendTermination(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	service, err := New(Options{
		Pool:           pool,
		Deriver:        NoopDeriver{},
		DirtyCap:       10_000,
		PollInterval:   20 * time.Millisecond,
		InstallationID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	listeners := make(chan uint32, 2)
	service.listenerReady = func(pid uint32) { listeners <- pid }

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- service.Run(runCtx) }()

	var listenerPID uint32
	select {
	case listenerPID = <-listeners:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var terminated bool
	if err := pool.QueryRow(
		ctx,
		`SELECT pg_terminate_backend($1)`,
		int32(listenerPID),
	).Scan(&terminated); err != nil {
		t.Fatal(err)
	}
	if !terminated {
		t.Fatalf("deriver listener backend %d was not terminated", listenerPID)
	}

	scope, _ := insertLoosePullScope(t, ctx, pool, "listener-reconnect")
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var dirty int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM derivation_dirty WHERE scope_key = $1
		`, scope).Scan(&dirty); err != nil {
			t.Fatal(err)
		}
		if dirty == 0 {
			break
		}
		select {
		case err := <-runErr:
			t.Fatalf("deriver stopped after listener termination: %v", err)
		case <-deadline.C:
			t.Fatal("poll fallback did not drain after listener termination")
		case <-ticker.C:
		}
	}

	select {
	case <-listeners:
		// The listener reconnected while interval polling kept doing work.
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
}

func TestDirtyMarkArrivingMidPassSurvives(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scope, _ := insertLoosePullScope(t, ctx, pool, "survive")
	entered := make(chan Snapshot, 1)
	release := make(chan struct{})
	service, err := New(Options{
		Pool:           pool,
		InstallationID: 1,
		Deriver: blockingDeriver{
			entered: entered,
			release: release,
		},
		DirtyCap: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	passErr := make(chan error, 1)
	go func() {
		_, err := service.RunOnce(ctx)
		passErr <- err
	}()
	snapshot := <-entered
	found := false
	for _, item := range snapshot.Scopes {
		if item.ScopeKey == scope {
			found = true
			if item.OrgID != 1 || !json.Valid(item.Data) {
				t.Errorf("loaded scope = %+v", item)
			}
		}
	}
	if !found {
		t.Fatalf("claimed snapshot omitted %s", scope)
	}

	remarkedAt := time.Now().UTC().Add(time.Minute)
	markDone := make(chan error, 1)
	go func() {
		_, err := pool.Exec(ctx, `
			INSERT INTO derivation_dirty (scope_key, marked_at)
			VALUES ($1, $2)
			ON CONFLICT (scope_key) DO UPDATE
			SET marked_at = GREATEST(
			    derivation_dirty.marked_at,
			    EXCLUDED.marked_at
			)
		`, scope, remarkedAt)
		markDone <- err
	}()
	select {
	case err := <-markDone:
		t.Fatalf("mid-pass mark did not wait for claimed row lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-passErr; err != nil {
		t.Fatal(err)
	}
	if err := <-markDone; err != nil {
		t.Fatal(err)
	}
	var marked time.Time
	if err := pool.QueryRow(ctx, `
		SELECT marked_at
		FROM derivation_dirty
		WHERE scope_key = $1
	`, scope).Scan(&marked); err != nil {
		t.Fatal(err)
	}
	if marked.Before(remarkedAt) {
		t.Fatalf("surviving mark = %s, want >= %s", marked, remarkedAt)
	}
}

func TestFencedDirtyMarkWithConcurrentWatermarkerDoesNotStall(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "fence-order")
	identity := PullRequestIdentity(repositoryID, 42)
	entered := make(chan Snapshot, 1)
	release := make(chan struct{})
	service, err := New(Options{
		Pool:           pool,
		InstallationID: 1,
		Deriver: blockingDeriver{
			entered: entered,
			release: release,
			next: fixedDeriver{item: &WorkItem{
				IdentityKey: identity,
				OrgID:       1,
				Payload:     json.RawMessage(`{"state":"before-writer"}`),
			}},
		},
		DirtyCap: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}

	type passResult struct {
		count int
		err   error
	}
	passDone := make(chan passResult, 1)
	go func() {
		count, err := service.RunOnce(ctx)
		passDone <- passResult{count: count, err: err}
	}()
	<-entered

	// Establish how many shared fence holders the deriver contributes. Before
	// the Q1 fix this is zero; after it, this is one. Waiting for one additional
	// holder proves the real entity writer has acquired the fence before the
	// watermarker queues its exclusive request.
	baselineWriters := fenceLockCount(t, ctx, pool, "ShareLock", true)
	writer := store.NewEntityWriter(pool)
	entityKey := store.PullRequestEntityKey(1, repositoryID, 42)
	observation, err := writer.BeginObservation(ctx, entityKey)
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close() //nolint:errcheck
	now := time.Now().UTC().Add(time.Minute)
	writerDone := make(chan error, 1)
	go func() {
		_, err := writer.ApplyPullRequestObserved(
			ctx,
			observation,
			store.PullRequestRecord{
				Repository: store.RepositoryRecord{
					InstallationID: 1,
					OrgID:          1,
					GitHubID:       repositoryID,
					NodeID:         fmt.Sprintf("R_%d", repositoryID),
					Owner:          "m5-fence-order",
					Name:           fmt.Sprintf("repo-%d", repositoryID),
					FullName: fmt.Sprintf(
						"m5-fence-order/repo-%d",
						repositoryID,
					),
					DefaultBranch:   "main",
					DefaultHeadSHA:  "base-sha",
					GitHubUpdatedAt: now,
				},
				GitHubID:        repositoryID*100 + 42,
				NodeID:          fmt.Sprintf("PR_%d", repositoryID),
				Number:          42,
				Title:           "M5 test updated mid-pass",
				State:           "open",
				AuthorLogin:     "tester",
				HeadRef:         "feature",
				HeadSHA:         fmt.Sprintf("head-%d", repositoryID),
				BaseRef:         "main",
				BaseSHA:         "base-sha",
				GitHubUpdatedAt: now,
				SyncedAt:        now,
				Source:          store.SyncSourceManual,
			},
			nil,
		)
		writerDone <- err
	}()
	waitForFenceLocks(
		t,
		ctx,
		pool,
		"ShareLock",
		true,
		baselineWriters+1,
	)

	watermarker, err := stream.NewWatermarker(
		pool,
		stream.WatermarkOptions{
			RefreshInterval: 10 * time.Millisecond,
			LeaseTTL:        time.Second,
			Owner: fmt.Sprintf(
				"derive-fence-order-%d",
				time.Now().UnixNano(),
			),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.Background()) //nolint:errcheck
	type stepResult struct {
		progress stream.WatermarkProgress
		err      error
	}
	stepDone := make(chan stepResult, 1)
	go func() {
		progress, err := watermarker.Step(ctx)
		stepDone <- stepResult{progress: progress, err: err}
	}()
	waitForFenceLocks(t, ctx, pool, "ExclusiveLock", false, 1)

	releasedAt := time.Now()
	close(release)
	pass := <-passDone
	passDuration := time.Since(releasedAt)
	if pass.err != nil {
		t.Fatal(pass.err)
	}
	if pass.count != 1 {
		t.Fatalf("claimed dirty scopes = %d, want 1", pass.count)
	}
	if passDuration >= 500*time.Millisecond {
		t.Fatalf(
			"derivation pass stalled %s after release; want < 500ms",
			passDuration,
		)
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	step := <-stepDone
	if step.err != nil {
		t.Fatal(step.err)
	}

	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM derivation_dirty
		WHERE scope_key = $1
	`, scope).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("mid-pass fenced writer left %d dirty marks, want 1", dirty)
	}

	var eventCount int
	var greatestSeq int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(max(seq), 0)
		FROM change_events
		WHERE (
		    stream = $1
		    AND kind = $2
		    AND entity_key = $3
		) OR (
		    stream = $4
		    AND kind = $5
		    AND entity_key = $6
		)
	`,
		outbox.WorkItemsStream,
		outbox.WorkItemChangedKind,
		identity,
		outbox.EntitiesStream,
		outbox.PullRequestChangedKind,
		entityKey,
	).Scan(&eventCount, &greatestSeq); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 {
		t.Fatalf("concurrent transactions retained %d events, want 2", eventCount)
	}
	if step.progress.SafeSeq < greatestSeq {
		t.Fatalf(
			"watermark safe seq %d did not cover concurrent event seq %d",
			step.progress.SafeSeq,
			greatestSeq,
		)
	}

	service.deriver = NoopDeriver{}
	if count, err := service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("follow-up pass claimed %d dirty scopes, want 1", count)
	}
}

func TestDeriverWritesWorkItemAndReferenceEventInDirtyTransaction(
	t *testing.T,
) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "work-item")
	identity := PullRequestIdentity(repositoryID, 42)
	service, err := New(Options{
		Pool:           pool,
		InstallationID: 1,
		Deriver: fixedDeriver{item: &WorkItem{
			IdentityKey: identity,
			OrgID:       1,
			Payload:     json.RawMessage(`{"state":"ready"}`),
		}},
		DirtyCap: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var owner, payload, eventPayload string
	if err := pool.QueryRow(ctx, `
		SELECT scope_key, payload::text
		FROM work_items
		WHERE identity_key = $1
	`, identity).Scan(&owner, &payload); err != nil {
		t.Fatal(err)
	}
	if owner != scope {
		t.Fatalf("work-item owner = %q, want %q", owner, scope)
	}
	if !strings.Contains(payload, `"state": "ready"`) {
		t.Fatalf("work-item payload = %s", payload)
	}
	if err := pool.QueryRow(ctx, `
		SELECT payload::text
		FROM change_events
		WHERE stream = 'work_items'
		  AND kind = 'work_item.changed'
		  AND entity_key = $1
		ORDER BY seq DESC
		LIMIT 1
	`, identity).Scan(&eventPayload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(eventPayload, `"state"`) ||
		!strings.Contains(eventPayload, `"version": 1`) ||
		!strings.Contains(eventPayload, `"scope_key"`) {
		t.Fatalf("C-S6 event payload is not a versioned reference: %s", eventPayload)
	}
	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM derivation_dirty WHERE scope_key = $1
	`, scope).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 0 {
		t.Fatalf("derived scope remains dirty")
	}
}

func TestScopeReconciliationRemovesPriorWorkItemAndEmitsReference(
	t *testing.T,
) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "remove")
	identity := PullRequestIdentity(repositoryID, 42)
	service, err := New(Options{
		Pool:           pool,
		InstallationID: 1,
		Deriver: fixedDeriver{item: &WorkItem{
			IdentityKey: identity,
			OrgID:       1,
			Payload:     json.RawMessage(`{"state":"ready"}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO derivation_dirty (scope_key, marked_at)
		VALUES ($1, clock_timestamp())
		ON CONFLICT (scope_key) DO UPDATE
		SET marked_at = EXCLUDED.marked_at
	`, scope); err != nil {
		t.Fatal(err)
	}
	service.deriver = NoopDeriver{}
	if _, err := service.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM work_items WHERE identity_key = $1
	`, identity).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("scope reconciliation retained %d stale work items", count)
	}
	var payload string
	if err := pool.QueryRow(ctx, `
		SELECT payload::text
		FROM change_events
		WHERE stream = 'work_items'
		  AND kind = 'work_item.removed'
		  AND entity_key = $1
		ORDER BY seq DESC
		LIMIT 1
	`, identity).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"scope_key": "`+scope+`"`) {
		t.Fatalf("removed event payload = %s", payload)
	}
}

func TestDeriverRejectsIdentityNotOwnedByClaimedScope(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, _ := insertLoosePullScope(t, ctx, pool, "identity")
	service, err := New(Options{
		Pool:           pool,
		InstallationID: 1,
		Deriver: fixedDeriver{item: &WorkItem{
			IdentityKey: "arbitrary",
			OrgID:       1,
			Payload:     json.RawMessage(`{"state":"bad"}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunOnce(ctx); err == nil ||
		!strings.Contains(err.Error(), "not owned by scope") {
		t.Fatalf("invalid identity error = %v", err)
	}
	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM derivation_dirty WHERE scope_key = $1
	`, scope).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("invalid result cleared dirty scope: count=%d", dirty)
	}
}

func TestScopeSnapshotContainsOnlyLiveScopeOwnedRows(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	looseScope, repositoryID := insertLoosePullScope(
		t,
		ctx,
		pool,
		"snapshot-filter",
	)
	var localRepoID int64
	if err := pool.QueryRow(ctx, `
		SELECT id FROM repos WHERE gh_id = $1
	`, repositoryID).Scan(&localRepoID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	batch := &pgx.Batch{}
	batch.Queue(`
		UPDATE pull_requests
		SET stack_number = 7, stack_position = 1
		WHERE repo_id = $1 AND number = 42
	`, localRepoID)
	batch.Queue(`
		INSERT INTO pull_requests (
		    repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, stack_number, stack_position,
		    gh_updated_at, synced_at, last_checked_at, etag, sync_source,
		    tombstoned_at
		)
		VALUES (
		    $1, $2, $3, 43, 'tombstoned PR', 'open', false,
		    'tester', 'tombstoned', 'tombstoned-head', 'main', 'base-sha',
		    '', '', 7, 2, $4, $4, $4, '', 'manual', $4
		)
	`,
		localRepoID,
		repositoryID*100+43,
		fmt.Sprintf("PR_%d_43", repositoryID),
		now,
	)
	batch.Queue(`
		INSERT INTO stacks (
		    repo_id, gh_id, node_id, number, base_ref, base_sha, open,
		    entries, gh_updated_at, head_sha, synced_at, last_checked_at, etag,
		    sync_source, tombstoned_at
		)
		VALUES (
		    $1, $2, $3, 7, 'main', 'base-sha', true, '[]', $4,
		    'head', $4, $4, '', 'manual', $4
		)
	`,
		localRepoID,
		repositoryID*100+7,
		fmt.Sprintf("S_%d_7", repositoryID),
		now,
	)
	batch.Queue(`
		INSERT INTO repo_rules (
		    repo_id, rule_key, rule, gh_updated_at, head_sha, synced_at,
		    last_checked_at, etag, sync_source, tombstoned_at
		)
		VALUES (
		    $1, 'tombstoned', '{}', $2, 'head', $2, $2, '', 'manual', $2
		)
	`, localRepoID, now)
	batch.Queue(`
		INSERT INTO review_threads (
		    id, repo_id, pr_number, head_sha, synced_at, last_checked_at,
		    sync_source, tombstoned_at
		)
		VALUES ($1, $2, 42, $3, $4, $4, 'manual', $4)
	`,
		fmt.Sprintf("thread-%d", repositoryID),
		localRepoID,
		fmt.Sprintf("head-%d", repositoryID),
		now,
	)
	batch.Queue(`
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, head_sha, synced_at,
		    last_checked_at, sync_source, tombstoned_at
		)
		VALUES (
		    $1, $2, $3, 'tombstoned', 'completed', $4, $5, $5,
		    'manual', $5
		)
	`,
		repositoryID*100+99,
		localRepoID,
		fmt.Sprintf("CR_%d", repositoryID),
		fmt.Sprintf("head-%d", repositoryID),
		now,
	)
	results := pool.SendBatch(ctx, batch)
	if err := results.Close(); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	stackScope := fmt.Sprintf("stack:1:%d:7", repositoryID)
	snapshot, err := (SnapshotLoader{}).Load(
		ctx,
		tx,
		[]string{looseScope, stackScope},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Scopes) != 2 {
		t.Fatalf("snapshot scopes = %d, want 2", len(snapshot.Scopes))
	}
	type snapshotData struct {
		Stack        json.RawMessage   `json:"stack"`
		RepoRules    []json.RawMessage `json:"repo_rules"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
		ReviewThreads []json.RawMessage `json:"review_threads"`
		CheckRuns     []json.RawMessage `json:"check_runs"`
	}
	decoded := make(map[string]snapshotData, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		var data snapshotData
		if err := json.Unmarshal(scope.Data, &data); err != nil {
			t.Fatalf("decode %s snapshot: %v", scope.ScopeKey, err)
		}
		decoded[scope.ScopeKey] = data
	}
	if got := decoded[looseScope].PullRequests; len(got) != 0 {
		t.Fatalf("loose scope contains stack-owned PRs: %+v", got)
	}
	stackData := decoded[stackScope]
	if len(stackData.PullRequests) != 1 ||
		stackData.PullRequests[0].Number != 42 {
		t.Fatalf(
			"stack scope pull requests = %+v, want only live PR 42",
			stackData.PullRequests,
		)
	}
	if string(stackData.Stack) != "null" ||
		len(stackData.RepoRules) != 0 ||
		len(stackData.ReviewThreads) != 0 ||
		len(stackData.CheckRuns) != 0 {
		t.Fatalf("snapshot retained tombstoned rows: %+v", stackData)
	}
}

func TestStableIdentityHelpers(t *testing.T) {
	if got := StackIdentity(991, 12); got != "repo:991:stack:12" {
		t.Fatalf("stack identity = %q", got)
	}
	if got := PullRequestIdentity(991, 34); got != "repo:991:pr:34" {
		t.Fatalf("PR identity = %q", got)
	}
}

type blockingDeriver struct {
	entered chan<- Snapshot
	release <-chan struct{}
	next    Deriver
}

func (d blockingDeriver) Derive(snapshot Snapshot) []ScopeResult {
	d.entered <- snapshot
	<-d.release
	if d.next != nil {
		return d.next.Derive(snapshot)
	}
	return NoopDeriver{}.Derive(snapshot)
}

type fixedDeriver struct {
	item *WorkItem
}

func (d fixedDeriver) Derive(snapshot Snapshot) []ScopeResult {
	results := NoopDeriver{}.Derive(snapshot)
	if d.item == nil {
		return results
	}
	for index := range results {
		scope, err := parseScope(results[index].ScopeKey)
		if err == nil && identityForScope(scope) == d.item.IdentityKey {
			results[index].WorkItems = []WorkItem{*d.item}
			return results
		}
	}
	if len(results) > 0 {
		results[0].WorkItems = []WorkItem{*d.item}
	}
	return results
}

func insertLoosePullScope(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	label string,
) (string, int64) {
	t.Helper()
	id := deriveTestID.Add(1)
	repositoryID := time.Now().UnixMicro() + id
	fullName := fmt.Sprintf("m5-%s/repo-%d", label, repositoryID)
	now := time.Now().UTC()
	var localRepoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO repos (
		    installation_id, org_id, gh_id, node_id, owner, name,
		    full_name, default_branch, archived, gh_updated_at,
		    head_sha, synced_at, last_checked_at, etag, sync_source
		)
		VALUES (
		    1, 1, $1, $2, $3, $4, $5, 'main', false, $6,
		    'base-sha', $6, $6, '', 'manual'
		)
		RETURNING id
	`,
		repositoryID,
		fmt.Sprintf("R_%d", repositoryID),
		"m5-"+label,
		fmt.Sprintf("repo-%d", repositoryID),
		fullName,
		now,
	).Scan(&localRepoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pull_requests (
		    repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, gh_updated_at, synced_at,
		    last_checked_at, etag, sync_source
		)
		VALUES (
		    $1, $2, $3, 42, 'M5 test', 'open', false, 'tester',
		    'feature', $4, 'main', 'base-sha', '', '', $5, $5, $5,
		    '', 'manual'
		)
	`,
		localRepoID,
		repositoryID*100+42,
		fmt.Sprintf("PR_%d", repositoryID),
		fmt.Sprintf("head-%d", repositoryID),
		now,
	); err != nil {
		t.Fatal(err)
	}
	scope := fmt.Sprintf("pr:1:%d:42", repositoryID)
	if _, err := pool.Exec(ctx, `
		INSERT INTO derivation_dirty (scope_key, marked_at)
		VALUES ($1, $2)
		ON CONFLICT (scope_key) DO UPDATE SET marked_at = EXCLUDED.marked_at
	`, scope, now); err != nil {
		t.Fatal(err)
	}
	return scope, repositoryID
}

func deriveDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := testdb.Open(ctx, url, "derive")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	return database.Pool
}

func waitForFenceLocks(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	mode string,
	granted bool,
	want int,
) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fenceLockCount(t, ctx, pool, mode, granted) >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf(
				"timed out waiting for %d outbox fence locks (%s, granted=%t)",
				want,
				mode,
				granted,
			)
		case <-ticker.C:
		}
	}
}

func fenceLockCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	mode string,
	granted bool,
) int {
	t.Helper()
	fenceKey := uint64(outbox.FenceKey)
	classID := int64(fenceKey >> 32)
	objectID := int64(uint32(fenceKey))
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = $1
		  AND objid = $2
		  AND objsubid = 1
		  AND mode = $3
		  AND granted = $4
	`, classID, objectID, mode, granted).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
