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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/testdb"
)

var deriveTestID atomic.Int64

func TestNoopDeriverEndToEnd(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "noop")
	service, err := New(Options{
		Pool:     pool,
		Deriver:  NoopDeriver{},
		DirtyCap: 10_000,
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
}

func TestDirtyMarkArrivingMidPassSurvives(t *testing.T) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scope, _ := insertLoosePullScope(t, ctx, pool, "survive")
	entered := make(chan Snapshot, 1)
	release := make(chan struct{})
	service, err := New(Options{
		Pool: pool,
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

func TestDeriverWritesWorkItemAndReferenceEventInDirtyTransaction(
	t *testing.T,
) {
	pool := deriveDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scope, repositoryID := insertLoosePullScope(t, ctx, pool, "work-item")
	identity := PullRequestIdentity(repositoryID, 42)
	service, err := New(Options{
		Pool: pool,
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
		Pool: pool,
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
		Pool: pool,
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
}

func (d blockingDeriver) Derive(snapshot Snapshot) []ScopeResult {
	d.entered <- snapshot
	<-d.release
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
