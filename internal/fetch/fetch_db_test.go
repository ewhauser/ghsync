package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/dispatch"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/pipeline"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

const fetchTestSecret = "fetch-test-secret"

func TestWriteRaceBothOrdersNewerWins(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for index, order := range []string{"old-new", "new-old"} {
		repo := fmt.Sprintf("acme/write-race-%d", index)
		repository := testRepository(repo, int64(2000+index), baseTime)
		old := testPull(&repository, baseTime, "old-head")
		newer := testPull(&repository, baseTime.Add(time.Minute), "new-head")
		var sequence []store.PullRequestRecord
		if order == "old-new" {
			sequence = []store.PullRequestRecord{old, newer}
		} else {
			sequence = []store.PullRequestRecord{newer, old}
		}
		for _, pull := range sequence {
			if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
				t.Fatalf("%s apply: %v", order, err)
			}
		}
		row, err := dbgen.New(pool).GetPullRequestByKey(
			context.Background(),
			dbgen.GetPullRequestByKeyParams{
				RepoFullName: repo,
				PrNumber:     42,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if row.HeadSha != "new-head" ||
			!row.GhUpdatedAt.Valid ||
			!row.GhUpdatedAt.Time.Equal(newer.GitHubUpdatedAt) {
			t.Fatalf("%s final row = %+v, newer write did not win", order, row)
		}
	}
}

func TestEqualTimestampDomainChangeAndTombstoneResurrection(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	updatedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/equal-version", 2100, updatedAt)
	first := testPull(&repository, updatedAt, "head-one")
	second := testPull(&repository, updatedAt, "head-two")
	second.Title = "equal timestamp changed truth"
	second.SyncedAt = first.SyncedAt.Add(time.Second)
	if _, err := writer.ApplyPullRequest(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	result, err := writer.ApplyPullRequest(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DomainChanged || result.NewHeadSHA != "head-two" {
		t.Fatalf("equal timestamp change result = %+v", result)
	}

	observation, err := writer.BeginObservation(
		context.Background(),
		store.PullRequestEntityKey(1, 2100, 42),
	)
	if err != nil {
		t.Fatal(err)
	}
	tombstonedAt := second.SyncedAt.Add(time.Second)
	if _, err := writer.TombstonePullRequestObserved(
		context.Background(),
		observation,
		repository,
		42,
		store.SyncSourceWebhook,
		tombstonedAt,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}
	resurrected := second
	resurrected.SyncedAt = tombstonedAt.Add(time.Second)
	observation, err = writer.BeginObservation(
		context.Background(),
		store.PullRequestEntityKey(1, 2100, 42),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ApplyPullRequestObserved(
		context.Background(),
		observation,
		resurrected,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.TombstonedAt.Valid || row.HeadSha != "head-two" {
		t.Fatalf("resurrected row = %+v", row)
	}
}

func TestRepositoryRenameKeepsAliasesImmutableEventsAndDirtyScopes(
	t *testing.T,
) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	oldRepository := testRepository("acme/old-name", 2200, baseTime)
	pull := testPull(&oldRepository, baseTime, "rename-head")
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(
		context.Background(),
		`TRUNCATE derivation_dirty`,
	); err != nil {
		t.Fatal(err)
	}
	renamed := oldRepository
	renamed.Owner = "platform"
	renamed.Name = "new-name"
	renamed.FullName = "platform/new-name"
	renamed.GitHubUpdatedAt = baseTime.Add(time.Minute)
	if _, err := writer.ApplyRepository(
		context.Background(),
		renamed,
		store.SyncSourceWebhook,
		`"renamed"`,
		baseTime.Add(2*time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"acme/old-name", "platform/new-name"} {
		got, err := writer.Repository(context.Background(), alias)
		if err != nil {
			t.Fatalf("resolve alias %s: %v", alias, err)
		}
		if got.GitHubID != 2200 || got.FullName != "platform/new-name" {
			t.Fatalf("alias %s resolved to %+v", alias, got)
		}
	}
	var repos, dirty, immutableEvents int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM repos WHERE gh_id = 2200`,
	).Scan(&repos); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM derivation_dirty
		WHERE scope_key = 'pr:1:2200:42'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'repository.changed'
		  AND entity_key = 'repo:1:2200'
	`).Scan(&immutableEvents); err != nil {
		t.Fatal(err)
	}
	if repos != 1 || dirty != 1 || immutableEvents < 1 {
		t.Fatalf(
			"rename repos=%d dirty=%d immutable_events=%d",
			repos,
			dirty,
			immutableEvents,
		)
	}

	if _, err := pool.Exec(
		context.Background(),
		`TRUNCATE derivation_dirty`,
	); err != nil {
		t.Fatal(err)
	}
	checksObservation, err := writer.BeginObservation(
		context.Background(),
		store.ChecksEntityKey(
			oldRepository.InstallationID,
			oldRepository.GitHubID,
			pull.HeadSHA,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer checksObservation.Close() //nolint:errcheck // deferred cleanup cannot change the primary operation result
	changed, err := writer.ApplyChecksObserved(
		context.Background(),
		checksObservation,
		store.ChecksRecord{
			// Model a queued job carrying the pre-rename alias. Dirty-scope
			// resolution must use immutable GitHub identity, not this name.
			Repository: oldRepository,
			HeadSHA:    pull.HeadSHA,
			Runs: []store.CheckRunRecord{{
				GitHubID:   22001,
				NodeID:     "rename-check",
				Name:       "unit",
				Status:     "completed",
				Conclusion: "success",
				Observed:   json.RawMessage(`{"id":22001}`),
			}},
			SyncedAt: baseTime.Add(3 * time.Minute),
			Source:   store.SyncSourceWebhook,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("stale-name check observation was not applied")
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM derivation_dirty
		WHERE scope_key = 'pr:1:2200:42'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("stale-name check dirty scopes = %d, want 1", dirty)
	}
}

func TestTimestampLessChecksOnlyAppendAcceptedTransitions(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/check-transitions", 2300, baseTime)
	pull := testPull(&repository, baseTime, "checks-head")
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	apply := func(status string, observedAt time.Time) bool {
		t.Helper()
		observation, err := writer.BeginObservation(
			context.Background(),
			store.ChecksEntityKey(1, 2300, "checks-head"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer observation.Close() //nolint:errcheck // deferred cleanup cannot change the primary operation result
		changed, err := writer.ApplyChecksObserved(
			context.Background(),
			observation,
			store.ChecksRecord{
				Repository: repository,
				HeadSHA:    "checks-head",
				Runs: []store.CheckRunRecord{{
					GitHubID:   9001,
					NodeID:     "check-node",
					Name:       "unit",
					Status:     status,
					DetailsURL: "https://example.test/check/9001",
					Observed:   json.RawMessage(`{"id":9001}`),
				}},
				SyncedAt: observedAt,
				Source:   store.SyncSourceWebhook,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if !apply("queued", baseTime.Add(time.Minute)) {
		t.Fatal("initial queued check was not accepted")
	}
	var initialSynced, initialChecked time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at, last_checked_at FROM check_runs WHERE gh_id = 9001
	`).Scan(&initialSynced, &initialChecked); err != nil {
		t.Fatal(err)
	}
	if apply("queued", baseTime.Add(2*time.Minute)) {
		t.Fatal("identical timestamp-less check was reported changed")
	}
	var afterSynced, afterChecked time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at, last_checked_at FROM check_runs WHERE gh_id = 9001
	`).Scan(&afterSynced, &afterChecked); err != nil {
		t.Fatal(err)
	}
	if !afterSynced.Equal(initialSynced) ||
		!afterChecked.After(initialChecked) {
		t.Fatalf(
			"identical check provenance synced=%s->%s checked=%s->%s",
			initialSynced,
			afterSynced,
			initialChecked,
			afterChecked,
		)
	}
	if !apply("in_progress", baseTime.Add(3*time.Minute)) {
		t.Fatal("timestamp-less status transition was rejected")
	}
	var history, events int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM check_history WHERE check_run_gh_id = 9001`,
	).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:2300:checks-head'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if history != 2 || events != 2 {
		t.Fatalf("accepted transition history=%d events=%d, want 2/2", history, events)
	}
}

func TestIdenticalPR200OnlyAdvancesLastCheckedAt(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := testRepository("acme/recheck", 2400, baseTime)
	pull := testPull(&repository, baseTime, "same-head")
	pull.ChangeInputsKnown = true
	pull.ChangeSnapshot = &store.PullRequestChangeSnapshotRecord{
		BaseSHA: "base", HeadSHA: "same-head", FilesTotalCount: 1,
		CodeownersRef: "main", CodeownersSHA: "base",
		CodeownersPath: "CODEOWNERS", CodeownersState: "present",
		CodeownersSource: "* @unknown", CodeownersHash: "hash-one",
		Files: []store.ChangedFileRecord{{
			Path: "src/main.go", ChangeType: "modified",
		}},
		Owners: []store.FileOwnerRecord{{
			Path: "src/main.go", OwnerToken: "@unknown", OwnerType: "user",
			OwnerName: "unknown", ResolutionState: "unresolved",
			SourcePattern: "*", SourceLine: 1,
		}},
	}
	if _, err := writer.ApplyPullRequest(context.Background(), pull); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	initialSynced := row.SyncedAt.Time
	initialChecked := row.LastCheckedAt.Time
	pull.SyncedAt = pull.SyncedAt.Add(time.Minute)
	result, err := writer.ApplyPullRequest(context.Background(), pull)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied || result.DomainChanged {
		t.Fatalf("identical PR result = %+v", result)
	}
	row, err = dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !row.SyncedAt.Time.Equal(initialSynced) ||
		!row.LastCheckedAt.Time.After(initialChecked) {
		t.Fatalf(
			"identical PR synced=%s->%s checked=%s->%s",
			initialSynced,
			row.SyncedAt.Time,
			initialChecked,
			row.LastCheckedAt.Time,
		)
	}
	var events int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'pull_request.changed'
		  AND entity_key = 'pr:1:2400:42'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("identical PR change events = %d, want 1 initial event", events)
	}
	var snapshotSynced, snapshotChecked time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at, last_checked_at
		FROM pull_request_change_snapshots
		WHERE repo_id = (SELECT id FROM repos WHERE gh_id = 2400)
		  AND pr_number = 42
	`).Scan(&snapshotSynced, &snapshotChecked); err != nil {
		t.Fatal(err)
	}
	if !snapshotSynced.Equal(initialSynced) ||
		!snapshotChecked.After(initialChecked) {
		t.Fatalf(
			"identical change inputs synced=%s checked=%s",
			snapshotSynced, snapshotChecked,
		)
	}

	ownershipChanged := pull
	ownershipChanged.SyncedAt = pull.SyncedAt.Add(time.Minute)
	changedSnapshot := *pull.ChangeSnapshot
	changedSnapshot.CodeownersSource = "* @new-owner"
	changedSnapshot.CodeownersHash = "hash-two"
	changedSnapshot.Owners = []store.FileOwnerRecord{{
		Path: "src/main.go", OwnerToken: "@new-owner", OwnerType: "user",
		OwnerName: "new-owner", ResolutionState: "unresolved",
		SourcePattern: "*", SourceLine: 1,
	}}
	ownershipChanged.ChangeSnapshot = &changedSnapshot
	result, err = writer.ApplyPullRequest(
		context.Background(), ownershipChanged,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ChangeInputsChanged || !result.Applied || result.DomainChanged {
		t.Fatalf("equal-parent ownership change result = %+v", result)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM change_events
		WHERE kind = 'pull_request.changed'
		  AND entity_key = 'pr:1:2400:42'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Fatalf("ownership change events = %d, want exactly 2 total", events)
	}

	stale := ownershipChanged
	stale.GitHubUpdatedAt = baseTime.Add(-time.Minute)
	stale.SyncedAt = ownershipChanged.SyncedAt.Add(time.Minute)
	staleSnapshot := changedSnapshot
	staleSnapshot.Files = []store.ChangedFileRecord{{
		Path: "stale.go", ChangeType: "added",
	}}
	staleSnapshot.CodeownersSource = "* @stale-owner"
	staleSnapshot.CodeownersHash = "stale-hash"
	staleSnapshot.Owners = nil
	stale.ChangeSnapshot = &staleSnapshot
	result, err = writer.ApplyPullRequest(context.Background(), stale)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied || result.ChangeInputsChanged {
		t.Fatalf("stale parent changed ownership children: %+v", result)
	}
	var livePath string
	if err := pool.QueryRow(context.Background(), `
		SELECT path FROM pull_request_changed_files
		WHERE repo_id = (SELECT id FROM repos WHERE gh_id = 2400)
		  AND pr_number = 42 AND tombstoned_at IS NULL
	`).Scan(&livePath); err != nil {
		t.Fatal(err)
	}
	if livePath != "src/main.go" {
		t.Fatalf("stale parent replaced changed files with %q", livePath)
	}
	var liveHash, liveOwner string
	if err := pool.QueryRow(context.Background(), `
		SELECT snapshot.codeowners_hash, owner.owner_token
		FROM pull_request_change_snapshots AS snapshot
		JOIN pull_request_file_owners AS owner
		  ON owner.repo_id = snapshot.repo_id
		 AND owner.pr_number = snapshot.pr_number
		 AND owner.tombstoned_at IS NULL
		WHERE snapshot.repo_id = (SELECT id FROM repos WHERE gh_id = 2400)
		  AND snapshot.pr_number = 42
		  AND snapshot.tombstoned_at IS NULL
	`).Scan(&liveHash, &liveOwner); err != nil {
		t.Fatal(err)
	}
	if liveHash != "hash-two" || liveOwner != "@new-owner" {
		t.Fatalf(
			"stale parent replaced snapshot/owner with %q/%q",
			liveHash, liveOwner,
		)
	}
	var beforeParentAdvance time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at
		FROM pull_request_change_snapshots
		WHERE repo_id = (SELECT id FROM repos WHERE gh_id = 2400)
		  AND pr_number = 42
	`).Scan(&beforeParentAdvance); err != nil {
		t.Fatal(err)
	}
	newerParent := ownershipChanged
	newerParent.GitHubUpdatedAt = baseTime.Add(time.Minute)
	newerParent.SyncedAt = stale.SyncedAt.Add(time.Minute)
	result, err = writer.ApplyPullRequest(context.Background(), newerParent)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DomainChanged || result.ChangeInputsChanged {
		t.Fatalf("parent-only freshness advance result = %+v", result)
	}
	var afterParentAdvance time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT synced_at
		FROM pull_request_change_snapshots
		WHERE repo_id = (SELECT id FROM repos WHERE gh_id = 2400)
		  AND pr_number = 42
	`).Scan(&afterParentAdvance); err != nil {
		t.Fatal(err)
	}
	if !afterParentAdvance.Equal(beforeParentAdvance) {
		t.Fatalf(
			"parent-only freshness changed snapshot synced_at %s -> %s",
			beforeParentAdvance, afterParentAdvance,
		)
	}
}

func TestPullRequestBatchIsolatesPoisonEntity(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	writer := store.NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	goodRepo := testRepository("acme/good-batch", 2500, baseTime)
	badRepo := testRepository("acme/bad-batch", 2501, baseTime)
	good := testPull(&goodRepo, baseTime, "good-head")
	bad := testPull(&badRepo, baseTime, "bad-head")
	bad.NodeID = ""
	outcomes := writer.ApplyPullRequestBatch(
		context.Background(),
		[]store.PullRequestApply{
			{Record: bad},
			{Record: good},
		},
	)
	goodKey := store.PullRequestEntityKey(1, 2500, 42)
	badKey := store.PullRequestEntityKey(1, 2501, 42)
	if outcomes[goodKey].Err != nil ||
		!outcomes[goodKey].Result.DomainChanged {
		t.Fatalf("healthy batch outcome = %+v", outcomes[goodKey])
	}
	if outcomes[badKey].Err == nil {
		t.Fatalf("poison batch outcome = %+v", outcomes[badKey])
	}
	if _, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: goodRepo.FullName,
			PrNumber:     42,
		},
	); err != nil {
		t.Fatalf("healthy batch row missing: %v", err)
	}
}

func TestCoordinatorReturnsPerKeyWriterErrors(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	for _, number := range []int{4812, 4815} {
		request := queue.RefreshRequest{
			Args: queue.NewResolveStackMembershipArgs(
				fmt.Sprintf("pr:acme/monolith:%d", number),
			).RefreshArgs,
			Queue: queue.QueueEvent,
		}
		if err := handler.ResolveStackMembership(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	fixture.PullRequests[1].Head.SHA = ""
	fixture.PullRequests[1].Title = "poison should not apply"
	fixture.PullRequests[2].Title = "healthy sibling applied"
	fake.SetFixture(fixture)
	baseline := fake.RequestCount(http.MethodPost, "/graphql")
	type result struct {
		number int
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, number := range []int{4812, 4815} {
		go func() {
			<-start
			err := handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
			results <- result{number: number, err: err}
		}()
	}
	close(start)
	outcomes := map[int]error{}
	for range 2 {
		outcome := <-results
		outcomes[outcome.number] = outcome.err
	}
	if outcomes[4812] == nil {
		t.Fatal("poison entity unexpectedly succeeded")
	}
	if outcomes[4815] != nil {
		t.Fatalf("healthy sibling failed: %v", outcomes[4815])
	}
	if got := fake.RequestCount(
		http.MethodPost,
		"/graphql",
	) - baseline; got != 1 {
		t.Fatalf("coordinator GraphQL batches = %d, want 1", got)
	}
	healthy, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4815,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if healthy.Title != "healthy sibling applied" {
		t.Fatalf("healthy sibling row = %+v", healthy)
	}
}

func TestCoordinatorStampsEveryGangedItemEventContext(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	numbers := []int{4812, 4815, 4816}
	for _, number := range numbers {
		if err := handler.ResolveStackMembership(
			context.Background(),
			queue.RefreshRequest{
				Args: queue.NewResolveStackMembershipArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Title += " ganged latency"
			}
		}
	}
	fake.SetFixture(fixture)
	baseline := fake.RequestCount(http.MethodPost, "/graphql")
	eventContexts := make([]context.Context, len(numbers))
	results := make(chan error, len(numbers))
	start := make(chan struct{})
	for index, number := range numbers {
		eventContexts[index] = pipeline.WithEvent(
			context.Background(),
			time.Now().Add(-time.Duration(index+1)*time.Second),
		)
		ctx := eventContexts[index]
		number := number
		go func() {
			<-start
			results <- handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
		}()
	}
	close(start)
	for range numbers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	stamps := make(map[time.Time]struct{}, len(eventContexts))
	for index, eventCtx := range eventContexts {
		stamp := pipeline.CacheCommittedAt(eventCtx)
		if stamp.IsZero() {
			t.Fatalf("ganged item %d has no cache commit stamp", index)
		}
		stamps[stamp] = struct{}{}
	}
	if len(stamps) != len(eventContexts) {
		t.Fatalf(
			"ganged cache commit stamps = %d, want %d distinct item stamps",
			len(stamps),
			len(eventContexts),
		)
	}
	if got := fake.RequestCount(
		http.MethodPost,
		"/graphql",
	) - baseline; got != 1 {
		t.Fatalf("coordinator GraphQL batches = %d, want 1", got)
	}
}

func TestCoordinatorObservationsDoNotReservePoolDuringGitHubBatch(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	waiterPool := fetchPoolWithStatementTimeout(
		t,
		database.URL,
		100*time.Millisecond,
	)
	fixture := fakegithub.DefaultFixture()
	_, seedServer, seedHandler, seedRiver := newDirectHandler(
		t,
		database.Pool,
		fixture,
		5*time.Millisecond,
		100,
	)
	defer seedServer.Close()
	seedHandler.SetRiverClient(seedRiver)
	seedCoordinatorPulls(t, seedHandler, 4812, 4815)

	fastFake, fastServer, fastHandler, fastRiver := newDirectHandler(
		t,
		waiterPool,
		fixture,
		5*time.Millisecond,
		100,
	)
	defer fastServer.Close()
	fastHandler.SetRiverClient(fastRiver)

	slowGate := make(chan struct{})
	var releaseSlow sync.Once
	releaseSlowResponse := func() { releaseSlow.Do(func() { close(slowGate) }) }
	defer releaseSlowResponse()
	slowPool := fetchPoolWithMaxConns(t, database.URL, 2)
	slowFake, slowServer, slowHandler, slowRiver := newDirectHandler(
		t,
		slowPool,
		fixture,
		5*time.Millisecond,
		100,
		fakegithub.WithResponseGate(http.MethodPost, "/graphql", slowGate),
	)
	defer slowServer.Close()
	slowHandler.SetRiverClient(slowRiver)
	slowDone := make(chan error, 1)
	go func() {
		slowDone <- slowHandler.RefreshPR(t.Context(), refreshPRRequest(4812))
	}()
	waitForFakeRequest(t, slowFake, http.MethodPost, "/graphql")
	probe, err := slowPool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Release()
	assertObservationLockAvailability(
		t,
		probe,
		store.PullRequestEntityKey(1, fixture.Repository.ID, 4812),
		true,
	)
	assertObservationLockAvailability(
		t,
		probe,
		store.RepositoryEntityKey(1, fixture.Repository.ID),
		true,
	)
	controlCtx, cancelControl := context.WithTimeout(
		t.Context(), 250*time.Millisecond,
	)
	defer cancelControl()
	var controlPlaneHealthy bool
	if err := slowPool.QueryRow(controlCtx, `SELECT true`).Scan(
		&controlPlaneHealthy,
	); err != nil {
		t.Fatalf("control-plane query during slow GitHub batch: %v", err)
	}
	if !controlPlaneHealthy {
		t.Fatal("control-plane query did not complete")
	}

	fixture.PullRequests[2].Title = "fast sibling escaped repository lock"
	fastFake.SetFixture(fixture)
	fastDone := make(chan error, 1)
	go func() {
		fastDone <- fastHandler.RefreshPR(t.Context(), refreshPRRequest(4815))
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("fast batch hit repository lock wait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast batch did not complete while slow GitHub request was in flight")
	}
	select {
	case err := <-slowDone:
		t.Fatalf("slow batch completed before the fast batch proved overlap: %v", err)
	default:
	}
	releaseSlowResponse()
	if err := <-slowDone; err != nil {
		t.Fatalf("slow batch: %v", err)
	}
}

func TestCoordinatorRepositoryMetadataConvergesWhenOlderBatchLandsLast(
	t *testing.T,
) {
	t.Parallel()
	database := testdb.New(t)
	newerPool := fetchPoolWithStatementTimeout(t, database.URL, 5*time.Second)
	baseFixture := fakegithub.DefaultFixture()
	_, seedServer, seedHandler, seedRiver := newDirectHandler(
		t,
		newerPool,
		baseFixture,
		5*time.Millisecond,
		100,
	)
	defer seedServer.Close()
	seedHandler.SetRiverClient(seedRiver)
	seedCoordinatorPulls(t, seedHandler, 4812, 4815)

	olderFixture := baseFixture
	olderFixture.Repository.DefaultBranch = "legacy"
	olderFixture.Repository.UpdatedAt = baseFixture.Repository.UpdatedAt
	olderFixture.PullRequests[1].Title = "older delayed batch"
	olderGate := make(chan struct{})
	var releaseOlder sync.Once
	releaseOlderResponse := func() { releaseOlder.Do(func() { close(olderGate) }) }
	defer releaseOlderResponse()
	olderFake, olderServer, olderHandler, olderRiver := newDirectHandler(
		t,
		database.Pool,
		olderFixture,
		5*time.Millisecond,
		100,
		fakegithub.WithResponseGate(http.MethodPost, "/graphql", olderGate),
	)
	defer olderServer.Close()
	olderHandler.SetRiverClient(olderRiver)

	newerFixture := baseFixture
	newerFixture.Repository.DefaultBranch = "trunk"
	newerFixture.Repository.DefaultBranchSHA = "newer-default-head"
	newerFixture.Repository.UpdatedAt = baseFixture.Repository.UpdatedAt.Add(time.Minute)
	newerFixture.PullRequests[2].Title = "newer fast batch"
	newerFake, newerServer, newerHandler, newerRiver := newDirectHandler(
		t,
		newerPool,
		newerFixture,
		5*time.Millisecond,
		100,
	)
	defer newerServer.Close()
	newerHandler.SetRiverClient(newerRiver)

	olderDone := make(chan error, 1)
	go func() {
		olderDone <- olderHandler.RefreshPR(t.Context(), refreshPRRequest(4812))
	}()
	waitForFakeRequest(t, olderFake, http.MethodPost, "/graphql")
	if err := newerHandler.RefreshPR(t.Context(), refreshPRRequest(4815)); err != nil {
		t.Fatalf("newer batch: %v", err)
	}
	if got := newerFake.RequestCount(http.MethodPost, "/graphql"); got == 0 {
		t.Fatal("newer batch did not fetch repository metadata")
	}
	select {
	case err := <-olderDone:
		t.Fatalf("older batch applied before its response gate was released: %v", err)
	default:
	}
	releaseOlderResponse()
	if err := <-olderDone; err != nil {
		t.Fatalf("older batch: %v", err)
	}

	repository, err := store.NewEntityWriter(database.Pool).Repository(
		t.Context(),
		"acme/monolith",
	)
	if err != nil {
		t.Fatal(err)
	}
	if repository.DefaultBranch != "trunk" ||
		repository.DefaultHeadSHA != "newer-default-head" ||
		!repository.GitHubUpdatedAt.Equal(newerFixture.Repository.UpdatedAt) {
		t.Fatalf("repository metadata after older-last batches = %+v", repository)
	}
}

func TestCoordinatorDropsSupersededParentButCommitsUnrelatedDirectPR(
	t *testing.T,
) {
	t.Parallel()
	database := testdb.New(t)
	fixture := fakegithub.DefaultFixture()
	_, seedServer, seedHandler, seedRiver := newDirectHandler(
		t, database.Pool, fixture, 5*time.Millisecond, 100,
	)
	defer seedServer.Close()
	seedHandler.SetRiverClient(seedRiver)
	if err := seedHandler.RefreshRepository(t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshRepositoryHeadArgs(
			"repo:acme/monolith:metadata",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	seedCoordinatorPulls(t, seedHandler, 4812)

	fixture.PullRequests[1].Title = "unrelated direct event survives branch bulk"
	gate := make(chan struct{})
	var release sync.Once
	releaseResponse := func() { release.Do(func() { close(gate) }) }
	defer releaseResponse()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		database.Pool,
		fixture,
		5*time.Millisecond,
		100,
		fakegithub.WithResponseGate(http.MethodPost, "/graphql", gate),
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)

	done := make(chan error, 1)
	go func() {
		done <- handler.RefreshPR(t.Context(), refreshPRRequest(4812))
	}()
	waitForFakeRequest(t, fake, http.MethodPost, "/graphql")

	tx, err := database.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context()) //nolint:errcheck // rollback after commit
	if err := outbox.AcquireWriterFence(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	bulk, err := store.NewEntityWriter(database.Pool).ApplyBranchPushTx(
		t.Context(),
		tx,
		&store.BranchPushHint{
			RepoFullName: "acme/monolith", Branch: "main",
			BeforeSHA: "aaaa000", AfterSHA: "new-default-head",
			TransitionKnown: true, DeliveryGUID: "parent-fence-direct-pr",
			ReceivedAt: time.Date(2026, 8, 5, 19, 30, 0, 0, time.UTC),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bulk.Targets) != 0 || bulk.RepositoryRefreshKey == "" {
		t.Fatalf("unrelated branch bulk result = %+v", bulk)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	releaseResponse()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	var title, repositoryHead string
	if err := database.Pool.QueryRow(t.Context(), `
		SELECT pull.title, repos.head_sha
		FROM pull_requests AS pull
		JOIN repos ON repos.id = pull.repo_id
		WHERE repos.full_name = 'acme/monolith' AND pull.number = 4812
	`).Scan(&title, &repositoryHead); err != nil {
		t.Fatal(err)
	}
	if title != fixture.PullRequests[1].Title ||
		repositoryHead != "new-default-head" {
		t.Fatalf(
			"direct PR/parent after bulk = %q/%q",
			title, repositoryHead,
		)
	}
}

func TestCoordinatorIsolatesReviewThreadTransportFailure(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	targetThread := fixture.PullRequests[1].ReviewThreads[0].ID
	comments := make([]fakegithub.ReviewComment, 101)
	for index := range comments {
		comments[index] = fakegithub.ReviewComment{
			ID:          fmt.Sprintf("comment-%03d", index),
			Body:        fmt.Sprintf("comment %d", index),
			UpdatedAt:   fixture.PullRequests[1].UpdatedAt,
			AuthorLogin: "reviewer",
		}
	}
	fixture.PullRequests[1].ReviewThreads[0].Comments = comments
	var injected atomic.Int64
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/graphql" {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				var request struct {
					Query     string                     `json:"query"`
					Variables map[string]json.RawMessage `json:"variables"`
				}
				if err := json.Unmarshal(body, &request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var nodeID string
				_ = json.Unmarshal(request.Variables["id"], &nodeID)
				if strings.Contains(
					request.Query,
					"GhsyncReviewThreadCommentsPage",
				) && nodeID == targetThread {
					injected.Add(1)
					http.Error(
						w,
						"injected review-thread transport failure",
						http.StatusBadGateway,
					)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	fake, server, handler, riverClient := newDirectHandlerWithMiddleware(
		t,
		pool,
		fixture,
		50*time.Millisecond,
		100,
		middleware,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	for _, number := range []int{4812, 4815} {
		if err := handler.ResolveStackMembership(
			ctx,
			queue.RefreshRequest{
				Args: queue.NewResolveStackMembershipArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	fixture.PullRequests[1].Title = "poison transport update"
	fixture.PullRequests[2].Title = "healthy transport sibling"
	fake.SetFixture(fixture)
	type refreshResult struct {
		number int
		err    error
	}
	results := make(chan refreshResult, 2)
	start := make(chan struct{})
	for _, number := range []int{4812, 4815} {
		go func() {
			<-start
			err := handler.RefreshPR(ctx, queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			})
			results <- refreshResult{number: number, err: err}
		}()
	}
	close(start)
	outcomes := make(map[int]error, 2)
	for range 2 {
		result := <-results
		outcomes[result.number] = result.err
	}
	if outcomes[4812] == nil {
		t.Fatal("transport-poisoned PR unexpectedly succeeded")
	}
	if outcomes[4815] != nil {
		t.Fatalf("healthy sibling failed: %v", outcomes[4815])
	}
	if injected.Load() < 2 {
		t.Fatalf(
			"injected transport failures = %d, want batch and isolated attempts",
			injected.Load(),
		)
	}
	healthy, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4815,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if healthy.Title != "healthy transport sibling" {
		t.Fatalf("healthy sibling row = %+v", healthy)
	}
	poisoned, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if poisoned.Title == "poison transport update" {
		t.Fatalf("transport-poisoned row was committed: %+v", poisoned)
	}
}

func TestPullRequestStateAndFollowupGenerationsCommitAtomically(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	_, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	repository := testRepository(
		"acme/monolith",
		1001,
		fixture.Repository.UpdatedAt,
	)
	repository.NodeID = fixture.Repository.NodeID
	repository.DefaultBranch = fixture.Repository.DefaultBranch
	repository.DefaultHeadSHA = fixture.Repository.DefaultBranchSHA
	repository.ETag = `"repo"`
	repository.LastCheckedAt = time.Now()
	if _, err := handler.writer.ApplyRepository(
		context.Background(),
		repository,
		store.SyncSourceWebhook,
		repository.ETag,
		repository.LastCheckedAt,
	); err != nil {
		t.Fatal(err)
	}
	pull := pullRecordFromREST(
		&repository,
		toGHPullRequest(t, &fixture.PullRequests[1]),
		`"pull"`,
		store.SyncSourceWebhook,
		time.Now(),
	)
	apply := func() error {
		observation, err := handler.writer.BeginObservation(
			context.Background(),
			store.PullRequestEntityKey(1, 1001, pull.Number),
		)
		if err != nil {
			return err
		}
		defer observation.Close() //nolint:errcheck // deferred cleanup cannot change the primary operation result
		_, err = handler.writer.ApplyPullRequestObserved(
			context.Background(),
			observation,
			pull,
			handler.pullRequestHook(
				repository.FullName,
				queue.QueueEvent,
			),
		)
		return err
	}
	if err := apply(); err == nil ||
		!strings.Contains(err.Error(), "river client missing") {
		t.Fatalf("missing River transaction error = %v", err)
	}
	var rolledBackRows int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pull_requests
	`).Scan(&rolledBackRows); err != nil {
		t.Fatal(err)
	}
	if rolledBackRows != 0 {
		t.Fatalf("cache row committed without followups: %d", rolledBackRows)
	}
	handler.SetRiverClient(riverClient)
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	var generations, jobs int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM refresh_intent_generations
		WHERE (kind = 'refresh_checks' AND refresh_key = 'checks:acme/monolith:8f31c2d')
		   OR (kind = 'refresh_stack' AND refresh_key = 'stack:acme/monolith:142')
	`).Scan(&generations); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM river_job
		WHERE kind IN ('refresh_checks', 'refresh_stack')
		  AND args->>'key' LIKE '%acme/monolith%'
	`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if generations != 2 || jobs != 2 {
		t.Fatalf("transactional followups generations=%d jobs=%d", generations, jobs)
	}
}

func TestBatchObservationLockBlocksConcurrentWorkerAndCommitsFollowupGeneration(
	t *testing.T,
) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
		fakegithub.WithResponseDelay(200*time.Millisecond),
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	resolveRequest := queue.RefreshRequest{
		Args: queue.NewResolveStackMembershipArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	fake.ScriptNotFound(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
		1,
	)
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	tombstoned, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !tombstoned.TombstonedAt.Valid {
		t.Fatal("pre-race authoritative 404 did not tombstone PR")
	}
	baselineREST := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
	)
	refreshRequest := queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- handler.RefreshPR(ctx, refreshRequest)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for fake.RequestCount(http.MethodPost, "/graphql") == 0 &&
		time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.RequestCount(http.MethodPost, "/graphql") == 0 {
		t.Fatal("GraphQL batch did not reach paused fake")
	}
	// The fake snapshots source truth after its configured delay, so this
	// equal-updated_at/head change lands while the authoritative request is in
	// flight and must resurrect the preceding tombstone.
	fixture.PullRequests[1].Head.SHA = "equal-time-new-head"
	fixture.PullRequests[1].Title = "equal timestamp concurrent truth"
	fake.SetFixture(fixture)
	resolveDone := make(chan error, 1)
	go func() {
		resolveDone <- handler.ResolveStackMembership(ctx, resolveRequest)
	}()
	time.Sleep(50 * time.Millisecond)
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls/4812",
	); got != baselineREST {
		t.Fatalf(
			"concurrent REST worker fetched while batch held lock: %d -> %d",
			baselineREST,
			got,
		)
	}
	if err := <-refreshDone; err != nil {
		t.Fatal(err)
	}
	if err := <-resolveDone; err != nil {
		t.Fatal(err)
	}
	var generation int64
	if err := pool.QueryRow(ctx, `
		SELECT generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_checks'
		  AND refresh_key = 'checks:acme/monolith:equal-time-new-head'
	`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation < 1 {
		t.Fatalf("new-head follow-up generation = %d", generation)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.HeadSha != "equal-time-new-head" ||
		row.Title != "equal timestamp concurrent truth" {
		t.Fatalf("concurrent final PR = %+v", row)
	}
}

func TestPR404CreatesTombstoneAndEvent(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		100*time.Millisecond,
		100,
	)
	handler.SetRiverClient(riverClient)
	request := queue.RefreshRequest{
		Args: queue.NewResolveStackMembershipArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	path := "/repos/acme/monolith/pulls/4812"
	fake.ScriptNotFound(http.MethodGet, path, 1)
	if err := handler.ResolveStackMembership(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !row.TombstonedAt.Valid {
		t.Fatalf("PR was hard-deleted or left live: %+v", row)
	}
	var events int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM change_events
		WHERE kind = 'pull_request.tombstoned'
		  AND entity_key = 'pr:1:1001:4812'
	`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("tombstone events = %d, want 1", events)
	}
	if got := fake.RequestCount(http.MethodGet, path); got != 2 {
		t.Fatalf("PR fetches = %d, want initial + scripted 404", got)
	}
	server.Close()
}

func TestRepositoryRulesLockedCASDirtyEventAndConditionalRecheck(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	stackRequest := queue.RefreshRequest{
		Args: queue.NewRefreshStackArgs(
			"stack:acme/monolith:142",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshStack(ctx, stackRequest); err != nil {
		t.Fatal(err)
	}
	rulesRequest := queue.RefreshRequest{
		Args: queue.NewRefreshRepoRulesArgs(
			"repo_rules:acme/monolith:rules",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshRepoRules(ctx, rulesRequest); err != nil {
		t.Fatal(err)
	}
	var ruleCount, eventCount int
	var firstChecked time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*), max(last_checked_at)
		FROM repo_rules
	`).Scan(&ruleCount, &firstChecked); err != nil {
		t.Fatal(err)
	}
	if ruleCount != 1 {
		t.Fatalf("repository rules = %d, want 1", ruleCount)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM change_events
		WHERE kind = 'repo_rules.changed'
		  AND entity_key = 'repo_rules:1:1001'
	`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("repository rule events = %d, want 1", eventCount)
	}
	if err := handler.RefreshRepoRules(ctx, rulesRequest); err != nil {
		t.Fatal(err)
	}
	var secondChecked time.Time
	if err := pool.QueryRow(ctx, `
		SELECT max(last_checked_at)
		FROM repo_rules
	`).Scan(&secondChecked); err != nil {
		t.Fatal(err)
	}
	if secondChecked.Before(firstChecked) {
		t.Fatalf(
			"conditional recheck moved checked_at backwards: %s -> %s",
			firstChecked,
			secondChecked,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'repo_rules.changed'
	`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("304 emitted repository rule event; count = %d", eventCount)
	}
	var dirty int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM derivation_dirty
		WHERE scope_key = 'stack:1:1001:142'
	`).Scan(&dirty); err != nil {
		t.Fatal(err)
	}
	if dirty != 1 {
		t.Fatalf("repository rule dirty scope count = %d, want 1", dirty)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/rulesets",
	); got != 2 {
		t.Fatalf("repository rules fetches = %d, want 2", got)
	}
}

func TestPullRequestRefreshMirrorsFencedFilesAndCodeowners(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	_, server, handler, riverClient := newDirectHandler(
		t, pool, fixture, 5*time.Millisecond, 100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	request := queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshPR(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var baseSHA, headSHA, sourcePath, sourceState string
	var total int
	var truncated bool
	if err := pool.QueryRow(context.Background(), `
		SELECT base_sha, head_sha, files_total_count, files_truncated,
		       codeowners_path, codeowners_state
		FROM pull_request_change_snapshots
		WHERE pr_number = 4812 AND tombstoned_at IS NULL
	`).Scan(
		&baseSHA, &headSHA, &total, &truncated, &sourcePath, &sourceState,
	); err != nil {
		t.Fatal(err)
	}
	if baseSHA != "bbbb001" || headSHA != "8f31c2d" || total != 2 ||
		truncated || sourcePath != ".github/CODEOWNERS" ||
		sourceState != "present" {
		t.Fatalf(
			"change snapshot = %q/%q total=%d truncated=%v source=%q/%q",
			baseSHA, headSHA, total, truncated, sourcePath, sourceState,
		)
	}
	var previous string
	if err := pool.QueryRow(context.Background(), `
		SELECT previous_path
		FROM pull_request_changed_files
		WHERE pr_number = 4812 AND path = 'docs/ranking.md'
		  AND tombstoned_at IS NULL
	`).Scan(&previous); err != nil {
		t.Fatal(err)
	}
	if previous != "docs/search.md" {
		t.Fatalf("rename previous path = %q", previous)
	}
	type ownerState struct {
		token string
		kind  string
		state string
		id    string
	}
	rows, err := pool.Query(context.Background(), `
		SELECT owner_token, owner_type, resolution_state,
		       COALESCE(owner_node_id, '')
		FROM pull_request_file_owners
		WHERE pr_number = 4812 AND tombstoned_at IS NULL
		ORDER BY path, owner_token
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var owners []ownerState
	for rows.Next() {
		var owner ownerState
		if err := rows.Scan(
			&owner.token, &owner.kind, &owner.state, &owner.id,
		); err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []ownerState{
		{token: "docs@example.com", kind: "email", state: "unresolved"},
		{token: "malformed-owner", kind: "malformed", state: "unresolved"},
		{
			token: "@acme/search-platform", kind: "team", state: "resolved",
			id: "T_kwDOABCDEF6001",
		},
		{token: "@unknown-user", kind: "user", state: "unresolved"},
	}
	if !reflect.DeepEqual(owners, want) {
		t.Fatalf("file owners = %#v, want %#v", owners, want)
	}
}

func TestPullRequestSweepReusesMirroredCodeownersAndInvalidationFetchesOnce(
	t *testing.T,
) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	numbers := []int{4812, 4815, 4816}
	const (
		baseSHA = "codeowners-base-one"
		newSHA  = "codeowners-base-two"
		source  = "* @reviewer\ninternal/** @acme/search-platform @unknown-user\n"
	)
	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Base = fakegithub.Base{
					Ref: "main", SHA: baseSHA,
				}
			}
		}
	}
	fixture.Contents = map[string]map[string]string{
		baseSHA: {"CODEOWNERS": source},
		newSHA:  {"CODEOWNERS": source},
	}

	seedFake, seedServer, seedHandler, seedRiver := newDirectHandler(
		t, pool, fixture, 50*time.Millisecond, 100,
	)
	defer seedServer.Close()
	seedHandler.SetRiverClient(seedRiver)
	seedCoordinatorPulls(t, seedHandler, numbers...)
	refreshCodeownersPulls(t, seedHandler, numbers, queue.QueueEvent)
	githubPath := "/repos/acme/monolith/contents/.github/CODEOWNERS"
	rootPath := "/repos/acme/monolith/contents/CODEOWNERS"
	if got := seedFake.RequestCount(http.MethodGet, githubPath); got != 1 {
		t.Fatalf("cold .github/CODEOWNERS probes = %d, want 1", got)
	}
	if got := seedFake.RequestCount(http.MethodGet, rootPath); got != 1 {
		t.Fatalf("cold root CODEOWNERS probes = %d, want 1", got)
	}
	before := readCodeownersResults(t, pool, numbers)
	databaseNumbers := codeownersDatabaseNumbers(numbers)
	var mirroredETags int
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM pull_request_change_snapshots
		WHERE pr_number = ANY($1::integer[])
		  AND codeowners_ref = 'main'
		  AND codeowners_sha = $2
		  AND codeowners_path = 'CODEOWNERS'
		  AND etag <> ''
		  AND tombstoned_at IS NULL
	`, databaseNumbers, baseSHA).Scan(&mirroredETags); err != nil {
		t.Fatal(err)
	}
	if mirroredETags != len(numbers) {
		t.Fatalf("durable CODEOWNERS ETags = %d, want %d", mirroredETags, len(numbers))
	}

	reuseFake, reuseServer, reuseHandler, reuseRiver := newDirectHandler(
		t, pool, fixture, 50*time.Millisecond, 100,
	)
	defer reuseServer.Close()
	reuseHandler.SetRiverClient(reuseRiver)
	refreshCodeownersPulls(t, reuseHandler, numbers, queue.QueueSweep)
	for _, request := range reuseFake.Requests() {
		if strings.Contains(request.Path, "/contents/") {
			t.Fatalf("unchanged sweep fetched CODEOWNERS: %+v", request)
		}
	}

	const newHeadSHA = "codeowners-head-two"
	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Head.SHA = newHeadSHA
			}
		}
	}
	reuseFake.SetFixture(fixture)
	baseline := len(reuseFake.Requests())
	refreshCodeownersPulls(t, reuseHandler, numbers, queue.QueueEvent)
	for _, request := range reuseFake.Requests()[baseline:] {
		if strings.Contains(request.Path, "/contents/") {
			t.Fatalf("head-only change fetched CODEOWNERS: %+v", request)
		}
	}

	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Base.Ref = "release"
			}
		}
	}
	reuseFake.SetFixture(fixture)
	baseline = len(reuseFake.Requests())
	refreshCodeownersPulls(t, reuseHandler, numbers, queue.QueueEvent)
	var refChangeContents int
	for _, request := range reuseFake.Requests()[baseline:] {
		if strings.Contains(request.Path, "/contents/") {
			refChangeContents++
		}
	}
	if refChangeContents != 2 {
		t.Fatalf("base-ref change CODEOWNERS probes = %d, want 2", refChangeContents)
	}

	for index := range fixture.PullRequests {
		for _, number := range numbers {
			if fixture.PullRequests[index].Number == number {
				fixture.PullRequests[index].Base.SHA = newSHA
			}
		}
	}
	reuseFake.SetFixture(fixture)
	baseline = len(reuseFake.Requests())
	refreshCodeownersPulls(t, reuseHandler, numbers, queue.QueueEvent)
	requests := reuseFake.Requests()[baseline:]
	var githubRequests, rootRequests int
	for _, request := range requests {
		switch request.Path {
		case githubPath:
			githubRequests++
		case rootPath:
			rootRequests++
			if request.IfNoneMatch == "" {
				t.Fatalf("invalidated root CODEOWNERS request was not conditional: %+v", request)
			}
			if request.RawQuery != "ref="+newSHA {
				t.Fatalf("invalidated CODEOWNERS ref query = %q", request.RawQuery)
			}
		}
	}
	if githubRequests != 1 || rootRequests != 1 {
		t.Fatalf(
			"invalidated CODEOWNERS probes .github/root = %d/%d, want 1/1",
			githubRequests,
			rootRequests,
		)
	}
	if got := reuseFake.NotModifiedCount(http.MethodGet, rootPath); got != 1 {
		t.Fatalf("conditional CODEOWNERS 304s = %d, want 1", got)
	}
	after := readCodeownersResults(t, pool, numbers)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("ownership results changed after provenance refresh:\nbefore=%v\nafter=%v", before, after)
	}

	if _, err := pool.Exec(t.Context(), `
		UPDATE pull_request_change_snapshots
		SET tombstoned_at = clock_timestamp()
		WHERE pr_number = $1
	`, numbers[0]); err != nil {
		t.Fatal(err)
	}
	missingFake, missingServer, missingHandler, missingRiver := newDirectHandler(
		t, pool, fixture, 50*time.Millisecond, 100,
	)
	defer missingServer.Close()
	missingHandler.SetRiverClient(missingRiver)
	if err := missingHandler.RefreshPR(t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			fmt.Sprintf("pr:acme/monolith:%d", numbers[0]),
		).RefreshArgs,
		Queue: queue.QueueSweep,
	}); err != nil {
		t.Fatal(err)
	}
	var missingRowContents int
	for _, request := range missingFake.Requests() {
		if strings.Contains(request.Path, "/contents/") {
			missingRowContents++
		}
	}
	if missingRowContents != 2 {
		t.Fatalf("missing mirror row CODEOWNERS probes = %d, want 2", missingRowContents)
	}
}

func TestPullRequestSweepReusesMissingAndOversizedCodeownersStates(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		contents map[string]string
		state    string
	}{
		{name: "missing", state: gh.CodeownersMissing},
		{
			name: "oversized",
			contents: map[string]string{
				".github/CODEOWNERS": strings.Repeat("x", gh.MaxCodeownersBytes),
			},
			state: gh.CodeownersOversized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pool := fetchTestDatabase(t)
			fixture := fakegithub.DefaultFixture()
			pull := &fixture.PullRequests[1]
			fixture.PullRequests = []fakegithub.PullRequest{*pull}
			fixture.Contents = map[string]map[string]string{
				pull.Base.SHA: test.contents,
			}
			_, seedServer, seedHandler, seedRiver := newDirectHandler(
				t, pool, fixture, 5*time.Millisecond, 100,
			)
			defer seedServer.Close()
			seedHandler.SetRiverClient(seedRiver)
			if err := seedHandler.RefreshPR(t.Context(), queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			}); err != nil {
				t.Fatal(err)
			}
			var state string
			if err := pool.QueryRow(t.Context(), `
				SELECT codeowners_state
				FROM pull_request_change_snapshots
				WHERE pr_number = $1 AND tombstoned_at IS NULL
			`, pull.Number).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != test.state {
				t.Fatalf("mirrored state = %q, want %q", state, test.state)
			}

			reuseFake, reuseServer, reuseHandler, reuseRiver := newDirectHandler(
				t, pool, fixture, 5*time.Millisecond, 100,
			)
			defer reuseServer.Close()
			reuseHandler.SetRiverClient(reuseRiver)
			if err := reuseHandler.RefreshPR(t.Context(), queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
				).RefreshArgs,
				Queue: queue.QueueSweep,
			}); err != nil {
				t.Fatal(err)
			}
			for _, request := range reuseFake.Requests() {
				if strings.Contains(request.Path, "/contents/") {
					t.Fatalf("exact %s mirror state fetched CODEOWNERS: %+v", test.state, request)
				}
			}
		})
	}
}

func TestPullRequestCodeownersUsesExactSHAWithoutBaseRefName(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	pull := &fixture.PullRequests[1]
	pull.Base.Ref = ""
	fixture.PullRequests = []fakegithub.PullRequest{*pull}
	fake, server, handler, riverClient := newDirectHandler(
		t, pool, fixture, 5*time.Millisecond, 100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	if err := handler.RefreshPR(t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			fmt.Sprintf("pr:acme/monolith:%d", pull.Number),
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	var ref, sha, state string
	if err := pool.QueryRow(t.Context(), `
		SELECT codeowners_ref, codeowners_sha, codeowners_state
		FROM pull_request_change_snapshots
		WHERE pr_number = $1 AND tombstoned_at IS NULL
	`, pull.Number).Scan(&ref, &sha, &state); err != nil {
		t.Fatal(err)
	}
	if ref != "" || sha != pull.Base.SHA || state != gh.CodeownersPresent {
		t.Fatalf("CODEOWNERS provenance = %q/%q/%q", ref, sha, state)
	}
	for _, request := range fake.Requests() {
		if strings.Contains(request.Path, "/contents/") &&
			request.RawQuery != "ref="+pull.Base.SHA {
			t.Fatalf("CODEOWNERS request did not use exact SHA: %+v", request)
		}
	}
}

func refreshCodeownersPulls(
	t *testing.T,
	handler *Handler,
	numbers []int,
	queueName string,
) {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, len(numbers))
	for _, number := range numbers {
		go func() {
			<-start
			results <- handler.RefreshPR(t.Context(), queue.RefreshRequest{
				Args: queue.NewRefreshPRArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queueName,
			})
		}()
	}
	close(start)
	for range numbers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func readCodeownersResults(
	t *testing.T,
	pool *pgxpool.Pool,
	numbers []int,
) []string {
	t.Helper()
	databaseNumbers := codeownersDatabaseNumbers(numbers)
	rows, err := pool.Query(t.Context(), `
		SELECT snapshot.pr_number, snapshot.codeowners_path,
		       snapshot.codeowners_state, snapshot.codeowners_hash,
		       file.path, COALESCE(owner.owner_token, ''),
		       COALESCE(owner.owner_type, ''),
		       COALESCE(owner.resolution_state, ''),
		       COALESCE(owner.source_pattern, ''),
		       COALESCE(owner.source_line, 0)
		FROM pull_request_change_snapshots AS snapshot
		JOIN pull_request_changed_files AS file
		  ON file.repo_id = snapshot.repo_id
		 AND file.pr_number = snapshot.pr_number
		 AND file.tombstoned_at IS NULL
		LEFT JOIN pull_request_file_owners AS owner
		  ON owner.repo_id = file.repo_id
		 AND owner.pr_number = file.pr_number
		 AND owner.path = file.path
		 AND owner.tombstoned_at IS NULL
		WHERE snapshot.pr_number = ANY($1::integer[])
		  AND snapshot.tombstoned_at IS NULL
		ORDER BY snapshot.pr_number, file.path, owner.owner_token
	`, databaseNumbers)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var results []string
	for rows.Next() {
		var (
			number, line                         int
			path, state, hash, file, token, kind string
			resolution, pattern                  string
		)
		if err := rows.Scan(
			&number,
			&path,
			&state,
			&hash,
			&file,
			&token,
			&kind,
			&resolution,
			&pattern,
			&line,
		); err != nil {
			t.Fatal(err)
		}
		results = append(results, fmt.Sprintf(
			"%d:%s:%s:%s:%s:%s:%s:%s:%s:%d",
			number,
			path,
			state,
			hash,
			file,
			token,
			kind,
			resolution,
			pattern,
			line,
		))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return results
}

func codeownersDatabaseNumbers(numbers []int) []int32 {
	result := make([]int32, len(numbers))
	for index, number := range numbers {
		result[index] = int32(number)
	}
	return result
}

func TestPullRequestRefreshResolvesRenameByNewPathAndOwnerCase(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	contents, ok := fixture.Contents["bbbb001"]
	if !ok {
		t.Fatal("fixture has no CODEOWNERS content at rename base")
	}
	contents[".github/CODEOWNERS"] =
		"* @Reviewer\n" +
			"internal/ranker.go\n" +
			"docs/search.md @old-owner\n" +
			"docs/ranking.md @Reviewer\n"
	_, server, handler, riverClient := newDirectHandler(
		t, pool, fixture, 5*time.Millisecond, 100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	request := queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshPR(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var token, state, login, pattern string
	if err := pool.QueryRow(context.Background(), `
		SELECT owner_token, resolution_state, owner_login, source_pattern
		FROM pull_request_file_owners
		WHERE pr_number = 4812
		  AND path = 'docs/ranking.md'
		  AND tombstoned_at IS NULL
	`).Scan(&token, &state, &login, &pattern); err != nil {
		t.Fatal(err)
	}
	if token != "@Reviewer" || state != "resolved" || login != "reviewer" ||
		pattern != "docs/ranking.md" {
		t.Fatalf(
			"renamed-path owner = %q/%q/%q via %q",
			token, state, login, pattern,
		)
	}
	var oldPathOwners int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM pull_request_file_owners
		WHERE pr_number = 4812
		  AND owner_token = '@old-owner'
		  AND tombstoned_at IS NULL
	`).Scan(&oldPathOwners); err != nil {
		t.Fatal(err)
	}
	if oldPathOwners != 0 {
		t.Fatalf("rename resolved %d owners from previous_path", oldPathOwners)
	}
	var clearedOwners int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM pull_request_file_owners
		WHERE pr_number = 4812
		  AND path = 'internal/ranker.go'
		  AND tombstoned_at IS NULL
	`).Scan(&clearedOwners); err != nil {
		t.Fatal(err)
	}
	if clearedOwners != 0 {
		t.Fatalf("ownerless later rule retained %d owners", clearedOwners)
	}
}

func TestPullRequestRefreshRejectsSynchronizeDuringHydration(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	path := "/repos/acme/monolith/pulls/4812"
	_, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		5*time.Millisecond,
		100,
		fakegithub.WithRequestHook(func(method, requestPath string, count int, fx *fakegithub.Fixture) {
			if method != "GET" || requestPath != path || count != 2 {
				return
			}
			fx.PullRequests[1].Head.SHA = "synchronized-head"
			fx.PullRequests[1].ChangedFiles = []fakegithub.ChangedFile{{
				Path: "new/from-synchronize.go", ChangeType: "added",
			}}
		}),
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	err := handler.RefreshPR(context.Background(), queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			"pr:acme/monolith:4812",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	})
	if err == nil || !strings.Contains(err.Error(), "base/head changed") {
		t.Fatalf("synchronize race error = %v", err)
	}
	var snapshots int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pull_request_change_snapshots
	`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if snapshots != 0 {
		t.Fatalf("synchronize race persisted %d snapshots", snapshots)
	}
}

func TestRefreshRESTETagsProduceDominant304Share(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	prKey := "pr:acme/monolith:4812"
	resolveRequest := queue.RefreshRequest{
		Args:  queue.NewResolveStackMembershipArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	refreshRequest := queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	beforeGraphQL, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if beforeGraphQL.Etag == "" {
		t.Fatal("REST PR refresh stored an empty ETag")
	}
	if err := handler.RefreshPR(ctx, refreshRequest); err != nil {
		t.Fatal(err)
	}
	afterGraphQL, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterGraphQL.Etag != beforeGraphQL.Etag {
		t.Fatalf(
			"GraphQL gang changed PR ETag %q -> %q",
			beforeGraphQL.Etag,
			afterGraphQL.Etag,
		)
	}
	confirmedBefore := afterGraphQL.LastCheckedAt.Time.Add(-time.Hour)
	if _, err := pool.Exec(ctx, `
		UPDATE pull_requests
		SET last_checked_at = $1
		WHERE number = 4812
	`, confirmedBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE pull_request_change_snapshots
		SET last_checked_at = $1
		WHERE pr_number = 4812
		  AND tombstoned_at IS NULL
	`, confirmedBefore); err != nil {
		t.Fatal(err)
	}
	var changeCheckedBefore time.Time
	if err := pool.QueryRow(ctx, `
		SELECT last_checked_at
		FROM pull_request_change_snapshots
		WHERE pr_number = 4812
		  AND tombstoned_at IS NULL
	`).Scan(&changeCheckedBefore); err != nil {
		t.Fatal(err)
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	afterMetadata304, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var changeCheckedAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT last_checked_at
		FROM pull_request_change_snapshots
		WHERE pr_number = 4812
		  AND tombstoned_at IS NULL
	`).Scan(&changeCheckedAfter); err != nil {
		t.Fatal(err)
	}
	if !afterMetadata304.LastCheckedAt.Time.After(
		confirmedBefore,
	) || !afterMetadata304.SyncedAt.Time.Equal(afterGraphQL.SyncedAt.Time) ||
		!changeCheckedAfter.Equal(changeCheckedBefore) {
		t.Fatalf(
			"metadata 304 provenance parent checked=%s->%s synced=%s->%s change_checked=%s->%s",
			confirmedBefore,
			afterMetadata304.LastCheckedAt.Time,
			afterGraphQL.SyncedAt.Time,
			afterMetadata304.SyncedAt.Time,
			changeCheckedBefore,
			changeCheckedAfter,
		)
	}
	prPath := "/repos/acme/monolith/pulls/4812"
	if got := fake.NotModifiedCount(http.MethodGet, prPath); got != 2 {
		t.Fatalf("conditional PR 304s = %d, want 2", got)
	}
	for range 2 {
		if err := handler.RefreshPR(ctx, refreshRequest); err != nil {
			t.Fatal(err)
		}
	}
	filesPath := "/repos/acme/monolith/pulls/4812/files"
	if got := fake.NotModifiedCount(http.MethodGet, prPath); got != 4 {
		t.Fatalf("conditional PR metadata 304s = %d, want 4", got)
	}
	if got := fake.NotModifiedCount(http.MethodGet, filesPath); got != 2 {
		t.Fatalf("conditional PR files 304s = %d, want 2", got)
	}
	var filesETag, codeownersETag, previousPath string
	if err := pool.QueryRow(ctx, `
		SELECT snapshot.files_etag, snapshot.etag, file.previous_path
		FROM pull_request_change_snapshots AS snapshot
		JOIN pull_request_changed_files AS file
		  ON file.repo_id = snapshot.repo_id
		 AND file.pr_number = snapshot.pr_number
		WHERE snapshot.pr_number = 4812
		  AND file.path = 'docs/ranking.md'
	`).Scan(&filesETag, &codeownersETag, &previousPath); err != nil {
		t.Fatal(err)
	}
	if filesETag == "" || codeownersETag == "" ||
		filesETag == codeownersETag || previousPath != "docs/search.md" {
		t.Fatalf(
			"cached files/CODEOWNERS validators and rename = %q/%q/%q",
			filesETag,
			codeownersETag,
			previousPath,
		)
	}

	checksKey := "checks:acme/monolith:8f31c2d"
	checksRequest := queue.RefreshRequest{
		Args:  queue.NewRefreshChecksArgs(checksKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshChecks(ctx, checksRequest); err != nil {
		t.Fatal(err)
	}
	var runCount, emptyETags, historyBefore, eventsBefore int
	var syncedBefore, checkedBefore time.Time
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE etag = ''),
		       max(synced_at),
		       max(last_checked_at)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(
		&runCount,
		&emptyETags,
		&syncedBefore,
		&checkedBefore,
	); err != nil {
		t.Fatal(err)
	}
	if runCount != 2 || emptyETags != 0 {
		t.Fatalf(
			"stored checks rows=%d empty_etags=%d, want 2/0",
			runCount,
			emptyETags,
		)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&historyBefore); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:1001:8f31c2d'
	`).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	if err := handler.RefreshChecks(ctx, checksRequest); err != nil {
		t.Fatal(err)
	}
	checksPath := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	if err := handler.RefreshChecks(ctx, checksRequest); err != nil {
		t.Fatal(err)
	}
	if got := fake.NotModifiedCount(http.MethodGet, checksPath); got != 2 {
		t.Fatalf("conditional checks 304s = %d, want 2", got)
	}
	var historyAfter, eventsAfter, tombstoned int
	var syncedAfter, checkedAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT max(synced_at),
		       max(last_checked_at),
		       count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&syncedAfter, &checkedAfter, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&historyAfter); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM change_events
		WHERE kind = 'checks.changed'
		  AND entity_key = 'checks:1:1001:8f31c2d'
	`).Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if !syncedAfter.Equal(syncedBefore) ||
		!checkedAfter.After(checkedBefore) ||
		historyAfter != historyBefore ||
		eventsAfter != eventsBefore ||
		tombstoned != 0 {
		t.Fatalf(
			"checks 304 synced=%s->%s checked=%s->%s history=%d->%d events=%d->%d tombstoned=%d",
			syncedBefore,
			syncedAfter,
			checkedBefore,
			checkedAfter,
			historyBefore,
			historyAfter,
			eventsBefore,
			eventsAfter,
			tombstoned,
		)
	}
	refreshRequests := fake.RequestCount(http.MethodGet, prPath) +
		fake.RequestCount(http.MethodGet, filesPath) +
		fake.RequestCount(http.MethodGet, checksPath)
	notModified := fake.NotModifiedCount(http.MethodGet, prPath) +
		fake.NotModifiedCount(http.MethodGet, filesPath) +
		fake.NotModifiedCount(http.MethodGet, checksPath)
	if notModified*2 <= refreshRequests {
		t.Fatalf(
			"refresh REST 304 share = %d/%d, want dominant",
			notModified,
			refreshRequests,
		)
	}
}

func TestStalePR304CannotConfirmLaterGraphQLVersion(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	prKey := "pr:acme/monolith:4812"
	resolveRequest := queue.RefreshRequest{
		Args:  queue.NewResolveStackMembershipArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	responseA, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if responseA.Etag == "" {
		t.Fatal("response A did not persist a PR metadata validator")
	}

	// GraphQL observation B advances the parent version. It must invalidate
	// response A's validator association instead of copying that ETag onto B.
	for index := range fixture.PullRequests {
		if fixture.PullRequests[index].Number != 4812 {
			continue
		}
		fixture.PullRequests[index].Title = "GraphQL response B"
	}
	fake.SetFixture(fixture)
	if err := handler.RefreshPR(ctx, queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	responseB, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if responseB.Title != "GraphQL response B" || responseB.Etag == "" ||
		responseB.Etag == responseA.Etag {
		t.Fatalf(
			"response B title/etag = %q/%q, want changed state with its own validator",
			responseB.Title,
			responseB.Etag,
		)
	}

	// Model response A's 304 reaching the writer after B. The validator CAS
	// must reject the confirmation and leave both domain and freshness
	// provenance on B.
	observation, err := handler.writer.BeginObservation(
		ctx,
		store.PullRequestEntityKey(1, fixture.Repository.ID, 4812),
	)
	if err != nil {
		t.Fatal(err)
	}
	confirmed, touchErr := handler.writer.TouchPullRequest(
		ctx,
		observation,
		store.RepositoryRecord{
			InstallationID: 1,
			GitHubID:       fixture.Repository.ID,
		},
		4812,
		time.Now().Add(time.Hour),
		responseA.Etag,
	)
	if closeErr := observation.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if touchErr != nil {
		t.Fatal(touchErr)
	}
	if confirmed {
		t.Fatal("response A's 304 confirmed response B")
	}
	after, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     4812,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if after.Title != responseB.Title || after.Etag != responseB.Etag ||
		!after.SyncedAt.Time.Equal(responseB.SyncedAt.Time) ||
		!after.LastCheckedAt.Time.Equal(responseB.LastCheckedAt.Time) {
		t.Fatalf(
			"stale 304 changed response B: before=%+v after=%+v",
			responseB,
			after,
		)
	}
}

func TestGraphQLFencePairsNewValidatorWithRESTStackMembership(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	const number = 4812
	prKey := fmt.Sprintf("pr:acme/monolith:%d", number)
	resolveRequest := queue.RefreshRequest{
		Args:  queue.NewResolveStackMembershipArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	refreshRequest := queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(prKey).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}

	target := -1
	var finalStack fakegithub.StackRef
	for index := range fixture.PullRequests {
		if fixture.PullRequests[index].Number != number {
			continue
		}
		target = index
		finalStack = *fixture.PullRequests[index].Stack
		fixture.PullRequests[index].Title = "intermediate unstacked version"
		fixture.PullRequests[index].Stack = nil
		break
	}
	if target < 0 {
		t.Fatalf("fixture has no PR %d", number)
	}
	fake.SetFixture(fixture)
	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	intermediate, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     number,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if intermediate.StackNumber.Valid || intermediate.StackPosition.Valid {
		t.Fatalf("intermediate membership = %+v", intermediate)
	}

	fixture.PullRequests[target].Title = "final restacked version"
	fixture.PullRequests[target].Stack = &finalStack
	fake.SetFixture(fixture)
	if err := handler.RefreshPR(ctx, refreshRequest); err != nil {
		t.Fatal(err)
	}
	final, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     number,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if final.Title != "final restacked version" ||
		!final.StackNumber.Valid ||
		int(final.StackNumber.Int32) != finalStack.Number ||
		!final.StackPosition.Valid ||
		int(final.StackPosition.Int32) != finalStack.Position ||
		final.Etag == "" || final.Etag == intermediate.Etag {
		t.Fatalf(
			"final title/membership/etag = %q/%v/%v/%q",
			final.Title,
			final.StackNumber,
			final.StackPosition,
			final.Etag,
		)
	}

	if err := handler.ResolveStackMembership(ctx, resolveRequest); err != nil {
		t.Fatal(err)
	}
	prPath := fmt.Sprintf("/repos/acme/monolith/pulls/%d", number)
	if got := fake.NotModifiedCount(http.MethodGet, prPath); got != 1 {
		t.Fatalf("final validator 304s = %d, want 1", got)
	}
	after304, err := dbgen.New(pool).GetPullRequestByKey(
		ctx,
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: "acme/monolith",
			PrNumber:     number,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if after304.StackNumber != final.StackNumber ||
		after304.StackPosition != final.StackPosition ||
		after304.Etag != final.Etag {
		t.Fatalf("304 changed validator-associated membership: %+v", after304)
	}
}

func TestChecksMidPagination404DoesNotReplaceOrTombstone(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	template := fixture.CheckRuns[0]
	fixture.CheckRuns = make([]fakegithub.CheckRun, 101)
	for index := range fixture.CheckRuns {
		run := template
		run.ID = int64(100_000 + index)
		run.NodeID = fmt.Sprintf("CR_page_%03d", index)
		run.Name = fmt.Sprintf("check-%03d", index)
		fixture.CheckRuns[index] = run
	}
	fake, server, handler, _ := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	ctx := context.Background()
	request := queue.RefreshRequest{
		Args: queue.NewRefreshChecksArgs(
			"checks:acme/monolith:8f31c2d",
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.RefreshChecks(ctx, request); err != nil {
		t.Fatal(err)
	}
	path := "/repos/acme/monolith/commits/8f31c2d/check-runs"
	baseline := fake.RequestCount(http.MethodGet, path)
	if baseline != 2 {
		t.Fatalf("initial checks pages = %d, want 2", baseline)
	}
	fixture.CheckRuns[0].Status = "in_progress"
	fixture.CheckRuns[0].Conclusion = ""
	fixture.CheckRuns[0].CompletedAt = nil
	fake.SetFixture(fixture)
	fake.ScriptNotFoundOnRequest(
		http.MethodGet,
		path,
		baseline+2,
	)
	err := handler.RefreshChecks(ctx, request)
	if err == nil ||
		!strings.Contains(err.Error(), "page 2") ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("mid-pagination 404 error = %v", err)
	}
	var total, live, tombstoned, history int
	var firstStatus string
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE tombstoned_at IS NULL),
		       count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&total, &live, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT status FROM check_runs WHERE gh_id = 100000
	`).Scan(&firstStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM check_history WHERE head_sha = '8f31c2d'
	`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if total != 101 ||
		live != 101 ||
		tombstoned != 0 ||
		firstStatus != "completed" ||
		history != 101 {
		t.Fatalf(
			"post-404 checks total/live/tombstoned=%d/%d/%d first_status=%q history=%d",
			total,
			live,
			tombstoned,
			firstStatus,
			history,
		)
	}

	fake.ScriptNotFoundOnRequest(
		http.MethodGet,
		path,
		fake.RequestCount(http.MethodGet, path)+1,
	)
	if err := handler.RefreshChecks(ctx, request); err != nil {
		t.Fatalf("entry-listing 404: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE tombstoned_at IS NOT NULL)
		FROM check_runs
		WHERE head_sha = '8f31c2d'
	`).Scan(&tombstoned); err != nil {
		t.Fatal(err)
	}
	if tombstoned != 101 {
		t.Fatalf(
			"entry-listing 404 tombstoned %d checks, want 101",
			tombstoned,
		)
	}
}

func TestBackfillResumesFromDurableCursor(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		100*time.Millisecond,
		2,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := t.Context()
	if err := riverClient.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	cursor, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Phase != "repository" {
		t.Fatalf("initial cursor = %+v", cursor)
	}
	deadline := time.Now().Add(20 * time.Second)
	var completed dbgen.BackfillCursor
	for time.Now().Before(deadline) {
		completed, err = dbgen.New(pool).GetBackfillCursor(
			ctx,
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if completed.Phase != "done" || !completed.CompletedAt.Valid {
		t.Fatalf("backfill did not complete after children: %+v", completed)
	}
	resumed, err := StartBackfill(ctx, pool, riverClient, 1, "acme/monolith")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Phase != "done" || !resumed.CompletedAt.Valid {
		t.Fatalf("resume cursor = %+v, want completed", resumed)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith",
	); got != 1 {
		t.Fatalf("repository fetches = %d, want 1 across restart", got)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/stacks",
	); got != 1 {
		t.Fatalf("stack list fetches = %d, want 1", got)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf(
			"PR list fetches = %d, want two durable overlap passes",
			got,
		)
	}
	var refreshes int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM (
			SELECT DISTINCT kind, args->>'key'
			FROM river_job
			WHERE kind IN ('refresh_pr', 'refresh_stack')
			  AND args->>'key' LIKE '%:acme/monolith:%'
		) AS distinct_refreshes
	`).Scan(&refreshes); err != nil {
		t.Fatal(err)
	}
	if refreshes != 5 {
		t.Fatalf("backfill refresh jobs = %d, want 1 stack + 4 open PRs", refreshes)
	}
	var backfilledPRs, emptyPRETags int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE pull_requests.etag = '')
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.full_name = 'acme/monolith'
	`).Scan(&backfilledPRs, &emptyPRETags); err != nil {
		t.Fatal(err)
	}
	if backfilledPRs != 5 || emptyPRETags != 0 {
		t.Fatalf(
			"backfilled PR rows/empty ETags = %d/%d, want 5/0",
			backfilledPRs,
			emptyPRETags,
		)
	}
	requests := readCachedReviewRequests(t, pool, "acme/monolith", 4812)
	if len(requests) != 2 || requests[0].Kind != "team" ||
		requests[1].Kind != "user" {
		t.Fatalf("backfilled review requests = %+v", requests)
	}
	var reviews, comments, deletedAuthors, commentBodies int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM pull_request_reviews AS review
		   JOIN repos ON repos.id = review.repo_id
		   WHERE repos.full_name = 'acme/monolith'
		     AND review.pr_number = 4812
		     AND review.tombstoned_at IS NULL),
		  (SELECT count(*) FROM pull_request_comments AS comment
		   JOIN repos ON repos.id = comment.repo_id
		   WHERE repos.full_name = 'acme/monolith'
		     AND comment.pr_number = 4812
		     AND comment.tombstoned_at IS NULL),
		  (SELECT count(*) FROM pull_request_comments AS comment
		   JOIN repos ON repos.id = comment.repo_id
		   WHERE repos.full_name = 'acme/monolith'
		     AND comment.author_kind = 'deleted'
		     AND comment.author_node_id IS NULL
		     AND comment.author_login IS NULL),
		  (SELECT count(*) FROM information_schema.columns
		   WHERE table_schema = current_schema()
		     AND table_name = 'pull_request_comments'
		     AND column_name IN ('body', 'body_text'))
	`).Scan(&reviews, &comments, &deletedAuthors, &commentBodies); err != nil {
		t.Fatal(err)
	}
	if reviews != 2 || comments != 2 || deletedAuthors != 1 ||
		commentBodies != 0 {
		t.Fatalf(
			"backfilled participation reviews/comments/deleted/body-columns = %d/%d/%d/%d",
			reviews,
			comments,
			deletedAuthors,
			commentBodies,
		)
	}
	var pendingChildren int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM backfill_children
		WHERE installation_id = 1
		  AND repo_full_name = 'acme/monolith'
		  AND completed_at IS NULL
	`).Scan(&pendingChildren); err != nil {
		t.Fatal(err)
	}
	if pendingChildren != 0 {
		t.Fatalf("pending backfill children = %d, want 0", pendingChildren)
	}
}

func TestInstallationBackfillEnumeratesAndWaitsForRepoChildren(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := t.Context()
	if err := riverClient.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	if _, err := StartInstallationBackfill(ctx, pool, riverClient, 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var cursor dbgen.InstallationBackfillCursor
	var err error
	for time.Now().Before(deadline) {
		cursor, err = dbgen.New(pool).GetInstallationBackfillCursor(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if cursor.Phase != "done" || !cursor.CompletedAt.Valid {
		t.Fatalf("installation backfill did not complete: %+v", cursor)
	}
	repoCursor, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if repoCursor.Phase != "done" || !repoCursor.CompletedAt.Valid {
		t.Fatalf("repository child cursor = %+v", repoCursor)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/installation/repositories",
	); got != 1 {
		t.Fatalf("installation repository pages = %d, want 1", got)
	}
}

func TestBackfillStableCreatedSnapshotSurvivesMidScanUpdate(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	var mutated atomic.Bool
	hook := fakegithub.WithRequestHook(func(
		method string,
		path string,
		count int,
		fixture *fakegithub.Fixture,
	) {
		if method != http.MethodGet ||
			path != "/repos/acme/monolith/pulls" ||
			count != 3 {
			return
		}
		fixture.PullRequests[1].UpdatedAt =
			fixture.PullRequests[1].UpdatedAt.Add(24 * time.Hour)
		mutated.Store(true)
	})
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
		hook,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := t.Context()
	if err := riverClient.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	if _, err := StartBackfill(
		ctx,
		pool,
		riverClient,
		1,
		"acme/monolith",
	); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cursor, err := dbgen.New(pool).GetBackfillCursor(
			ctx,
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Phase == "done" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !mutated.Load() {
		t.Fatal("mid-scan fake mutation did not run")
	}
	var discovered int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM backfill_children
		WHERE installation_id = 1
		  AND repo_full_name = 'acme/monolith'
		  AND kind = 'refresh_pr'
	`).Scan(&discovered); err != nil {
		t.Fatal(err)
	}
	if discovered != 4 {
		t.Fatalf(
			"stable snapshot discovered %d open PRs after mutation, want 4",
			discovered,
		)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf(
			"pull list calls = %d, want two durable overlap passes",
			got,
		)
	}
}

func TestBackfillCancelMidScanResumesFromDurablePage(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	var pagesMu sync.Mutex
	var pages []int
	cancelPageTwo := make(chan context.CancelFunc, 1)
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet &&
				r.URL.Path == "/repos/acme/monolith/pulls" {
				page, err := strconv.Atoi(r.URL.Query().Get("page"))
				if err != nil {
					http.Error(w, "invalid page", http.StatusBadRequest)
					return
				}
				pagesMu.Lock()
				pages = append(pages, page)
				pagesMu.Unlock()
				if page == 2 {
					select {
					case cancel := <-cancelPageTwo:
						cancel()
						<-r.Context().Done()
						return
					default:
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	fake, server, handler, riverClient := newDirectHandlerWithMiddleware(
		t,
		pool,
		fakegithub.DefaultFixture(),
		10*time.Millisecond,
		2,
		middleware,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := context.Background()
	if _, err := StartBackfill(
		ctx,
		pool,
		riverClient,
		1,
		"acme/monolith",
	); err != nil {
		t.Fatal(err)
	}
	for _, args := range []queue.BackfillRepoPageArgs{
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"repository",
			1,
		),
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"stacks",
			1,
		),
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"pull_requests",
			1,
		),
	} {
		if err := handler.BackfillRepoPage(ctx, args); err != nil {
			t.Fatal(err)
		}
	}
	midScan, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	apiPage, passNewCount, _, err := decodePullBackfillCursor(
		int(midScan.Page),
	)
	if err != nil {
		t.Fatal(err)
	}
	if midScan.Phase != "pull_requests" ||
		apiPage != 2 ||
		passNewCount != 2 {
		t.Fatalf(
			"mid-scan cursor = %+v decoded page=%d pass_new_count=%d",
			midScan,
			apiPage,
			passNewCount,
		)
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelPageTwo <- cancel
	err = handler.BackfillRepoPage(
		cancelCtx,
		queue.NewBackfillRepoPageArgs(
			1,
			"acme/monolith",
			"pull_requests",
			int(midScan.Page),
		),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled mid-scan page error = %v", err)
	}
	afterCancel, err := dbgen.New(pool).GetBackfillCursor(
		ctx,
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   "acme/monolith",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterCancel.Page != midScan.Page ||
		afterCancel.Phase != midScan.Phase {
		t.Fatalf(
			"cancel changed durable cursor: before=%+v after=%+v",
			midScan,
			afterCancel,
		)
	}
	events, unsubscribe := riverClient.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
	)
	defer unsubscribe()
	runCtx := t.Context()
	if err := riverClient.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = riverClient.StopAndCancel(stopCtx)
	}()
	var completed dbgen.BackfillCursor
	for {
		completed, err = dbgen.New(pool).GetBackfillCursor(
			t.Context(),
			dbgen.GetBackfillCursorParams{
				InstallationID: 1,
				RepoFullName:   "acme/monolith",
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if completed.Phase == "done" {
			break
		}
		select {
		case <-events:
		case <-t.Context().Done():
			t.Fatalf("resumed backfill did not complete: %+v", completed)
		}
	}
	pagesMu.Lock()
	gotPages := append([]int(nil), pages...)
	pagesMu.Unlock()
	wantPages := []int{1, 2, 2, 1, 2}
	if !reflect.DeepEqual(gotPages, wantPages) {
		t.Fatalf(
			"pull page requests after restart = %v, want %v",
			gotPages,
			wantPages,
		)
	}
	if got := fake.RequestCount(
		http.MethodGet,
		"/repos/acme/monolith/pulls",
	); got != 4 {
		t.Fatalf("successful pull page requests = %d, want 4", got)
	}
}

func TestPipelineWaitIdleIncludesKeylessBackfillJobs(t *testing.T) {
	t.Parallel()
	harness := newPipelineHarness(t, "acme/keyless-idle")
	defer harness.close()
	if _, err := StartBackfill(
		t.Context(),
		harness.pool,
		harness.river,
		1,
		harness.repo,
	); err != nil {
		t.Fatal(err)
	}
	harness.waitIdle()
	cursor, err := dbgen.New(harness.pool).GetBackfillCursor(
		t.Context(),
		dbgen.GetBackfillCursorParams{
			InstallationID: 1,
			RepoFullName:   harness.repo,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Phase != "done" || !cursor.CompletedAt.Valid {
		t.Fatalf("waitIdle returned before keyless backfill completed: %+v", cursor)
	}
}

func TestReviewRequestWebhookAddsAndAuthoritativelyRemovesCurrentSet(
	t *testing.T,
) {
	t.Parallel()
	const repo = "acme/review-request-webhook"
	harness := newPipelineHarness(t, repo)
	defer harness.close()
	user := fakegithub.ReviewRequest{
		Kind: "user", ID: 5001, NodeID: "U_kwDOABCDEF5001",
		Login: "reviewer",
	}
	requestedPayload, err := harness.fake.
		PullRequestReviewRequestedWebhookPayload(4812, user)
	if err != nil {
		t.Fatal(err)
	}
	harness.emit("review-request-user-add", pipelineEvent{
		event:   "pull_request",
		payload: requestedPayload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	initial := readCachedReviewRequests(t, harness.pool, repo, 4812)
	if len(initial) != 2 || initial[0].Kind != "team" ||
		initial[1].Kind != "user" || initial[0].RequestedAt != nil ||
		initial[1].RequestedAt != nil ||
		initial[0].HeadSHA != "8f31c2d" ||
		initial[1].HeadSHA != "8f31c2d" {
		t.Fatalf("webhook-added review requests = %+v", initial)
	}

	eventsBefore := pullChangedEvents(t, harness.pool, repo)
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	fixture.PullRequests[1].ReviewRequests =
		fixture.PullRequests[1].ReviewRequests[1:]
	harness.fake.SetFixture(fixture)
	removedPayload, err := harness.fake.
		PullRequestReviewRequestRemovedWebhookPayload(4812, user)
	if err != nil {
		t.Fatal(err)
	}
	harness.emit("review-request-user-remove", pipelineEvent{
		event:   "pull_request",
		payload: removedPayload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	remaining := readCachedReviewRequests(t, harness.pool, repo, 4812)
	if len(remaining) != 1 || remaining[0].Kind != "team" ||
		remaining[0].Login != "search-platform" {
		t.Fatalf("review requests after authoritative removal = %+v", remaining)
	}
	eventsAfterRemoval := pullChangedEvents(t, harness.pool, repo)
	if eventsAfterRemoval <= eventsBefore {
		t.Fatalf(
			"request-set removal events = %d, want > %d",
			eventsAfterRemoval,
			eventsBefore,
		)
	}

	fixture.PullRequests[1].ReviewRequests = append(
		fixture.PullRequests[1].ReviewRequests,
		user,
	)
	harness.fake.SetFixture(fixture)
	rerequestedPayload, err := harness.fake.
		PullRequestReviewRequestedWebhookPayload(4812, user)
	if err != nil {
		t.Fatal(err)
	}
	harness.emit("review-request-user-rerequest", pipelineEvent{
		event:   "pull_request",
		payload: rerequestedPayload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	rerequested := readCachedReviewRequests(t, harness.pool, repo, 4812)
	if len(rerequested) != 2 || rerequested[1].Kind != "user" ||
		!rerequested[1].FirstSeenAt.After(initial[1].FirstSeenAt) {
		t.Fatalf("webhook re-request rows = %+v, initial = %+v", rerequested, initial)
	}
	eventsAfterRerequest := pullChangedEvents(t, harness.pool, repo)
	if eventsAfterRerequest <= eventsAfterRemoval {
		t.Fatalf(
			"re-request events = %d, want > %d",
			eventsAfterRerequest,
			eventsAfterRemoval,
		)
	}

	harness.emit("review-request-identical-refresh", pipelineEvent{
		event:   "pull_request",
		payload: rerequestedPayload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	if events := pullChangedEvents(t, harness.pool, repo); events !=
		eventsAfterRerequest {
		t.Fatalf(
			"identical request-set refresh events = %d, want %d",
			events,
			eventsAfterRerequest,
		)
	}
}

func TestIssueCommentWebhookRefreshesAndTombstonesParticipation(t *testing.T) {
	t.Parallel()
	const repo = "acme/issue-comment-webhook"
	harness := newPipelineHarness(t, repo)
	defer harness.close()
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	comment := fakegithub.IssueComment{
		ID: 8301, NodeID: "IC_kwDOABCDEF8301",
		Author: fakegithub.Actor{
			Kind: "user", NodeID: "U_kwDOABCDEF8301", Login: "participant",
		},
		Body:      "not part of the cache contract",
		CreatedAt: fixture.PullRequests[1].UpdatedAt.Add(time.Minute),
		UpdatedAt: fixture.PullRequests[1].UpdatedAt.Add(time.Minute),
	}
	fixture.PullRequests[1].UpdatedAt = comment.UpdatedAt
	fixture.PullRequests[1].Comments = append(
		fixture.PullRequests[1].Comments,
		comment,
	)
	harness.fake.SetFixture(fixture)
	payload, err := harness.fake.IssueCommentCreatedWebhookPayload(4812, comment.ID)
	if err != nil {
		t.Fatal(err)
	}
	harness.emit("issue-comment-created", pipelineEvent{
		event: "issue_comment", payload: payload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	var author, updated string
	var tombstoned bool
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT author_login, gh_updated_at::text, tombstoned_at IS NOT NULL
		FROM pull_request_comments
		WHERE node_id = $1
	`, comment.NodeID).Scan(&author, &updated, &tombstoned); err != nil {
		t.Fatal(err)
	}
	if author != "participant" || tombstoned {
		t.Fatalf("created issue-comment row = %q/%q/%v", author, updated, tombstoned)
	}

	comment.Author.Login = "participant-renamed"
	comment.UpdatedAt = comment.UpdatedAt.Add(time.Minute)
	fixture.PullRequests[1].Comments[len(fixture.PullRequests[1].Comments)-1] = comment
	harness.fake.SetFixture(fixture)
	payload, err = harness.fake.IssueCommentEditedWebhookPayload(4812, comment.ID)
	if err != nil {
		t.Fatal(err)
	}
	eventsBeforeEdit := pullChangedEvents(t, harness.pool, repo)
	harness.emit("issue-comment-edited", pipelineEvent{
		event: "issue_comment", payload: payload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT author_login FROM pull_request_comments WHERE node_id = $1
	`, comment.NodeID).Scan(&author); err != nil {
		t.Fatal(err)
	}
	if author != "participant-renamed" {
		t.Fatalf("edited issue-comment author = %q", author)
	}
	if events := pullChangedEvents(t, harness.pool, repo); events <= eventsBeforeEdit {
		t.Fatalf("edited issue comment events = %d, want > %d", events, eventsBeforeEdit)
	}

	deletePayload, err := harness.fake.IssueCommentDeletedWebhookPayload(
		4812,
		comment.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Deletion is visible through absence from the complete GraphQL connection;
	// neither the deleted comment nor a newer child timestamp is available to
	// version the tombstone, and the parent PR timestamp may remain unchanged.
	fixture.PullRequests[1].Comments = fixture.PullRequests[1].Comments[:len(fixture.PullRequests[1].Comments)-1]
	harness.fake.SetFixture(fixture)
	eventsBeforeDelete := pullChangedEvents(t, harness.pool, repo)
	harness.emit("issue-comment-deleted", pipelineEvent{
		event: "issue_comment", payload: deletePayload,
	})
	harness.dispatchAll()
	harness.waitIdle()
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT tombstoned_at IS NOT NULL
		FROM pull_request_comments
		WHERE node_id = $1
	`, comment.NodeID).Scan(&tombstoned); err != nil {
		t.Fatal(err)
	}
	if !tombstoned {
		t.Fatal("deleted issue comment was not tombstoned")
	}
	if events := pullChangedEvents(t, harness.pool, repo); events != eventsBeforeDelete+1 {
		t.Fatalf(
			"deleted issue-comment events = %d, want %d",
			events,
			eventsBeforeDelete+1,
		)
	}
}

func TestReviewRequestSetDoesNotOscillateBetweenGraphQLAndREST(t *testing.T) {
	t.Parallel()
	pool := fetchTestDatabase(t)
	fixture := fakegithub.DefaultFixture()
	fake, server, handler, riverClient := newDirectHandler(
		t,
		pool,
		fixture,
		10*time.Millisecond,
		100,
	)
	defer server.Close()
	handler.SetRiverClient(riverClient)
	ctx := t.Context()
	key := "pr:acme/monolith:4812"
	resolve := queue.RefreshRequest{
		Args:  queue.NewResolveStackMembershipArgs(key).RefreshArgs,
		Queue: queue.QueueEvent,
	}
	if err := handler.ResolveStackMembership(ctx, resolve); err != nil {
		t.Fatal(err)
	}
	eventsBefore := pullChangedEvents(t, pool, "acme/monolith")
	fixture.PullRequests[1].ReviewRequests =
		fixture.PullRequests[1].ReviewRequests[1:]
	fake.SetFixture(fixture)
	if err := handler.RefreshPR(ctx, queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(key).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	eventsAfterGraphQL := pullChangedEvents(t, pool, "acme/monolith")
	if eventsAfterGraphQL != eventsBefore+1 {
		t.Fatalf(
			"GraphQL request-set events = %d, want %d",
			eventsAfterGraphQL,
			eventsBefore+1,
		)
	}
	if err := handler.ResolveStackMembership(ctx, resolve); err != nil {
		t.Fatal(err)
	}
	if events := pullChangedEvents(t, pool, "acme/monolith"); events !=
		eventsAfterGraphQL {
		t.Fatalf(
			"REST refresh oscillated after GraphQL: events=%d, want %d",
			events,
			eventsAfterGraphQL,
		)
	}
	fixture.PullRequests[1].ReviewRequests = append(
		fixture.PullRequests[1].ReviewRequests,
		fakegithub.ReviewRequest{
			Kind: "user", ID: 5001, NodeID: "U_kwDOABCDEF5001",
			Login: "reviewer",
		},
	)
	fake.SetFixture(fixture)
	if err := handler.ResolveStackMembership(ctx, resolve); err != nil {
		t.Fatal(err)
	}
	eventsAfterREST := pullChangedEvents(t, pool, "acme/monolith")
	if eventsAfterREST != eventsAfterGraphQL+1 {
		t.Fatalf(
			"REST re-request events = %d, want %d",
			eventsAfterREST,
			eventsAfterGraphQL+1,
		)
	}
	if err := handler.RefreshPR(ctx, queue.RefreshRequest{
		Args:  queue.NewRefreshPRArgs(key).RefreshArgs,
		Queue: queue.QueueEvent,
	}); err != nil {
		t.Fatal(err)
	}
	if events := pullChangedEvents(t, pool, "acme/monolith"); events !=
		eventsAfterREST {
		t.Fatalf(
			"GraphQL refresh oscillated after REST: events=%d, want %d",
			events,
			eventsAfterREST,
		)
	}
}

func TestResolveStackMembershipCompletesWithNullHistoricalBaseSHA(
	t *testing.T,
) {
	t.Parallel()
	const repo = "acme/historical-stack"
	harness := newPipelineHarness(t, repo)
	defer harness.close()
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	historical := &fixture.PullRequests[0]
	if historical.Stack == nil {
		t.Fatal("historical fixture PR has no stack summary")
	}
	historical.Stack.Base.Ref = "deleted/historical-base"
	historical.Stack.Base.SHA = ""
	fixture.Stacks[0].Base.Ref = "deleted/historical-base"
	fixture.Stacks[0].Base.SHA = ""
	if !fixture.Stacks[0].Open {
		t.Fatal("regression fixture must exercise an open stack")
	}
	harness.fake.SetFixture(fixture)

	payload, err := harness.fake.PullRequestWebhookPayload(
		"closed",
		historical.Number,
	)
	if err != nil {
		t.Fatal(err)
	}
	wirePull, ok := payload["pull_request"].(map[string]any)
	if !ok {
		t.Fatalf("webhook pull_request = %#v", payload["pull_request"])
	}
	wireStack, ok := wirePull["stack"].(map[string]any)
	if !ok {
		t.Fatalf("webhook stack = %#v", wirePull["stack"])
	}
	wireBase, ok := wireStack["base"].(map[string]any)
	if !ok || wireBase["ref"] != "deleted/historical-base" ||
		wireBase["sha"] != nil {
		t.Fatalf("webhook historical stack base = %#v", wireStack["base"])
	}

	harness.emit("historical-null-stack-base", pipelineEvent{
		event:   "pull_request",
		payload: payload,
	})
	harness.dispatchAll()
	harness.waitIdle()

	ctx := t.Context()
	var stackNumber, stackPosition int
	if err := harness.pool.QueryRow(ctx, `
		SELECT stack_number, stack_position
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.full_name = $1
		  AND pull_requests.number = $2
	`, repo, historical.Number).Scan(&stackNumber, &stackPosition); err != nil {
		t.Fatal(err)
	}
	if stackNumber != historical.Stack.Number ||
		stackPosition != historical.Stack.Position {
		t.Fatalf(
			"persisted membership = stack %d position %d, want %d/%d",
			stackNumber,
			stackPosition,
			historical.Stack.Number,
			historical.Stack.Position,
		)
	}
	var cachedStackBaseSHA string
	if err := harness.pool.QueryRow(ctx, `
		SELECT stacks.base_sha
		FROM stacks
		JOIN repos ON repos.id = stacks.repo_id
		WHERE repos.full_name = $1
		  AND stacks.number = $2
	`, repo, historical.Stack.Number).Scan(&cachedStackBaseSHA); err != nil {
		t.Fatal(err)
	}
	if cachedStackBaseSHA != "" {
		t.Fatalf("cached historical stack base SHA = %q, want unknown", cachedStackBaseSHA)
	}

	var jobs, completed, maxAttempts int
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'completed'),
		       COALESCE(max(attempt), 0)
		FROM river_job
		WHERE kind = 'resolve_stack_membership'
		  AND args->>'key' = $1
	`, fmt.Sprintf("pr:%s:%d", repo, historical.Number)).Scan(
		&jobs,
		&completed,
		&maxAttempts,
	); err != nil {
		t.Fatal(err)
	}
	if jobs == 0 || completed != jobs || maxAttempts != 1 {
		t.Fatalf(
			"resolve_stack_membership jobs/completed/max-attempt = %d/%d/%d",
			jobs,
			completed,
			maxAttempts,
		)
	}
	var stackJobs, completedStackJobs, maxStackAttempts int
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'completed'),
		       COALESCE(max(attempt), 0)
		FROM river_job
		WHERE kind = 'refresh_stack'
		  AND args->>'key' = $1
	`, fmt.Sprintf("stack:%s:%d", repo, historical.Stack.Number)).Scan(
		&stackJobs,
		&completedStackJobs,
		&maxStackAttempts,
	); err != nil {
		t.Fatal(err)
	}
	if stackJobs < 1 || stackJobs > 4 || completedStackJobs != stackJobs ||
		maxStackAttempts != 1 {
		t.Fatalf(
			"open unknown-SHA stack refreshes/completed/max-attempt = %d/%d/%d, want 1..4/all/1",
			stackJobs,
			completedStackJobs,
			maxStackAttempts,
		)
	}
	if got := harness.fake.RequestCount(
		http.MethodGet,
		fmt.Sprintf("/repos/%s/pulls/%d", repo, historical.Number),
	); got < 1 {
		t.Fatalf("historical PR fetches = %d, want at least one", got)
	}
}

func TestStackWorkersCompleteWithOpenHistoricalPositionBeyondSize(
	t *testing.T,
) {
	t.Parallel()
	const repo = "acme/historical-stack-position"
	harness := newPipelineHarness(t, repo)
	defer harness.close()
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	historical := &fixture.PullRequests[4]
	if historical.Stack == nil {
		t.Fatal("historical fixture PR has no stack summary")
	}
	if historical.State != "open" {
		t.Fatalf("historical fixture PR state = %q, want open", historical.State)
	}
	historical.Stack.Size = 2
	historical.Stack.Position = 5
	historical.Stack.Base.SHA = ""
	fixture.Stacks[0].Base.SHA = ""
	fixture.Stacks[0].PullRequests = append(
		[]fakegithub.StackPullRequest(nil),
		fixture.Stacks[0].PullRequests[1:3]...,
	)
	for offset, pullIndex := range []int{1, 2} {
		current := &fixture.PullRequests[pullIndex]
		if current.Stack == nil {
			t.Fatalf("current fixture PR %d has no stack summary", current.Number)
		}
		current.Stack.Size = 2
		current.Stack.Position = offset + 1
		current.Stack.Base.SHA = ""
	}
	harness.fake.SetFixture(fixture)

	payload, err := harness.fake.PullRequestWebhookPayload(
		"synchronize",
		historical.Number,
	)
	if err != nil {
		t.Fatal(err)
	}
	wirePull, ok := payload["pull_request"].(map[string]any)
	if !ok {
		t.Fatalf("webhook pull_request = %#v", payload["pull_request"])
	}
	wireStack, ok := wirePull["stack"].(map[string]any)
	if !ok || wirePull["state"] != "open" ||
		wireStack["size"] != 2 || wireStack["position"] != 5 {
		t.Fatalf("webhook historical stack tuple = %#v", wirePull["stack"])
	}
	wireBase, ok := wireStack["base"].(map[string]any)
	if !ok || wireBase["ref"] != "main" || wireBase["sha"] != nil {
		t.Fatalf("webhook historical stack base = %#v", wireStack["base"])
	}

	harness.emit("historical-position-beyond-size", pipelineEvent{
		event:   "pull_request",
		payload: payload,
	})
	harness.dispatchAll()
	harness.waitIdle()

	ctx := t.Context()
	var stackNumber, stackPosition int
	if err := harness.pool.QueryRow(ctx, `
		SELECT stack_number, stack_position
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.full_name = $1
		  AND pull_requests.number = $2
	`, repo, historical.Number).Scan(&stackNumber, &stackPosition); err != nil {
		t.Fatal(err)
	}
	if stackNumber != historical.Stack.Number ||
		stackPosition != historical.Stack.Position {
		t.Fatalf(
			"persisted historical membership = stack %d position %d, want %d/%d",
			stackNumber,
			stackPosition,
			historical.Stack.Number,
			historical.Stack.Position,
		)
	}
	var currentSize int
	if err := harness.pool.QueryRow(ctx, `
		SELECT jsonb_array_length(stacks.entries)
		FROM stacks
		JOIN repos ON repos.id = stacks.repo_id
		WHERE repos.full_name = $1
		  AND stacks.number = $2
	`, repo, historical.Stack.Number).Scan(&currentSize); err != nil {
		t.Fatal(err)
	}
	if currentSize != historical.Stack.Size {
		t.Fatalf(
			"cached current stack size = %d, want %d",
			currentSize,
			historical.Stack.Size,
		)
	}

	key := fmt.Sprintf("pr:%s:%d", repo, historical.Number)
	assertCompletedAtAttemptOne := func(kind string) {
		t.Helper()
		var jobs, completed, maxAttempts int
		if err := harness.pool.QueryRow(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE state = 'completed'),
			       COALESCE(max(attempt), 0)
			FROM river_job
			WHERE kind = $1
			  AND args->>'key' = $2
		`, kind, key).Scan(&jobs, &completed, &maxAttempts); err != nil {
			t.Fatal(err)
		}
		if jobs == 0 || completed != jobs || maxAttempts != 1 {
			t.Fatalf(
				"%s jobs/completed/max-attempt = %d/%d/%d, want nonzero/all/1",
				kind,
				jobs,
				completed,
				maxAttempts,
			)
		}
	}
	assertCompletedAtAttemptOne("resolve_stack_membership")
	assertCompletedAtAttemptOne("refresh_pr")
	var stackJobs, completedStackJobs, maxStackAttempts int
	if err := harness.pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'completed'),
		       COALESCE(max(attempt), 0)
		FROM river_job
		WHERE kind = 'refresh_stack'
		  AND args->>'key' = $1
	`, fmt.Sprintf("stack:%s:%d", repo, historical.Stack.Number)).Scan(
		&stackJobs,
		&completedStackJobs,
		&maxStackAttempts,
	); err != nil {
		t.Fatal(err)
	}
	if stackJobs < 1 || stackJobs > 4 || completedStackJobs != stackJobs ||
		maxStackAttempts != 1 {
		t.Fatalf(
			"historical-position stack refreshes/completed/max-attempt = %d/%d/%d, want 1..4/all/1",
			stackJobs,
			completedStackJobs,
			maxStackAttempts,
		)
	}
	if got := harness.fake.RequestCount(
		http.MethodGet,
		fmt.Sprintf("/repos/%s/pulls/%d", repo, historical.Number),
	); got < 1 {
		t.Fatalf("historical PR fetches = %d, want at least one", got)
	}
	if got := harness.fake.RequestCount(http.MethodPost, "/graphql"); got < 1 {
		t.Fatalf("historical PR GraphQL hydrations = %d, want at least one", got)
	}
}

func TestRefreshPRWorkerPersistsNullGraphQLBaseRefOID(t *testing.T) {
	t.Parallel()
	const repo = "acme/graphql-null-base"
	harness := newPipelineHarness(t, repo)
	defer harness.close()
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	pull := &fixture.PullRequests[1]
	harness.fake.SetFixture(fixture)

	payload, err := harness.fake.PullRequestWebhookPayload(
		"synchronize",
		pull.Number,
	)
	if err != nil {
		t.Fatal(err)
	}
	harness.emit("graphql-null-base-seed", pipelineEvent{
		event:   "pull_request",
		payload: payload,
	})
	harness.dispatchAll()
	harness.waitIdle()

	pull.Base.SHA = ""
	pull.UpdatedAt = pull.UpdatedAt.Add(time.Minute)
	harness.fake.SetFixture(fixture)
	graphQLBefore := harness.fake.RequestCount(http.MethodPost, "/graphql")
	key := fmt.Sprintf("pr:%s:%d", repo, pull.Number)
	if _, err := harness.river.Insert(
		t.Context(),
		queue.NewRefreshPRArgs(key),
		queue.NewRefreshInsertOptsForQueue(queue.QueueEvent, time.Time{}),
	); err != nil {
		t.Fatal(err)
	}
	harness.waitIdle()

	if got := harness.fake.RequestCount(http.MethodPost, "/graphql") -
		graphQLBefore; got != 1 {
		t.Fatalf("GraphQL null-base refresh calls = %d, want 1", got)
	}
	var cachedBaseSHA string
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT pull_requests.base_sha
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.full_name = $1
		  AND pull_requests.number = $2
	`, repo, pull.Number).Scan(&cachedBaseSHA); err != nil {
		t.Fatal(err)
	}
	if cachedBaseSHA != "" {
		t.Fatalf("GraphQL null baseRefOid cached as %q, want unknown", cachedBaseSHA)
	}
	var jobs, completed, maxAttempts int
	var jobErrors string
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'completed'),
		       COALESCE(max(attempt), 0),
		       COALESCE(string_agg(errors::text, E'\n'), '')
		FROM river_job
		WHERE kind = 'refresh_pr'
		  AND args->>'key' = $1
	`, key).Scan(&jobs, &completed, &maxAttempts, &jobErrors); err != nil {
		t.Fatal(err)
	}
	if jobs != 2 || completed != 2 || maxAttempts != 1 {
		t.Fatalf(
			"GraphQL refresh_pr jobs/completed/max-attempt = %d/%d/%d, want 2/2/1; errors: %s",
			jobs,
			completed,
			maxAttempts,
			jobErrors,
		)
	}
}

func TestOrderIndependenceFinalCacheState(t *testing.T) {
	t.Parallel()
	want := expectedOrderCacheSnapshot()
	for run := range 4 {
		repo := "acme/order"
		harness := newPipelineHarness(t, repo)
		random := rand.New(rand.NewSource(int64(900 + run))) //nolint:gosec // deterministic non-security use
		events := []pipelineEvent{
			{
				event: "pull_request",
				payload: map[string]any{
					"action": "synchronize",
					"number": 4812,
					"pull_request": map[string]any{
						"number": 4812,
						"stack":  map[string]any{"number": 142},
					},
				},
			},
			{
				event: "check_run",
				payload: map[string]any{
					"action":    "completed",
					"check_run": map[string]any{"head_sha": "8f31c2d"},
				},
			},
			{
				event: "push",
				payload: map[string]any{
					"ref":   "refs/heads/refactor/bm25f-ranker",
					"stack": map[string]any{"number": 142},
				},
			},
		}
		random.Shuffle(len(events), func(i, j int) {
			events[i], events[j] = events[j], events[i]
		})
		for index, event := range events {
			harness.emit(
				fmt.Sprintf("order-%d-%d", run, index),
				event,
			)
			if random.Intn(2) == 0 {
				harness.emit(
					fmt.Sprintf("order-%d-%d-duplicate", run, index),
					event,
				)
			}
		}
		harness.dispatchAll()
		harness.waitIdle()
		// Reconciliation must converge the source-rich PR observation after any
		// event-order race with parent-only repository or stack observations.
		if _, err := harness.river.Insert(
			t.Context(),
			queue.NewRefreshPRArgs(fmt.Sprintf("pr:%s:4812", repo)),
			queue.NewRefreshInsertOptsForQueue(queue.QueueSweep, time.Time{}),
		); err != nil {
			t.Fatal(err)
		}
		harness.waitIdle()
		got := snapshotCache(t, harness.pool, repo)
		harness.close()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf(
				"run %d final cache differs\n got: %+v\nwant: %+v",
				run,
				got,
				want,
			)
		}
	}
}

func expectedOrderCacheSnapshot() cacheSnapshot {
	// This golden is deliberately authored independently of fakegithub.Fixture
	// and snapshotCache. It prevents an identically empty or consistently
	// malformed implementation from satisfying C-I4 by self-comparison.
	return cacheSnapshot{
		Repos:        `[{"gh_id": 2001, "node_id": "R_acme_order", "archived": false, "head_sha": "aaaa000", "full_name": "acme/order", "tombstoned": false, "sync_source": "reconcile", "gh_updated_at": "2026-07-28T12:00:00Z", "default_branch": "main"}]`,
		RepoRules:    `[]`,
		Stacks:       `[{"open": true, "gh_id": 9876543, "number": 142, "entries": [{"draft": false, "state": "closed", "number": 4810, "head_ref": "refactor/tokenizer", "head_sha": "bbbb001", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4812, "head_ref": "refactor/bm25f-ranker", "head_sha": "8f31c2d", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4815, "head_ref": "feat/relevance-debug", "head_sha": "bbbb003", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4816, "head_ref": "feat/results-rewire", "head_sha": "bbbb004", "updated_at": "2026-07-28T12:00:00Z"}, {"draft": false, "state": "open", "number": 4820, "head_ref": "feat/relevance-telemetry", "head_sha": "bbbb005", "updated_at": "2026-07-28T12:00:00Z"}], "node_id": "S_kwDOABCDEF4AAAAA", "base_ref": "main", "base_sha": "aaaa000", "head_sha": "bbbb005", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T12:00:00Z"}]`,
		Pulls:        `[{"gh_id": 804810, "state": "closed", "title": "Tokenizer rewrite for query parser", "number": 4810, "node_id": "PR_kwDOABCDEF4810", "base_ref": "main", "base_sha": "aaaa000", "head_ref": "refactor/tokenizer", "head_sha": "bbbb001", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 1, "review_decision": "APPROVED"}, {"gh_id": 804812, "state": "open", "title": "BM25F ranker integration", "number": 4812, "node_id": "PR_kwDOABCDEF4812", "base_ref": "refactor/tokenizer", "base_sha": "bbbb001", "head_ref": "refactor/bm25f-ranker", "head_sha": "8f31c2d", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 2, "review_decision": "CHANGES_REQUESTED"}, {"gh_id": 804815, "state": "open", "title": "Relevance debug API endpoint", "number": 4815, "node_id": "PR_kwDOABCDEF4815", "base_ref": "refactor/bm25f-ranker", "base_sha": "8f31c2d", "head_ref": "feat/relevance-debug", "head_sha": "bbbb003", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 3, "review_decision": "REVIEW_REQUIRED"}, {"gh_id": 804816, "state": "open", "title": "Results page rewiring", "number": 4816, "node_id": "PR_kwDOABCDEF4816", "base_ref": "feat/relevance-debug", "base_sha": "bbbb003", "head_ref": "feat/results-rewire", "head_sha": "bbbb004", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 4, "review_decision": "REVIEW_REQUIRED"}, {"gh_id": 804820, "state": "open", "title": "Relevance telemetry dashboards", "number": 4820, "node_id": "PR_kwDOABCDEF4820", "base_ref": "feat/results-rewire", "base_sha": "bbbb004", "head_ref": "feat/relevance-telemetry", "head_sha": "bbbb005", "tombstoned": false, "sync_source": "webhook", "stack_number": 142, "gh_updated_at": "2026-07-28T12:00:00Z", "stack_position": 5, "review_decision": "REVIEW_REQUIRED"}]`,
		Threads:      `[{"id": "PRRT_kwDOABCDEF4812_1", "line": null, "path": "internal/ranker.go", "comments": [{"id": "PRRC_kwDOABCDEF4812_1", "body": "Please cover the tie case.", "updated_at": "2026-07-28T12:00:00Z", "author_login": "reviewer"}], "head_sha": "8f31c2d", "pr_number": 4812, "tombstoned": false, "is_outdated": false, "is_resolved": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T12:00:00Z"}]`,
		Checks:       `[{"name": "unit", "gh_id": 99001, "status": "completed", "head_sha": "8f31c2d", "conclusion": "failure", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z"}, {"name": "lint", "gh_id": 99002, "status": "completed", "head_sha": "8f31c2d", "conclusion": "success", "tombstoned": false, "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z"}]`,
		CheckHistory: `[{"name": "unit", "status": "completed", "head_sha": "8f31c2d", "conclusion": "failure", "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z", "check_run_gh_id": 99001}, {"name": "lint", "status": "completed", "head_sha": "8f31c2d", "conclusion": "success", "sync_source": "webhook", "gh_updated_at": "2026-07-28T11:55:00Z", "check_run_gh_id": 99002}]`,
		Dirty:        `["pr:1:2001:4810", "pr:1:2001:4812", "pr:1:2001:4815", "pr:1:2001:4816", "pr:1:2001:4820", "stack:1:2001:142"]`,
	}
}

func TestStormAssertsFetchCount(t *testing.T) {
	t.Parallel()
	harness := newPipelineHarness(t, "acme/storm")
	defer harness.close()
	warm := pipelineEvent{
		event: "push",
		payload: map[string]any{
			"ref":   "refs/heads/refactor/bm25f-ranker",
			"stack": map[string]any{"number": 142},
		},
	}
	if _, err := harness.river.Insert(
		t.Context(),
		queue.NewRefreshStackArgs("stack:acme/storm:142"),
		queue.NewRefreshInsertOptsForQueue(queue.QueueEvent, time.Time{}),
	); err != nil {
		t.Fatal(err)
	}
	// A real branch push fans out from previously mirrored branch membership.
	// Seed that membership directly so this test measures storm coalescing,
	// not cold-start behavior.
	harness.waitIdle()
	harness.emit("storm-warm", warm)
	harness.dispatchAll()
	harness.waitIdle()
	path := "/repos/acme/storm/stacks/142"
	baseline := harness.fake.RequestCount(http.MethodGet, path)

	for index := range 20 {
		harness.emit(fmt.Sprintf("storm-%02d", index), warm)
	}
	harness.dispatchAll()
	harness.waitIdle()
	delta := harness.fake.RequestCount(http.MethodGet, path) - baseline
	if delta != 1 {
		t.Fatalf(
			"20-event storm caused %d stack fetches, want one coalesced fetch",
			delta,
		)
	}
}

func TestBranchBulkPagesConvergeWithIndividualRefreshes(t *testing.T) {
	t.Parallel()
	const (
		branch     = "refactor/bm25f-ranker"
		oldSHA     = "force-pushed-away"
		currentSHA = "8f31c2d"
	)

	baseline := newManualBranchHarness(t, "acme/branch-convergence")
	bulk := newManualBranchHarness(t, "acme/branch-convergence")
	for _, harness := range []*manualBranchHarness{baseline, bulk} {
		harness.warmBranchDependents()
		harness.rewindBranchCache(branch, currentSHA, oldSHA)
	}

	// The control runs the equivalent independent observations: the interleaved
	// PR on the direct lane and the remaining reference targets through their
	// ordinary per-entity reconciliation semantics.
	baseline.refreshPR(4812, queue.QueueEvent)
	baseline.refreshPR(4815, queue.QueueSweep)
	baseline.refreshStack(142, queue.QueueSweep)

	// The new path exposes the exact force-push transition locally, then runs
	// one bounded page. A newer direct generation interleaved after page
	// creation supersedes PR 4812's page target; its direct refresh supplies
	// that entity's authoritative observation instead.
	page := bulk.applyBranchPage(&store.BranchPushHint{
		RepoFullName:    bulk.repo,
		Branch:          branch,
		BeforeSHA:       oldSHA,
		AfterSHA:        currentSHA,
		TransitionKnown: true,
		Forced:          true,
		DeliveryGUID:    "branch-convergence-force-push",
		ReceivedAt: time.Date(
			2026, 8, 5, 18, 0, 0, 0, time.UTC,
		),
	})
	var direct queue.BranchReconcileTarget
	for _, target := range page.Targets {
		if target.Kind == queue.KindRefreshPR &&
			target.Key == "pr:"+bulk.repo+":4812" {
			direct = target
			break
		}
	}
	if direct.Key == "" {
		t.Fatalf("page has no interleaved direct target: %+v", page.Targets)
	}
	gotTargets := make([]string, 0, len(page.Targets))
	for _, target := range page.Targets {
		gotTargets = append(gotTargets, target.Kind+"/"+target.Key)
	}
	sort.Strings(gotTargets)
	wantTargets := []string{
		queue.KindRefreshPR + "/pr:" + bulk.repo + ":4812",
		queue.KindRefreshPR + "/pr:" + bulk.repo + ":4815",
		queue.KindRefreshStack + "/stack:" + bulk.repo + ":142",
	}
	if !reflect.DeepEqual(gotTargets, wantTargets) {
		t.Fatalf("mixed branch targets = %v, want %v", gotTargets, wantTargets)
	}
	bulk.bumpDirectGeneration(direct)
	// Event workers are reserved independently of the bulk lane. Complete the
	// newer direct observation first so this oracle exercises direct -> stale
	// page superseding under the production lane ordering. Delayed-response
	// tests separately cover stale page/direct responses completing last.
	bulk.refreshPR(4812, queue.QueueEvent)
	if err := bulk.handler.ReconcileBranchPage(t.Context(), &page); err != nil {
		t.Fatal(err)
	}

	want := snapshotBranchConvergenceCache(t, baseline.pool, baseline.repo)
	got := snapshotBranchConvergenceCache(t, bulk.pool, bulk.repo)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bulk+page cache did not converge\ngot:  %+v\nwant: %+v", got, want)
	}

	var status string
	var superseded int
	if err := bulk.pool.QueryRow(t.Context(), `
		SELECT status, superseded_targets
		FROM branch_reconciliation_pages
		WHERE repo_id = $1 AND branch = $2 AND generation = $3
	`, page.RepoID, page.Branch, page.Generation).Scan(
		&status, &superseded,
	); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || superseded != 1 {
		t.Fatalf(
			"page status/superseded = %q/%d, want completed/1",
			status, superseded,
		)
	}
}

func TestSlowBranchPagesDoNotStarveSmallDatabasePool(t *testing.T) {
	t.Parallel()
	const branch = "refactor/bm25f-ranker"
	repo := "acme/branch-small-pool"
	database := testdb.New(t)
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	_, seedServer, seedHandler, seedRiver := newDirectHandler(
		t, database.Pool, fixture, 5*time.Millisecond, 100,
	)
	defer seedServer.Close()
	seedHandler.SetRiverClient(seedRiver)
	seed := &manualBranchHarness{
		t: t, repo: repo, pool: database.Pool, handler: seedHandler,
		river: seedRiver,
	}
	seed.warmBranchDependents()
	seed.rewindBranchCache(branch, "8f31c2d", "slow-page-old-head")
	page := seed.applyBranchPage(&store.BranchPushHint{
		RepoFullName:    repo,
		Branch:          branch,
		BeforeSHA:       "slow-page-old-head",
		AfterSHA:        "8f31c2d",
		TransitionKnown: true,
		DeliveryGUID:    "slow-pages-small-pool",
		ReceivedAt:      time.Date(2026, 8, 6, 20, 0, 0, 0, time.UTC),
	})
	if len(page.Targets) < 2 {
		t.Fatalf("branch fixture produced %d targets, want at least 2", len(page.Targets))
	}

	// Split the bounded target set into two durable pages so both page workers
	// can have slow GitHub calls in flight against the same two-connection pool.
	split := (len(page.Targets) + 1) / 2
	if _, err := database.Pool.Exec(t.Context(), `
		UPDATE branch_reconciliation_pages
		SET target_count = $5
		WHERE repo_id = $1 AND branch = $2 AND generation = $3
		  AND page_number = $4
	`, page.RepoID, page.Branch, page.Generation, page.Page, split); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(t.Context(), `
		INSERT INTO branch_reconciliation_pages (
		    repo_id, branch, generation, page_number, target_count,
		    status, created_at
		) VALUES ($1, $2, $3, 2, $4, 'pending', clock_timestamp())
	`, page.RepoID, page.Branch, page.Generation,
		len(page.Targets)-split); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(t.Context(), `
		UPDATE branch_reconciliations
		SET page_count = 2
		WHERE repo_id = $1 AND branch = $2 AND generation = $3
	`, page.RepoID, page.Branch, page.Generation); err != nil {
		t.Fatal(err)
	}
	pages := []queue.BranchReconcilePageArgs{
		queue.NewBranchReconcilePageArgs(
			page.RepoID, page.RepoFullName, page.Branch, page.Generation, 1,
			page.Targets[:split],
		),
		queue.NewBranchReconcilePageArgs(
			page.RepoID, page.RepoFullName, page.Branch, page.Generation, 2,
			page.Targets[split:],
		),
	}

	smallPool := fetchPoolWithMaxConns(t, database.URL, 2)
	slowFake, slowServer, slowHandler, slowRiver := newDirectHandler(
		t, smallPool, fixture, 5*time.Millisecond, 100,
		fakegithub.WithResponseDelay(750*time.Millisecond),
	)
	defer slowServer.Close()
	slowHandler.SetRiverClient(slowRiver)
	done := make(chan error, len(pages))
	for index := range pages {
		go func(page *queue.BranchReconcilePageArgs) {
			done <- slowHandler.ReconcileBranchPage(t.Context(), page)
		}(&pages[index])
	}

	deadline := time.Now().Add(10 * time.Second)
	for slowFake.Concurrent() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("two slow branch-page requests were not concurrently in flight")
		}
		time.Sleep(10 * time.Millisecond)
	}
	controlCtx, cancelControl := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancelControl()
	var controlPlaneHealthy bool
	if err := smallPool.QueryRow(controlCtx, `SELECT true`).Scan(
		&controlPlaneHealthy,
	); err != nil {
		t.Fatalf("control-plane query starved by slow branch pages: %v", err)
	}
	if !controlPlaneHealthy {
		t.Fatal("control-plane query did not complete")
	}
	for range pages {
		if err := <-done; err != nil {
			t.Fatalf("slow branch page: %v", err)
		}
	}
}

// snapshotBranchConvergenceCache compares every persisted semantic,
// authoritative-version, validator, and provenance field in both PR and stack
// cache scopes. It strips only database-local identities and wall-clock
// bookkeeping, preserves tombstone state as a boolean, and sorts rows in Go so
// PostgreSQL collation cannot make the oracle agree accidentally.
func snapshotBranchConvergenceCache(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
) map[string][]string {
	t.Helper()
	type tableSpec struct {
		name     string
		localIDs []string
	}
	tables := []tableSpec{
		{name: "repos", localIDs: []string{"id"}},
		{name: "repo_rules"},
		{name: "pull_requests", localIDs: []string{"id"}},
		{name: "stacks", localIDs: []string{"id"}},
		{name: "review_threads"},
		{name: "pull_request_review_requests"},
		{name: "pull_request_reviews"},
		{name: "pull_request_comments"},
		{name: "pull_request_change_snapshots"},
		{name: "pull_request_changed_files"},
		{name: "pull_request_file_owners"},
		{name: "check_runs"},
		{name: "check_history", localIDs: []string{"id"}},
	}
	ignored := []string{
		"repo_id", "synced_at", "last_checked_at", "tombstoned_at",
		"display_until", "first_seen_at",
	}
	snapshot := make(map[string][]string, len(tables)+1)
	for _, table := range tables {
		removed := append(append([]string(nil), ignored...), table.localIDs...)
		filter := "JOIN repos ON repos.id = cache_row.repo_id " +
			"WHERE repos.full_name = $1"
		if table.name == "repos" {
			filter = "WHERE cache_row.full_name = $1"
		}
		query := fmt.Sprintf(`
			SELECT (to_jsonb(cache_row) - $2::text[]) ||
			       jsonb_build_object(
			           'tombstoned', cache_row.tombstoned_at IS NOT NULL
			       )
			FROM %s AS cache_row
			%s
		`, table.name, filter)
		rows, err := pool.Query(t.Context(), query, repo, removed)
		if err != nil {
			t.Fatalf("snapshot %s: %v", table.name, err)
		}
		values := make([]string, 0)
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				t.Fatalf("scan snapshot %s: %v", table.name, err)
			}
			values = append(values, string(raw))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("iterate snapshot %s: %v", table.name, err)
		}
		rows.Close()
		sort.Strings(values)
		snapshot[table.name] = values
	}

	rows, err := pool.Query(t.Context(), `
		SELECT scope_key
		FROM derivation_dirty
		WHERE scope_key LIKE (
			SELECT '%:' || installation_id::text || ':' || gh_id::text || ':%'
			FROM repos WHERE full_name = $1
		)
	`, repo)
	if err != nil {
		t.Fatalf("snapshot dirty scopes: %v", err)
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot["derivation_dirty"] = append(
			snapshot["derivation_dirty"], key,
		)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	sort.Strings(snapshot["derivation_dirty"])
	return snapshot
}

func TestBranchPageDoesNotOverwriteRepositoryBranchHint(t *testing.T) {
	t.Parallel()
	harness := newManualBranchHarness(t, "acme/branch-parent-fence")
	harness.refreshPR(4812, queue.QueueEvent)
	harness.rewindBranchCache(
		"refactor/tokenizer", "bbbb001", "old-tokenizer-head",
	)
	if _, err := harness.pool.Exec(t.Context(), `
		UPDATE repos
		SET head_sha = 'newer-local-default-head', etag = ''
		WHERE full_name = $1
	`, harness.repo); err != nil {
		t.Fatal(err)
	}
	page := harness.applyBranchPage(&store.BranchPushHint{
		RepoFullName:    harness.repo,
		Branch:          "refactor/tokenizer",
		BeforeSHA:       "old-tokenizer-head",
		AfterSHA:        "bbbb001",
		TransitionKnown: true,
		DeliveryGUID:    "branch-parent-fence",
		ReceivedAt: time.Date(
			2026, 8, 5, 18, 30, 0, 0, time.UTC,
		),
	})
	if err := harness.handler.ReconcileBranchPage(t.Context(), &page); err != nil {
		t.Fatal(err)
	}
	var repoHead, baseHead string
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT repos.head_sha, pull_requests.base_sha
		FROM repos
		JOIN pull_requests ON pull_requests.repo_id = repos.id
		WHERE repos.full_name = $1 AND pull_requests.number = 4812
	`, harness.repo).Scan(&repoHead, &baseHead); err != nil {
		t.Fatal(err)
	}
	if repoHead != "newer-local-default-head" || baseHead != "bbbb001" {
		t.Fatalf(
			"page repository/PR heads = %q/%q, want newer hint/bbbb001",
			repoHead, baseHead,
		)
	}
}

func TestReverseOrderedDefaultBranchTransitionsConvergeRepository(t *testing.T) {
	t.Parallel()
	harness := newManualBranchHarness(t, "acme/branch-reverse-order")
	harness.refreshRepository(queue.QueueEvent)
	if _, err := harness.pool.Exec(t.Context(), `
		UPDATE repos SET head_sha = 'old-default-head'
		WHERE full_name = $1
	`, harness.repo); err != nil {
		t.Fatal(err)
	}

	// GitHub emitted old->middle and then middle->aaaa000. Delivering the
	// second push first cannot be ordered by comparing SHA strings: its exact
	// CAS is initially a no-op, then the delayed predecessor advances the
	// local hint only to middle.
	harness.applyBranchPage(&store.BranchPushHint{
		RepoFullName: harness.repo, Branch: "main",
		BeforeSHA: "middle-default-head", AfterSHA: "aaaa000",
		TransitionKnown: true, DeliveryGUID: "reverse-order-newer",
		ReceivedAt: time.Date(2026, 8, 5, 19, 0, 0, 0, time.UTC),
	})
	harness.applyBranchPage(&store.BranchPushHint{
		RepoFullName: harness.repo, Branch: "main",
		BeforeSHA: "old-default-head", AfterSHA: "middle-default-head",
		TransitionKnown: true, DeliveryGUID: "reverse-order-older",
		ReceivedAt: time.Date(2026, 8, 5, 19, 0, 1, 0, time.UTC),
	})
	var hintedHead string
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT head_sha FROM repos WHERE full_name = $1
	`, harness.repo).Scan(&hintedHead); err != nil {
		t.Fatal(err)
	}
	if hintedHead != "middle-default-head" {
		t.Fatalf("reverse-ordered exact CAS head = %q, want middle", hintedHead)
	}

	// Dispatch schedules this one constant-size repository observation on the
	// bulk lane for every default-branch bulk generation. The real repository
	// refresh path closes the partial-order gap with authoritative truth.
	harness.refreshRepository(queue.QueueBulk)
	var convergedHead string
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT head_sha FROM repos WHERE full_name = $1
	`, harness.repo).Scan(&convergedHead); err != nil {
		t.Fatal(err)
	}
	if convergedHead != "aaaa000" {
		t.Fatalf("authoritative repository head = %q, want aaaa000", convergedHead)
	}
}

func TestBranchPageFailureLeavesOnlyBoundedPagePendingForRetry(t *testing.T) {
	t.Parallel()
	harness := newManualBranchHarness(t, "acme/branch-page-retry")
	harness.warmBranchDependents()
	harness.rewindBranchCache(
		"refactor/bm25f-ranker", "8f31c2d", "old-retry-head",
	)
	page := harness.applyBranchPage(&store.BranchPushHint{
		RepoFullName:    harness.repo,
		Branch:          "refactor/bm25f-ranker",
		BeforeSHA:       "old-retry-head",
		AfterSHA:        "8f31c2d",
		TransitionKnown: true,
		DeliveryGUID:    "branch-page-retry",
		ReceivedAt: time.Date(
			2026, 8, 5, 18, 45, 0, 0, time.UTC,
		),
	})
	if len(page.Targets) > dispatch.DefaultBranchReconcilePageSize {
		t.Fatalf("page has %d unbounded targets", len(page.Targets))
	}
	failed := page
	failed.Targets = append([]queue.BranchReconcileTarget(nil), page.Targets...)
	failed.Targets[0].Kind = "unsupported_retry_fixture"
	if err := harness.handler.ReconcileBranchPage(t.Context(), &failed); err == nil {
		t.Fatal("poisoned page unexpectedly succeeded")
	}
	var status string
	var attempts int64
	var startedAt, heartbeatAt pgtype.Timestamptz
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT status, attempt_count, last_started_at, heartbeat_at
		FROM branch_reconciliation_pages
		WHERE repo_id = $1 AND branch = $2 AND generation = $3
	`, page.RepoID, page.Branch, page.Generation).Scan(
		&status, &attempts, &startedAt, &heartbeatAt,
	); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 1 || !startedAt.Valid ||
		!heartbeatAt.Valid {
		t.Fatalf(
			"failed page manifest = %q attempts=%d started=%v heartbeat=%v",
			status, attempts, startedAt.Valid, heartbeatAt.Valid,
		)
	}
	if err := harness.handler.ReconcileBranchPage(t.Context(), &page); err != nil {
		t.Fatal(err)
	}
	if err := harness.pool.QueryRow(t.Context(), `
		SELECT status, attempt_count, heartbeat_at
		FROM branch_reconciliation_pages
		WHERE repo_id = $1 AND branch = $2 AND generation = $3
	`, page.RepoID, page.Branch, page.Generation).Scan(
		&status, &attempts, &heartbeatAt,
	); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || attempts != 2 || !heartbeatAt.Valid {
		t.Fatalf(
			"retried page manifest = %q attempts=%d heartbeat=%v",
			status, attempts, heartbeatAt.Valid,
		)
	}
}

type manualBranchHarness struct {
	t       *testing.T
	repo    string
	pool    *pgxpool.Pool
	handler *Handler
	river   *river.Client[pgx.Tx]
}

func newManualBranchHarness(t *testing.T, repo string) *manualBranchHarness {
	t.Helper()
	pool := fetchTestDatabase(t)
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	_, server, handler, riverClient := newDirectHandler(
		t, pool, fixture, 5*time.Millisecond, 100,
	)
	t.Cleanup(server.Close)
	handler.SetRiverClient(riverClient)
	return &manualBranchHarness{
		t: t, repo: repo, pool: pool, handler: handler, river: riverClient,
	}
}

func (h *manualBranchHarness) warmBranchDependents() {
	h.t.Helper()
	h.refreshStack(142, queue.QueueEvent)
	h.refreshPR(4812, queue.QueueEvent)
	h.refreshPR(4815, queue.QueueEvent)
}

func (h *manualBranchHarness) refreshPR(number int, queueName string) {
	h.t.Helper()
	err := h.handler.RefreshPR(h.t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			fmt.Sprintf("pr:%s:%d", h.repo, number),
		).RefreshArgs,
		Queue: queueName,
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *manualBranchHarness) refreshRepository(queueName string) {
	h.t.Helper()
	err := h.handler.RefreshRepository(h.t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshRepositoryHeadArgs(
			"repo:" + h.repo + ":metadata",
		).RefreshArgs,
		Queue: queueName,
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *manualBranchHarness) refreshStack(number int, queueName string) {
	h.t.Helper()
	err := h.handler.RefreshStack(h.t.Context(), queue.RefreshRequest{
		Args: queue.NewRefreshStackArgs(
			fmt.Sprintf("stack:%s:%d", h.repo, number),
		).RefreshArgs,
		Queue: queueName,
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *manualBranchHarness) rewindBranchCache(
	branch string,
	currentSHA string,
	oldSHA string,
) {
	h.t.Helper()
	if _, err := h.pool.Exec(h.t.Context(), `
		UPDATE pull_requests AS pull
		SET head_sha = CASE
		        WHEN pull.head_ref = $2 AND pull.head_sha = $3 THEN $4
		        ELSE pull.head_sha
		    END,
		    base_sha = CASE
		        WHEN pull.base_ref = $2 AND pull.base_sha = $3 THEN $4
		        ELSE pull.base_sha
		    END,
		    etag = ''
		FROM repos
		WHERE repos.id = pull.repo_id
		  AND repos.full_name = $1
		  AND pull.state = 'open'
		  AND (pull.head_ref = $2 OR pull.base_ref = $2)
	`, h.repo, branch, currentSHA, oldSHA); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.t.Context(), `
		UPDATE stacks AS stack
		SET entries = (
		        SELECT jsonb_agg(
		            CASE
		              WHEN entry.value->>'head_ref' = $2
		               AND entry.value->>'head_sha' = $3
		              THEN jsonb_set(
		                  entry.value, '{head_sha}', to_jsonb($4::text)
		              )
		              ELSE entry.value
		            END
		            ORDER BY entry.ordinality
		        )
		        FROM jsonb_array_elements(stack.entries)
		             WITH ORDINALITY AS entry(value, ordinality)
		    ),
		    etag = ''
		FROM repos
		WHERE repos.id = stack.repo_id
		  AND repos.full_name = $1
		  AND stack.open
	`, h.repo, branch, currentSHA, oldSHA); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.t.Context(), `
		UPDATE pull_request_change_snapshots AS snapshot
		SET head_sha = CASE
		        WHEN pull.head_ref = $2 AND snapshot.head_sha = $3
		        THEN $4 ELSE snapshot.head_sha
		    END,
		    base_sha = CASE
		        WHEN pull.base_ref = $2 AND snapshot.base_sha = $3
		        THEN $4 ELSE snapshot.base_sha
		    END,
		    codeowners_sha = CASE
		        WHEN pull.base_ref = $2 AND snapshot.codeowners_sha = $3
		        THEN $4 ELSE snapshot.codeowners_sha
		    END,
		    etag = ''
		FROM pull_requests AS pull, repos
		WHERE repos.id = snapshot.repo_id
		  AND pull.repo_id = snapshot.repo_id
		  AND pull.number = snapshot.pr_number
		  AND repos.full_name = $1
		  AND (pull.head_ref = $2 OR pull.base_ref = $2)
	`, h.repo, branch, currentSHA, oldSHA); err != nil {
		h.t.Fatal(err)
	}
	// Preserve a cache state the old per-entity path could actually have
	// produced: dependent rows and their parent snapshot share one exact fence.
	// Rewinding only the snapshot would manufacture a validator association
	// that never existed and make the reference path reuse it incorrectly.
	if _, err := h.pool.Exec(h.t.Context(), `
		UPDATE pull_request_changed_files AS file
		SET head_sha = CASE
		        WHEN pull.head_ref = $2 AND file.head_sha = $3
		        THEN $4 ELSE file.head_sha
		    END,
		    base_sha = CASE
		        WHEN pull.base_ref = $2 AND file.base_sha = $3
		        THEN $4 ELSE file.base_sha
		    END
		FROM pull_requests AS pull, repos
		WHERE repos.id = file.repo_id
		  AND pull.repo_id = file.repo_id
		  AND pull.number = file.pr_number
		  AND repos.full_name = $1
		  AND (pull.head_ref = $2 OR pull.base_ref = $2)
	`, h.repo, branch, currentSHA, oldSHA); err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.t.Context(), `
		UPDATE pull_request_file_owners AS owner
		SET head_sha = CASE
		        WHEN pull.head_ref = $2 AND owner.head_sha = $3
		        THEN $4 ELSE owner.head_sha
		    END,
		    base_sha = CASE
		        WHEN pull.base_ref = $2 AND owner.base_sha = $3
		        THEN $4 ELSE owner.base_sha
		    END
		FROM pull_requests AS pull, repos
		WHERE repos.id = owner.repo_id
		  AND pull.repo_id = owner.repo_id
		  AND pull.number = owner.pr_number
		  AND repos.full_name = $1
		  AND (pull.head_ref = $2 OR pull.base_ref = $2)
	`, h.repo, branch, currentSHA, oldSHA); err != nil {
		h.t.Fatal(err)
	}
}

func (h *manualBranchHarness) applyBranchPage(
	hint *store.BranchPushHint,
) queue.BranchReconcilePageArgs {
	h.t.Helper()
	tx, err := h.pool.Begin(h.t.Context())
	if err != nil {
		h.t.Fatal(err)
	}
	defer tx.Rollback(h.t.Context()) //nolint:errcheck // rollback after commit
	if err := outbox.AcquireWriterFence(h.t.Context(), tx); err != nil {
		h.t.Fatal(err)
	}
	writer := store.NewEntityWriter(h.pool)
	result, err := writer.ApplyBranchPushTx(h.t.Context(), tx, hint)
	if err != nil {
		h.t.Fatal(err)
	}
	pageCounts := []int(nil)
	if len(result.Targets) > 0 {
		pageCounts = []int{len(result.Targets)}
	}
	if err := writer.RecordBranchReconciliationPagesTx(
		h.t.Context(), tx, result.RepoID, hint.Branch, result.Generation,
		pageCounts,
	); err != nil {
		h.t.Fatal(err)
	}
	if err := tx.Commit(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
	targets := make([]queue.BranchReconcileTarget, 0, len(result.Targets))
	for _, target := range result.Targets {
		targets = append(targets, queue.BranchReconcileTarget{
			Kind:       target.RefreshKind,
			Key:        target.RefreshKey,
			EntityKey:  target.EntityKey,
			Generation: target.RefreshGeneration,
		})
	}
	return queue.NewBranchReconcilePageArgs(
		result.RepoID, hint.RepoFullName, hint.Branch,
		result.Generation, 1, targets,
	)
}

func (h *manualBranchHarness) bumpDirectGeneration(
	target queue.BranchReconcileTarget,
) {
	h.t.Helper()
	tx, err := h.pool.Begin(h.t.Context())
	if err != nil {
		h.t.Fatal(err)
	}
	defer tx.Rollback(h.t.Context()) //nolint:errcheck // rollback after commit
	if err := queue.InsertRefreshesTx(
		h.t.Context(), tx, h.river,
		[]queue.RefreshSpec{{Kind: target.Kind, Key: target.Key}},
		queue.QueueEvent,
	); err != nil {
		h.t.Fatal(err)
	}
	if err := tx.Commit(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
}

type pipelineEvent struct {
	event   string
	payload map[string]any
}

type pipelineHarness struct {
	t          *testing.T
	repo       string
	pool       *pgxpool.Pool
	fake       *fakegithub.Server
	fakeServer *httptest.Server
	ingress    *httptest.Server
	river      *river.Client[pgx.Tx]
	dispatcher *dispatch.Dispatcher
	cancel     context.CancelFunc
}

func newPipelineHarness(t *testing.T, repo string) *pipelineHarness {
	t.Helper()
	pool := fetchTestDatabase(t)
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), repo)
	fake := fakegithub.New(fixture, fetchTestSecret)
	fakeServer := httptest.NewServer(fake)
	gate := budget.New(fakeServer.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		fakeServer.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		fakeServer.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(&Options{
		Pool:           pool,
		REST:           rest,
		GraphQL:        graphQL,
		InstallationID: 1,
		OrgID:          1,
		BatchWindow:    5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithRefreshHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetRiverClient(riverClient)
	ctx, cancel := context.WithCancel(context.Background())
	if err := riverClient.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	ingressServer := httptest.NewServer(ingress.NewMux(
		ingress.NewHandler(
			dbgen.New(pool),
			fetchTestSecret,
			1<<20,
			5*time.Second,
		),
	))
	dispatcher, err := dispatch.New(pool, riverClient, dispatch.Config{
		BatchSize:    100,
		MaxAttempts:  3,
		Debounce:     time.Millisecond,
		PollInterval: time.Millisecond,
		Now:          time.Now,
		Classifier:   dispatch.DefaultClassifier(),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return &pipelineHarness{
		t:          t,
		repo:       repo,
		pool:       pool,
		fake:       fake,
		fakeServer: fakeServer,
		ingress:    ingressServer,
		river:      riverClient,
		dispatcher: dispatcher,
		cancel:     cancel,
	}
}

func (h *pipelineHarness) emit(guid string, event pipelineEvent) {
	h.t.Helper()
	payload := clonePayload(h.t, event.payload)
	payload["repository"] = map[string]any{"full_name": h.repo}
	if _, err := h.fake.EmitWebhookWithGUID(
		context.Background(),
		h.ingress.URL+ingress.WebhookPath,
		event.event,
		guid,
		payload,
	); err != nil {
		h.t.Fatal(err)
	}
}

func (h *pipelineHarness) dispatchAll() {
	h.t.Helper()
	for {
		count, err := h.dispatcher.DispatchBatch(context.Background())
		if err != nil {
			h.t.Fatal(err)
		}
		if count == 0 {
			return
		}
	}
}

func (h *pipelineHarness) waitIdle() {
	h.t.Helper()
	events, unsubscribe := h.river.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
	)
	defer unsubscribe()
	for {
		var count int
		if err := h.pool.QueryRow(h.t.Context(), `
			SELECT count(*)
			FROM river_job
			WHERE state IN ('available', 'pending', 'retryable', 'running', 'scheduled')
		`).Scan(&count); err != nil {
			h.t.Fatal(err)
		}
		if count == 0 {
			return
		}
		select {
		case <-events:
		case <-h.t.Context().Done():
			var states string
			_ = h.pool.QueryRow(context.Background(), `
				SELECT string_agg(
					kind || ':' || state,
					', ' ORDER BY kind, state
				)
				FROM river_job
				WHERE state IN (
					'available', 'pending', 'retryable',
					'running', 'scheduled'
				)
			`).Scan(&states)
			h.t.Fatalf("pipeline did not quiesce: %s", states)
		}
	}
}

func (h *pipelineHarness) close() {
	h.ingress.Close()
	h.cancel()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.river.StopAndCancel(stopCtx)
	h.fakeServer.Close()
}

type cacheSnapshot struct {
	Repos        string
	RepoRules    string
	Stacks       string
	Pulls        string
	Threads      string
	Checks       string
	CheckHistory string
	Dirty        string
}

func snapshotCache(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
) cacheSnapshot {
	t.Helper()
	return cacheSnapshot{
		Repos: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data)), '[]'::jsonb)
			FROM (
				SELECT gh_id, node_id, full_name, default_branch, archived,
				       to_char(gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       head_sha, sync_source,
				       tombstoned_at IS NOT NULL AS tombstoned
				FROM repos WHERE full_name = $1
			) AS row_data
		`, repo),
		RepoRules: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY rule_key),
			                '[]'::jsonb)
			FROM (
				SELECT repo_rules.rule_key, repo_rules.rule,
				       to_char(repo_rules.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       repo_rules.head_sha, repo_rules.sync_source,
				       repo_rules.tombstoned_at IS NOT NULL AS tombstoned
				FROM repo_rules JOIN repos ON repos.id = repo_rules.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Stacks: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY number), '[]'::jsonb)
			FROM (
				SELECT stacks.number, stacks.gh_id, stacks.node_id,
				       stacks.base_ref, stacks.base_sha, stacks.open,
				       stacks.entries,
				       to_char(stacks.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       stacks.head_sha,
				       stacks.sync_source,
				       stacks.tombstoned_at IS NOT NULL AS tombstoned
				FROM stacks JOIN repos ON repos.id = stacks.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Pulls: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY number), '[]'::jsonb)
			FROM (
				SELECT pull_requests.number, pull_requests.gh_id,
				       pull_requests.node_id, pull_requests.title,
				       pull_requests.state, pull_requests.head_ref,
				       pull_requests.head_sha, pull_requests.base_ref,
				       pull_requests.base_sha, pull_requests.review_decision,
				       pull_requests.stack_number, pull_requests.stack_position,
				       to_char(pull_requests.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       pull_requests.sync_source,
				       pull_requests.tombstoned_at IS NOT NULL AS tombstoned
				FROM pull_requests JOIN repos ON repos.id = pull_requests.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Threads: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY id), '[]'::jsonb)
			FROM (
				SELECT review_threads.id, review_threads.pr_number,
				       review_threads.is_resolved, review_threads.is_outdated,
				       review_threads.path, review_threads.line,
				       review_threads.comments,
				       to_char(review_threads.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       review_threads.head_sha, review_threads.sync_source,
				       review_threads.tombstoned_at IS NOT NULL AS tombstoned
				FROM review_threads JOIN repos ON repos.id = review_threads.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Checks: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY gh_id), '[]'::jsonb)
			FROM (
				SELECT check_runs.gh_id, check_runs.name, check_runs.status,
				       check_runs.conclusion, check_runs.head_sha,
				       to_char(check_runs.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       check_runs.sync_source,
				       check_runs.tombstoned_at IS NOT NULL AS tombstoned
				FROM check_runs JOIN repos ON repos.id = check_runs.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		CheckHistory: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(to_jsonb(row_data)
				ORDER BY check_run_gh_id, status, conclusion), '[]'::jsonb)
			FROM (
				SELECT check_history.check_run_gh_id, check_history.name,
				       check_history.status, check_history.conclusion,
				       to_char(check_history.gh_updated_at AT TIME ZONE 'UTC',
				               'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS gh_updated_at,
				       check_history.head_sha,
				       check_history.sync_source
				FROM check_history JOIN repos ON repos.id = check_history.repo_id
				WHERE repos.full_name = $1
			) AS row_data
		`, repo),
		Dirty: queryJSON(t, pool, `
			SELECT COALESCE(jsonb_agg(scope_key ORDER BY scope_key), '[]'::jsonb)
			FROM derivation_dirty
			WHERE scope_key LIKE (
				SELECT '%:' || installation_id::text || ':' ||
				       gh_id::text || ':%'
				FROM repos WHERE full_name = $1
			)
		`, repo),
	}
}

func queryJSON(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	arg string,
) string {
	t.Helper()
	var value []byte
	if err := pool.QueryRow(context.Background(), query, arg).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func newDirectHandler(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture fakegithub.Fixture, //nolint:gocritic // helper snapshots fixture input before the fake server clones it
	batchWindow time.Duration,
	pageSize int,
	fakeOptions ...fakegithub.Option,
) (
	*fakegithub.Server,
	*httptest.Server,
	*Handler,
	*river.Client[pgx.Tx],
) {
	return newDirectHandlerWithMiddleware(
		t,
		pool,
		fixture,
		batchWindow,
		pageSize,
		nil,
		fakeOptions...,
	)
}

func newDirectHandlerWithMiddleware(
	t *testing.T,
	pool *pgxpool.Pool,
	fixture fakegithub.Fixture, //nolint:gocritic // helper snapshots fixture input before the fake server clones it
	batchWindow time.Duration,
	pageSize int,
	middleware func(http.Handler) http.Handler,
	fakeOptions ...fakegithub.Option,
) (
	*fakegithub.Server,
	*httptest.Server,
	*Handler,
	*river.Client[pgx.Tx],
) {
	t.Helper()
	fake := fakegithub.New(fixture, fetchTestSecret, fakeOptions...)
	var serverHandler http.Handler = fake
	if middleware != nil {
		serverHandler = middleware(serverHandler)
	}
	server := httptest.NewServer(serverHandler)
	gate := budget.New(server.Client(), budget.Options{})
	rest, err := gh.NewRESTClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("fake-installation-fetch"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := New(&Options{
		Pool:             pool,
		REST:             rest,
		GraphQL:          graphQL,
		InstallationID:   1,
		OrgID:            1,
		BatchWindow:      batchWindow,
		BackfillPageSize: pageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	riverClient, err := queue.NewClient(
		pool,
		queue.WithRefreshHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	return fake, server, handler, riverClient
}

func fixtureForRepo(fixture fakegithub.Fixture, fullName string) fakegithub.Fixture { //nolint:gocritic // helper intentionally mutates and returns an isolated fixture copy
	owner, name, _ := strings.Cut(fullName, "/")
	fixture.Owner = owner
	fixture.Repo = name
	fixture.Repository.Owner = owner
	fixture.Repository.Name = name
	fixture.Repository.FullName = fullName
	fixture.Repository.ID += int64(len(fullName) * 100)
	fixture.Repository.NodeID = fmt.Sprintf("R_%s_%s", owner, name)
	return fixture
}

func testRepository(
	fullName string,
	id int64,
	updatedAt time.Time,
) store.RepositoryRecord {
	owner, name, _ := strings.Cut(fullName, "/")
	return store.RepositoryRecord{
		InstallationID:  1,
		OrgID:           1,
		GitHubID:        id,
		NodeID:          fmt.Sprintf("repo-node-%d", id),
		Owner:           owner,
		Name:            name,
		FullName:        fullName,
		DefaultBranch:   "main",
		DefaultHeadSHA:  "base",
		GitHubUpdatedAt: updatedAt,
	}
}

func testPull(
	repository *store.RepositoryRecord,
	updatedAt time.Time,
	headSHA string,
) store.PullRequestRecord {
	return store.PullRequestRecord{
		Repository:      *repository,
		GitHubID:        4200,
		NodeID:          "pr-node-42",
		Number:          42,
		Title:           "race",
		State:           "open",
		HeadRef:         "feature",
		HeadSHA:         headSHA,
		BaseRef:         "main",
		BaseSHA:         "base",
		MembershipKnown: true,
		GitHubUpdatedAt: updatedAt,
		SyncedAt:        updatedAt.Add(time.Second),
		Source:          store.SyncSourceWebhook,
	}
}

func clonePayload(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	cloned := make(map[string]any)
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func toGHPullRequest(
	t *testing.T,
	pull *fakegithub.PullRequest,
) *gh.PullRequest {
	t.Helper()
	encoded, err := json.Marshal(pull)
	if err != nil {
		t.Fatal(err)
	}
	var converted gh.PullRequest
	if err := json.Unmarshal(encoded, &converted); err != nil {
		t.Fatal(err)
	}
	return &converted
}

func fetchTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.New(t).Pool
}

func fetchPoolWithStatementTimeout(
	t *testing.T,
	databaseURL string,
	timeout time.Duration,
) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("statement_timeout", timeout.String())
	query.Set("application_name", fmt.Sprintf(
		"fetch-lock-waiter-%d",
		time.Now().UnixNano(),
	))
	parsed.RawQuery = query.Encode()
	pool, err := store.Connect(t.Context(), parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func fetchPoolWithMaxConns(
	t *testing.T,
	databaseURL string,
	maxConns int32,
) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func seedCoordinatorPulls(t *testing.T, handler *Handler, numbers ...int) {
	t.Helper()
	for _, number := range numbers {
		if err := handler.ResolveStackMembership(
			t.Context(),
			queue.RefreshRequest{
				Args: queue.NewResolveStackMembershipArgs(
					fmt.Sprintf("pr:acme/monolith:%d", number),
				).RefreshArgs,
				Queue: queue.QueueEvent,
			},
		); err != nil {
			t.Fatalf("seed PR %d: %v", number, err)
		}
	}
}

func refreshPRRequest(number int) queue.RefreshRequest {
	return queue.RefreshRequest{
		Args: queue.NewRefreshPRArgs(
			fmt.Sprintf("pr:acme/monolith:%d", number),
		).RefreshArgs,
		Queue: queue.QueueEvent,
	}
}

func waitForFakeRequest(
	t *testing.T,
	fake *fakegithub.Server,
	method string,
	path string,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for fake.RequestCount(method, path) == 0 || fake.Concurrent() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("fake request %s %s did not enter delayed response", method, path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertObservationLockAvailability(
	t *testing.T,
	connection *pgxpool.Conn,
	key string,
	want bool,
) {
	t.Helper()
	var acquired bool
	if err := connection.QueryRow(
		t.Context(),
		`SELECT pg_try_advisory_lock(hashtextextended($1::text, 0))`,
		key,
	).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if acquired != want {
		t.Fatalf("advisory lock %q available = %t, want %t", key, acquired, want)
	}
	if !acquired {
		return
	}
	var unlocked bool
	if err := connection.QueryRow(
		t.Context(),
		`SELECT pg_advisory_unlock(hashtextextended($1::text, 0))`,
		key,
	).Scan(&unlocked); err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Fatalf("release advisory lock probe %q returned false", key)
	}
}

type cachedReviewRequest struct {
	Kind        string
	ID          int64
	NodeID      string
	Login       string
	RequestedAt *time.Time
	FirstSeenAt time.Time
	HeadSHA     string
	Source      string
}

func readCachedReviewRequests(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
	prNumber int,
) []cachedReviewRequest {
	t.Helper()
	rows, err := pool.Query(t.Context(), `
		SELECT request.reviewer_kind, request.reviewer_gh_id,
		       request.reviewer_node_id, request.reviewer_login,
		       request.requested_at, request.first_seen_at,
		       request.head_sha, request.sync_source
		FROM pull_request_review_requests AS request
		JOIN repos ON repos.id = request.repo_id
		WHERE repos.full_name = $1
		  AND request.pr_number = $2
		  AND request.tombstoned_at IS NULL
		ORDER BY request.reviewer_kind, request.reviewer_gh_id
	`, repo, prNumber)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []cachedReviewRequest
	for rows.Next() {
		var row cachedReviewRequest
		if err := rows.Scan(
			&row.Kind,
			&row.ID,
			&row.NodeID,
			&row.Login,
			&row.RequestedAt,
			&row.FirstSeenAt,
			&row.HeadSHA,
			&row.Source,
		); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func pullChangedEvents(
	t *testing.T,
	pool *pgxpool.Pool,
	repo string,
) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM change_events
		JOIN repos
		  ON change_events.entity_key =
		     'pr:' || repos.installation_id || ':' || repos.gh_id || ':4812'
		WHERE repos.full_name = $1
		  AND change_events.kind = 'pull_request.changed'
	`, repo).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
