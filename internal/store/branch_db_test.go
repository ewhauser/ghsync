package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func TestApplyBranchPushBulkTransitionIsFencedAndIdempotent(t *testing.T) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/branch-bulk", 4801, now)
	repository.DefaultHeadSHA = "base-old"

	pull := storeTestPull(&repository, now, "feature-head")
	pull.Number, pull.GitHubID, pull.NodeID = 41, 4841, "pr-node-41"
	pull.BaseSHA = "base-old"
	pull.ReviewDecision = "APPROVED"
	pull.MergeableState = "MERGEABLE"
	pull.ETag = "pull-etag"
	pull.ChangeInputsKnown = true
	pull.ChangeSnapshot = &PullRequestChangeSnapshotRecord{
		BaseSHA: "base-old", HeadSHA: "feature-head",
		ETag: "files-etag", FilesTotalCount: 1,
		CodeownersRef: "main", CodeownersSHA: "base-old",
		CodeownersPath: "CODEOWNERS", CodeownersState: "present",
		CodeownersSource: "*.go @owner", CodeownersHash: "owner-hash",
		CodeownersETag: "owners-etag",
		Files: []ChangedFileRecord{{
			Path: "main.go", ChangeType: "modified",
		}},
		Owners: []FileOwnerRecord{{
			Path: "main.go", OwnerToken: "@owner", OwnerType: "user",
			OwnerName: "owner", ResolutionState: "unresolved",
			SourcePattern: "*.go", SourceLine: 1,
		}},
	}
	if _, err := writer.ApplyPullRequest(t.Context(), pull); err != nil {
		t.Fatal(err)
	}

	headPull := storeTestPull(&repository, now, "base-old")
	headPull.Number, headPull.GitHubID, headPull.NodeID = 42, 4842, "pr-node-42"
	headPull.HeadRef, headPull.BaseRef, headPull.BaseSHA = "main", "release", "release-base"
	if _, err := writer.ApplyPullRequest(t.Context(), headPull); err != nil {
		t.Fatal(err)
	}

	// A mismatched cached fence is still reconciled remotely, but cannot be
	// overwritten by the local hint.
	mismatch := storeTestPull(&repository, now, "mismatch-head")
	mismatch.Number, mismatch.GitHubID, mismatch.NodeID = 43, 4843, "pr-node-43"
	mismatch.BaseSHA = "unexpected-base"
	if _, err := writer.ApplyPullRequest(t.Context(), mismatch); err != nil {
		t.Fatal(err)
	}
	unrelated := storeTestPull(&repository, now, "unrelated-head")
	unrelated.Number, unrelated.GitHubID, unrelated.NodeID =
		44, 4844, "pr-node-44"
	unrelated.HeadRef, unrelated.BaseRef, unrelated.BaseSHA =
		"feature/unrelated", "release", "release-base"
	if _, err := writer.ApplyPullRequest(t.Context(), unrelated); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO refresh_intent_generations (
		    kind, refresh_key, generation, completed_generation,
		    event_received_at
		) VALUES ('refresh_pr', 'pr:acme/branch-bulk:44', 1, 0, $1)
	`, now); err != nil {
		t.Fatal(err)
	}

	stack := StackRecord{
		Repository: repository, GitHubID: 4901, NodeID: "stack-node-1",
		Number: 1, BaseRef: "main", BaseSHA: "base-old", Open: true,
		Entries: []StackEntry{{
			Number: 41, State: "open", UpdatedAt: now,
			HeadRef: "feature", HeadSHA: "feature-head",
		}},
		GitHubUpdatedAt: now, SyncedAt: now.Add(time.Second),
		Source: SyncSourceReconcile, ETag: "stack-etag",
	}
	if _, err := writer.ApplyStack(t.Context(), stack); err != nil {
		t.Fatal(err)
	}
	var wantSource string
	var wantGitHubUpdatedAt, wantSyncedAt time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT pull_requests.sync_source, pull_requests.gh_updated_at,
		       pull_requests.synced_at
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.gh_id = $1 AND pull_requests.number = 41
	`, repository.GitHubID).Scan(
		&wantSource, &wantGitHubUpdatedAt, &wantSyncedAt,
	); err != nil {
		t.Fatal(err)
	}

	hint := BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-old", AfterSHA: "base-new", TransitionKnown: true,
		DeliveryGUID: "delivery-bulk-1", ReceivedAt: now.Add(time.Minute),
	}
	first := applyBranchPushTestTx(t, pool, writer, &hint)
	if first.Repositories != 1 || first.PullRequests != 2 || first.Stacks != 1 {
		t.Fatalf("bulk changed counts = %+v", first)
	}
	if len(first.Targets) != 4 {
		t.Fatalf("bulk targets = %d, want 4: %+v", len(first.Targets), first.Targets)
	}
	if first.RepositoryRefreshKey != "repo:acme/branch-bulk:metadata" {
		t.Fatalf(
			"default-branch repository reconciliation key = %q",
			first.RepositoryRefreshKey,
		)
	}
	var repositoryGeneration, repositoryCompleted int64
	if err := pool.QueryRow(t.Context(), `
		SELECT generation, completed_generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_repository'
		  AND refresh_key = 'repo:acme/branch-bulk:metadata'
	`).Scan(&repositoryGeneration, &repositoryCompleted); err != nil {
		t.Fatal(err)
	}
	if repositoryGeneration != repositoryCompleted || repositoryGeneration != 1 {
		t.Fatalf(
			"repository branch fence generation=%d completed=%d, want 1/1",
			repositoryGeneration, repositoryCompleted,
		)
	}
	var unrelatedGeneration, unrelatedCompleted int64
	if err := pool.QueryRow(t.Context(), `
		SELECT generation, completed_generation
		FROM refresh_intent_generations
		WHERE kind = 'refresh_pr'
		  AND refresh_key = 'pr:acme/branch-bulk:44'
	`).Scan(&unrelatedGeneration, &unrelatedCompleted); err != nil {
		t.Fatal(err)
	}
	if unrelatedGeneration != 1 || unrelatedCompleted != 0 {
		t.Fatalf(
			"unrelated direct PR generation was consumed by branch bulk: %d/%d",
			unrelatedGeneration, unrelatedCompleted,
		)
	}

	var repoHead, baseSHA, reviewDecision, mergeable, etag, syncSource string
	var githubUpdatedAt, syncedAt time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT repos.head_sha, pull_requests.base_sha,
		       pull_requests.review_decision, pull_requests.mergeable_state,
		       pull_requests.etag, pull_requests.sync_source,
		       pull_requests.gh_updated_at, pull_requests.synced_at
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.gh_id = $1 AND pull_requests.number = 41
	`, repository.GitHubID).Scan(
		&repoHead, &baseSHA, &reviewDecision, &mergeable, &etag,
		&syncSource, &githubUpdatedAt, &syncedAt,
	); err != nil {
		t.Fatal(err)
	}
	if repoHead != "base-new" || baseSHA != "base-new" ||
		reviewDecision != "" || mergeable != "" || etag != "" {
		t.Fatalf(
			"bulk-visible row = repo=%q base=%q review=%q mergeable=%q etag=%q",
			repoHead, baseSHA, reviewDecision, mergeable, etag,
		)
	}
	if syncSource != wantSource ||
		!githubUpdatedAt.Equal(wantGitHubUpdatedAt) ||
		!syncedAt.Equal(wantSyncedAt) {
		t.Fatalf(
			"authoritative provenance changed: source=%q gh=%s synced=%s",
			syncSource, githubUpdatedAt, syncedAt,
		)
	}
	exactRedelivery := applyBranchPushTestTx(t, pool, writer, &hint)
	if exactRedelivery.RepoID != 0 || exactRedelivery.Generation != 0 ||
		len(exactRedelivery.Targets) != 0 {
		t.Fatalf("same delivery was not an idempotent no-op: %+v", exactRedelivery)
	}
	var liveSnapshots, liveFiles, liveOwners int
	if err := pool.QueryRow(t.Context(), `
		SELECT
		  (SELECT count(*) FROM pull_request_change_snapshots AS snapshot
		   WHERE snapshot.repo_id = repos.id AND snapshot.pr_number = 41
		     AND snapshot.tombstoned_at IS NULL),
		  (SELECT count(*) FROM pull_request_changed_files AS file
		   WHERE file.repo_id = repos.id AND file.pr_number = 41
		     AND file.tombstoned_at IS NULL),
		  (SELECT count(*) FROM pull_request_file_owners AS owner
		   WHERE owner.repo_id = repos.id AND owner.pr_number = 41
		     AND owner.tombstoned_at IS NULL)
		FROM repos
		WHERE repos.gh_id = $1
	`, repository.GitHubID).Scan(&liveSnapshots, &liveFiles, &liveOwners); err != nil {
		t.Fatal(err)
	}
	if liveSnapshots != 1 || liveFiles != 1 || liveOwners != 1 {
		t.Fatalf(
			"retained historical inputs = snapshot:%d files:%d owners:%d",
			liveSnapshots, liveFiles, liveOwners,
		)
	}
	// Read paths must fail closed on retained historical rows whose SHA fence
	// no longer matches the transitioned PR.
	metadata, err := dbgen.New(pool).GetPullRequestFetchMetadata(
		t.Context(),
		dbgen.GetPullRequestFetchMetadataParams{
			RepoFullName: repository.FullName,
			PrNumber:     41,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.CodeownersState != "" || metadata.CodeownersEtag != "" {
		t.Fatalf(
			"mismatched snapshot validator was readable: state=%q etag=%q",
			metadata.CodeownersState, metadata.CodeownersEtag,
		)
	}
	filesMetadata, err := dbgen.New(pool).ListPullRequestChangeFetchMetadata(
		t.Context(),
		dbgen.ListPullRequestChangeFetchMetadataParams{
			RepoFullName: repository.FullName,
			PrNumber:     41,
			BaseSha:      "base-new",
			HeadSha:      "feature-head",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(filesMetadata) != 0 {
		t.Fatalf("mismatched changed-file snapshot was readable: %+v", filesMetadata)
	}
	var mismatchedBase string
	if err := pool.QueryRow(t.Context(), `
		SELECT base_sha FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.gh_id = $1 AND pull_requests.number = 43
	`, repository.GitHubID).Scan(&mismatchedBase); err != nil {
		t.Fatal(err)
	}
	if mismatchedBase != "unexpected-base" {
		t.Fatalf("mismatched base overwritten with %q", mismatchedBase)
	}

	semanticDuplicate := hint
	semanticDuplicate.DeliveryGUID = "delivery-bulk-semantic-duplicate"
	semanticDuplicate.ReceivedAt = semanticDuplicate.ReceivedAt.Add(time.Second)
	second := applyBranchPushTestTx(t, pool, writer, &semanticDuplicate)
	if second.Repositories != 0 || second.PullRequests != 0 || second.Stacks != 0 {
		t.Fatalf("semantic duplicate changed cache: %+v", second)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf(
			"semantic duplicate generation = %d, want %d",
			second.Generation, first.Generation+1,
		)
	}
	deleted := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-new", AfterSHA: "", TransitionKnown: true,
		Deleted: true, DeliveryGUID: "delivery-bulk-delete",
		ReceivedAt: now.Add(2 * time.Minute),
	})
	if deleted.Repositories != 1 || deleted.PullRequests != 2 || deleted.Stacks != 1 {
		t.Fatalf("deletion changed counts = %+v", deleted)
	}
	// A delayed pre-deletion delivery cannot move an exact-CAS fence backward.
	stale := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-old", AfterSHA: "stale-redelivery", TransitionKnown: true,
		DeliveryGUID: "delivery-bulk-out-of-order",
		ReceivedAt:   now.Add(3 * time.Minute),
	})
	if stale.Repositories != 0 || stale.PullRequests != 0 || stale.Stacks != 0 {
		t.Fatalf("out-of-order delivery moved cache backward: %+v", stale)
	}
	var finalRepoHead, finalPullBase, finalStackBase string
	if err := pool.QueryRow(t.Context(), `
		SELECT repos.head_sha, pull_requests.base_sha, stacks.base_sha
		FROM repos
		JOIN pull_requests
		  ON pull_requests.repo_id = repos.id AND pull_requests.number = 41
		JOIN stacks ON stacks.repo_id = repos.id AND stacks.number = 1
		WHERE repos.gh_id = $1
	`, repository.GitHubID).Scan(
		&finalRepoHead, &finalPullBase, &finalStackBase,
	); err != nil {
		t.Fatal(err)
	}
	if finalRepoHead != "" || finalPullBase != "" || finalStackBase != "" {
		t.Fatalf(
			"post-delete fences = repo:%q pull:%q stack:%q",
			finalRepoHead, finalPullBase, finalStackBase,
		)
	}
}

func TestBranchReconciliationFenceRejectsNewerDirectAndBranchWork(t *testing.T) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	now := time.Date(2026, 8, 5, 13, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/branch-fence", 4802, now)
	repository.DefaultHeadSHA = "base-old"
	pull := storeTestPull(&repository, now, "feature-head")
	pull.BaseSHA = "base-old"
	if _, err := writer.ApplyPullRequest(t.Context(), pull); err != nil {
		t.Fatal(err)
	}
	first := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-old", AfterSHA: "base-new", TransitionKnown: true,
		DeliveryGUID: "delivery-fence-1", ReceivedAt: now.Add(time.Minute),
	})
	if len(first.Targets) != 1 {
		t.Fatalf("targets = %+v", first.Targets)
	}
	target := first.Targets[0]

	if _, err := pool.Exec(t.Context(), `
		UPDATE refresh_intent_generations
		SET generation = generation + 1
		WHERE kind = $1 AND refresh_key = $2
	`, target.RefreshKind, target.RefreshKey); err != nil {
		t.Fatal(err)
	}

	newer := pull
	newer.BaseSHA = "authoritative-newer"
	newer.Title = "must not land from stale page"
	newer.GitHubUpdatedAt = now.Add(2 * time.Minute)
	newer.SyncedAt = now.Add(2 * time.Minute)
	observation, err := writer.BeginObservation(
		t.Context(),
		PullRequestEntityKey(1, repository.GitHubID, pull.Number),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close() //nolint:errcheck // test cleanup
	fenced := WithBranchReconciliationFence(
		t.Context(),
		&BranchReconciliationFence{
			RepoID: first.RepoID, Branch: "main",
			BranchGeneration: first.Generation,
			RefreshKind:      target.RefreshKind, RefreshKey: target.RefreshKey,
			RefreshGeneration: target.RefreshGeneration,
			EntityKey:         target.EntityKey,
		},
	)
	_, err = writer.ApplyPullRequestObserved(fenced, observation, newer, nil)
	if !errors.Is(err, ErrBranchReconciliationSuperseded) {
		t.Fatalf("direct-generation fence error = %v", err)
	}

	second := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-new", AfterSHA: "base-forced", TransitionKnown: true,
		Forced: true, DeliveryGUID: "delivery-fence-2",
		ReceivedAt: now.Add(3 * time.Minute),
	})
	if second.Generation <= first.Generation {
		t.Fatalf("new branch generation = %d", second.Generation)
	}
	// Make the entity generation current again so this attempt proves the
	// independent branch-generation half of the fence.
	if len(second.Targets) != 1 {
		t.Fatalf("second targets = %+v", second.Targets)
	}
	target.RefreshGeneration = second.Targets[0].RefreshGeneration
	fenced = WithBranchReconciliationFence(
		t.Context(),
		&BranchReconciliationFence{
			RepoID: first.RepoID, Branch: "main",
			BranchGeneration: first.Generation,
			RefreshKind:      target.RefreshKind, RefreshKey: target.RefreshKey,
			RefreshGeneration: target.RefreshGeneration,
			EntityKey:         target.EntityKey,
		},
	)
	_, err = writer.ApplyPullRequestObserved(fenced, observation, newer, nil)
	if !errors.Is(err, ErrBranchReconciliationSuperseded) {
		t.Fatalf("branch-generation fence error = %v", err)
	}
}

func TestDirectRefreshGenerationFenceRejectsBulkSupersededObservation(
	t *testing.T,
) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	now := time.Date(2026, 8, 5, 13, 30, 0, 0, time.UTC)
	repository := storeTestRepository("acme/direct-branch-fence", 4812, now)
	repository.DefaultHeadSHA = "base-old"
	pull := storeTestPull(&repository, now, "feature-head")
	pull.BaseSHA = "base-old"
	if _, err := writer.ApplyPullRequest(t.Context(), pull); err != nil {
		t.Fatal(err)
	}
	kind := "refresh_pr"
	refreshKey := "pr:" + repository.FullName + ":" + fmt.Sprint(pull.Number)
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO refresh_intent_generations (
		    kind, refresh_key, generation, completed_generation
		) VALUES ($1, $2, 1, 0)
	`, kind, refreshKey); err != nil {
		t.Fatal(err)
	}
	observation, err := writer.BeginObservation(
		t.Context(),
		PullRequestEntityKey(1, repository.GitHubID, pull.Number),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer observation.Close() //nolint:errcheck // test cleanup

	bulk := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base-old", AfterSHA: "base-new", TransitionKnown: true,
		DeliveryGUID: "delivery-direct-fence", ReceivedAt: now.Add(time.Minute),
	})
	if len(bulk.Targets) != 1 || bulk.Targets[0].RefreshGeneration != 2 {
		t.Fatalf("bulk target generations = %+v", bulk.Targets)
	}

	stale := pull
	stale.Title = "stale in-flight response"
	stale.GitHubUpdatedAt = now.Add(2 * time.Minute)
	stale.SyncedAt = stale.GitHubUpdatedAt
	fenced := WithRefreshGenerationFence(t.Context(), RefreshGenerationFence{
		Kind: kind, RefreshKey: refreshKey, Generation: 1,
	})
	_, err = writer.ApplyPullRequestObserved(fenced, observation, stale, nil)
	if !errors.Is(err, ErrRefreshGenerationSuperseded) {
		t.Fatalf("stale direct generation error = %v", err)
	}
	var baseSHA, title string
	if err := pool.QueryRow(t.Context(), `
		SELECT pull_requests.base_sha, pull_requests.title
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE repos.gh_id = $1 AND pull_requests.number = $2
	`, repository.GitHubID, pull.Number).Scan(&baseSHA, &title); err != nil {
		t.Fatal(err)
	}
	if baseSHA != "base-new" || title == stale.Title {
		t.Fatalf("stale direct response landed: base=%q title=%q", baseSHA, title)
	}
}

func applyBranchPushTestTx(
	t *testing.T,
	pool interface {
		Begin(context.Context) (pgx.Tx, error)
	},
	writer *EntityWriter,
	hint *BranchPushHint,
) BranchBulkResult {
	t.Helper()
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context()) //nolint:errcheck // rollback after commit
	if err := outbox.AcquireWriterFence(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	result, err := writer.ApplyBranchPushTx(t.Context(), tx, hint)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBranchPageStateUsesDurablePostgresClock(t *testing.T) {
	t.Parallel()
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	now := time.Date(2026, 8, 5, 14, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/branch-pages", 4803, now)
	if _, err := writer.ApplyRepository(
		t.Context(), repository, SyncSourceReconcile, "", now,
	); err != nil {
		t.Fatal(err)
	}
	bulk := applyBranchPushTestTx(t, pool, writer, &BranchPushHint{
		RepoFullName: repository.FullName, Branch: "main",
		BeforeSHA: "base", AfterSHA: "next", TransitionKnown: true,
		DeliveryGUID: "delivery-pages", ReceivedAt: now.Add(time.Minute),
	})
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context()) //nolint:errcheck // rollback after commit
	if err := writer.RecordBranchReconciliationPagesTx(
		t.Context(), tx, bulk.RepoID, "main", bulk.Generation, nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	var completed bool
	if err := pool.QueryRow(t.Context(), `
		SELECT completed_at IS NOT NULL
		FROM branch_reconciliations
		WHERE repo_id = $1 AND branch = 'main'
	`, bulk.RepoID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("zero-page branch reconciliation was not completed")
	}
	backlog, err := dbgen.New(pool).CollectBranchReconciliationBacklog(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if backlog.PendingPages != 0 || backlog.PendingTargets != 0 {
		t.Fatalf("zero-page backlog = %+v", backlog)
	}
}
