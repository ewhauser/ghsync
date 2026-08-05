// Package outbox owns the database-level protocol shared by every internal
// change-event writer and by the visibility watermarker.
package outbox

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	// FenceKey is a stable, database-wide advisory-lock key ("ghsync" in
	// ASCII). Writers hold its shared transaction lock before allocating an
	// outbox sequence; the watermarker briefly takes the exclusive counterpart.
	FenceKey int64 = 0x676873796e63

	EntitiesStream  = "entities"
	WorkItemsStream = "work_items"

	RepositoryChangedKind     = "repository.changed"
	RepositoryTombstonedKind  = "repository.tombstoned"
	PullRequestChangedKind    = "pull_request.changed"
	PullRequestTombstonedKind = "pull_request.tombstoned"
	StackChangedKind          = "stack.changed"
	StackTombstonedKind       = "stack.tombstoned"
	ChecksChangedKind         = "checks.changed"
	RepoRulesChangedKind      = "repo_rules.changed"
	WorkItemChangedKind       = "work_item.changed"
	WorkItemRemovedKind       = "work_item.removed"

	EntityWriterOrigin = "entity_writer"
	DeriverOrigin      = "deriver"

	SharedWriterFence       FenceRole = "shared_writer"
	ExclusiveWatermarkFence FenceRole = "exclusive_watermarker"
)

// FenceRole identifies one side of the C-S2 advisory fence.
type FenceRole string

// FenceObservation separates time queued for the advisory lock from time the
// acquired transaction retained it. Acquired is false when lock acquisition
// failed, in which case HoldDuration is zero.
type FenceObservation struct {
	Role         FenceRole
	WaitDuration time.Duration
	HoldDuration time.Duration
	Acquired     bool
}

// FenceObserver receives completed C-S2 fence attempts. Implementations must
// not block: successful observations run immediately after Commit or Rollback.
type FenceObserver interface {
	OutboxFence(context.Context, FenceObservation)
}

type observedFenceTx struct {
	pgx.Tx
	observer    FenceObserver
	observation FenceObservation
	acquiredAt  time.Time
	once        sync.Once
}

func (tx *observedFenceTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	tx.observe(ctx)
	return err
}

func (tx *observedFenceTx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	tx.observe(ctx)
	return err
}

func (tx *observedFenceTx) observe(ctx context.Context) {
	tx.once.Do(func() {
		tx.observation.HoldDuration = time.Since(tx.acquiredAt)
		tx.observer.OutboxFence(ctx, tx.observation)
	})
}

type sequenceAllocationHookKey struct{}

// WithSequenceAllocationHook installs a test-only transaction pause/failure
// seam that runs immediately after a real change_events sequence allocation.
// It lives beside the fence protocol so tests cannot replace the production
// entity-writer or deriver transaction with a synthetic writer. C-C6 still
// applies: the hook may coordinate or fail a test, but must not perform network
// I/O while the transaction and writer fence are open.
func WithSequenceAllocationHook(
	ctx context.Context,
	hook func(origin string, seq int64) error,
) context.Context {
	return context.WithValue(ctx, sequenceAllocationHookKey{}, hook)
}

// AfterSequenceAllocated runs the context hook, when present.
func AfterSequenceAllocated(
	ctx context.Context,
	origin string,
	seq int64,
) error {
	hook, _ := ctx.Value(sequenceAllocationHookKey{}).(func(string, int64) error)
	if hook == nil {
		return nil
	}
	return hook(origin, seq)
}

// Definition is one v1 event variant in the public change-stream contract.
// db/CONTRACT.md is schema-tested against this manifest.
type Definition struct {
	Stream           string
	Kind             string
	EntityKeyGrammar string
	LookupTarget     string
	PayloadRule      string
}

// V1Definitions enumerates every event variant emitted by internal writers.
var V1Definitions = []Definition{
	{EntitiesStream, RepositoryChangedKind, "repo:{installation_id}:{repo_gh_id}", "repos(installation_id,gh_id)", `{"version":1}`},
	{EntitiesStream, RepositoryTombstonedKind, "repo:{installation_id}:{repo_gh_id}", "repos(installation_id,gh_id)", `{"version":1}`},
	{EntitiesStream, PullRequestChangedKind, "pr:{installation_id}:{repo_gh_id}:{pr_number}", "pull_requests(repos.installation_id,repos.gh_id,number), pull_request_review_requests(repo_id,pr_number), pull_request_reviews(repo_id,pr_number), pull_request_comments(repo_id,pr_number), pull_request_change_snapshots(repo_id,pr_number), pull_request_changed_files(repo_id,pr_number), pull_request_file_owners(repo_id,pr_number)", `{"version":1}`},
	{EntitiesStream, PullRequestTombstonedKind, "pr:{installation_id}:{repo_gh_id}:{pr_number}", "pull_requests(repos.installation_id,repos.gh_id,number), pull_request_review_requests(repo_id,pr_number), pull_request_reviews(repo_id,pr_number), pull_request_comments(repo_id,pr_number), pull_request_change_snapshots(repo_id,pr_number), pull_request_changed_files(repo_id,pr_number), pull_request_file_owners(repo_id,pr_number)", `{"version":1}`},
	{EntitiesStream, StackChangedKind, "stack:{installation_id}:{repo_gh_id}:{stack_number}", "stacks(repos.installation_id,repos.gh_id,number)", `{"version":1}`},
	{EntitiesStream, StackTombstonedKind, "stack:{installation_id}:{repo_gh_id}:{stack_number}", "stacks(repos.installation_id,repos.gh_id,number)", `{"version":1}`},
	{EntitiesStream, ChecksChangedKind, "checks:{installation_id}:{repo_gh_id}:{head_sha}", "check_runs(repos.installation_id,repos.gh_id,head_sha)", `{"version":1}`},
	{EntitiesStream, RepoRulesChangedKind, "repo_rules:{installation_id}:{repo_gh_id}", "repo_rules(repos.installation_id,repos.gh_id)", `{"version":1}`},
	{WorkItemsStream, WorkItemChangedKind, "repo:{repo_gh_id}:{work_item_kind}:{number}", "work_items(identity_key)", `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}`},
	{WorkItemsStream, WorkItemRemovedKind, "repo:{repo_gh_id}:{work_item_kind}:{number}", "work_items(identity_key), absent after removal", `{"version":1,"identity_key":"<entity_key>","scope_key":"<owning_scope>"}`},
}

// AcquireWriterFence registers tx as an outbox writer. It must be called
// before tx inserts its first change_events row and remains held through commit.
func AcquireWriterFence(ctx context.Context, tx pgx.Tx) error {
	if err := dbgen.New(tx).AcquireOutboxWriterFence(ctx, FenceKey); err != nil {
		return fmt.Errorf("acquire outbox writer fence: %w", err)
	}
	return nil
}

// AcquireObservedWriterFence is AcquireWriterFence with wait/hold
// observation. Callers must use the returned transaction for all subsequent
// work so Commit or Rollback records the hold interval.
func AcquireObservedWriterFence(
	ctx context.Context,
	tx pgx.Tx,
	observer FenceObserver,
) (pgx.Tx, error) {
	return acquireObservedFence(
		ctx,
		tx,
		SharedWriterFence,
		observer,
		func() error { return AcquireWriterFence(ctx, tx) },
	)
}

// AcquireWatermarkFence waits for every registered writer transaction to end
// and prevents a new writer from allocating a sequence until tx commits.
func AcquireWatermarkFence(ctx context.Context, tx pgx.Tx) error {
	if err := dbgen.New(tx).AcquireOutboxWatermarkFence(ctx, FenceKey); err != nil {
		return fmt.Errorf("acquire outbox watermark fence: %w", err)
	}
	return nil
}

// AcquireObservedWatermarkFence is AcquireWatermarkFence with wait/hold
// observation. Callers must use the returned transaction for the fenced read,
// publication, and Commit or Rollback.
func AcquireObservedWatermarkFence(
	ctx context.Context,
	tx pgx.Tx,
	observer FenceObserver,
) (pgx.Tx, error) {
	return acquireObservedFence(
		ctx,
		tx,
		ExclusiveWatermarkFence,
		observer,
		func() error { return AcquireWatermarkFence(ctx, tx) },
	)
}

func acquireObservedFence(
	ctx context.Context,
	tx pgx.Tx,
	role FenceRole,
	observer FenceObserver,
	acquire func() error,
) (pgx.Tx, error) {
	if observer == nil {
		if err := acquire(); err != nil {
			return nil, err
		}
		return tx, nil
	}
	waitStartedAt := time.Now()
	err := acquire()
	observation := FenceObservation{
		Role:         role,
		WaitDuration: time.Since(waitStartedAt),
		Acquired:     err == nil,
	}
	if err != nil {
		observer.OutboxFence(ctx, observation)
		return nil, err
	}
	return &observedFenceTx{
		Tx:          tx,
		observer:    observer,
		observation: observation,
		acquiredAt:  time.Now(),
	}, nil
}

func RepositoryKey(installationID, repositoryGitHubID int64) string {
	return "repo:" + strconv.FormatInt(installationID, 10) + ":" +
		strconv.FormatInt(repositoryGitHubID, 10)
}

func PullRequestKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return "pr:" + strconv.FormatInt(installationID, 10) + ":" +
		strconv.FormatInt(repositoryGitHubID, 10) + ":" +
		strconv.Itoa(number)
}

func StackKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return "stack:" + strconv.FormatInt(installationID, 10) + ":" +
		strconv.FormatInt(repositoryGitHubID, 10) + ":" +
		strconv.Itoa(number)
}

func ChecksKey(
	installationID int64,
	repositoryGitHubID int64,
	sha string,
) string {
	return "checks:" + strconv.FormatInt(installationID, 10) + ":" +
		strconv.FormatInt(repositoryGitHubID, 10) + ":" + sha
}

func RepoRulesKey(installationID, repositoryGitHubID int64) string {
	return "repo_rules:" + strconv.FormatInt(installationID, 10) + ":" +
		strconv.FormatInt(repositoryGitHubID, 10)
}

func StackWorkItemKey(repositoryGitHubID int64, stackNumber int) string {
	return "repo:" + strconv.FormatInt(repositoryGitHubID, 10) +
		":stack:" + strconv.Itoa(stackNumber)
}

func PullRequestWorkItemKey(repositoryGitHubID int64, pullNumber int) string {
	return "repo:" + strconv.FormatInt(repositoryGitHubID, 10) +
		":pr:" + strconv.Itoa(pullNumber)
}
