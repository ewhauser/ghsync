// Package outbox owns the database-level protocol shared by every internal
// change-event writer and by the visibility watermarker.
package outbox

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const (
	// FenceKey is a stable, database-wide advisory-lock key ("Frontier" in
	// ASCII). Writers hold its shared transaction lock before allocating an
	// outbox sequence; the watermarker briefly takes the exclusive counterpart.
	FenceKey int64 = 0x46726f6e74696572

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
)

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
	{EntitiesStream, PullRequestChangedKind, "pr:{installation_id}:{repo_gh_id}:{pr_number}", "pull_requests(repos.installation_id,repos.gh_id,number)", `{"version":1}`},
	{EntitiesStream, PullRequestTombstonedKind, "pr:{installation_id}:{repo_gh_id}:{pr_number}", "pull_requests(repos.installation_id,repos.gh_id,number)", `{"version":1}`},
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
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, FenceKey); err != nil {
		return fmt.Errorf("acquire outbox writer fence: %w", err)
	}
	return nil
}

// AcquireWatermarkFence waits for every registered writer transaction to end
// and prevents a new writer from allocating a sequence until tx commits.
func AcquireWatermarkFence(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, FenceKey); err != nil {
		return fmt.Errorf("acquire outbox watermark fence: %w", err)
	}
	return nil
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
