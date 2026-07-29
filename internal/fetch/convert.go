package fetch

import (
	"strings"
	"time"

	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store"
)

func repositoryRecordFromREST(
	repository *gh.Repository,
	installationID int64,
	orgID int64,
) store.RepositoryRecord {
	return store.RepositoryRecord{
		InstallationID:  installationID,
		OrgID:           orgID,
		GitHubID:        repository.ID,
		NodeID:          repository.NodeID,
		Owner:           repository.Owner,
		Name:            repository.Name,
		FullName:        repository.FullName,
		DefaultBranch:   repository.DefaultBranch,
		Archived:        repository.Archived,
		GitHubUpdatedAt: repository.UpdatedAt,
	}
}

func repositoryRecordFromNode(
	repository gh.RepositoryNode,
	installationID int64,
	orgID int64,
) store.RepositoryRecord {
	var defaultBranch, defaultHeadSHA string
	if repository.DefaultBranchRef != nil {
		defaultBranch = repository.DefaultBranchRef.Name
		defaultHeadSHA = repository.DefaultBranchRef.Target.OID
	}
	return store.RepositoryRecord{
		InstallationID:  installationID,
		OrgID:           orgID,
		GitHubID:        repository.DatabaseID,
		NodeID:          repository.ID,
		Owner:           repository.Owner.Login,
		Name:            repository.Name,
		FullName:        repository.NameWithOwner,
		DefaultBranch:   defaultBranch,
		DefaultHeadSHA:  defaultHeadSHA,
		Archived:        repository.IsArchived,
		GitHubUpdatedAt: repository.UpdatedAt,
	}
}

func pullRecordFromREST(
	repository store.RepositoryRecord,
	pull *gh.PullRequest,
	etag string,
	source store.SyncSource,
	syncedAt time.Time,
) store.PullRequestRecord {
	var stackNumber, stackPosition *int
	if pull.Stack != nil {
		number := pull.Stack.Number
		position := pull.Stack.Position
		stackNumber = &number
		stackPosition = &position
	}
	var updatedAt time.Time
	if pull.UpdatedAt != nil {
		updatedAt = pull.UpdatedAt.Time
	}
	return store.PullRequestRecord{
		Repository:      repository,
		GitHubID:        pull.GetID(),
		NodeID:          pull.GetNodeID(),
		Number:          pull.GetNumber(),
		Title:           pull.GetTitle(),
		State:           pull.GetState(),
		Draft:           pull.GetDraft(),
		AuthorLogin:     pull.GetUser().GetLogin(),
		HeadRef:         pull.GetHead().GetRef(),
		HeadSHA:         pull.GetHead().GetSHA(),
		BaseRef:         pull.GetBase().GetRef(),
		BaseSHA:         pull.GetBase().GetSHA(),
		ReviewDecision:  pull.ReviewDecision,
		MergeableState:  pull.GetMergeableState(),
		StackNumber:     stackNumber,
		StackPosition:   stackPosition,
		MembershipKnown: true,
		GitHubUpdatedAt: updatedAt,
		ETag:            etag,
		SyncedAt:        syncedAt,
		Source:          source,
	}
}

func pullRecordFromNode(
	node *gh.PullRequestNode,
	item *pullBatchItem,
	installationID int64,
	orgID int64,
) store.PullRequestRecord {
	threads := make([]store.ReviewThreadRecord, 0, len(node.ReviewThreads.Nodes))
	for _, thread := range node.ReviewThreads.Nodes {
		comments := make(
			[]store.ReviewCommentRecord,
			0,
			len(thread.Comments.Nodes),
		)
		updatedAt := node.UpdatedAt
		for _, comment := range thread.Comments.Nodes {
			author := ""
			if comment.Author != nil {
				author = comment.Author.Login
			}
			comments = append(comments, store.ReviewCommentRecord{
				ID:          comment.ID,
				Body:        comment.Body,
				UpdatedAt:   comment.UpdatedAt,
				AuthorLogin: author,
			})
			if comment.UpdatedAt.After(updatedAt) {
				updatedAt = comment.UpdatedAt
			}
		}
		threads = append(threads, store.ReviewThreadRecord{
			ID:              thread.ID,
			IsResolved:      thread.IsResolved,
			IsOutdated:      thread.IsOutdated,
			Path:            thread.Path,
			Line:            thread.Line,
			Comments:        comments,
			GitHubUpdatedAt: updatedAt,
		})
	}
	return store.PullRequestRecord{
		Repository: repositoryRecordFromNode(
			node.Repository,
			installationID,
			orgID,
		),
		GitHubID:        node.DatabaseID,
		NodeID:          node.ID,
		Number:          node.Number,
		Title:           node.Title,
		State:           strings.ToLower(node.State),
		Draft:           node.IsDraft,
		AuthorLogin:     node.Author.Login,
		HeadRef:         node.HeadRefName,
		HeadSHA:         node.HeadRefOID,
		BaseRef:         node.BaseRefName,
		BaseSHA:         node.BaseRefOID,
		ReviewDecision:  node.ReviewDecision,
		MergeableState:  node.Mergeable,
		StackNumber:     item.metadata.StackNumber,
		StackPosition:   item.metadata.StackPosition,
		MembershipKnown: false,
		GitHubUpdatedAt: node.UpdatedAt,
		ReviewThreads:   threads,
		ThreadsKnown:    true,
		SyncedAt:        item.startedAt,
		Source:          item.source,
	}
}

func stackRecordFromREST(
	repository store.RepositoryRecord,
	stack *gh.Stack,
	etag string,
	source store.SyncSource,
	syncedAt time.Time,
) store.StackRecord {
	entries := make([]store.StackEntry, 0, len(stack.PullRequests))
	updatedAt := stack.UpdatedAt
	for _, pull := range stack.PullRequests {
		entry := store.StackEntry{
			Number:    pull.Number,
			State:     pull.State,
			Draft:     pull.Draft,
			MergedAt:  pull.MergedAt,
			UpdatedAt: pull.UpdatedAt,
			HeadRef:   pull.Head.Ref,
			HeadSHA:   pull.Head.SHA,
		}
		entries = append(entries, entry)
		if pull.UpdatedAt.After(updatedAt) {
			updatedAt = pull.UpdatedAt
		}
	}
	if updatedAt.IsZero() {
		updatedAt = stack.CreatedAt
	}
	return store.StackRecord{
		Repository:      repository,
		GitHubID:        stack.ID,
		NodeID:          stack.NodeID,
		Number:          stack.Number,
		BaseRef:         stack.Base.Ref,
		BaseSHA:         stack.Base.SHA,
		Open:            stack.Open,
		Entries:         entries,
		GitHubUpdatedAt: updatedAt,
		ETag:            etag,
		SyncedAt:        syncedAt,
		Source:          source,
	}
}

func pullRecordsFromList(
	repository store.RepositoryRecord,
	pulls []gh.PullRequest,
	etag string,
	source store.SyncSource,
	syncedAt time.Time,
) []store.PullRequestRecord {
	records := make([]store.PullRequestRecord, 0, len(pulls))
	for index := range pulls {
		records = append(
			records,
			pullRecordFromREST(
				repository,
				&pulls[index],
				etag,
				source,
				syncedAt,
			),
		)
	}
	return records
}
