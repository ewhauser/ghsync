package fetch

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store"
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
	repository *gh.RepositoryNode,
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
	repository *store.RepositoryRecord,
	pull *gh.PullRequest,
	etag string,
	source store.SyncSource,
	syncedAt time.Time,
) store.PullRequestRecord {
	var stackNumber, stackPosition *int
	var stackSummary *store.StackSummaryRecord
	if pull.Stack != nil {
		number := pull.Stack.Number
		position := pull.Stack.Position
		stackNumber = &number
		stackPosition = &position
		// Preserve both independently reported values. GitHub may retain a
		// historical position beyond the current stack size.
		stackSummary = &store.StackSummaryRecord{
			GitHubID: pull.Stack.ID,
			Number:   pull.Stack.Number,
			Size:     pull.Stack.Size,
			Position: pull.Stack.Position,
			BaseRef:  pull.Stack.Base.Ref,
			BaseSHA:  pull.Stack.Base.SHA,
		}
	}
	var updatedAt time.Time
	if pull.UpdatedAt != nil {
		updatedAt = pull.UpdatedAt.Time
	}
	reviewRequests := make(
		[]store.ReviewRequestRecord,
		0,
		len(pull.GetRequestedReviewers())+len(pull.GetRequestedTeams()),
	)
	for _, reviewer := range pull.GetRequestedReviewers() {
		if !gh.IsSupportedReviewRequestUser(reviewer) {
			continue
		}
		reviewRequests = append(reviewRequests, store.ReviewRequestRecord{
			Kind:     store.ReviewRequestUser,
			GitHubID: reviewer.GetID(),
			NodeID:   reviewer.GetNodeID(),
			Login:    reviewer.GetLogin(),
		})
	}
	for _, team := range pull.GetRequestedTeams() {
		if !gh.IsSupportedReviewRequestTeam(team) {
			continue
		}
		reviewRequests = append(reviewRequests, store.ReviewRequestRecord{
			Kind:     store.ReviewRequestTeam,
			GitHubID: team.GetID(),
			NodeID:   team.GetNodeID(),
			Login:    team.GetSlug(),
		})
	}
	return store.PullRequestRecord{
		Repository:      *repository,
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
		StackSummary:    stackSummary,
		MembershipKnown: true,
		GitHubUpdatedAt: updatedAt,
		ReviewRequests:  reviewRequests,
		ReviewRequestsKnown: pull.RequestedReviewers != nil ||
			pull.RequestedTeams != nil,
		ETag:     etag,
		SyncedAt: syncedAt,
		Source:   source,
	}
}

func pullRecordFromNode(
	node *gh.PullRequestNode,
	item *pullBatchItem,
	installationID int64,
	orgID int64,
) store.PullRequestRecord {
	reviewRequests := make(
		[]store.ReviewRequestRecord,
		0,
		len(node.ReviewRequests.Nodes),
	)
	for _, request := range node.ReviewRequests.Nodes {
		reviewer := request.RequestedReviewer
		kind := store.ReviewRequestKind("")
		login := ""
		switch reviewer.Typename {
		case "User":
			kind = store.ReviewRequestUser
			login = reviewer.Login
		case "Team":
			kind = store.ReviewRequestTeam
			login = reviewer.Slug
		default:
			// RequestedReviewer also permits Bot, Mannequin,
			// EnterpriseTeam, nil, and future union members. The v1
			// public contract intentionally exposes only User and Team;
			// skip other variants instead of emitting an invalid or
			// mislabeled set member.
			continue
		}
		if reviewer.DatabaseID <= 0 || reviewer.ID == "" || login == "" {
			// databaseId is nullable in GitHub's GraphQL schema. Without
			// both stable identities this node cannot satisfy the public
			// contract, so exclude it without invalidating supported peers.
			continue
		}
		reviewRequests = append(reviewRequests, store.ReviewRequestRecord{
			Kind:     kind,
			GitHubID: reviewer.DatabaseID,
			NodeID:   reviewer.ID,
			Login:    login,
		})
	}
	reviews := make(
		[]store.PullRequestReviewRecord,
		0,
		len(node.Reviews.Nodes),
	)
	for _, review := range node.Reviews.Nodes {
		authorKind, authorNodeID, authorLogin := actorIdentity(review.Author)
		commitOID := ""
		if review.Commit != nil {
			commitOID = review.Commit.OID
		}
		reviews = append(reviews, store.PullRequestReviewRecord{
			GitHubID:        graphQLDatabaseID(review.FullDatabaseID),
			NodeID:          review.ID,
			AuthorKind:      authorKind,
			AuthorNodeID:    authorNodeID,
			AuthorLogin:     authorLogin,
			State:           strings.ToLower(review.State),
			SubmittedAt:     review.SubmittedAt,
			CommitOID:       commitOID,
			GitHubUpdatedAt: review.UpdatedAt,
		})
	}
	comments := make(
		[]store.PullRequestCommentRecord,
		0,
		len(node.Comments.Nodes),
	)
	for _, comment := range node.Comments.Nodes {
		authorKind, authorNodeID, authorLogin := actorIdentity(comment.Author)
		comments = append(comments, store.PullRequestCommentRecord{
			GitHubID:        graphQLDatabaseID(comment.FullDatabaseID),
			NodeID:          comment.ID,
			AuthorKind:      authorKind,
			AuthorNodeID:    authorNodeID,
			AuthorLogin:     authorLogin,
			CreatedAt:       comment.CreatedAt,
			GitHubUpdatedAt: comment.UpdatedAt,
		})
	}
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
			&node.Repository,
			installationID,
			orgID,
		),
		GitHubID:            node.DatabaseID,
		NodeID:              node.ID,
		Number:              node.Number,
		Title:               node.Title,
		State:               strings.ToLower(node.State),
		Draft:               node.IsDraft,
		AuthorLogin:         node.Author.Login,
		HeadRef:             node.HeadRefName,
		HeadSHA:             node.HeadRefOID,
		BaseRef:             node.BaseRefName,
		BaseSHA:             node.BaseRefOID,
		ReviewDecision:      node.ReviewDecision,
		MergeableState:      node.Mergeable,
		StackNumber:         item.metadata.StackNumber,
		StackPosition:       item.metadata.StackPosition,
		MembershipKnown:     false,
		GitHubUpdatedAt:     node.UpdatedAt,
		ReviewThreads:       threads,
		ThreadsKnown:        true,
		ReviewRequests:      reviewRequests,
		ReviewRequestsKnown: true,
		Reviews:             reviews,
		ReviewsKnown:        true,
		Comments:            comments,
		CommentsKnown:       true,
		ETag:                item.metadata.ETag,
		SyncedAt:            item.startedAt,
		Source:              item.source,
	}
}

func graphQLDatabaseID(id *gh.BigInt) int64 {
	if id == nil {
		return 0
	}
	return int64(*id)
}

func actorIdentity(actor *gh.ActorNode) (string, string, string) {
	if actor == nil {
		return "deleted", "", ""
	}
	kind := map[string]string{
		"User":                  "user",
		"Bot":                   "bot",
		"Mannequin":             "mannequin",
		"Organization":          "organization",
		"EnterpriseUserAccount": "enterprise_user_account",
	}[actor.Typename]
	if kind == "" {
		kind = "unknown"
	}
	return kind, actor.ID, actor.Login
}

func stackRecordFromREST(
	repository *store.RepositoryRecord,
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
		Repository:      *repository,
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

func checkRecordFromREST(run *gh.CheckRun) (store.CheckRunRecord, error) {
	if !json.Valid(run.Raw) {
		return store.CheckRunRecord{}, fmt.Errorf(
			"check run %d has no valid raw observation",
			run.ID,
		)
	}
	observed := append(json.RawMessage(nil), run.Raw...)
	var semanticTime *time.Time
	if run.CompletedAt != nil {
		semanticTime = run.CompletedAt
	} else if run.StartedAt != nil {
		semanticTime = run.StartedAt
	}
	return store.CheckRunRecord{
		GitHubID:        run.ID,
		NodeID:          run.NodeID,
		Name:            run.Name,
		Status:          run.Status,
		Conclusion:      run.Conclusion,
		DetailsURL:      run.DetailsURL,
		AppSlug:         run.AppSlug,
		StartedAt:       run.StartedAt,
		CompletedAt:     run.CompletedAt,
		GitHubUpdatedAt: semanticTime,
		Observed:        observed,
	}, nil
}

func pullRecordsFromList(
	repository *store.RepositoryRecord,
	pulls []gh.PullRequest,
	etags map[int]string,
	source store.SyncSource,
	syncedAt time.Time,
) []store.PullRequestRecord {
	records := make([]store.PullRequestRecord, 0, len(pulls))
	for index := range pulls {
		etag := ""
		if etags != nil {
			etag = etags[pulls[index].GetNumber()]
		}
		record := pullRecordFromREST(
			repository,
			&pulls[index],
			etag,
			source,
			syncedAt,
		)
		// The list endpoint is discovery input, not an authoritative
		// replace-set observation. Its response is fetched before the PR
		// entity observation lock and may omit or lag review requests. The
		// backfill child refresh writes the complete detail/GraphQL set while
		// holding that lock.
		record.ReviewRequests = nil
		record.ReviewRequestsKnown = false
		records = append(records, record)
	}
	return records
}
