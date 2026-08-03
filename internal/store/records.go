package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// SyncSource identifies the workflow that supplied an authoritative cache
// observation.
type SyncSource string

const (
	// SyncSourceWebhook identifies observations triggered by GitHub webhooks.
	SyncSourceWebhook SyncSource = "webhook"
	// SyncSourceReconcile identifies observations from reconciliation work.
	SyncSourceReconcile SyncSource = "reconcile"
	// SyncSourceBackfill identifies observations from historical backfills.
	SyncSourceBackfill SyncSource = "backfill"
	// SyncSourceManual identifies operator-requested observations.
	SyncSourceManual SyncSource = "manual"
	// SyncSourceInteractive identifies user-blocking interactive refreshes.
	SyncSourceInteractive SyncSource = "interactive"
)

// Valid reports whether s is a supported cache observation source.
func (s SyncSource) Valid() bool {
	return s == SyncSourceWebhook || s == SyncSourceReconcile ||
		s == SyncSourceBackfill || s == SyncSourceManual ||
		s == SyncSourceInteractive
}

const displayWindow = 30 * 24 * time.Hour

// RepositoryRecord is the validated repository state accepted by the cache.
type RepositoryRecord struct {
	InstallationID  int64
	OrgID           int64
	GitHubID        int64
	NodeID          string
	Owner           string
	Name            string
	FullName        string
	DefaultBranch   string
	DefaultHeadSHA  string
	Archived        bool
	GitHubUpdatedAt time.Time
	ETag            string
	LastCheckedAt   time.Time
}

// ReviewCommentRecord is one review comment embedded in a review thread.
type ReviewCommentRecord struct {
	ID          string    `json:"id"`
	Body        string    `json:"body"`
	UpdatedAt   time.Time `json:"updated_at"`
	AuthorLogin string    `json:"author_login"`
}

// ReviewThreadRecord is one authoritative pull-request review thread.
type ReviewThreadRecord struct {
	ID              string
	IsResolved      bool
	IsOutdated      bool
	Path            string
	Line            *int
	Comments        []ReviewCommentRecord
	GitHubUpdatedAt time.Time
}

// ReviewRequestKind distinguishes GitHub user and team review requests.
type ReviewRequestKind string

const (
	ReviewRequestUser ReviewRequestKind = "user"
	ReviewRequestTeam ReviewRequestKind = "team"
)

func (k ReviewRequestKind) valid() bool {
	return k == ReviewRequestUser || k == ReviewRequestTeam
}

// ReviewRequestRecord is one member of GitHub's authoritative current
// pull-request reviewRequests set. RequestedAt is nil when GitHub does not
// expose an authoritative request timestamp.
type ReviewRequestRecord struct {
	Kind        ReviewRequestKind
	GitHubID    int64
	NodeID      string
	Login       string
	RequestedAt *time.Time
}

// PullRequestReviewRecord is one identity-keyed GitHub pull-request review.
// Lifecycle state plus SubmittedAt and GitHubUpdatedAt form the per-row
// monotonic basis; a dismissed review remains present with State set to
// "dismissed".
type PullRequestReviewRecord struct {
	GitHubID        int64
	NodeID          string
	AuthorKind      string
	AuthorNodeID    string
	AuthorLogin     string
	State           string
	SubmittedAt     *time.Time
	CommitOID       string
	GitHubUpdatedAt time.Time
}

// PullRequestCommentRecord is one identity-keyed ordinary issue comment on a
// pull request. Bodies are deliberately absent from the public fact record.
type PullRequestCommentRecord struct {
	GitHubID        int64
	NodeID          string
	AuthorKind      string
	AuthorNodeID    string
	AuthorLogin     string
	CreatedAt       time.Time
	GitHubUpdatedAt time.Time
}

// PullRequestRecord is the authoritative pull-request state accepted by the
// cache.
type PullRequestRecord struct {
	Repository          RepositoryRecord
	GitHubID            int64
	NodeID              string
	Number              int
	Title               string
	State               string
	Draft               bool
	AuthorLogin         string
	HeadRef             string
	HeadSHA             string
	BaseRef             string
	BaseSHA             string
	ReviewDecision      string
	MergeableState      string
	StackNumber         *int
	StackPosition       *int
	StackSummary        *StackSummaryRecord
	MembershipKnown     bool
	GitHubUpdatedAt     time.Time
	ReviewThreads       []ReviewThreadRecord
	ThreadsKnown        bool
	ReviewRequests      []ReviewRequestRecord
	ReviewRequestsKnown bool
	Reviews             []PullRequestReviewRecord
	ReviewsKnown        bool
	Comments            []PullRequestCommentRecord
	CommentsKnown       bool
	ETag                string
	SyncedAt            time.Time
	Source              SyncSource
}

// StackSummaryRecord is the stack tuple embedded in an authoritative
// pull-request response. BaseSHA is empty when GitHub reports the stack base
// ref but can no longer resolve its commit, including historical stacks whose
// base branch was deleted.
type StackSummaryRecord struct {
	GitHubID int64
	Number   int
	Size     int
	Position int
	BaseRef  string
	BaseSHA  string
}

// StackEntry is one ordered pull request in a stack snapshot.
type StackEntry struct {
	Number    int        `json:"number"`
	State     string     `json:"state"`
	Draft     bool       `json:"draft"`
	MergedAt  *time.Time `json:"merged_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	HeadRef   string     `json:"head_ref"`
	HeadSHA   string     `json:"head_sha"`
}

// StackRecord is the authoritative stack state accepted by the cache. BaseSHA
// is empty when GitHub reports the base ref without a resolvable commit. This
// is authoritative upstream truth, including for open stacks.
type StackRecord struct {
	Repository      RepositoryRecord
	GitHubID        int64
	NodeID          string
	Number          int
	BaseRef         string
	BaseSHA         string
	Open            bool
	Entries         []StackEntry
	GitHubUpdatedAt time.Time
	ETag            string
	SyncedAt        time.Time
	Source          SyncSource
}

// CheckRunRecord is one check run in an authoritative head-SHA snapshot.
type CheckRunRecord struct {
	GitHubID        int64           `json:"gh_id"`
	NodeID          string          `json:"node_id"`
	Name            string          `json:"name"`
	Status          string          `json:"status"`
	Conclusion      string          `json:"conclusion"`
	DetailsURL      string          `json:"details_url"`
	AppSlug         string          `json:"app_slug"`
	StartedAt       *time.Time      `json:"started_at"`
	CompletedAt     *time.Time      `json:"completed_at"`
	GitHubUpdatedAt *time.Time      `json:"gh_updated_at"`
	SemanticVersion string          `json:"semantic_version"`
	Observed        json.RawMessage `json:"observed"`
}

// ChecksRecord is the authoritative check-run set for one repository head SHA.
type ChecksRecord struct {
	Repository RepositoryRecord
	HeadSHA    string
	Runs       []CheckRunRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

// RepoRuleRecord is one normalized repository rule.
type RepoRuleRecord struct {
	Key             string          `json:"rule_key"`
	Rule            json.RawMessage `json:"rule"`
	GitHubUpdatedAt *time.Time      `json:"gh_updated_at"`
	HeadSHA         string          `json:"head_sha"`
}

// RepoRulesRecord is the authoritative repository-rules snapshot.
type RepoRulesRecord struct {
	Repository RepositoryRecord
	Rules      []RepoRuleRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

// ApplyPullRequestResult describes the accepted pull-request transition.
type ApplyPullRequestResult struct {
	Applied               bool
	DomainChanged         bool
	ReviewRequestsChanged bool
	ReviewsChanged        bool
	CommentsChanged       bool
	StackStateChanged     bool
	OldStackNumber        *int
	NewStackNumber        *int
	OldHeadSHA            string
	NewHeadSHA            string
}

// ApplyStackResult describes stack membership changes caused by a write.
type ApplyStackResult struct {
	Applied        bool
	JoinedPRs      []int
	LeftPRs        []int
	MovedPRs       []int
	PriorStackByPR map[int]int
}

// FetchMetadata is the cache metadata needed for a conditional GitHub fetch.
type FetchMetadata struct {
	NodeID         string
	ETag           string
	StackNumber    *int
	StackPosition  *int
	HeadSHA        string
	RepoGitHubID   int64
	InstallationID int64
	RepoFullName   string
}

// TransactionHook runs after the accepted cache mutation, dirty marking, and
// event insert but before commit. Fetch workers use it for durable generation
// bumps and River follow-ups (C-C3).
type TransactionHook func(context.Context, pgx.Tx) error

// PullRequestHook derives transaction work from a pull-request write result.
type PullRequestHook func(ApplyPullRequestResult) TransactionHook

// StackHook derives transaction work from a stack write result.
type StackHook func(ApplyStackResult) TransactionHook

// PullRequestApply describes one independently handled batch write.
type PullRequestApply struct {
	Context     context.Context //nolint:containedctx // each batched item retains its independent cancellation and values
	Record      PullRequestRecord
	Observation *Observation
	Hook        PullRequestHook
}

// PullRequestApplyOutcome captures one batch write's result and error.
type PullRequestApplyOutcome struct {
	Result ApplyPullRequestResult
	Err    error
}

func validateRepository(repository *RepositoryRecord, source SyncSource) error {
	if !source.Valid() || repository.InstallationID <= 0 ||
		repository.OrgID <= 0 || repository.GitHubID <= 0 ||
		repository.Owner == "" || repository.Name == "" ||
		repository.FullName != repository.Owner+"/"+repository.Name ||
		repository.GitHubUpdatedAt.IsZero() {
		return fmt.Errorf("invalid repository record")
	}
	return nil
}

func validatePullRequest(pull *PullRequestRecord) error {
	if err := validateRepository(&pull.Repository, pull.Source); err != nil {
		return err
	}
	if pull.Number <= 0 || pull.GitHubID <= 0 || pull.NodeID == "" ||
		pull.HeadSHA == "" || pull.GitHubUpdatedAt.IsZero() ||
		pull.SyncedAt.IsZero() {
		return fmt.Errorf("invalid pull request record")
	}
	if pull.MembershipKnown &&
		((pull.StackNumber == nil) != (pull.StackPosition == nil)) {
		return fmt.Errorf("PR stack number and position must both be set or nil")
	}
	if pull.StackSummary != nil &&
		(!pull.MembershipKnown ||
			pull.StackNumber == nil ||
			pull.StackPosition == nil ||
			pull.StackSummary.GitHubID <= 0 ||
			pull.StackSummary.Number != *pull.StackNumber ||
			pull.StackSummary.Size <= 0 ||
			pull.StackSummary.Position != *pull.StackPosition ||
			pull.StackSummary.Position > pull.StackSummary.Size ||
			pull.StackSummary.BaseRef == "") {
		return fmt.Errorf("invalid PR stack summary")
	}
	seenRequests := make(map[string]struct{}, len(pull.ReviewRequests))
	for _, request := range pull.ReviewRequests {
		if !request.Kind.valid() || request.GitHubID <= 0 ||
			request.NodeID == "" || request.Login == "" {
			return fmt.Errorf("invalid PR review request")
		}
		key := fmt.Sprintf("%s:%d", request.Kind, request.GitHubID)
		if _, duplicate := seenRequests[key]; duplicate {
			return fmt.Errorf("duplicate PR review request %s", key)
		}
		seenRequests[key] = struct{}{}
	}
	seenReviews := make(map[string]struct{}, len(pull.Reviews))
	for index := range pull.Reviews {
		review := &pull.Reviews[index]
		if review.GitHubID < 0 || review.NodeID == "" ||
			!validParticipationAuthor(
				review.AuthorKind,
				review.AuthorNodeID,
				review.AuthorLogin,
			) || review.State == "" ||
			review.GitHubUpdatedAt.IsZero() {
			return fmt.Errorf("invalid PR review")
		}
		if _, duplicate := seenReviews[review.NodeID]; duplicate {
			return fmt.Errorf("duplicate PR review %s", review.NodeID)
		}
		seenReviews[review.NodeID] = struct{}{}
	}
	seenComments := make(map[string]struct{}, len(pull.Comments))
	for _, comment := range pull.Comments {
		if comment.GitHubID < 0 || comment.NodeID == "" ||
			!validParticipationAuthor(
				comment.AuthorKind,
				comment.AuthorNodeID,
				comment.AuthorLogin,
			) || comment.CreatedAt.IsZero() ||
			comment.GitHubUpdatedAt.IsZero() {
			return fmt.Errorf("invalid PR issue comment")
		}
		if _, duplicate := seenComments[comment.NodeID]; duplicate {
			return fmt.Errorf("duplicate PR issue comment %s", comment.NodeID)
		}
		seenComments[comment.NodeID] = struct{}{}
	}
	return nil
}

func validParticipationAuthor(kind, nodeID, login string) bool {
	switch kind {
	case "user", "bot", "mannequin", "organization",
		"enterprise_user_account", "unknown":
		return true
	case "deleted":
		return nodeID == "" && login == ""
	default:
		return false
	}
}

func validateStack(stack *StackRecord) error {
	if err := validateRepository(&stack.Repository, stack.Source); err != nil {
		return err
	}
	if stack.Number <= 0 || stack.GitHubID <= 0 ||
		stack.GitHubUpdatedAt.IsZero() || stack.SyncedAt.IsZero() {
		return fmt.Errorf("invalid stack record")
	}
	return nil
}
