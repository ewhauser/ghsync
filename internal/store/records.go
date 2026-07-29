package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type SyncSource string

const (
	SyncSourceWebhook     SyncSource = "webhook"
	SyncSourceReconcile   SyncSource = "reconcile"
	SyncSourceBackfill    SyncSource = "backfill"
	SyncSourceManual      SyncSource = "manual"
	SyncSourceInteractive SyncSource = "interactive"
)

func (s SyncSource) Valid() bool {
	return s == SyncSourceWebhook || s == SyncSourceReconcile ||
		s == SyncSourceBackfill || s == SyncSourceManual ||
		s == SyncSourceInteractive
}

const displayWindow = 30 * 24 * time.Hour

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

type ReviewCommentRecord struct {
	ID          string    `json:"id"`
	Body        string    `json:"body"`
	UpdatedAt   time.Time `json:"updated_at"`
	AuthorLogin string    `json:"author_login"`
}

type ReviewThreadRecord struct {
	ID              string
	IsResolved      bool
	IsOutdated      bool
	Path            string
	Line            *int
	Comments        []ReviewCommentRecord
	GitHubUpdatedAt time.Time
}

type PullRequestRecord struct {
	Repository      RepositoryRecord
	GitHubID        int64
	NodeID          string
	Number          int
	Title           string
	State           string
	Draft           bool
	AuthorLogin     string
	HeadRef         string
	HeadSHA         string
	BaseRef         string
	BaseSHA         string
	ReviewDecision  string
	MergeableState  string
	StackNumber     *int
	StackPosition   *int
	MembershipKnown bool
	GitHubUpdatedAt time.Time
	ReviewThreads   []ReviewThreadRecord
	ThreadsKnown    bool
	ETag            string
	SyncedAt        time.Time
	Source          SyncSource
}

type StackEntry struct {
	Number    int        `json:"number"`
	State     string     `json:"state"`
	Draft     bool       `json:"draft"`
	MergedAt  *time.Time `json:"merged_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	HeadRef   string     `json:"head_ref"`
	HeadSHA   string     `json:"head_sha"`
}

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

type ChecksRecord struct {
	Repository RepositoryRecord
	HeadSHA    string
	Runs       []CheckRunRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

type RepoRuleRecord struct {
	Key             string          `json:"rule_key"`
	Rule            json.RawMessage `json:"rule"`
	GitHubUpdatedAt *time.Time      `json:"gh_updated_at"`
	HeadSHA         string          `json:"head_sha"`
}

type RepoRulesRecord struct {
	Repository RepositoryRecord
	Rules      []RepoRuleRecord
	ETag       string
	SyncedAt   time.Time
	Source     SyncSource
}

type ApplyPullRequestResult struct {
	Applied        bool
	DomainChanged  bool
	OldStackNumber *int
	NewStackNumber *int
	OldHeadSHA     string
	NewHeadSHA     string
}

type ApplyStackResult struct {
	Applied        bool
	JoinedPRs      []int
	LeftPRs        []int
	MovedPRs       []int
	PriorStackByPR map[int]int
}

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

type PullRequestHook func(ApplyPullRequestResult) TransactionHook
type StackHook func(ApplyStackResult) TransactionHook

type PullRequestApply struct {
	Context     context.Context
	Record      PullRequestRecord
	Observation *Observation
	Hook        PullRequestHook
}

type PullRequestApplyOutcome struct {
	Result ApplyPullRequestResult
	Err    error
}

func validateRepository(repository RepositoryRecord, source SyncSource) error {
	if !source.Valid() || repository.InstallationID <= 0 ||
		repository.OrgID <= 0 || repository.GitHubID <= 0 ||
		repository.Owner == "" || repository.Name == "" ||
		repository.FullName != repository.Owner+"/"+repository.Name ||
		repository.GitHubUpdatedAt.IsZero() {
		return fmt.Errorf("invalid repository record")
	}
	return nil
}

func validatePullRequest(pull PullRequestRecord) error {
	if err := validateRepository(pull.Repository, pull.Source); err != nil {
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
	return nil
}

func validateStack(stack StackRecord) error {
	if err := validateRepository(stack.Repository, stack.Source); err != nil {
		return err
	}
	if stack.Number <= 0 || stack.GitHubID <= 0 ||
		stack.GitHubUpdatedAt.IsZero() || stack.SyncedAt.IsZero() {
		return fmt.Errorf("invalid stack record")
	}
	return nil
}
