package fakegithub

import (
	"fmt"
	"sort"
	"time"
)

// TruthMutation is the replay compiler's fixture-mutation wire contract.
// It deliberately lives in fakegithub so the standalone fake can apply
// mutations without importing the replay compiler (which itself uses the
// fake's production-shaped payload builders).
type TruthMutation struct {
	Kind          string              `json:"kind"`
	Action        string              `json:"action,omitempty"`
	Repository    TruthRepository     `json:"repository"`
	PullRequest   *TruthPullRequest   `json:"pull_request,omitempty"`
	Review        *TruthReview        `json:"review,omitempty"`
	ReviewThread  *TruthReviewThread  `json:"review_thread,omitempty"`
	ReviewComment *TruthReviewComment `json:"review_comment,omitempty"`
	CheckSuite    *TruthCheckSuite    `json:"check_suite,omitempty"`
	CheckRun      *TruthCheckRun      `json:"check_run,omitempty"`
	Commit        *TruthCommit        `json:"commit,omitempty"`
	Push          *TruthPush          `json:"push,omitempty"`
	Stack         *TruthStack         `json:"stack,omitempty"`
}

type TruthRepository struct {
	ID               int64     `json:"id"`
	NodeID           string    `json:"node_id"`
	Owner            string    `json:"owner"`
	Name             string    `json:"name"`
	DefaultBranch    string    `json:"default_branch"`
	DefaultBranchSHA string    `json:"default_branch_sha"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (r TruthRepository) FullName() string {
	return r.Owner + "/" + r.Name
}

type TruthBranch struct {
	Ref        string `json:"ref"`
	SHA        string `json:"sha"`
	Repository string `json:"repository,omitempty"`
}

type TruthPullRequest struct {
	ID             int64       `json:"id"`
	NodeID         string      `json:"node_id"`
	Number         int         `json:"number"`
	Title          string      `json:"title"`
	State          string      `json:"state"`
	Draft          bool        `json:"draft"`
	Merged         bool        `json:"merged"`
	AuthorLogin    string      `json:"author_login"`
	ReviewDecision string      `json:"review_decision,omitempty"`
	MergeableState string      `json:"mergeable_state,omitempty"`
	Head           TruthBranch `json:"head"`
	Base           TruthBranch `json:"base"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	ClosedAt       *time.Time  `json:"closed_at,omitempty"`
	MergedAt       *time.Time  `json:"merged_at,omitempty"`
}

type TruthReview struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	State       string    `json:"state"`
	Body        string    `json:"body,omitempty"`
	AuthorLogin string    `json:"author_login"`
	CommitSHA   string    `json:"commit_sha,omitempty"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type TruthReviewThread struct {
	ID         string               `json:"id"`
	IsResolved bool                 `json:"is_resolved"`
	IsOutdated bool                 `json:"is_outdated"`
	Path       string               `json:"path"`
	Line       *int                 `json:"line,omitempty"`
	Comments   []TruthReviewComment `json:"comments"`
}

type TruthReviewComment struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	ReviewID    int64     `json:"review_id,omitempty"`
	Body        string    `json:"body,omitempty"`
	Path        string    `json:"path"`
	Line        *int      `json:"line,omitempty"`
	AuthorLogin string    `json:"author_login"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type TruthCheckSuite struct {
	ID         int64     `json:"id"`
	NodeID     string    `json:"node_id"`
	HeadSHA    string    `json:"head_sha"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion,omitempty"`
	AppSlug    string    `json:"app_slug,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type TruthCheckRun struct {
	ID          int64      `json:"id"`
	NodeID      string     `json:"node_id"`
	HeadSHA     string     `json:"head_sha"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion,omitempty"`
	DetailsURL  string     `json:"details_url"`
	AppSlug     string     `json:"app_slug,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type TruthCommit struct {
	SHA               string    `json:"sha"`
	ParentSHA         string    `json:"parent_sha,omitempty"`
	Ref               string    `json:"ref"`
	CommittedAt       time.Time `json:"committed_at"`
	PullRequestNumber int       `json:"pull_request_number,omitempty"`
	DefaultBranch     bool      `json:"default_branch,omitempty"`
}

type TruthPush struct {
	Ref           string    `json:"ref"`
	Before        string    `json:"before"`
	After         string    `json:"after"`
	Forced        bool      `json:"forced,omitempty"`
	DefaultBranch bool      `json:"default_branch,omitempty"`
	PushedAt      time.Time `json:"pushed_at"`
}

type TruthStack struct {
	ID                int64              `json:"id"`
	Number            int                `json:"number"`
	Base              TruthBranch        `json:"base"`
	PullRequests      []int              `json:"pull_requests"`
	PullRequestStates []TruthPullRequest `json:"pull_request_states"`
	Synthetic         bool               `json:"synthetic,omitempty"`
}

// TruthSnapshot is the complete final-oracle state exposed by
// ControlTruthPath. Only repositories touched by replay mutations are
// included, so a standalone fake may retain unrelated development fixtures.
type TruthSnapshot struct {
	Repositories []TruthFixtureSnapshot `json:"repositories"`
	Faults       TruthFaultSnapshot     `json:"faults"`
}

type TruthFixtureSnapshot struct {
	Repository    Repository                  `json:"repository"`
	PullRequests  []TruthPullRequestSnapshot  `json:"pull_requests"`
	Stacks        []Stack                     `json:"stacks"`
	CheckRuns     []TruthCheckRunSnapshot     `json:"check_runs"`
	ReviewThreads []TruthReviewThreadSnapshot `json:"review_threads"`
}

type TruthPullRequestSnapshot struct {
	ID             int64             `json:"id"`
	NodeID         string            `json:"node_id"`
	Number         int               `json:"number"`
	Title          string            `json:"title"`
	State          string            `json:"state"`
	Draft          bool              `json:"draft"`
	AuthorLogin    string            `json:"author_login"`
	ReviewDecision string            `json:"review_decision"`
	MergeableState string            `json:"mergeable_state"`
	Head           PullRequestBranch `json:"head"`
	Base           PullRequestBranch `json:"base"`
	UpdatedAt      time.Time         `json:"updated_at"`
	Stack          *StackRef         `json:"stack"`
	ReviewRequests []ReviewRequest   `json:"review_requests"`
}

type TruthReviewThreadSnapshot struct {
	PullRequest int             `json:"pull_request"`
	ID          string          `json:"id"`
	IsResolved  bool            `json:"is_resolved"`
	IsOutdated  bool            `json:"is_outdated"`
	Path        string          `json:"path"`
	Line        *int            `json:"line,omitempty"`
	Comments    []ReviewComment `json:"comments"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type TruthCheckRunSnapshot struct {
	ID          int64      `json:"id"`
	NodeID      string     `json:"node_id"`
	HeadSHA     string     `json:"head_sha"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion"`
	DetailsURL  string     `json:"details_url"`
	AppSlug     string     `json:"app_slug"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type TruthFaultSnapshot struct {
	Configured500 int `json:"configured_500"`
	Configured429 int `json:"configured_429"`
	Applied500    int `json:"applied_500"`
	Applied429    int `json:"applied_429"`
}

// EmptyFixture returns a single-repository fixture suitable for recording
// replay. The repository listing is populated so installation backfill can
// enroll it before traffic begins.
func EmptyFixture(repository Repository) Fixture {
	return Fixture{
		Owner:        repository.Owner,
		Repo:         repository.Name,
		Repository:   repository,
		Repositories: []Repository{repository},
	}
}

func (s *Server) applyTruthMutation(mutation TruthMutation) error {
	if mutation.Kind == "" {
		return fmt.Errorf("truth mutation kind is required")
	}
	if mutation.Repository.ID <= 0 ||
		mutation.Repository.Owner == "" ||
		mutation.Repository.Name == "" {
		return fmt.Errorf("truth mutation repository is incomplete")
	}
	if err := validateTruthMutation(mutation); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := mutation.Repository.FullName()
	fixture := s.fixtureByKeyLocked(key)
	var candidate Fixture
	if fixture == nil {
		candidate = EmptyFixture(truthRepository(mutation.Repository))
	} else {
		candidate = cloneFixture(fixture)
	}
	applyTruthRepository(
		&candidate,
		mutation.Repository,
		mutation.Kind == "repository",
	)

	if mutation.PullRequest != nil {
		applyTruthPullRequest(&candidate, *mutation.PullRequest)
	}
	switch mutation.Kind {
	case "repository":
	case "commit":
		if mutation.Commit.DefaultBranch {
			candidate.Repository.DefaultBranchSHA = mutation.Commit.SHA
		}
	case "push":
		if mutation.Push.DefaultBranch {
			candidate.Repository.DefaultBranchSHA = mutation.Push.After
		}
		candidate.Repository.PushedAt = mutation.Push.PushedAt
	case "pull_request", "pull_request_review":
	case "review_comment":
		applyTruthReviewComment(&candidate, *mutation.ReviewComment)
	case "review_thread":
		if err := applyTruthReviewThread(
			&candidate,
			mutation.PullRequest.Number,
			*mutation.ReviewThread,
		); err != nil {
			return err
		}
	case "check_suite":
	case "check_run":
		applyTruthCheckRun(&candidate, *mutation.CheckRun)
	case "stack":
		for _, pull := range mutation.Stack.PullRequestStates {
			applyTruthPullRequest(&candidate, pull)
		}
		if err := applyTruthStack(&candidate, *mutation.Stack); err != nil {
			return err
		}
	}
	// Keep installation-list reads consistent with the per-repository REST
	// endpoint after commit/push mutations change repository metadata.
	candidate.Repositories = []Repository{candidate.Repository}
	if fixture == nil {
		s.additionalFixtures[key] = &candidate
	} else {
		*fixture = candidate
	}
	s.truthKeys[key] = struct{}{}
	return nil
}

func validateTruthMutation(mutation TruthMutation) error {
	switch mutation.Kind {
	case "repository":
	case "commit":
		if mutation.Commit == nil {
			return fmt.Errorf("commit mutation is missing commit")
		}
	case "push":
		if mutation.Push == nil {
			return fmt.Errorf("push mutation is missing push")
		}
	case "pull_request":
		if mutation.PullRequest == nil {
			return fmt.Errorf("pull-request mutation is missing pull request")
		}
	case "pull_request_review":
		if mutation.PullRequest == nil || mutation.Review == nil {
			return fmt.Errorf("pull-request-review mutation is incomplete")
		}
	case "review_comment":
		if mutation.PullRequest == nil || mutation.ReviewComment == nil {
			return fmt.Errorf("review-comment mutation is incomplete")
		}
	case "review_thread":
		if mutation.PullRequest == nil || mutation.ReviewThread == nil {
			return fmt.Errorf("review-thread mutation is incomplete")
		}
	case "check_suite":
		if mutation.CheckSuite == nil {
			return fmt.Errorf("check-suite mutation is missing suite")
		}
	case "check_run":
		if mutation.CheckRun == nil {
			return fmt.Errorf("check-run mutation is missing run")
		}
	case "stack":
		if mutation.Stack == nil {
			return fmt.Errorf("stack mutation is missing stack")
		}
		if mutation.Stack.ID <= 0 || mutation.Stack.Number <= 0 ||
			len(mutation.Stack.PullRequests) < 2 {
			return fmt.Errorf("stack identity or members are incomplete")
		}
		if len(mutation.Stack.PullRequestStates) !=
			len(mutation.Stack.PullRequests) {
			return fmt.Errorf(
				"stack has %d members but %d member states",
				len(mutation.Stack.PullRequests),
				len(mutation.Stack.PullRequestStates),
			)
		}
		seen := make(map[int]struct{}, len(mutation.Stack.PullRequests))
		for index, pull := range mutation.Stack.PullRequestStates {
			if pull.ID <= 0 || pull.NodeID == "" || pull.Number <= 0 ||
				pull.Head.Ref == "" || pull.Head.SHA == "" ||
				pull.Base.Ref == "" || pull.CreatedAt.IsZero() ||
				pull.UpdatedAt.IsZero() {
				return fmt.Errorf(
					"stack member %d identity or timestamp is incomplete",
					index,
				)
			}
			if pull.Number != mutation.Stack.PullRequests[index] {
				return fmt.Errorf(
					"stack member %d number %d does not match member list %d",
					index,
					pull.Number,
					mutation.Stack.PullRequests[index],
				)
			}
			if _, duplicate := seen[pull.Number]; duplicate {
				return fmt.Errorf(
					"stack repeats pull request %d",
					pull.Number,
				)
			}
			seen[pull.Number] = struct{}{}
		}
	default:
		return fmt.Errorf("unsupported truth mutation kind %q", mutation.Kind)
	}
	return nil
}

func applyTruthRepository(
	fixture *Fixture,
	repository TruthRepository,
	replaceMutableState bool,
) {
	priorDefaultBranchSHA := fixture.Repository.DefaultBranchSHA
	priorPushedAt := fixture.Repository.PushedAt
	fixture.Owner = repository.Owner
	fixture.Repo = repository.Name
	fixture.Repository = truthRepository(repository)
	if !replaceMutableState && priorDefaultBranchSHA != "" {
		fixture.Repository.DefaultBranchSHA = priorDefaultBranchSHA
	}
	if !replaceMutableState && !priorPushedAt.IsZero() {
		fixture.Repository.PushedAt = priorPushedAt
	}
	fixture.Repositories = []Repository{fixture.Repository}
}

func truthRepository(repository TruthRepository) Repository {
	return Repository{
		ID:               repository.ID,
		NodeID:           repository.NodeID,
		Owner:            repository.Owner,
		Name:             repository.Name,
		FullName:         repository.FullName(),
		DefaultBranch:    repository.DefaultBranch,
		DefaultBranchSHA: repository.DefaultBranchSHA,
		UpdatedAt:        repository.UpdatedAt,
		PushedAt:         repository.UpdatedAt,
	}
}

func applyTruthPullRequest(fixture *Fixture, mutation TruthPullRequest) {
	index := -1
	var threads []ReviewThread
	var reviewRequests []ReviewRequest
	var stack *StackRef
	for candidate := range fixture.PullRequests {
		if fixture.PullRequests[candidate].Number != mutation.Number {
			continue
		}
		index = candidate
		threads = fixture.PullRequests[candidate].ReviewThreads
		reviewRequests = fixture.PullRequests[candidate].ReviewRequests
		stack = fixture.PullRequests[candidate].Stack
		break
	}
	pull := PullRequest{
		ID:             mutation.ID,
		NodeID:         mutation.NodeID,
		Number:         mutation.Number,
		Title:          mutation.Title,
		State:          mutation.State,
		Draft:          mutation.Draft,
		AuthorLogin:    mutation.AuthorLogin,
		ReviewDecision: mutation.ReviewDecision,
		MergeableState: mutation.MergeableState,
		Head: PullRequestBranch{
			Ref: mutation.Head.Ref,
			SHA: mutation.Head.SHA,
		},
		Base: PullRequestBranch{
			Ref: mutation.Base.Ref,
			SHA: mutation.Base.SHA,
		},
		UpdatedAt:      mutation.UpdatedAt,
		CreatedAt:      mutation.CreatedAt,
		MergedAt:       cloneTime(mutation.MergedAt),
		Stack:          stack,
		ReviewThreads:  threads,
		ReviewRequests: reviewRequests,
	}
	if index >= 0 {
		fixture.PullRequests[index] = pull
	} else {
		fixture.PullRequests = append(fixture.PullRequests, pull)
	}
	for stackIndex := range fixture.Stacks {
		member := false
		for entryIndex := range fixture.Stacks[stackIndex].PullRequests {
			entry := &fixture.Stacks[stackIndex].PullRequests[entryIndex]
			if entry.Number == pull.Number {
				member = true
				entry.State = pull.State
				entry.Draft = pull.Draft
				entry.UpdatedAt = pull.UpdatedAt
				entry.Head = pull.Head
				entry.MergedAt = cloneTime(mutation.MergedAt)
			}
		}
		if !member {
			continue
		}
		stack := &fixture.Stacks[stackIndex]
		stack.Open = false
		stack.UpdatedAt = stack.CreatedAt
		for _, entry := range stack.PullRequests {
			stack.Open = stack.Open || entry.State == "open"
			if entry.UpdatedAt.After(stack.UpdatedAt) {
				stack.UpdatedAt = entry.UpdatedAt
			}
		}
	}
}

func applyTruthReviewThread(
	fixture *Fixture,
	pullNumber int,
	mutation TruthReviewThread,
) error {
	for pullIndex := range fixture.PullRequests {
		pull := &fixture.PullRequests[pullIndex]
		if pull.Number != pullNumber {
			continue
		}
		thread := ReviewThread{
			ID:         mutation.ID,
			IsResolved: mutation.IsResolved,
			IsOutdated: mutation.IsOutdated,
			Path:       mutation.Path,
			Line:       cloneInt(mutation.Line),
			Comments:   make([]ReviewComment, 0, len(mutation.Comments)),
		}
		for _, comment := range mutation.Comments {
			thread.Comments = append(thread.Comments, ReviewComment{
				ID:          comment.NodeID,
				Body:        comment.Body,
				UpdatedAt:   comment.UpdatedAt,
				AuthorLogin: comment.AuthorLogin,
			})
		}
		for index := range pull.ReviewThreads {
			if pull.ReviewThreads[index].ID == thread.ID {
				pull.ReviewThreads[index] = thread
				return nil
			}
		}
		pull.ReviewThreads = append(pull.ReviewThreads, thread)
		return nil
	}
	return fmt.Errorf(
		"review-thread mutation references unknown pull request %d",
		pullNumber,
	)
}

func applyTruthReviewComment(
	fixture *Fixture,
	mutation TruthReviewComment,
) {
	for pullIndex := range fixture.PullRequests {
		pull := &fixture.PullRequests[pullIndex]
		for threadIndex := range pull.ReviewThreads {
			thread := &pull.ReviewThreads[threadIndex]
			for commentIndex := range thread.Comments {
				if thread.Comments[commentIndex].ID != mutation.NodeID {
					continue
				}
				thread.Comments[commentIndex] = ReviewComment{
					ID:          mutation.NodeID,
					Body:        mutation.Body,
					UpdatedAt:   mutation.UpdatedAt,
					AuthorLogin: mutation.AuthorLogin,
				}
				return
			}
		}
	}
}

func applyTruthCheckRun(fixture *Fixture, mutation TruthCheckRun) {
	run := CheckRun{
		ID:          mutation.ID,
		NodeID:      mutation.NodeID,
		HeadSHA:     mutation.HeadSHA,
		Name:        mutation.Name,
		Status:      mutation.Status,
		Conclusion:  mutation.Conclusion,
		DetailsURL:  mutation.DetailsURL,
		AppSlug:     mutation.AppSlug,
		StartedAt:   cloneTime(mutation.StartedAt),
		CompletedAt: cloneTime(mutation.CompletedAt),
	}
	for index := range fixture.CheckRuns {
		if fixture.CheckRuns[index].ID == run.ID {
			fixture.CheckRuns[index] = run
			return
		}
	}
	fixture.CheckRuns = append(fixture.CheckRuns, run)
}

func applyTruthStack(fixture *Fixture, mutation TruthStack) error {
	now := fixture.Repository.UpdatedAt
	if now.IsZero() {
		now = time.Unix(0, 0).UTC()
	}
	stack := Stack{
		ID:        mutation.ID,
		Number:    mutation.Number,
		NodeID:    fmt.Sprintf("S_replay_%d", mutation.ID),
		URL:       fmt.Sprintf("https://api.github.com/repos/%s/stacks/%d", fixture.Repository.FullName, mutation.Number),
		Base:      Base{Ref: mutation.Base.Ref, SHA: mutation.Base.SHA},
		Open:      false,
		CreatedAt: now,
		UpdatedAt: now,
	}
	byNumber := make(map[int]*PullRequest, len(fixture.PullRequests))
	for index := range fixture.PullRequests {
		byNumber[fixture.PullRequests[index].Number] =
			&fixture.PullRequests[index]
	}
	for position, number := range mutation.PullRequests {
		pull := byNumber[number]
		if pull == nil {
			return fmt.Errorf(
				"stack mutation references unknown pull request %d",
				number,
			)
		}
		if pull.State == "open" {
			stack.Open = true
		}
		if pull.UpdatedAt.After(stack.UpdatedAt) {
			stack.UpdatedAt = pull.UpdatedAt
		}
		pull.Stack = &StackRef{
			ID:       stack.ID,
			Number:   stack.Number,
			Size:     len(mutation.PullRequests),
			Position: position + 1,
			Base:     stack.Base,
		}
		stack.PullRequests = append(stack.PullRequests, StackPullRequest{
			Number:    pull.Number,
			State:     pull.State,
			Draft:     pull.Draft,
			MergedAt:  cloneTime(pull.MergedAt),
			UpdatedAt: pull.UpdatedAt,
			Head:      pull.Head,
		})
	}
	for index := range fixture.Stacks {
		if fixture.Stacks[index].Number == stack.Number {
			stack.CreatedAt = fixture.Stacks[index].CreatedAt
			fixture.Stacks[index] = stack
			return nil
		}
	}
	fixture.Stacks = append(fixture.Stacks, stack)
	return nil
}

func snapshotFixture(fixture Fixture) TruthFixtureSnapshot {
	snapshot := TruthFixtureSnapshot{
		Repository:   fixture.Repository,
		PullRequests: make([]TruthPullRequestSnapshot, 0, len(fixture.PullRequests)),
		Stacks:       append([]Stack(nil), fixture.Stacks...),
		CheckRuns:    make([]TruthCheckRunSnapshot, 0, len(fixture.CheckRuns)),
	}
	for _, pull := range fixture.PullRequests {
		snapshot.PullRequests = append(
			snapshot.PullRequests,
			TruthPullRequestSnapshot{
				ID:             pull.ID,
				NodeID:         pull.NodeID,
				Number:         pull.Number,
				Title:          pull.Title,
				State:          pull.State,
				Draft:          pull.Draft,
				AuthorLogin:    pull.AuthorLogin,
				ReviewDecision: pull.ReviewDecision,
				MergeableState: pull.MergeableState,
				Head:           pull.Head,
				Base:           pull.Base,
				UpdatedAt:      pull.UpdatedAt,
				Stack:          pull.Stack,
				ReviewRequests: append(
					[]ReviewRequest(nil),
					pull.ReviewRequests...,
				),
			},
		)
		for _, thread := range pull.ReviewThreads {
			updatedAt := pull.UpdatedAt
			for _, comment := range thread.Comments {
				if comment.UpdatedAt.After(updatedAt) {
					updatedAt = comment.UpdatedAt
				}
			}
			snapshot.ReviewThreads = append(
				snapshot.ReviewThreads,
				TruthReviewThreadSnapshot{
					PullRequest: pull.Number,
					ID:          thread.ID,
					IsResolved:  thread.IsResolved,
					IsOutdated:  thread.IsOutdated,
					Path:        thread.Path,
					Line:        cloneInt(thread.Line),
					Comments: append(
						[]ReviewComment(nil),
						thread.Comments...,
					),
					UpdatedAt: updatedAt,
				},
			)
		}
	}
	for _, run := range fixture.CheckRuns {
		snapshot.CheckRuns = append(
			snapshot.CheckRuns,
			TruthCheckRunSnapshot{
				ID:          run.ID,
				NodeID:      run.NodeID,
				HeadSHA:     run.HeadSHA,
				Name:        run.Name,
				Status:      run.Status,
				Conclusion:  run.Conclusion,
				DetailsURL:  run.DetailsURL,
				AppSlug:     run.AppSlug,
				StartedAt:   cloneTime(run.StartedAt),
				CompletedAt: cloneTime(run.CompletedAt),
			},
		)
	}
	sort.Slice(snapshot.PullRequests, func(i, j int) bool {
		return snapshot.PullRequests[i].Number <
			snapshot.PullRequests[j].Number
	})
	sort.Slice(snapshot.Stacks, func(i, j int) bool {
		return snapshot.Stacks[i].Number < snapshot.Stacks[j].Number
	})
	sort.Slice(snapshot.CheckRuns, func(i, j int) bool {
		return snapshot.CheckRuns[i].ID < snapshot.CheckRuns[j].ID
	})
	sort.Slice(snapshot.ReviewThreads, func(i, j int) bool {
		return snapshot.ReviewThreads[i].ID < snapshot.ReviewThreads[j].ID
	})
	return snapshot
}
