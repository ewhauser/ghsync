package fakegithub

import (
	"fmt"
	"strings"
	"time"

	"github.com/ewhauser/ghsync/internal/conformance"
)

// PullRequestWebhookPayload builds a production-shaped pull_request payload
// from the current fixture and a canonical octokit example.
func (s *Server) PullRequestWebhookPayload(
	action string,
	number int,
) (map[string]any, error) {
	payload, err := s.pullRequestRelatedWebhookPayload(
		"pull_request",
		action,
		number,
	)
	if err != nil {
		return nil, err
	}
	payload["number"] = number
	return payload, nil
}

// PullRequestReviewRequestedWebhookPayload builds a production-shaped
// pull_request.review_requested payload for a requested user or team.
func (s *Server) PullRequestReviewRequestedWebhookPayload(
	number int,
	reviewer ReviewRequest,
) (map[string]any, error) {
	return s.pullRequestReviewRequestWebhookPayload(
		"review_requested",
		number,
		reviewer,
	)
}

// PullRequestReviewRequestRemovedWebhookPayload builds a production-shaped
// pull_request.review_request_removed payload for a requested user or team.
func (s *Server) PullRequestReviewRequestRemovedWebhookPayload(
	number int,
	reviewer ReviewRequest,
) (map[string]any, error) {
	return s.pullRequestReviewRequestWebhookPayload(
		"review_request_removed",
		number,
		reviewer,
	)
}

func (s *Server) pullRequestReviewRequestWebhookPayload(
	action string,
	number int,
	reviewer ReviewRequest,
) (map[string]any, error) {
	if reviewer.ID <= 0 || reviewer.NodeID == "" || reviewer.Login == "" {
		return nil, fmt.Errorf("review-request reviewer is incomplete")
	}
	payload, err := s.PullRequestWebhookPayload(action, number)
	if err != nil {
		return nil, err
	}
	switch reviewer.Kind {
	case "user":
		wireReviewer, err := payloadObject(payload, "requested_reviewer")
		if err != nil {
			return nil, err
		}
		wireReviewer["id"] = reviewer.ID
		wireReviewer["node_id"] = reviewer.NodeID
		wireReviewer["login"] = reviewer.Login
		delete(payload, "requested_team")
	case "team":
		delete(payload, "requested_reviewer")
		payload["requested_team"] = reviewRequestTeamPayload(reviewer)
	default:
		return nil, fmt.Errorf(
			"unsupported review-request reviewer kind %q",
			reviewer.Kind,
		)
	}
	return payload, nil
}

func reviewRequestTeamPayload(reviewer ReviewRequest) map[string]any {
	baseURL := "https://api.github.com/teams/" + reviewer.Login
	return map[string]any{
		"name":             reviewer.Login,
		"id":               reviewer.ID,
		"node_id":          reviewer.NodeID,
		"slug":             reviewer.Login,
		"description":      nil,
		"privacy":          "closed",
		"url":              baseURL,
		"html_url":         "https://github.com/orgs/acme/teams/" + reviewer.Login,
		"members_url":      baseURL + "/members{/member}",
		"repositories_url": baseURL + "/repos",
		"permission":       "pull",
	}
}

// PullRequestReviewWebhookPayload builds a production-shaped
// pull_request_review payload from the current fixture and a canonical
// octokit example.
func (s *Server) PullRequestReviewWebhookPayload(
	action string,
	number int,
) (map[string]any, error) {
	return s.pullRequestRelatedWebhookPayload(
		"pull_request_review",
		action,
		number,
	)
}

// PullRequestReviewCommentWebhookPayload builds a production-shaped
// pull_request_review_comment payload from the current fixture and a canonical
// octokit example.
func (s *Server) PullRequestReviewCommentWebhookPayload(
	action string,
	number int,
) (map[string]any, error) {
	return s.pullRequestRelatedWebhookPayload(
		"pull_request_review_comment",
		action,
		number,
	)
}

// PullRequestReviewThreadWebhookPayload builds a production-shaped
// pull_request_review_thread payload from the current fixture and a canonical
// octokit example.
func (s *Server) PullRequestReviewThreadWebhookPayload(
	action string,
	number int,
) (map[string]any, error) {
	return s.pullRequestRelatedWebhookPayload(
		"pull_request_review_thread",
		action,
		number,
	)
}

// CheckRunWebhookPayload builds a production-shaped check_run payload from
// the current fixture and a canonical octokit example.
func (s *Server) CheckRunWebhookPayload(
	action string,
	checkRunID int64,
) (map[string]any, error) {
	payload, err := conformance.ExamplePayload("check_run", action)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	fixture := cloneFixture(&s.fixture)
	s.mu.Unlock()
	var run *CheckRun
	for index := range fixture.CheckRuns {
		if fixture.CheckRuns[index].ID == checkRunID {
			run = &fixture.CheckRuns[index]
			break
		}
	}
	if run == nil {
		return nil, fmt.Errorf("check run %d is not in the fixture", checkRunID)
	}
	repository, err := payloadObject(payload, "repository")
	if err != nil {
		return nil, err
	}
	overlayRepositoryPayload(repository, fixture.Repository)
	wireRun, err := payloadObject(payload, "check_run")
	if err != nil {
		return nil, err
	}
	wireRun["id"] = run.ID
	wireRun["node_id"] = run.NodeID
	wireRun["head_sha"] = run.HeadSHA
	wireRun["name"] = run.Name
	wireRun["status"] = run.Status
	wireRun["details_url"] = run.DetailsURL
	wireRun["started_at"] = nullableTime(run.StartedAt)
	wireRun["completed_at"] = nullableTime(run.CompletedAt)
	if run.Conclusion == "" {
		wireRun["conclusion"] = nil
	} else {
		wireRun["conclusion"] = run.Conclusion
	}
	app, err := payloadObject(wireRun, "app")
	if err != nil {
		return nil, err
	}
	app["slug"] = run.AppSlug
	normalizeAppTimestamps(app)
	if suite, ok := wireRun["check_suite"].(map[string]any); ok {
		if suiteApp, ok := suite["app"].(map[string]any); ok {
			normalizeAppTimestamps(suiteApp)
		}
	}
	return payload, nil
}

// CheckSuiteWebhookPayload builds a production-shaped check_suite payload for
// one fixture head SHA from a canonical octokit example.
func (s *Server) CheckSuiteWebhookPayload(
	action string,
	headSHA string,
) (map[string]any, error) {
	payload, err := conformance.ExamplePayload("check_suite", action)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	fixture := cloneFixture(&s.fixture)
	s.mu.Unlock()
	if headSHA == "" {
		return nil, fmt.Errorf("check suite head SHA is required")
	}
	repository, err := payloadObject(payload, "repository")
	if err != nil {
		return nil, err
	}
	overlayRepositoryPayload(repository, fixture.Repository)
	suite, err := payloadObject(payload, "check_suite")
	if err != nil {
		return nil, err
	}
	suite["head_sha"] = headSHA
	for _, run := range fixture.CheckRuns {
		if run.HeadSHA != headSHA || run.AppSlug == "" {
			continue
		}
		app, err := payloadObject(suite, "app")
		if err != nil {
			return nil, err
		}
		app["slug"] = run.AppSlug
		normalizeAppTimestamps(app)
		break
	}
	return payload, nil
}

// PushWebhookPayload builds a production-shaped push payload for a fixture
// branch. Empty before or after values use the fixture's default-branch SHA.
func (s *Server) PushWebhookPayload(
	ref string,
	before string,
	after string,
) (map[string]any, error) {
	payload, err := conformance.ExamplePayload("push", "")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	fixture := cloneFixture(&s.fixture)
	s.mu.Unlock()
	if !strings.HasPrefix(ref, "refs/") {
		return nil, fmt.Errorf("push ref %q must start with refs/", ref)
	}
	if before == "" {
		before = fixture.Repository.DefaultBranchSHA
	}
	if after == "" {
		after = fixture.Repository.DefaultBranchSHA
	}
	repository, err := payloadObject(payload, "repository")
	if err != nil {
		return nil, err
	}
	overlayRepositoryPayload(repository, fixture.Repository)
	payload["ref"] = ref
	payload["before"] = before
	payload["after"] = after
	return payload, nil
}

func payloadObject(
	parent map[string]any,
	key string,
) (map[string]any, error) {
	value, ok := parent[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("octokit payload field %q is not an object", key)
	}
	return value, nil
}

func overlayRepositoryPayload(
	payload map[string]any,
	repository Repository,
) {
	payload["id"] = repository.ID
	payload["node_id"] = repository.NodeID
	payload["name"] = repository.Name
	payload["full_name"] = repository.FullName
	payload["default_branch"] = repository.DefaultBranch
	payload["archived"] = repository.Archived
	payload["updated_at"] = repository.UpdatedAt.UTC().Format(time.RFC3339)
	payload["pushed_at"] = repository.PushedAt.UTC().Format(time.RFC3339)
	if owner, ok := payload["owner"].(map[string]any); ok {
		owner["login"] = repository.Owner
	}
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}

// Some upstream webhook examples predate the schema's RFC 3339 requirement
// for GitHub App timestamps.
func normalizeAppTimestamps(app map[string]any) {
	const legacyTimestamp = "2006-01-02 15:04:05"
	for _, key := range []string{"created_at", "updated_at"} {
		value, ok := app[key].(string)
		if !ok {
			continue
		}
		parsed, err := time.Parse(legacyTimestamp, value)
		if err == nil {
			app[key] = parsed.UTC().Format(time.RFC3339)
		}
	}
}

func (s *Server) pullRequestRelatedWebhookPayload(
	event string,
	action string,
	number int,
) (map[string]any, error) {
	payload, err := conformance.ExamplePayload(event, action)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	fixture := cloneFixture(&s.fixture)
	s.mu.Unlock()
	var pull *PullRequest
	for index := range fixture.PullRequests {
		if fixture.PullRequests[index].Number == number {
			pull = &fixture.PullRequests[index]
			break
		}
	}
	if pull == nil {
		return nil, fmt.Errorf("pull request %d is not in the fixture", number)
	}
	repository, err := payloadObject(payload, "repository")
	if err != nil {
		return nil, err
	}
	overlayRepositoryPayload(repository, fixture.Repository)
	wirePull, err := payloadObject(payload, "pull_request")
	if err != nil {
		return nil, err
	}
	if err := overlayPullRequestPayload(
		wirePull,
		*pull,
		fixture.Repository,
	); err != nil {
		return nil, err
	}
	return payload, nil
}

func overlayPullRequestPayload(
	payload map[string]any,
	pull PullRequest,
	repository Repository,
) error {
	payload["id"] = pull.ID
	payload["node_id"] = pull.NodeID
	payload["number"] = pull.Number
	payload["title"] = pull.Title
	payload["state"] = pull.State
	payload["draft"] = pull.Draft
	payload["updated_at"] = pull.UpdatedAt.UTC().Format(time.RFC3339)
	payload["created_at"] = pull.CreatedAt.UTC().Format(time.RFC3339)
	user, err := payloadObject(payload, "user")
	if err != nil {
		return err
	}
	user["login"] = pull.AuthorLogin
	head, err := payloadObject(payload, "head")
	if err != nil {
		return err
	}
	head["ref"] = pull.Head.Ref
	head["sha"] = pull.Head.SHA
	if headRepository, ok := head["repo"].(map[string]any); ok {
		overlayRepositoryPayload(headRepository, repository)
	}
	base, err := payloadObject(payload, "base")
	if err != nil {
		return err
	}
	base["ref"] = pull.Base.Ref
	// The vendored public webhook schema requires this ordinary PR field to
	// remain a string. The private stack extension below is the field GitHub
	// reports as JSON null when its base commit cannot be resolved.
	base["sha"] = pull.Base.SHA
	if baseRepository, ok := base["repo"].(map[string]any); ok {
		overlayRepositoryPayload(baseRepository, repository)
	}
	if pull.Stack == nil {
		payload["stack"] = nil
	} else {
		payload["stack"] = map[string]any{
			"id":       pull.Stack.ID,
			"number":   pull.Stack.Number,
			"size":     pull.Stack.Size,
			"position": pull.Stack.Position,
			"base": map[string]any{
				"ref": pull.Stack.Base.Ref,
				"sha": nullableSHA(pull.Stack.Base.SHA),
			},
		}
	}
	return nil
}
