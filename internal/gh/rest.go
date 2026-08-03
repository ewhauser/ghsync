package gh

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v88/github"

	"github.com/ewhauser/ghsync/internal/budget"
)

// StackBase identifies a stack's base ref and commit.
type StackBase struct {
	Ref string `json:"ref"`
	SHA string `json:"sha,omitempty"`
}

// StackPullRequest is one ordered layer in a stack.
type StackPullRequest struct {
	Number    int               `json:"number"`
	State     string            `json:"state"`
	Draft     bool              `json:"draft"`
	MergedAt  *time.Time        `json:"merged_at"`
	UpdatedAt time.Time         `json:"updated_at"`
	Head      PullRequestBranch `json:"head"`
}

// PullRequestBranch identifies one pull request branch.
type PullRequestBranch struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// Stack is the gh-stack private-preview REST resource.
type Stack struct {
	ID           int64              `json:"id"`
	Number       int                `json:"number"`
	NodeID       string             `json:"node_id"`
	URL          string             `json:"url"`
	Base         StackBase          `json:"base"`
	Open         bool               `json:"open"`
	CreatedAt    time.Time          `json:"created_at"`
	UpdatedAt    time.Time          `json:"updated_at"`
	PullRequests []StackPullRequest `json:"pull_requests"` // bottom to top
}

// StackRef is the private-preview extension carried by ordinary pull request
// responses.
type StackRef struct {
	ID       int64     `json:"id"`
	Number   int       `json:"number"`
	Size     int       `json:"size"`
	Position int       `json:"position"`
	Base     StackBase `json:"base"`
}

// PullRequest keeps google/go-github's typed pull request while preserving
// the preview-only stack extension that the library does not yet model.
type PullRequest struct {
	*github.PullRequest
	Stack          *StackRef `json:"stack,omitempty"`
	ReviewDecision string    `json:"review_decision,omitempty"`
}

// IsSupportedReviewRequestUser reports whether a REST requested_reviewer is
// a complete GitHub User identity covered by the v1 review-request contract.
// GitHub also uses this shape for Bots and other account types; those are not
// user requests and must not be mislabeled as one.
func IsSupportedReviewRequestUser(user *github.User) bool {
	if user == nil || user.GetID() <= 0 || user.GetNodeID() == "" ||
		user.GetLogin() == "" {
		return false
	}
	return user.GetType() == "" || user.GetType() == "User"
}

// IsSupportedReviewRequestTeam reports whether a REST requested_team has the
// stable identities and slug required by the v1 review-request contract.
func IsSupportedReviewRequestTeam(team *github.Team) bool {
	return team != nil && team.GetID() > 0 && team.GetNodeID() != "" &&
		team.GetSlug() != ""
}

// UnmarshalJSON preserves the private-preview stack extension.
func (p *PullRequest) UnmarshalJSON(data []byte) error {
	var core github.PullRequest
	if err := json.Unmarshal(data, &core); err != nil {
		return err
	}
	var extension struct {
		Stack          *StackRef `json:"stack"`
		ReviewDecision string    `json:"review_decision"`
	}
	if err := json.Unmarshal(data, &extension); err != nil {
		return err
	}
	p.PullRequest = &core
	p.Stack = extension.Stack
	p.ReviewDecision = extension.ReviewDecision
	return nil
}

// Repository is the subset of repository truth needed by the mirror.
type Repository struct {
	ID            int64     `json:"id"`
	NodeID        string    `json:"node_id"`
	Owner         string    `json:"-"`
	Name          string    `json:"name"`
	FullName      string    `json:"full_name"`
	DefaultBranch string    `json:"default_branch"`
	Archived      bool      `json:"archived"`
	UpdatedAt     time.Time `json:"updated_at"`
	PushedAt      time.Time `json:"pushed_at"`
}

// UnmarshalJSON flattens GitHub's nested owner login.
func (r *Repository) UnmarshalJSON(data []byte) error {
	type wireRepository struct {
		ID            int64     `json:"id"`
		NodeID        string    `json:"node_id"`
		Name          string    `json:"name"`
		FullName      string    `json:"full_name"`
		DefaultBranch string    `json:"default_branch"`
		Archived      bool      `json:"archived"`
		UpdatedAt     time.Time `json:"updated_at"`
		PushedAt      time.Time `json:"pushed_at"`
		Owner         struct {
			Login string `json:"login"`
		} `json:"owner"`
	}
	var wire wireRepository
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*r = Repository{
		ID:            wire.ID,
		NodeID:        wire.NodeID,
		Owner:         wire.Owner.Login,
		Name:          wire.Name,
		FullName:      wire.FullName,
		DefaultBranch: wire.DefaultBranch,
		Archived:      wire.Archived,
		UpdatedAt:     wire.UpdatedAt,
		PushedAt:      wire.PushedAt,
	}
	return nil
}

// CheckRun is the cache-relevant check-run shape.
type CheckRun struct {
	ID          int64           `json:"id"`
	NodeID      string          `json:"node_id"`
	HeadSHA     string          `json:"head_sha"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	Conclusion  string          `json:"conclusion"`
	DetailsURL  string          `json:"details_url"`
	AppSlug     string          `json:"-"`
	StartedAt   *time.Time      `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at"`
	Raw         json.RawMessage `json:"-"`
}

// UnmarshalJSON flattens GitHub's nested App slug.
func (c *CheckRun) UnmarshalJSON(data []byte) error {
	type wireCheckRun struct {
		ID          int64      `json:"id"`
		NodeID      string     `json:"node_id"`
		HeadSHA     string     `json:"head_sha"`
		Name        string     `json:"name"`
		Status      string     `json:"status"`
		Conclusion  string     `json:"conclusion"`
		DetailsURL  string     `json:"details_url"`
		StartedAt   *time.Time `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at"`
		App         struct {
			Slug string `json:"slug"`
		} `json:"app"`
	}
	var wire wireCheckRun
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*c = CheckRun{
		ID:          wire.ID,
		NodeID:      wire.NodeID,
		HeadSHA:     wire.HeadSHA,
		Name:        wire.Name,
		Status:      wire.Status,
		Conclusion:  wire.Conclusion,
		DetailsURL:  wire.DetailsURL,
		AppSlug:     wire.App.Slug,
		StartedAt:   wire.StartedAt,
		CompletedAt: wire.CompletedAt,
		Raw:         append(json.RawMessage(nil), data...),
	}
	return nil
}

// ListCheckRunsOptions controls checks pagination.
type ListCheckRunsOptions struct {
	PerPage int
	Page    int
}

// ListStacksOptions controls stack filtering and pagination.
type ListStacksOptions struct {
	PullRequest int
	PerPage     int
	Page        int
}

// ListPullsOptions controls pull-request filtering and pagination.
type ListPullsOptions struct {
	State     string
	Sort      string
	Direction string
	PerPage   int
	Page      int
}

// ListRepositoriesOptions controls installation repository pagination.
type ListRepositoriesOptions struct {
	PerPage int
	Page    int
}

// RepositoryRule preserves the complete ruleset payload while exposing the
// immutable key and optional semantic timestamp used by the mirror CAS.
type RepositoryRule struct {
	ID        int64
	UpdatedAt *time.Time
	Raw       json.RawMessage
}

// UnmarshalJSON retains the complete raw rule while extracting CAS metadata.
func (r *RepositoryRule) UnmarshalJSON(data []byte) error {
	var metadata struct {
		ID        int64      `json:"id"`
		UpdatedAt *time.Time `json:"updated_at"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	r.ID = metadata.ID
	r.UpdatedAt = metadata.UpdatedAt
	r.Raw = append(r.Raw[:0], data...)
	return nil
}

// RESTResponse is typed 304/pagination metadata. NotModified is a successful
// conditional result, not an error (C-B4).
type RESTResponse struct {
	StatusCode  int
	ETag        string
	NotModified bool
	NextPage    int
	NextCursor  string
}

// RESTClient fetches installation-authenticated GitHub REST resources.
type RESTClient struct {
	client client
}

// NewRESTClient validates dependencies and constructs a REST client.
func NewRESTClient(
	baseURL string,
	gate budget.Doer,
	tokens TokenProvider,
) (*RESTClient, error) {
	common, err := newClient(baseURL, gate, tokens)
	if err != nil {
		return nil, err
	}
	return &RESTClient{client: common}, nil
}

// GetRepository fetches one repository. A conditional 304 returns
// (nil, response, nil); callers must inspect response.NotModified before
// dereferencing the repository.
func (c *RESTClient) GetRepository(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	etag string,
) (*Repository, *RESTResponse, error) {
	path := fmt.Sprintf("repos/%s/%s", url.PathEscape(owner), url.PathEscape(repo))
	var repository Repository
	response, err := c.client.getJSON(ctx, class, path, nil, etag, &repository)
	if err != nil || response.NotModified {
		return nil, response, err
	}
	return &repository, response, nil
}

// ListInstallationRepositories fetches one installation repository page.
func (c *RESTClient) ListInstallationRepositories(
	ctx context.Context,
	class budget.Class,
	options ListRepositoriesOptions,
	etag string,
) ([]Repository, *RESTResponse, error) {
	query := make(url.Values)
	setPagination(query, options.PerPage, options.Page)
	var payload struct {
		Repositories []Repository `json:"repositories"`
	}
	response, err := c.client.getJSON(
		ctx,
		class,
		"installation/repositories",
		query,
		etag,
		&payload,
	)
	return payload.Repositories, response, err
}

// ListRepositoryRules fetches the complete ruleset page for one repository.
func (c *RESTClient) ListRepositoryRules(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	etag string,
) ([]RepositoryRule, *RESTResponse, error) {
	path := fmt.Sprintf(
		"repos/%s/%s/rulesets",
		url.PathEscape(owner),
		url.PathEscape(repo),
	)
	var rules []RepositoryRule
	response, err := c.client.getJSON(ctx, class, path, nil, etag, &rules)
	return rules, response, err
}

// ListStacks fetches one gh-stack preview page.
func (c *RESTClient) ListStacks(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	options ListStacksOptions,
	etag string,
) ([]Stack, *RESTResponse, error) {
	query := make(url.Values)
	if options.PullRequest > 0 {
		query.Set("pull_request", strconv.Itoa(options.PullRequest))
	}
	setPagination(query, options.PerPage, options.Page)
	path := fmt.Sprintf("repos/%s/%s/stacks", url.PathEscape(owner), url.PathEscape(repo))
	var stacks []Stack
	response, err := c.client.getJSON(ctx, class, path, query, etag, &stacks)
	return stacks, response, err
}

// GetStack fetches one stack. A conditional 304 returns
// (nil, response, nil); callers must inspect response.NotModified before
// dereferencing the stack.
func (c *RESTClient) GetStack(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	number int,
	etag string,
) (*Stack, *RESTResponse, error) {
	path := fmt.Sprintf(
		"repos/%s/%s/stacks/%d",
		url.PathEscape(owner),
		url.PathEscape(repo),
		number,
	)
	var stack Stack
	response, err := c.client.getJSON(ctx, class, path, nil, etag, &stack)
	if err != nil || response.NotModified {
		return nil, response, err
	}
	return &stack, response, nil
}

// ListPulls fetches one pull-request page.
func (c *RESTClient) ListPulls(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	options ListPullsOptions,
	etag string,
) ([]PullRequest, *RESTResponse, error) {
	query := make(url.Values)
	if options.State != "" {
		query.Set("state", options.State)
	}
	if options.Sort != "" {
		query.Set("sort", options.Sort)
	}
	if options.Direction != "" {
		query.Set("direction", options.Direction)
	}
	setPagination(query, options.PerPage, options.Page)
	path := fmt.Sprintf("repos/%s/%s/pulls", url.PathEscape(owner), url.PathEscape(repo))
	var pulls []PullRequest
	response, err := c.client.getJSON(ctx, class, path, query, etag, &pulls)
	return pulls, response, err
}

// GetPull fetches one pull request. A conditional 304 returns
// (nil, response, nil); callers must inspect response.NotModified before
// dereferencing the pull request.
func (c *RESTClient) GetPull(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	number int,
	etag string,
) (*PullRequest, *RESTResponse, error) {
	path := fmt.Sprintf(
		"repos/%s/%s/pulls/%d",
		url.PathEscape(owner),
		url.PathEscape(repo),
		number,
	)
	var pull PullRequest
	response, err := c.client.getJSON(ctx, class, path, nil, etag, &pull)
	if err != nil || response.NotModified {
		return nil, response, err
	}
	return &pull, response, nil
}

// ListCheckRuns fetches one checks page for a head SHA.
func (c *RESTClient) ListCheckRuns(
	ctx context.Context,
	class budget.Class,
	owner string,
	repo string,
	headSHA string,
	options ListCheckRunsOptions,
	etag string,
) ([]CheckRun, *RESTResponse, error) {
	query := make(url.Values)
	setPagination(query, options.PerPage, options.Page)
	path := fmt.Sprintf(
		"repos/%s/%s/commits/%s/check-runs",
		url.PathEscape(owner),
		url.PathEscape(repo),
		url.PathEscape(headSHA),
	)
	var payload struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	response, err := c.client.getJSON(ctx, class, path, query, etag, &payload)
	return payload.CheckRuns, response, err
}

func (c client) getJSON(
	ctx context.Context,
	class budget.Class,
	path string,
	query url.Values,
	etag string,
	target any,
) (*RESTResponse, error) {
	req, err := c.request(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return nil, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	gated, err := c.gate.Do(
		ctx,
		class,
		budget.NewRESTRequest(req).BeforeSend(c.authorize),
	)
	if err != nil {
		if gated != nil {
			_ = closeResponseBody(gated.HTTP)
		}
		return nil, err
	}
	resp := gated.HTTP
	responseETag := resp.Header.Get("ETag")
	if resp.StatusCode == http.StatusNotModified && responseETag == "" {
		responseETag = req.Header.Get("If-None-Match")
	}
	nextPage, nextCursor := parseNextLink(resp.Header.Get("Link"))
	meta := &RESTResponse{
		StatusCode: resp.StatusCode,
		ETag:       responseETag,
		NextPage:   nextPage,
		NextCursor: nextCursor,
	}
	if resp.StatusCode == http.StatusNotModified {
		_ = resp.Body.Close()
		meta.NotModified = true
		return meta, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return meta, decodeHTTPError(resp)
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return meta, fmt.Errorf("decode GitHub response: %w", err)
	}
	return meta, nil
}

func setPagination(query url.Values, perPage, page int) {
	if perPage > 0 {
		query.Set("per_page", strconv.Itoa(perPage))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
}

func parseNextLink(link string) (int, string) {
	for part := range strings.SplitSeq(link, ",") {
		if !strings.Contains(part, `rel="next"`) {
			continue
		}
		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start < 0 || end <= start {
			continue
		}
		endpoint, err := url.Parse(part[start+1 : end])
		if err != nil {
			continue
		}
		query := endpoint.Query()
		page, _ := strconv.Atoi(query.Get("page"))
		return page, query.Get("cursor")
	}
	return 0, ""
}
