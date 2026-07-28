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

	"github.com/acme/frontier/internal/budget"
)

type StackBase struct {
	Ref string `json:"ref"`
	SHA string `json:"sha,omitempty"`
}

type StackPullRequest struct {
	Number   int               `json:"number"`
	State    string            `json:"state"`
	Draft    bool              `json:"draft"`
	MergedAt *time.Time        `json:"merged_at"`
	Head     PullRequestBranch `json:"head"`
}

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
	Stack *StackRef
}

func (p *PullRequest) UnmarshalJSON(data []byte) error {
	var core github.PullRequest
	if err := json.Unmarshal(data, &core); err != nil {
		return err
	}
	var extension struct {
		Stack *StackRef `json:"stack"`
	}
	if err := json.Unmarshal(data, &extension); err != nil {
		return err
	}
	p.PullRequest = &core
	p.Stack = extension.Stack
	return nil
}

type ListStacksOptions struct {
	PullRequest int
	PerPage     int
	Page        int
}

type ListPullsOptions struct {
	State     string
	Sort      string
	Direction string
	PerPage   int
	Page      int
}

// RESTResponse is typed 304/pagination metadata. NotModified is a successful
// conditional result, not an error (C-B4).
type RESTResponse struct {
	StatusCode  int
	ETag        string
	NotModified bool
	NextPage    int
}

type RESTClient struct {
	client client
}

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
	response, err := c.getJSON(ctx, class, path, query, etag, &stacks)
	return stacks, response, err
}

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
	response, err := c.getJSON(ctx, class, path, nil, etag, &stack)
	if err != nil || response.NotModified {
		return nil, response, err
	}
	return &stack, response, nil
}

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
	response, err := c.getJSON(ctx, class, path, query, etag, &pulls)
	return pulls, response, err
}

func (c *RESTClient) getJSON(
	ctx context.Context,
	class budget.Class,
	path string,
	query url.Values,
	etag string,
	target any,
) (*RESTResponse, error) {
	req, err := c.client.request(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return nil, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	gated, err := c.client.gate.Do(
		ctx,
		class,
		budget.NewRESTRequest(req).BeforeSend(c.client.authorize),
	)
	if err != nil {
		if gated != nil && gated.HTTP != nil {
			gated.HTTP.Body.Close()
		}
		return nil, err
	}
	resp := gated.HTTP
	meta := &RESTResponse{
		StatusCode: resp.StatusCode,
		ETag:       resp.Header.Get("ETag"),
		NextPage:   nextPage(resp.Header.Get("Link")),
	}
	if resp.StatusCode == http.StatusNotModified {
		resp.Body.Close()
		meta.NotModified = true
		return meta, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return meta, decodeHTTPError(resp)
	}
	defer resp.Body.Close()
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

func nextPage(link string) int {
	for _, part := range strings.Split(link, ",") {
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
		page, _ := strconv.Atoi(endpoint.Query().Get("page"))
		return page
	}
	return 0
}
