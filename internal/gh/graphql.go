package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ewhauser/ghsync/internal/budget"
)

const defaultGraphQLResponseBytes = 10 << 20

// MaxPullRequestBatch is GitHub's nodes-per-gang cap used by the coordinator.
const MaxPullRequestBatch = 25

// MaxPullRequestFiles is GitHub's documented changed-file listing cap. The
// mirror stops at this boundary and records explicit truncation when the
// connection reports more files or remains pageable.
const MaxPullRequestFiles = 3000

// GraphQLClient executes budget-gated installation GraphQL calls.
type GraphQLClient struct {
	client           client
	maxResponseBytes int64
}

// GraphQLClientOptions bounds response buffering.
type GraphQLClientOptions struct {
	MaxResponseBytes int64
}

// NewGraphQLClient validates dependencies and constructs a GraphQL client.
func NewGraphQLClient(
	baseURL string,
	gate budget.Doer,
	tokens TokenProvider,
	options ...GraphQLClientOptions,
) (*GraphQLClient, error) {
	common, err := newClient(
		baseURL,
		gate,
		tokens,
		budget.InstallationAuth,
	)
	if err != nil {
		return nil, err
	}
	maxResponseBytes := int64(defaultGraphQLResponseBytes)
	if len(options) > 0 && options[0].MaxResponseBytes > 0 {
		maxResponseBytes = options[0].MaxResponseBytes
	}
	return &GraphQLClient{
		client:           common,
		maxResponseBytes: maxResponseBytes,
	}, nil
}

// GraphQLError is one GitHub GraphQL error entry.
type GraphQLError struct {
	Type       string         `json:"type"`
	Message    string         `json:"message"`
	Path       []any          `json:"path"`
	Extensions map[string]any `json:"extensions"`
}

// GraphQLErrors implements error for a non-empty GraphQL error list.
type GraphQLErrors []GraphQLError

// Error reports the first GraphQL error message.
func (e GraphQLErrors) Error() string {
	if len(e) == 0 {
		return "GitHub GraphQL error"
	}
	return "GitHub GraphQL: " + e[0].Message
}

// GraphQLResponse carries authoritative point accounting and GraphQL errors.
type GraphQLResponse struct {
	RateLimit budget.GraphQLRate
	Errors    []GraphQLError
}

// PullRequestNode is the authoritative GraphQL detail shape used by the M3
// nodes() coordinator. Stack membership is intentionally absent from the
// query because the private-preview stack extension is REST-only; the
// resolve_stack_membership worker covers that authoritative dimension.
type PullRequestNode struct {
	ID             string    `json:"id"`
	DatabaseID     int64     `json:"databaseId"`
	Number         int       `json:"number"`
	Title          string    `json:"title"`
	State          string    `json:"state"`
	IsDraft        bool      `json:"isDraft"`
	UpdatedAt      time.Time `json:"updatedAt"`
	ReviewDecision string    `json:"reviewDecision"`
	Mergeable      string    `json:"mergeable"`
	HeadRefName    string    `json:"headRefName"`
	HeadRefOID     string    `json:"headRefOid"`
	BaseRefName    string    `json:"baseRefName"`
	BaseRefOID     string    `json:"baseRefOid"`
	ChangedFiles   int       `json:"changedFiles"`
	Author         struct {
		Login string `json:"login"`
	} `json:"author"`
	Repository     RepositoryNode              `json:"repository"`
	Files          *PullRequestFilesConnection `json:"files"`
	ReviewRequests struct {
		Nodes    []ReviewRequestNode `json:"nodes"`
		PageInfo PageInfo            `json:"pageInfo"`
	} `json:"reviewRequests"`
	Reviews struct {
		Nodes    []PullRequestReviewNode `json:"nodes"`
		PageInfo PageInfo                `json:"pageInfo"`
	} `json:"reviews"`
	Comments struct {
		Nodes    []IssueCommentNode `json:"nodes"`
		PageInfo PageInfo           `json:"pageInfo"`
	} `json:"comments"`
	ReviewThreads struct {
		Nodes    []ReviewThreadNode `json:"nodes"`
		PageInfo PageInfo           `json:"pageInfo"`
	} `json:"reviewThreads"`
}

// PullRequestChangedFileNode is one GraphQL changed-file fact. GitHub's
// GraphQL type does not expose the prior path for a rename; the fetch layer
// supplements renamed nodes from the bounded REST files endpoint.
type PullRequestChangedFileNode struct {
	Path         string `json:"path"`
	PreviousPath string `json:"-"`
	ChangeType   string `json:"changeType"`
}

// PullRequestFilesConnection carries the authoritative page set and its
// completeness state. Truncated is derived locally from GitHub's 3,000-file
// cap, an inconsistent total, or an unfinished cursor.
type PullRequestFilesConnection struct {
	Nodes      []PullRequestChangedFileNode `json:"nodes"`
	PageInfo   PageInfo                     `json:"pageInfo"`
	TotalCount int                          `json:"totalCount"`
	Truncated  bool                         `json:"-"`
}

// ActorNode preserves every GraphQL Actor variant instead of projecting only
// users. Author is nil when GitHub retains the fact but no longer exposes the
// deleted actor.
type ActorNode struct {
	Typename string `json:"__typename"`
	ID       string `json:"id"`
	Login    string `json:"login"`
}

// BigInt decodes GitHub's GraphQL BigInt scalar, which is serialized as a
// decimal string because it may exceed GraphQL's 32-bit Int range.
type BigInt int64

func (value *BigInt) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		*value = 0
		return nil
	}
	encoded := string(data)
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &encoded); err != nil {
			return fmt.Errorf("decode GraphQL BigInt string: %w", err)
		}
	}
	parsed, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return fmt.Errorf("decode GraphQL BigInt %q: %w", encoded, err)
	}
	*value = BigInt(parsed)
	return nil
}

// PullRequestReviewNode is one identity-keyed review fact.
type PullRequestReviewNode struct {
	ID             string     `json:"id"`
	FullDatabaseID *BigInt    `json:"fullDatabaseId"`
	State          string     `json:"state"`
	SubmittedAt    *time.Time `json:"submittedAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	Author         *ActorNode `json:"author"`
	Commit         *struct {
		OID string `json:"oid"`
	} `json:"commit"`
}

// IssueCommentNode is one ordinary pull-request issue comment. Body is
// intentionally not queried because participation is the public contract.
type IssueCommentNode struct {
	ID             string     `json:"id"`
	FullDatabaseID *BigInt    `json:"fullDatabaseId"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	Author         *ActorNode `json:"author"`
}

// ReviewRequestNode is one current GitHub review request. RequestedReviewer is
// nullable and the live union also includes Bot, Mannequin, EnterpriseTeam,
// and potentially future members. The v1 projection consumes only complete
// User and Team variants; __typename lets the converter exclude the rest
// without misclassifying them.
type ReviewRequestNode struct {
	RequestedReviewer struct {
		Typename   string `json:"__typename"`
		ID         string `json:"id"`
		DatabaseID int64  `json:"databaseId"`
		Login      string `json:"login"`
		Slug       string `json:"slug"`
	} `json:"requestedReviewer"`
}

// PageInfo is GraphQL connection pagination metadata.
type PageInfo struct {
	HasNextPage bool    `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

// RepositoryNode is the cache-relevant repository GraphQL shape.
type RepositoryNode struct {
	ID               string    `json:"id"`
	DatabaseID       int64     `json:"databaseId"`
	Name             string    `json:"name"`
	NameWithOwner    string    `json:"nameWithOwner"`
	IsArchived       bool      `json:"isArchived"`
	UpdatedAt        time.Time `json:"updatedAt"`
	DefaultBranchRef *struct {
		Name   string `json:"name"`
		Target struct {
			OID string `json:"oid"`
		} `json:"target"`
	} `json:"defaultBranchRef"`
	Owner struct {
		Login string `json:"login"`
	} `json:"owner"`
}

// ReviewThreadNode is one review-thread connection node.
type ReviewThreadNode struct {
	ID         string `json:"id"`
	IsResolved bool   `json:"isResolved"`
	IsOutdated bool   `json:"isOutdated"`
	Path       string `json:"path"`
	Line       *int   `json:"line"`
	Comments   struct {
		Nodes    []ReviewCommentNode `json:"nodes"`
		PageInfo PageInfo            `json:"pageInfo"`
	} `json:"comments"`
}

// ReviewCommentNode is one review-comment connection node.
type ReviewCommentNode struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	UpdatedAt time.Time `json:"updatedAt"`
	Author    *struct {
		Login string `json:"login"`
	} `json:"author"`
}

const pullRequestNodesQuery = `query GhsyncPullRequestNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on PullRequest {
      id
      databaseId
      number
      title
      state
      isDraft
      updatedAt
      reviewDecision
      mergeable
      headRefName
      headRefOid
      baseRefName
      baseRefOid
	  changedFiles
      author { login }
      repository {
        id
        databaseId
        name
        nameWithOwner
        isArchived
        updatedAt
        owner { login }
        defaultBranchRef { name target { oid } }
      }
	  files(first: 100) {
		pageInfo { hasNextPage endCursor }
		totalCount
		nodes { path changeType }
	  }
      reviewRequests(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes {
          requestedReviewer {
            __typename
            ... on User { id databaseId login }
            ... on Team { id databaseId slug }
          }
        }
      }
      reviews(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fullDatabaseId
          state
          submittedAt
          updatedAt
          commit { oid }
          author { __typename login ... on Node { id } }
        }
      }
      comments(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fullDatabaseId
          createdAt
          updatedAt
          author { __typename login ... on Node { id } }
        }
      }
      reviewThreads(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 100) {
            pageInfo { hasNextPage endCursor }
            nodes { id body updatedAt author { login } }
          }
        }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const pullRequestFilesPageQuery = `query GhsyncPullRequestFilesPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequest {
      baseRefOid
      headRefOid
      files(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        totalCount
        nodes { path changeType }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const pullRequestReviewsPageQuery = `query GhsyncPullRequestReviewsPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequest {
      reviews(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fullDatabaseId
          state
          submittedAt
          updatedAt
          commit { oid }
          author { __typename login ... on Node { id } }
        }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const pullRequestCommentsPageQuery = `query GhsyncPullRequestCommentsPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequest {
      comments(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fullDatabaseId
          createdAt
          updatedAt
          author { __typename login ... on Node { id } }
        }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const pullRequestReviewRequestsPageQuery = `query GhsyncPullRequestReviewRequestsPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequest {
      reviewRequests(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          requestedReviewer {
            __typename
            ... on User { id databaseId login }
            ... on Team { id databaseId slug }
          }
        }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const pullRequestReviewThreadsPageQuery = `query GhsyncPullRequestReviewThreadsPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequest {
      reviewThreads(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 100) {
            pageInfo { hasNextPage endCursor }
            nodes { id body updatedAt author { login } }
          }
        }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

const reviewThreadCommentsPageQuery = `query GhsyncReviewThreadCommentsPage(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      comments(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes { id body updatedAt author { login } }
      }
    }
  }
  rateLimit { cost limit remaining resetAt }
}`

// BatchPullRequests satisfies up to 25 due PR refreshes with one nodes() call
// (C-P4). The returned order matches the input node-ID order.
func (c *GraphQLClient) BatchPullRequests(
	ctx context.Context,
	class budget.Class,
	nodeIDs []string,
) ([]*PullRequestNode, *GraphQLResponse, error) {
	if len(nodeIDs) == 0 {
		return nil, nil, fmt.Errorf("GraphQL PR batch must not be empty")
	}
	if len(nodeIDs) > MaxPullRequestBatch {
		return nil, nil, fmt.Errorf(
			"GraphQL PR batch has %d nodes, maximum is %d",
			len(nodeIDs),
			MaxPullRequestBatch,
		)
	}
	var data struct {
		Nodes []*PullRequestNode `json:"nodes"`
	}
	response, err := c.Call(
		ctx,
		class,
		pullRequestNodesQuery,
		map[string]any{"ids": nodeIDs},
		&data,
	)
	if err != nil {
		return nil, response, err
	}
	if len(data.Nodes) != len(nodeIDs) {
		return nil, response, fmt.Errorf(
			"GraphQL PR batch returned %d nodes for %d IDs",
			len(data.Nodes),
			len(nodeIDs),
		)
	}
	for _, node := range data.Nodes {
		if node == nil {
			continue
		}
		if err := c.completePullRequestReviewConnections(
			ctx,
			class,
			node,
		); err != nil {
			return nil, response, err
		}
	}
	return data.Nodes, response, nil
}

func (c *GraphQLClient) completePullRequestReviewConnections(
	ctx context.Context,
	class budget.Class,
	pull *PullRequestNode,
) error {
	if pull.Files != nil {
		if len(pull.Files.Nodes) > MaxPullRequestFiles {
			pull.Files.Nodes = pull.Files.Nodes[:MaxPullRequestFiles]
			pull.Files.Truncated = true
		}
		for pull.Files.PageInfo.HasNextPage &&
			len(pull.Files.Nodes) < MaxPullRequestFiles {
			if pull.Files.PageInfo.EndCursor == nil {
				return fmt.Errorf("files hasNextPage without endCursor")
			}
			var data struct {
				Node *struct {
					BaseRefOID string                      `json:"baseRefOid"`
					HeadRefOID string                      `json:"headRefOid"`
					Files      *PullRequestFilesConnection `json:"files"`
				} `json:"node"`
			}
			_, err := c.Call(
				ctx,
				class,
				pullRequestFilesPageQuery,
				map[string]any{
					"id":    pull.ID,
					"after": *pull.Files.PageInfo.EndCursor,
				},
				&data,
			)
			if err != nil {
				return fmt.Errorf("paginate files for %s: %w", pull.ID, err)
			}
			if data.Node == nil || data.Node.Files == nil {
				pull.Files.Truncated = true
				break
			}
			if data.Node.BaseRefOID != pull.BaseRefOID ||
				data.Node.HeadRefOID != pull.HeadRefOID {
				return fmt.Errorf(
					"paginate files for %s: pull request SHA fence changed",
					pull.ID,
				)
			}
			remaining := MaxPullRequestFiles - len(pull.Files.Nodes)
			pageNodes := data.Node.Files.Nodes
			if len(pageNodes) > remaining {
				pageNodes = pageNodes[:remaining]
				pull.Files.Truncated = true
			}
			pull.Files.Nodes = append(pull.Files.Nodes, pageNodes...)
			pull.Files.PageInfo = data.Node.Files.PageInfo
			if data.Node.Files.TotalCount != pull.Files.TotalCount {
				pull.Files.Truncated = true
			}
		}
		if pull.Files.PageInfo.HasNextPage ||
			pull.Files.TotalCount != len(pull.Files.Nodes) ||
			pull.Files.TotalCount > MaxPullRequestFiles {
			pull.Files.Truncated = true
		}
	}
	for pull.ReviewRequests.PageInfo.HasNextPage {
		if pull.ReviewRequests.PageInfo.EndCursor == nil {
			return fmt.Errorf("reviewRequests hasNextPage without endCursor")
		}
		var data struct {
			Node *struct {
				ReviewRequests struct {
					Nodes    []ReviewRequestNode `json:"nodes"`
					PageInfo PageInfo            `json:"pageInfo"`
				} `json:"reviewRequests"`
			} `json:"node"`
		}
		_, err := c.Call(
			ctx,
			class,
			pullRequestReviewRequestsPageQuery,
			map[string]any{
				"id":    pull.ID,
				"after": *pull.ReviewRequests.PageInfo.EndCursor,
			},
			&data,
		)
		if err != nil {
			return fmt.Errorf("paginate reviewRequests for %s: %w", pull.ID, err)
		}
		if data.Node == nil {
			return fmt.Errorf(
				"paginate reviewRequests for %s: node disappeared",
				pull.ID,
			)
		}
		pull.ReviewRequests.Nodes = append(
			pull.ReviewRequests.Nodes,
			data.Node.ReviewRequests.Nodes...,
		)
		pull.ReviewRequests.PageInfo = data.Node.ReviewRequests.PageInfo
	}
	for pull.Reviews.PageInfo.HasNextPage {
		if pull.Reviews.PageInfo.EndCursor == nil {
			return fmt.Errorf("reviews hasNextPage without endCursor")
		}
		var data struct {
			Node *struct {
				Reviews struct {
					Nodes    []PullRequestReviewNode `json:"nodes"`
					PageInfo PageInfo                `json:"pageInfo"`
				} `json:"reviews"`
			} `json:"node"`
		}
		_, err := c.Call(
			ctx,
			class,
			pullRequestReviewsPageQuery,
			map[string]any{
				"id":    pull.ID,
				"after": *pull.Reviews.PageInfo.EndCursor,
			},
			&data,
		)
		if err != nil {
			return fmt.Errorf("paginate reviews for %s: %w", pull.ID, err)
		}
		if data.Node == nil {
			return fmt.Errorf("paginate reviews for %s: node disappeared", pull.ID)
		}
		pull.Reviews.Nodes = append(pull.Reviews.Nodes, data.Node.Reviews.Nodes...)
		pull.Reviews.PageInfo = data.Node.Reviews.PageInfo
	}
	for pull.Comments.PageInfo.HasNextPage {
		if pull.Comments.PageInfo.EndCursor == nil {
			return fmt.Errorf("comments hasNextPage without endCursor")
		}
		var data struct {
			Node *struct {
				Comments struct {
					Nodes    []IssueCommentNode `json:"nodes"`
					PageInfo PageInfo           `json:"pageInfo"`
				} `json:"comments"`
			} `json:"node"`
		}
		_, err := c.Call(
			ctx,
			class,
			pullRequestCommentsPageQuery,
			map[string]any{
				"id":    pull.ID,
				"after": *pull.Comments.PageInfo.EndCursor,
			},
			&data,
		)
		if err != nil {
			return fmt.Errorf("paginate comments for %s: %w", pull.ID, err)
		}
		if data.Node == nil {
			return fmt.Errorf("paginate comments for %s: node disappeared", pull.ID)
		}
		pull.Comments.Nodes = append(
			pull.Comments.Nodes,
			data.Node.Comments.Nodes...,
		)
		pull.Comments.PageInfo = data.Node.Comments.PageInfo
	}
	for index := range pull.ReviewThreads.Nodes {
		if err := c.completeReviewThreadComments(
			ctx,
			class,
			&pull.ReviewThreads.Nodes[index],
		); err != nil {
			return err
		}
	}
	for pull.ReviewThreads.PageInfo.HasNextPage {
		if pull.ReviewThreads.PageInfo.EndCursor == nil {
			return fmt.Errorf("reviewThreads hasNextPage without endCursor")
		}
		var data struct {
			Node *struct {
				ReviewThreads struct {
					Nodes    []ReviewThreadNode `json:"nodes"`
					PageInfo PageInfo           `json:"pageInfo"`
				} `json:"reviewThreads"`
			} `json:"node"`
		}
		_, err := c.Call(
			ctx,
			class,
			pullRequestReviewThreadsPageQuery,
			map[string]any{
				"id":    pull.ID,
				"after": *pull.ReviewThreads.PageInfo.EndCursor,
			},
			&data,
		)
		if err != nil {
			return fmt.Errorf("paginate reviewThreads for %s: %w", pull.ID, err)
		}
		if data.Node == nil {
			return fmt.Errorf(
				"paginate reviewThreads for %s: node disappeared",
				pull.ID,
			)
		}
		for index := range data.Node.ReviewThreads.Nodes {
			if err := c.completeReviewThreadComments(
				ctx,
				class,
				&data.Node.ReviewThreads.Nodes[index],
			); err != nil {
				return err
			}
		}
		pull.ReviewThreads.Nodes = append(
			pull.ReviewThreads.Nodes,
			data.Node.ReviewThreads.Nodes...,
		)
		pull.ReviewThreads.PageInfo = data.Node.ReviewThreads.PageInfo
	}
	return nil
}

func (c *GraphQLClient) completeReviewThreadComments(
	ctx context.Context,
	class budget.Class,
	thread *ReviewThreadNode,
) error {
	for thread.Comments.PageInfo.HasNextPage {
		if thread.Comments.PageInfo.EndCursor == nil {
			return fmt.Errorf(
				"review thread %s comments hasNextPage without endCursor",
				thread.ID,
			)
		}
		var data struct {
			Node *struct {
				Comments struct {
					Nodes    []ReviewCommentNode `json:"nodes"`
					PageInfo PageInfo            `json:"pageInfo"`
				} `json:"comments"`
			} `json:"node"`
		}
		_, err := c.Call(
			ctx,
			class,
			reviewThreadCommentsPageQuery,
			map[string]any{
				"id":    thread.ID,
				"after": *thread.Comments.PageInfo.EndCursor,
			},
			&data,
		)
		if err != nil {
			return fmt.Errorf(
				"paginate review thread %s comments: %w",
				thread.ID,
				err,
			)
		}
		if data.Node == nil {
			return fmt.Errorf(
				"paginate review thread %s comments: node disappeared",
				thread.ID,
			)
		}
		thread.Comments.Nodes = append(
			thread.Comments.Nodes,
			data.Node.Comments.Nodes...,
		)
		thread.Comments.PageInfo = data.Node.Comments.PageInfo
	}
	return nil
}

// Call executes a query that includes a top-level data.rateLimit block.
// extractGraphQLRate reads that block for Gate before Call decodes data.
func (c *GraphQLClient) Call(
	ctx context.Context,
	class budget.Class,
	query string,
	variables map[string]any,
	target any,
) (*GraphQLResponse, error) {
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal GraphQL request: %w", err)
	}
	req, err := c.client.request(
		ctx,
		http.MethodPost,
		"graphql",
		nil,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	gated, err := c.client.gate.Do(
		ctx,
		class,
		budget.NewGraphQLRequest(
			req,
			func(resp *http.Response) (budget.GraphQLRate, bool, error) {
				return extractGraphQLRate(resp, c.maxResponseBytes)
			},
		).BeforeSend(c.client.authorize),
	)
	if err != nil {
		if gated != nil {
			_ = closeResponseBody(gated.HTTP)
		}
		return nil, err
	}
	resp := gated.HTTP
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, decodeHTTPError(resp)
	}
	defer func() { _ = resp.Body.Close() }()
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []GraphQLError  `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode GraphQL response: %w", err)
	}
	if gated.GraphQLRate == nil {
		return nil, fmt.Errorf("GraphQL response omitted data.rateLimit")
	}
	result := &GraphQLResponse{
		RateLimit: *gated.GraphQLRate,
		Errors:    envelope.Errors,
	}
	if target != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return result, fmt.Errorf("decode GraphQL data: %w", err)
		}
	}
	if len(envelope.Errors) > 0 {
		return result, GraphQLErrors(envelope.Errors)
	}
	return result, nil
}

func extractGraphQLRate(
	resp *http.Response,
	maxResponseBytes int64,
) (budget.GraphQLRate, bool, error) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return budget.GraphQLRate{}, false, fmt.Errorf("read GraphQL rateLimit: %w", err)
	}
	if int64(len(body)) > maxResponseBytes {
		_ = resp.Body.Close()
		resp.Body = http.NoBody
		return budget.GraphQLRate{}, false, fmt.Errorf(
			"GraphQL response exceeds %d bytes",
			maxResponseBytes,
		)
	}
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	var envelope struct {
		Data struct {
			RateLimit *struct {
				Cost      int64     `json:"cost"`
				Limit     int64     `json:"limit"`
				Remaining int64     `json:"remaining"`
				ResetAt   time.Time `json:"resetAt"`
			} `json:"rateLimit"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		// The normal response decoder reports malformed JSON with the public
		// GraphQL response context; this observer only extracts optional rate data.
		return budget.GraphQLRate{}, false, nil //nolint:nilerr // the primary decoder reports malformed JSON
	}
	if envelope.Data.RateLimit == nil {
		return budget.GraphQLRate{}, false, nil
	}
	rate := budget.GraphQLRate{
		Cost:      envelope.Data.RateLimit.Cost,
		Limit:     envelope.Data.RateLimit.Limit,
		Remaining: envelope.Data.RateLimit.Remaining,
		ResetAt:   envelope.Data.RateLimit.ResetAt,
	}
	if rate.Limit <= 0 || rate.Remaining < 0 || rate.ResetAt.IsZero() {
		return budget.GraphQLRate{}, false, fmt.Errorf("invalid GraphQL rateLimit block")
	}
	return rate, true, nil
}
