package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/acme/frontier/internal/budget"
)

const defaultGraphQLResponseBytes = 10 << 20
const MaxPullRequestBatch = 25

type GraphQLClient struct {
	client           client
	maxResponseBytes int64
}

type GraphQLClientOptions struct {
	MaxResponseBytes int64
}

func NewGraphQLClient(
	baseURL string,
	gate budget.Doer,
	tokens TokenProvider,
	options ...GraphQLClientOptions,
) (*GraphQLClient, error) {
	common, err := newClient(baseURL, gate, tokens)
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

type GraphQLError struct {
	Type       string         `json:"type"`
	Message    string         `json:"message"`
	Path       []any          `json:"path"`
	Extensions map[string]any `json:"extensions"`
}

type GraphQLErrors []GraphQLError

func (e GraphQLErrors) Error() string {
	if len(e) == 0 {
		return "GitHub GraphQL error"
	}
	return "GitHub GraphQL: " + e[0].Message
}

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
	Author         struct {
		Login string `json:"login"`
	} `json:"author"`
	Repository    RepositoryNode `json:"repository"`
	ReviewThreads struct {
		Nodes    []ReviewThreadNode `json:"nodes"`
		PageInfo PageInfo           `json:"pageInfo"`
	} `json:"reviewThreads"`
}

type PageInfo struct {
	HasNextPage bool    `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

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

type ReviewCommentNode struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	UpdatedAt time.Time `json:"updatedAt"`
	Author    *struct {
		Login string `json:"login"`
	} `json:"author"`
}

const pullRequestNodesQuery = `query FrontierPullRequestNodes($ids: [ID!]!) {
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

const pullRequestReviewThreadsPageQuery = `query FrontierPullRequestReviewThreadsPage(
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

const reviewThreadCommentsPageQuery = `query FrontierReviewThreadCommentsPage(
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
	defer resp.Body.Close()
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
		return budget.GraphQLRate{}, false, nil
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
