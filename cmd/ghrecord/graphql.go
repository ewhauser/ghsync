package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxGraphQLResponseBytes = 16 << 20

type graphQLClient struct {
	url        string
	token      string
	httpClient *http.Client
}

type graphQLError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type graphQLRateLimit struct {
	Cost      int       `json:"cost"`
	Remaining int       `json:"remaining"`
	ResetAt   time.Time `json:"resetAt"`
}

type rateLimitError struct {
	ResetAt time.Time
	Message string
}

func (e *rateLimitError) Error() string {
	if e.ResetAt.IsZero() {
		return "GitHub rate limit paused crawl: " + e.Message
	}
	return fmt.Sprintf(
		"GitHub rate limit paused crawl until %s: %s",
		e.ResetAt.UTC().Format(time.RFC3339),
		e.Message,
	)
}

func newGraphQLClient(
	endpoint string,
	token string,
	httpClient *http.Client,
) (*graphQLClient, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("GraphQL URL must be absolute")
	}
	if httpClient == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}
	return &graphQLClient{
		url:        endpoint,
		token:      token,
		httpClient: httpClient,
	}, nil
}

func (c *graphQLClient) call(
	ctx context.Context,
	query string,
	variables map[string]any,
	target any,
) error {
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return fmt.Errorf("marshal GraphQL request: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.url,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create GraphQL request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "ghsync-ghrecord/1")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call GitHub GraphQL: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message, readErr := io.ReadAll(
			io.LimitReader(response.Body, 64<<10),
		)
		if readErr != nil {
			return fmt.Errorf(
				"read GraphQL HTTP %d response: %w",
				response.StatusCode,
				readErr,
			)
		}
		if isRateLimitedResponse(response, message) {
			return &rateLimitError{
				ResetAt: parseRateReset(response.Header, time.Now()),
				Message: strings.TrimSpace(string(message)),
			}
		}
		return fmt.Errorf(
			"GitHub GraphQL returned HTTP %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(message)),
		)
	}
	limited := io.LimitReader(response.Body, maxGraphQLResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read GraphQL response: %w", err)
	}
	if len(responseBody) > maxGraphQLResponseBytes {
		return fmt.Errorf(
			"GraphQL response exceeds %d bytes",
			maxGraphQLResponseBytes,
		)
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []graphQLError  `json:"errors"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return fmt.Errorf("decode GraphQL response: %w", err)
	}
	var rateContainer struct {
		RateLimit graphQLRateLimit `json:"rateLimit"`
	}
	if len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, &rateContainer); err != nil {
			return fmt.Errorf(
				"decode GraphQL rate limit: %w",
				err,
			)
		}
	}
	if len(envelope.Errors) > 0 {
		first := envelope.Errors[0]
		if strings.Contains(strings.ToUpper(first.Type), "RATE_LIMIT") ||
			strings.Contains(strings.ToLower(first.Message), "rate limit") {
			return &rateLimitError{
				ResetAt: rateContainer.RateLimit.ResetAt,
				Message: first.Message,
			}
		}
		return fmt.Errorf(
			"GitHub GraphQL: %s",
			first.Message,
		)
	}
	if target != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return fmt.Errorf(
				"decode GraphQL data: %w",
				err,
			)
		}
	}
	return nil
}

func isRateLimitedResponse(response *http.Response, body []byte) bool {
	if response.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if response.StatusCode != http.StatusForbidden {
		return false
	}
	if response.Header.Get("X-RateLimit-Remaining") == "0" ||
		response.Header.Get("Retry-After") != "" {
		return true
	}
	message := strings.ToLower(string(body))
	return strings.Contains(message, "rate limit") ||
		strings.Contains(message, "secondary limit") ||
		strings.Contains(message, "abuse detection")
}

func parseRateReset(header http.Header, now time.Time) time.Time {
	raw := header.Get("X-RateLimit-Reset")
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err == nil {
		return time.Unix(seconds, 0).UTC()
	}
	retryAfter := header.Get("Retry-After")
	if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil &&
		seconds >= 0 {
		return now.UTC().Add(time.Duration(seconds) * time.Second)
	}
	if parsed, err := http.ParseTime(retryAfter); err == nil {
		return parsed.UTC()
	}
	return time.Time{}
}

const listPullRequestsQuery = `query GhsyncRecordingPulls(
  $owner: String!,
  $name: String!,
  $query: String!,
  $after: String
) {
  repository(owner: $owner, name: $name) {
    id
    databaseId
    name
    nameWithOwner
    updatedAt
    owner { login }
    defaultBranchRef { name target { oid } }
  }
  search(query: $query, type: ISSUE, first: 50, after: $after) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes { ... on PullRequest { number } }
  }
  rateLimit { cost remaining resetAt }
}`

const pullRequestTimelineQuery = `query GhsyncRecordingPull(
  $owner: String!,
  $name: String!,
  $number: Int!,
  $after: String
) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      id
      databaseId
      number
      title
      state
      isDraft
      merged
      createdAt
      updatedAt
      closedAt
      mergedAt
      reviewDecision
      mergeable
      headRefName
      headRefOid
      headRepository { nameWithOwner }
      baseRefName
      baseRefOid
      baseRepository { nameWithOwner }
      author { login }
      timelineItems(first: 50, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on PullRequestCommit {
            commit {
              oid
              committedDate
              parents(first: 1) { nodes { oid } }
              checkSuites(first: 10) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  databaseId
                  status
                  conclusion
                  createdAt
                  updatedAt
                  app { slug }
                  checkRuns(first: 20) {
                    pageInfo { hasNextPage endCursor }
                    nodes {
                      id
                      databaseId
                      name
                      status
                      conclusion
                      detailsUrl
                      startedAt
                      completedAt
                    }
                  }
                }
              }
            }
          }
          ... on PullRequestReview {
            id
            databaseId
            body
            state
            submittedAt
            updatedAt
            author { login }
            commit { oid }
          }
          ... on PullRequestReviewThread {
            id
            isResolved
            isOutdated
            path
            line
            comments(first: 100) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                databaseId
                pullRequestReview { databaseId }
                body
                path
                line
                createdAt
                updatedAt
                author { login }
              }
            }
          }
          ... on ReviewDismissedEvent {
            createdAt
            previousReviewState
            review {
              id
              databaseId
              body
              state
              submittedAt
              updatedAt
              author { login }
              commit { oid }
            }
          }
          ... on BaseRefChangedEvent {
            createdAt
            previousRefName
            currentRefName
          }
          ... on HeadRefForcePushedEvent {
            createdAt
            beforeCommit { oid }
            afterCommit {
              oid
              parents(first: 1) { nodes { oid } }
            }
          }
          ... on ClosedEvent { createdAt }
          ... on MergedEvent { createdAt commit { oid } }
          ... on ReopenedEvent { createdAt }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`

const pullRequestThreadsQuery = `query GhsyncRecordingThreads(
  $owner: String!,
  $name: String!,
  $number: Int!,
  $after: String
) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
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
            nodes {
              id
              databaseId
              pullRequestReview { databaseId }
              body
              path
              line
              createdAt
              updatedAt
              author { login }
            }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`

const reviewThreadCommentsQuery = `query GhsyncRecordingThreadComments(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      comments(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          pullRequestReview { databaseId }
          body
          path
          line
          createdAt
          updatedAt
          author { login }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`

const defaultBranchHistoryQuery = `query GhsyncRecordingDefaultHistory(
  $owner: String!,
  $name: String!,
  $since: GitTimestamp!,
  $until: GitTimestamp!,
  $after: String
) {
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      name
      target {
        ... on Commit {
          history(
            first: 50,
            after: $after,
            since: $since,
            until: $until
          ) {
            pageInfo { hasNextPage endCursor }
            nodes {
              oid
              committedDate
              parents(first: 1) { nodes { oid } }
              checkSuites(first: 10) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  databaseId
                  status
                  conclusion
                  createdAt
                  updatedAt
                  app { slug }
                  checkRuns(first: 20) {
                    pageInfo { hasNextPage endCursor }
                    nodes {
                      id
                      databaseId
                      name
                      status
                      conclusion
                      detailsUrl
                      startedAt
                      completedAt
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`

const commitCheckSuitesQuery = `query GhsyncRecordingCommitChecks(
  $owner: String!,
  $name: String!,
  $oid: GitObjectID!,
  $after: String
) {
  repository(owner: $owner, name: $name) {
    object(oid: $oid) {
      ... on Commit {
        checkSuites(first: 10, after: $after) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            databaseId
            status
            conclusion
            createdAt
            updatedAt
            app { slug }
            checkRuns(first: 20) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                databaseId
                name
                status
                conclusion
                detailsUrl
                startedAt
                completedAt
              }
            }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`

const checkSuiteRunsQuery = `query GhsyncRecordingCheckRuns(
  $id: ID!,
  $after: String
) {
  node(id: $id) {
    ... on CheckSuite {
      checkRuns(first: 50, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          name
          status
          conclusion
          detailsUrl
          startedAt
          completedAt
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}`
