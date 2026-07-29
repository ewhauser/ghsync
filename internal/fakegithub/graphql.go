package fakegithub

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Query     string                     `json:"query"`
		Variables map[string]json.RawMessage `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil &&
		!errors.Is(err, io.EOF) {
		http.Error(w, "invalid GraphQL request", http.StatusBadRequest)
		return
	}
	budget := s.snapshot("graphql")
	cost, _ := r.Context().Value(rateCostKey{}).(int64)
	if cost <= 0 {
		cost = 1
	}
	data := map[string]any{
		"rateLimit": map[string]any{
			"cost":      cost,
			"limit":     budget.limit,
			"remaining": budget.remaining,
			"resetAt":   budget.resetAt.Format(time.RFC3339),
			"used":      budget.limit - budget.remaining,
		},
	}
	s.mu.Lock()
	fx := cloneFixture(s.fixture)
	s.mu.Unlock()
	var ids []string
	if raw := request.Variables["ids"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &ids)
	}
	if len(ids) > 0 {
		nodes := make([]any, 0, len(ids))
		for _, id := range ids {
			var node any
			for _, pull := range fx.PullRequests {
				if pull.NodeID == id {
					node = graphQLPullRequest(fx.Repository, pull)
					break
				}
			}
			nodes = append(nodes, node)
		}
		data["nodes"] = nodes
	} else if strings.Contains(
		request.Query,
		"FrontierPullRequestReviewThreadsPage",
	) {
		id, after := graphQLCursorVariables(request.Variables)
		for _, pull := range fx.PullRequests {
			if pull.NodeID == id {
				data["node"] = map[string]any{
					"reviewThreads": graphQLReviewThreads(
						pull.ReviewThreads,
						after,
					),
				}
				break
			}
		}
	} else if strings.Contains(
		request.Query,
		"FrontierReviewThreadCommentsPage",
	) {
		id, after := graphQLCursorVariables(request.Variables)
		for _, pull := range fx.PullRequests {
			for _, thread := range pull.ReviewThreads {
				if thread.ID == id {
					data["node"] = map[string]any{
						"comments": graphQLReviewComments(
							thread.Comments,
							after,
						),
					}
					break
				}
			}
		}
	}
	writeJSON(w, map[string]any{
		"data": data,
	})
}

func graphQLPullRequest(repository Repository, pull PullRequest) map[string]any {
	return map[string]any{
		"id":             pull.NodeID,
		"databaseId":     pull.ID,
		"number":         pull.Number,
		"title":          pull.Title,
		"state":          strings.ToUpper(pull.State),
		"isDraft":        pull.Draft,
		"updatedAt":      pull.UpdatedAt.Format(time.RFC3339Nano),
		"reviewDecision": pull.ReviewDecision,
		"mergeable":      strings.ToUpper(pull.MergeableState),
		"headRefName":    pull.Head.Ref,
		"headRefOid":     pull.Head.SHA,
		"baseRefName":    pull.Base.Ref,
		"baseRefOid":     pull.Base.SHA,
		"author":         map[string]any{"login": pull.AuthorLogin},
		"repository": map[string]any{
			"id":            repository.NodeID,
			"databaseId":    repository.ID,
			"name":          repository.Name,
			"nameWithOwner": repository.FullName,
			"isArchived":    repository.Archived,
			"updatedAt":     repository.UpdatedAt.Format(time.RFC3339Nano),
			"owner":         map[string]any{"login": repository.Owner},
			"defaultBranchRef": map[string]any{
				"name": repository.DefaultBranch,
				"target": map[string]any{
					"oid": repository.DefaultBranchSHA,
				},
			},
		},
		"reviewThreads": graphQLReviewThreads(pull.ReviewThreads, 0),
	}
}

const fakeGraphQLConnectionLimit = 100

func graphQLReviewThreads(
	threads []ReviewThread,
	after int,
) map[string]any {
	start, end, pageInfo := graphQLPage(len(threads), after)
	nodes := make([]map[string]any, 0, end-start)
	for _, thread := range threads[start:end] {
		nodes = append(nodes, map[string]any{
			"id":         thread.ID,
			"isResolved": thread.IsResolved,
			"isOutdated": thread.IsOutdated,
			"path":       thread.Path,
			"line":       thread.Line,
			"comments":   graphQLReviewComments(thread.Comments, 0),
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLReviewComments(
	comments []ReviewComment,
	after int,
) map[string]any {
	start, end, pageInfo := graphQLPage(len(comments), after)
	nodes := make([]map[string]any, 0, end-start)
	for _, comment := range comments[start:end] {
		author := any(nil)
		if comment.AuthorLogin != "" {
			author = map[string]any{"login": comment.AuthorLogin}
		}
		nodes = append(nodes, map[string]any{
			"id":        comment.ID,
			"body":      comment.Body,
			"updatedAt": comment.UpdatedAt.Format(time.RFC3339Nano),
			"author":    author,
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLPage(length, after int) (int, int, map[string]any) {
	start := min(max(after, 0), length)
	end := min(start+fakeGraphQLConnectionLimit, length)
	var endCursor any
	if end > 0 {
		endCursor = strconv.Itoa(end)
	}
	return start, end, map[string]any{
		"hasNextPage": end < length,
		"endCursor":   endCursor,
	}
}

func graphQLCursorVariables(
	variables map[string]json.RawMessage,
) (string, int) {
	var id string
	_ = json.Unmarshal(variables["id"], &id)
	var cursor string
	_ = json.Unmarshal(variables["after"], &cursor)
	after, _ := strconv.Atoi(cursor)
	return id, after
}
