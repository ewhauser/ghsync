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
	fixtures := make([]Fixture, 0, len(s.additionalFixtures)+1)
	fixtures = append(fixtures, cloneFixture(&s.fixture))
	for _, fixture := range s.additionalFixtures {
		fixtures = append(fixtures, cloneFixture(fixture))
	}
	s.mu.Unlock()
	var ids []string
	if raw := request.Variables["ids"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &ids)
	}
	switch {
	case len(ids) > 0:
		nodes := make([]any, 0, len(ids))
		for _, id := range ids {
			var node any
			for fixtureIndex := range fixtures {
				fx := &fixtures[fixtureIndex]
				for pullIndex := range fx.PullRequests {
					pull := &fx.PullRequests[pullIndex]
					if pull.NodeID == id {
						node = graphQLPullRequest(&fx.Repository, pull)
						break
					}
				}
				if node != nil {
					break
				}
			}
			nodes = append(nodes, node)
		}
		data["nodes"] = nodes
	case strings.Contains(
		request.Query,
		"GhsyncPullRequestReviewRequestsPage",
	):
		id, after := graphQLCursorVariables(request.Variables)
		for fixtureIndex := range fixtures {
			fx := &fixtures[fixtureIndex]
			for pullIndex := range fx.PullRequests {
				pull := &fx.PullRequests[pullIndex]
				if pull.NodeID == id {
					data["node"] = map[string]any{
						"reviewRequests": graphQLReviewRequests(
							pull.ReviewRequests,
							after,
						),
					}
					break
				}
			}
			if data["node"] != nil {
				break
			}
		}
	case strings.Contains(request.Query, "GhsyncPullRequestReviewsPage"):
		id, after := graphQLCursorVariables(request.Variables)
		for fixtureIndex := range fixtures {
			fx := &fixtures[fixtureIndex]
			for pullIndex := range fx.PullRequests {
				pull := &fx.PullRequests[pullIndex]
				if pull.NodeID == id {
					data["node"] = map[string]any{
						"reviews": graphQLReviews(pull.Reviews, after),
					}
					break
				}
			}
			if data["node"] != nil {
				break
			}
		}
	case strings.Contains(request.Query, "GhsyncPullRequestCommentsPage"):
		id, after := graphQLCursorVariables(request.Variables)
		for fixtureIndex := range fixtures {
			fx := &fixtures[fixtureIndex]
			for pullIndex := range fx.PullRequests {
				pull := &fx.PullRequests[pullIndex]
				if pull.NodeID == id {
					data["node"] = map[string]any{
						"comments": graphQLIssueComments(pull.Comments, after),
					}
					break
				}
			}
			if data["node"] != nil {
				break
			}
		}
	case strings.Contains(
		request.Query,
		"GhsyncPullRequestReviewThreadsPage",
	):
		id, after := graphQLCursorVariables(request.Variables)
		for fixtureIndex := range fixtures {
			fx := &fixtures[fixtureIndex]
			for pullIndex := range fx.PullRequests {
				pull := &fx.PullRequests[pullIndex]
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
			if data["node"] != nil {
				break
			}
		}
	case strings.Contains(
		request.Query,
		"GhsyncReviewThreadCommentsPage",
	):
		id, after := graphQLCursorVariables(request.Variables)
		for fixtureIndex := range fixtures {
			fx := &fixtures[fixtureIndex]
			for pullIndex := range fx.PullRequests {
				pull := &fx.PullRequests[pullIndex]
				for threadIndex := range pull.ReviewThreads {
					thread := &pull.ReviewThreads[threadIndex]
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
				if data["node"] != nil {
					break
				}
			}
			if data["node"] != nil {
				break
			}
		}
	}
	writeJSON(w, map[string]any{
		"data": data,
	})
}

func graphQLPullRequest(
	repository *Repository,
	pull *PullRequest,
) map[string]any {
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
		"baseRefOid":     nullableSHA(pull.Base.SHA),
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
		"reviewRequests": graphQLReviewRequests(pull.ReviewRequests, 0),
		"reviews":        graphQLReviews(pull.Reviews, 0),
		"comments":       graphQLIssueComments(pull.Comments, 0),
		"reviewThreads":  graphQLReviewThreads(pull.ReviewThreads, 0),
	}
}

const fakeGraphQLConnectionLimit = 100

func graphQLReviewRequests(
	requests []ReviewRequest,
	after int,
) map[string]any {
	start, end, pageInfo := graphQLPage(len(requests), after)
	nodes := make([]map[string]any, 0, end-start)
	for _, request := range requests[start:end] {
		var reviewer any
		switch request.Kind {
		case "user":
			reviewer = map[string]any{
				"__typename": "User", "id": request.NodeID,
				"databaseId": request.ID, "login": request.Login,
			}
		case "team":
			reviewer = map[string]any{
				"__typename": "Team", "id": request.NodeID,
				"databaseId": request.ID, "slug": request.Login,
			}
		case "bot":
			reviewer = map[string]any{"__typename": "Bot"}
		case "mannequin":
			reviewer = map[string]any{"__typename": "Mannequin"}
		case "nil":
			reviewer = nil
		default:
			reviewer = map[string]any{"__typename": request.Kind}
		}
		nodes = append(nodes, map[string]any{
			"requestedReviewer": reviewer,
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLReviews(reviews []PullRequestReview, after int) map[string]any {
	start, end, pageInfo := graphQLPage(len(reviews), after)
	nodes := make([]map[string]any, 0, end-start)
	for index := start; index < end; index++ {
		review := &reviews[index]
		var commit any
		if review.CommitOID != "" {
			commit = map[string]any{"oid": review.CommitOID}
		}
		nodes = append(nodes, map[string]any{
			"id":             review.NodeID,
			"fullDatabaseId": nullableGraphQLBigInt(review.ID),
			"state":          strings.ToUpper(review.State),
			"submittedAt":    nullableTime(review.SubmittedAt),
			"updatedAt":      review.UpdatedAt.Format(time.RFC3339Nano),
			"author":         graphQLActor(review.Author),
			"commit":         commit,
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func nullableGraphQLBigInt(value int64) any {
	if value == 0 {
		return nil
	}
	return strconv.FormatInt(value, 10)
}

func graphQLIssueComments(comments []IssueComment, after int) map[string]any {
	start, end, pageInfo := graphQLPage(len(comments), after)
	nodes := make([]map[string]any, 0, end-start)
	for index := start; index < end; index++ {
		comment := &comments[index]
		nodes = append(nodes, map[string]any{
			"id":             comment.NodeID,
			"fullDatabaseId": nullableGraphQLBigInt(comment.ID),
			"createdAt":      comment.CreatedAt.Format(time.RFC3339Nano),
			"updatedAt":      comment.UpdatedAt.Format(time.RFC3339Nano),
			"author":         graphQLActor(comment.Author),
		})
	}
	return map[string]any{"nodes": nodes, "pageInfo": pageInfo}
}

func graphQLActor(actor Actor) any {
	if actor.Kind == "deleted" {
		return nil
	}
	typename := map[string]string{
		"user":                    "User",
		"bot":                     "Bot",
		"mannequin":               "Mannequin",
		"organization":            "Organization",
		"enterprise_user_account": "EnterpriseUserAccount",
	}[actor.Kind]
	if typename == "" {
		typename = "UnknownActor"
	}
	return map[string]any{
		"__typename": typename,
		"id":         nullableString(actor.NodeID),
		"login":      nullableString(actor.Login),
	}
}

func nullableDatabaseID(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

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
