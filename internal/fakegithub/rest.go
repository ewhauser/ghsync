package fakegithub

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

func (s *Server) checkRepo(w http.ResponseWriter, r *http.Request) (Fixture, bool) {
	s.mu.Lock()
	fixture := s.fixtureByKeyLocked(
		r.PathValue("owner") + "/" + r.PathValue("repo"),
	)
	var fx Fixture
	if fixture != nil {
		fx = cloneFixture(fixture)
	}
	s.mu.Unlock()
	if fixture == nil {
		http.NotFound(w, r)
		return Fixture{}, false
	}
	return fx, true
}

func (s *Server) getRepository(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	s.writeConditionalJSON(w, r, fx.Repository)
}

func (s *Server) getBranch(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	if r.PathValue("branch") != fx.Repository.DefaultBranch ||
		fx.Repository.DefaultBranchSHA == "" {
		http.NotFound(w, r)
		return
	}
	s.writeConditionalJSON(w, r, map[string]any{
		"name": fx.Repository.DefaultBranch,
		"commit": map[string]string{
			"sha": fx.Repository.DefaultBranchSHA,
		},
	})
}

func (s *Server) listInstallationRepositories(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.mu.Lock()
	fixtures := make([]Fixture, 0, len(s.additionalFixtures)+1)
	fixtures = append(fixtures, cloneFixture(&s.fixture))
	for _, fixture := range s.additionalFixtures {
		fixtures = append(fixtures, cloneFixture(fixture))
	}
	s.mu.Unlock()
	byID := make(map[int64]Repository)
	for fixtureIndex := range fixtures {
		fixture := &fixtures[fixtureIndex]
		repositories := fixture.Repositories
		if repositories == nil {
			repositories = []Repository{fixture.Repository}
		}
		for repositoryIndex := range repositories {
			repository := &repositories[repositoryIndex]
			byID[repository.ID] = *repository
		}
	}
	repositories := make([]Repository, 0, len(byID))
	for repositoryID := range byID {
		repositories = append(repositories, byID[repositoryID])
	}
	sort.SliceStable(repositories, func(i, j int) bool {
		return repositories[i].ID < repositories[j].ID
	})
	repositories = paginate(repositories, r, w)
	s.writeConditionalJSON(w, r, map[string]any{
		"total_count":  len(repositories),
		"repositories": repositories,
	})
}

func (s *Server) listRepositoryRules(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	rules := append([]RepositoryRule(nil), fx.RepoRules...)
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].ID < rules[j].ID
	})
	s.writeConditionalJSON(w, r, rules)
}

func (s *Server) listStacks(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	stacks := fx.Stacks
	if raw := r.URL.Query().Get("pull_request"); raw != "" {
		number, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "bad pull_request", http.StatusBadRequest)
			return
		}
		stacks = nil
		for index := range fx.Stacks {
			stack := &fx.Stacks[index]
			for _, pull := range stack.PullRequests {
				if pull.Number == number {
					stacks = append(stacks, *stack)
					break
				}
			}
		}
	}
	stacks = paginate(stacks, r, w)
	s.writeConditionalJSON(w, r, stacks)
}

func (s *Server) getStack(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		http.Error(w, "bad stack number", http.StatusBadRequest)
		return
	}
	for index := range fx.Stacks {
		stack := &fx.Stacks[index]
		if stack.Number == number {
			s.writeConditionalJSON(w, r, stack)
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) listPulls(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	state := r.URL.Query().Get("state")
	pulls := make([]PullRequest, 0, len(fx.PullRequests))
	for index := range fx.PullRequests {
		pull := &fx.PullRequests[index]
		if state == "" || state == "all" || pull.State == state {
			pulls = append(pulls, *pull)
		}
	}
	sortBy := r.URL.Query().Get("sort")
	if sortBy != "" {
		if sortBy != "updated" && sortBy != "created" {
			http.Error(w, "bad sort", http.StatusBadRequest)
			return
		}
		sort.SliceStable(pulls, func(i, j int) bool {
			if sortBy == "updated" {
				if pulls[i].UpdatedAt.Equal(pulls[j].UpdatedAt) {
					return pulls[i].Number < pulls[j].Number
				}
				return pulls[i].UpdatedAt.Before(pulls[j].UpdatedAt)
			}
			if pulls[i].CreatedAt.Equal(pulls[j].CreatedAt) {
				return pulls[i].Number < pulls[j].Number
			}
			return pulls[i].CreatedAt.Before(pulls[j].CreatedAt)
		})
		direction := r.URL.Query().Get("direction")
		if direction == "" {
			direction = "desc"
		}
		if direction == "desc" {
			slicesReverse(pulls)
		} else if direction != "asc" {
			http.Error(w, "bad direction", http.StatusBadRequest)
			return
		}
	}
	pulls = paginate(pulls, r, w)
	s.writeConditionalJSON(w, r, pulls)
}

func (s *Server) getPull(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		http.Error(w, "bad pull request number", http.StatusBadRequest)
		return
	}
	for index := range fx.PullRequests {
		pull := &fx.PullRequests[index]
		if pull.Number == number {
			requestedReviewers, requestedTeams := restReviewRequests(
				pull.ReviewRequests,
			)
			// GitHub's REST pull shape carries the author under user.login.
			// Keep AuthorLogin out of the fixture's serialized list golden,
			// but include the real field on authoritative detail fetches.
			s.writeConditionalJSON(w, r, struct {
				PullRequest
				User               map[string]string `json:"user"`
				RequestedReviewers []map[string]any  `json:"requested_reviewers"`
				RequestedTeams     []map[string]any  `json:"requested_teams"`
			}{
				PullRequest: *pull,
				User: map[string]string{
					"login": pull.AuthorLogin,
				},
				RequestedReviewers: requestedReviewers,
				RequestedTeams:     requestedTeams,
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) listPullFiles(w http.ResponseWriter, r *http.Request) {
	pull, ok := s.pullForRequest(w, r)
	if !ok {
		return
	}
	files := append([]ChangedFile(nil), pull.ChangedFiles...)
	files = paginate(files, r, w)
	s.writeConditionalJSON(w, r, files)
}

func (s *Server) getRepositoryContent(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	ref := r.URL.Query().Get("ref")
	path := r.PathValue("path")
	files := fx.Contents[ref]
	content, ok := files[path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.writeConditionalBytes(w, r, []byte(content), "application/octet-stream")
}

func (s *Server) listPullReviews(w http.ResponseWriter, r *http.Request) {
	pull, ok := s.pullForRequest(w, r)
	if !ok {
		return
	}
	values := make([]map[string]any, 0, len(pull.Reviews))
	for index := range pull.Reviews {
		review := &pull.Reviews[index]
		values = append(values, map[string]any{
			"id":           nullableDatabaseID(review.ID),
			"node_id":      review.NodeID,
			"user":         restActor(review.Author),
			"state":        review.State,
			"submitted_at": nullableTime(review.SubmittedAt),
			"updated_at":   review.UpdatedAt.Format(time.RFC3339Nano),
			"commit_id":    nullableString(review.CommitOID),
		})
	}
	s.writeConditionalJSON(w, r, paginate(values, r, w))
}

func (s *Server) listIssueComments(w http.ResponseWriter, r *http.Request) {
	pull, ok := s.pullForRequest(w, r)
	if !ok {
		return
	}
	values := make([]map[string]any, 0, len(pull.Comments))
	for index := range pull.Comments {
		comment := &pull.Comments[index]
		values = append(values, map[string]any{
			"id":         nullableDatabaseID(comment.ID),
			"node_id":    comment.NodeID,
			"user":       restActor(comment.Author),
			"body":       comment.Body,
			"created_at": comment.CreatedAt.Format(time.RFC3339Nano),
			"updated_at": comment.UpdatedAt.Format(time.RFC3339Nano),
		})
	}
	s.writeConditionalJSON(w, r, paginate(values, r, w))
}

func (s *Server) pullForRequest(
	w http.ResponseWriter,
	r *http.Request,
) (*PullRequest, bool) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return nil, false
	}
	number, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		http.Error(w, "bad pull request number", http.StatusBadRequest)
		return nil, false
	}
	for index := range fx.PullRequests {
		if fx.PullRequests[index].Number == number {
			return &fx.PullRequests[index], true
		}
	}
	http.NotFound(w, r)
	return nil, false
}

func restActor(actor Actor) any {
	if actor.Kind == "deleted" {
		return nil
	}
	return map[string]any{
		"node_id": nullableString(actor.NodeID),
		"login":   nullableString(actor.Login),
		"type":    actor.Kind,
	}
}

func restReviewRequests(
	requests []ReviewRequest,
) ([]map[string]any, []map[string]any) {
	users := make([]map[string]any, 0, len(requests))
	teams := make([]map[string]any, 0, len(requests))
	for _, request := range requests {
		value := map[string]any{
			"id":      request.ID,
			"node_id": request.NodeID,
		}
		switch request.Kind {
		case "user":
			value["login"] = request.Login
			users = append(users, value)
		case "team":
			value["slug"] = request.Login
			teams = append(teams, value)
		}
	}
	return users, teams
}

func (s *Server) listCheckRuns(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	headSHA := r.PathValue("sha")
	runs := make([]CheckRun, 0)
	for index := range fx.CheckRuns {
		run := &fx.CheckRuns[index]
		if run.HeadSHA == headSHA {
			runs = append(runs, *run)
		}
	}
	runs = paginate(runs, r, w)
	s.writeConditionalJSON(w, r, map[string]any{
		"total_count": len(runs),
		"check_runs":  runs,
	})
}

func slicesReverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right =
		left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func paginate[T any](values []T, r *http.Request, w http.ResponseWriter) []T {
	if len(values) == 0 {
		return nil
	}
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if perPage <= 0 {
		perPage = 30
	}
	if page <= 0 {
		page = 1
	}
	start := min((page-1)*perPage, len(values))
	end := min(start+perPage, len(values))
	if end < len(values) {
		next := *r.URL
		query := next.Query()
		query.Set("page", strconv.Itoa(page+1))
		query.Set("per_page", strconv.Itoa(perPage))
		next.RawQuery = query.Encode()
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		w.Header().Set(
			"Link",
			fmt.Sprintf("<%s://%s%s>; rel=\"next\"", scheme, r.Host, next.String()),
		)
	}
	return values[start:end]
}
