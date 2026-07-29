package fakegithub

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
)

func (s *Server) checkRepo(w http.ResponseWriter, r *http.Request) (Fixture, bool) {
	s.mu.Lock()
	fx := cloneFixture(s.fixture)
	s.mu.Unlock()
	if r.PathValue("owner") != fx.Owner || r.PathValue("repo") != fx.Repo {
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
	s.writeConditionalJSON(w, r, "core", fx.Repository)
}

func (s *Server) listInstallationRepositories(
	w http.ResponseWriter,
	r *http.Request,
) {
	s.mu.Lock()
	fx := cloneFixture(s.fixture)
	repositories := fx.Repositories
	if repositories == nil {
		repositories = []Repository{fx.Repository}
	}
	s.mu.Unlock()
	sort.SliceStable(repositories, func(i, j int) bool {
		return repositories[i].ID < repositories[j].ID
	})
	repositories = paginate(repositories, r, w)
	s.writeConditionalJSON(w, r, "core", map[string]any{
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
	s.writeConditionalJSON(w, r, "core", rules)
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
		for _, stack := range fx.Stacks {
			for _, pull := range stack.PullRequests {
				if pull.Number == number {
					stacks = append(stacks, stack)
					break
				}
			}
		}
	}
	stacks = paginate(stacks, r, w)
	s.writeConditionalJSON(w, r, "core", stacks)
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
	for _, stack := range fx.Stacks {
		if stack.Number == number {
			s.writeConditionalJSON(w, r, "core", stack)
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
	for _, pull := range fx.PullRequests {
		if state == "" || state == "all" || pull.State == state {
			pulls = append(pulls, pull)
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
	s.writeConditionalJSON(w, r, "core", pulls)
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
	for _, pull := range fx.PullRequests {
		if pull.Number == number {
			// GitHub's REST pull shape carries the author under user.login.
			// Keep AuthorLogin out of the fixture's serialized list golden,
			// but include the real field on authoritative detail fetches.
			s.writeConditionalJSON(w, r, "core", struct {
				PullRequest
				User map[string]string `json:"user"`
			}{
				PullRequest: pull,
				User: map[string]string{
					"login": pull.AuthorLogin,
				},
			})
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) listCheckRuns(w http.ResponseWriter, r *http.Request) {
	fx, ok := s.checkRepo(w, r)
	if !ok {
		return
	}
	headSHA := r.PathValue("sha")
	runs := make([]CheckRun, 0)
	for _, run := range fx.CheckRuns {
		if run.HeadSHA == headSHA {
			runs = append(runs, run)
		}
	}
	runs = paginate(runs, r, w)
	s.writeConditionalJSON(w, r, "core", map[string]any{
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
