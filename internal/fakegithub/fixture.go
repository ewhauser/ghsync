package fakegithub

import "time"

func cloneFixture(source *Fixture) Fixture {
	clone := *source
	clone.Repositories = append([]Repository(nil), source.Repositories...)
	clone.RepoRules = make([]RepositoryRule, len(source.RepoRules))
	for index, rule := range source.RepoRules {
		clone.RepoRules[index] = rule
		clone.RepoRules[index].UpdatedAt = cloneTime(rule.UpdatedAt)
		clone.RepoRules[index].Rules = make([]map[string]any, len(rule.Rules))
		for ruleIndex, value := range rule.Rules {
			clone.RepoRules[index].Rules[ruleIndex] = cloneStringMap(value)
		}
	}
	clone.Stacks = make([]Stack, len(source.Stacks))
	for index := range source.Stacks {
		stack := &source.Stacks[index]
		clone.Stacks[index] = *stack
		clone.Stacks[index].PullRequests = append(
			[]StackPullRequest(nil),
			stack.PullRequests...,
		)
		for pullIndex := range clone.Stacks[index].PullRequests {
			pull := &clone.Stacks[index].PullRequests[pullIndex]
			pull.MergedAt = cloneTime(pull.MergedAt)
		}
	}
	clone.PullRequests = make([]PullRequest, len(source.PullRequests))
	for index := range source.PullRequests {
		pull := &source.PullRequests[index]
		clone.PullRequests[index] = *pull
		clone.PullRequests[index].MergedAt = cloneTime(pull.MergedAt)
		clone.PullRequests[index].ReviewRequests = append(
			[]ReviewRequest(nil),
			pull.ReviewRequests...,
		)
		if pull.Stack != nil {
			stack := *pull.Stack
			clone.PullRequests[index].Stack = &stack
		}
		clone.PullRequests[index].ReviewThreads = make(
			[]ReviewThread,
			len(pull.ReviewThreads),
		)
		for threadIndex, thread := range pull.ReviewThreads {
			clone.PullRequests[index].ReviewThreads[threadIndex] = thread
			clone.PullRequests[index].ReviewThreads[threadIndex].Line = cloneInt(
				thread.Line,
			)
			clone.PullRequests[index].ReviewThreads[threadIndex].Comments = append(
				[]ReviewComment(nil),
				thread.Comments...,
			)
		}
	}
	clone.CheckRuns = append([]CheckRun(nil), source.CheckRuns...)
	for index := range clone.CheckRuns {
		clone.CheckRuns[index].StartedAt = cloneTime(
			clone.CheckRuns[index].StartedAt,
		)
		clone.CheckRuns[index].CompletedAt = cloneTime(
			clone.CheckRuns[index].CompletedAt,
		)
	}
	return clone
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneStringMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = cloneFixtureValue(value)
	}
	return clone
}

func cloneFixtureValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneStringMap(value)
	case []map[string]any:
		clone := make([]map[string]any, len(value))
		for index, item := range value {
			clone[index] = cloneStringMap(item)
		}
		return clone
	case []any:
		clone := make([]any, len(value))
		for index, item := range value {
			clone[index] = cloneFixtureValue(item)
		}
		return clone
	default:
		return value
	}
}

// DefaultFixture is a canned repo modeled on the S-142 scenario from the
// product prototype: a five-layer stack with one merged layer.
func DefaultFixture() Fixture {
	base := Base{Ref: "main", SHA: "aaaa000"}
	stackRef := func(position int) *StackRef {
		return &StackRef{ID: 9876543, Number: 142, Size: 5, Position: position, Base: base}
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	pulls := []PullRequest{
		{
			ID: 804810, NodeID: "PR_kwDOABCDEF4810",
			Number: 4810, Title: "Tokenizer rewrite for query parser", State: "closed",
			AuthorLogin: "octocat", ReviewDecision: "APPROVED", MergeableState: "MERGEABLE",
			Head:      PullRequestBranch{Ref: "refactor/tokenizer", SHA: "bbbb001"},
			Base:      PullRequestBranch{Ref: "main", SHA: "aaaa000"},
			UpdatedAt: now, Stack: stackRef(1),
		},
		{
			ID: 804812, NodeID: "PR_kwDOABCDEF4812",
			Number: 4812, Title: "BM25F ranker integration", State: "open",
			AuthorLogin: "octocat", ReviewDecision: "CHANGES_REQUESTED",
			MergeableState: "CONFLICTING",
			Head:           PullRequestBranch{Ref: "refactor/bm25f-ranker", SHA: "8f31c2d"},
			Base:           PullRequestBranch{Ref: "refactor/tokenizer", SHA: "bbbb001"},
			UpdatedAt:      now, Stack: stackRef(2),
			ReviewThreads: []ReviewThread{{
				ID: "PRRT_kwDOABCDEF4812_1", Path: "internal/ranker.go",
				IsResolved: false, IsOutdated: false,
				Comments: []ReviewComment{{
					ID: "PRRC_kwDOABCDEF4812_1", Body: "Please cover the tie case.",
					UpdatedAt: now, AuthorLogin: "reviewer",
				}},
			}},
			ReviewRequests: []ReviewRequest{
				{
					Kind: "user", ID: 5001, NodeID: "U_kwDOABCDEF5001",
					Login: "reviewer",
				},
				{
					Kind: "team", ID: 6001, NodeID: "T_kwDOABCDEF6001",
					Login: "search-platform",
				},
			},
		},
		{
			ID: 804815, NodeID: "PR_kwDOABCDEF4815",
			Number: 4815, Title: "Relevance debug API endpoint", State: "open",
			AuthorLogin: "octocat", ReviewDecision: "REVIEW_REQUIRED",
			MergeableState: "MERGEABLE",
			Head:           PullRequestBranch{Ref: "feat/relevance-debug", SHA: "bbbb003"},
			Base:           PullRequestBranch{Ref: "refactor/bm25f-ranker", SHA: "8f31c2d"},
			UpdatedAt:      now, Stack: stackRef(3),
		},
		{
			ID: 804816, NodeID: "PR_kwDOABCDEF4816",
			Number: 4816, Title: "Results page rewiring", State: "open",
			AuthorLogin: "octocat", ReviewDecision: "REVIEW_REQUIRED",
			MergeableState: "MERGEABLE",
			Head:           PullRequestBranch{Ref: "feat/results-rewire", SHA: "bbbb004"},
			Base:           PullRequestBranch{Ref: "feat/relevance-debug", SHA: "bbbb003"},
			UpdatedAt:      now, Stack: stackRef(4),
		},
		{
			ID: 804820, NodeID: "PR_kwDOABCDEF4820",
			Number: 4820, Title: "Relevance telemetry dashboards", State: "open",
			AuthorLogin: "octocat", ReviewDecision: "REVIEW_REQUIRED",
			MergeableState: "MERGEABLE",
			Head:           PullRequestBranch{Ref: "feat/relevance-telemetry", SHA: "bbbb005"},
			Base:           PullRequestBranch{Ref: "feat/results-rewire", SHA: "bbbb004"},
			UpdatedAt:      now, Stack: stackRef(5),
		},
	}
	for index := range pulls {
		pulls[index].CreatedAt = now.Add(
			-time.Duration(len(pulls)-index) * 24 * time.Hour,
		)
	}
	stackPulls := make([]StackPullRequest, 0, len(pulls))
	for index := range pulls {
		pull := &pulls[index]
		stackPulls = append(stackPulls, StackPullRequest{
			Number:    pull.Number,
			State:     pull.State,
			Draft:     pull.Draft,
			UpdatedAt: pull.UpdatedAt,
			Head:      pull.Head,
		})
	}
	started := now.Add(-8 * time.Minute)
	completed := now.Add(-5 * time.Minute)
	return Fixture{
		Owner: "acme",
		Repo:  "monolith",
		Repository: Repository{
			ID: 1001, NodeID: "R_kwDOABCDEF", Owner: "acme", Name: "monolith",
			FullName: "acme/monolith", DefaultBranch: "main",
			DefaultBranchSHA: base.SHA, UpdatedAt: now, PushedAt: now,
		},
		Repositories: []Repository{{
			ID: 1001, NodeID: "R_kwDOABCDEF", Owner: "acme", Name: "monolith",
			FullName: "acme/monolith", DefaultBranch: "main",
			DefaultBranchSHA: base.SHA, UpdatedAt: now, PushedAt: now,
		}},
		RepoRules: []RepositoryRule{{
			ID:          7001,
			Name:        "protect-main",
			Target:      "branch",
			Enforcement: "active",
			UpdatedAt:   &now,
			Rules: []map[string]any{{
				"type": "required_status_checks",
				"parameters": map[string]any{
					"required_status_checks": []map[string]any{
						{"context": "unit"},
					},
				},
			}},
		}},
		Stacks: []Stack{
			{
				ID:           9876543,
				Number:       142,
				NodeID:       "S_kwDOABCDEF4AAAAA",
				URL:          "https://api.github.com/repos/acme/monolith/stacks/142",
				Base:         base,
				Open:         true,
				CreatedAt:    now.Add(-24 * time.Hour),
				UpdatedAt:    now,
				PullRequests: stackPulls,
			},
		},
		PullRequests: pulls,
		CheckRuns: []CheckRun{
			{
				ID: 99001, NodeID: "CR_kwDOABCDEF99001", HeadSHA: "8f31c2d",
				Name: "unit", Status: "completed", Conclusion: "failure",
				DetailsURL: "https://github.com/acme/monolith/actions/runs/99001",
				AppSlug:    "github-actions", StartedAt: &started, CompletedAt: &completed,
			},
			{
				ID: 99002, NodeID: "CR_kwDOABCDEF99002", HeadSHA: "8f31c2d",
				Name: "lint", Status: "completed", Conclusion: "success",
				DetailsURL: "https://github.com/acme/monolith/actions/runs/99002",
				AppSlug:    "github-actions", StartedAt: &started, CompletedAt: &completed,
			},
		},
	}
}
