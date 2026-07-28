package fakegithub

import "time"

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
			Number: 4810, Title: "Tokenizer rewrite for query parser", State: "closed",
			Head:      PullRequestBranch{Ref: "refactor/tokenizer", SHA: "bbbb001"},
			Base:      PullRequestBranch{Ref: "main", SHA: "aaaa000"},
			UpdatedAt: now, Stack: stackRef(1),
		},
		{
			Number: 4812, Title: "BM25F ranker integration", State: "open",
			Head:      PullRequestBranch{Ref: "refactor/bm25f-ranker", SHA: "8f31c2d"},
			Base:      PullRequestBranch{Ref: "refactor/tokenizer", SHA: "bbbb001"},
			UpdatedAt: now, Stack: stackRef(2),
		},
		{
			Number: 4815, Title: "Relevance debug API endpoint", State: "open",
			Head:      PullRequestBranch{Ref: "feat/relevance-debug", SHA: "bbbb003"},
			Base:      PullRequestBranch{Ref: "refactor/bm25f-ranker", SHA: "8f31c2d"},
			UpdatedAt: now, Stack: stackRef(3),
		},
		{
			Number: 4816, Title: "Results page rewiring", State: "open",
			Head:      PullRequestBranch{Ref: "feat/results-rewire", SHA: "bbbb004"},
			Base:      PullRequestBranch{Ref: "feat/relevance-debug", SHA: "bbbb003"},
			UpdatedAt: now, Stack: stackRef(4),
		},
		{
			Number: 4820, Title: "Relevance telemetry dashboards", State: "open",
			Head:      PullRequestBranch{Ref: "feat/relevance-telemetry", SHA: "bbbb005"},
			Base:      PullRequestBranch{Ref: "feat/results-rewire", SHA: "bbbb004"},
			UpdatedAt: now, Stack: stackRef(5),
		},
	}
	stackPulls := make([]StackPullRequest, 0, len(pulls))
	for _, pull := range pulls {
		stackPulls = append(stackPulls, StackPullRequest{
			Number: pull.Number,
			State:  pull.State,
			Head:   pull.Head,
		})
	}
	return Fixture{
		Owner: "acme",
		Repo:  "monolith",
		Stacks: []Stack{
			{
				ID:           9876543,
				Number:       142,
				NodeID:       "S_kwDOABCDEF4AAAAA",
				URL:          "https://api.github.com/repos/acme/monolith/stacks/142",
				Base:         base,
				Open:         true,
				CreatedAt:    now.Add(-24 * time.Hour),
				PullRequests: stackPulls,
			},
		},
		PullRequests: pulls,
	}
}
