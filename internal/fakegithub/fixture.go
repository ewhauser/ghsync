package fakegithub

import "time"

// DefaultFixture is a canned repo modeled on the S-142 scenario from the
// product prototype: a five-layer stack with one merged layer.
func DefaultFixture() Fixture {
	base := Base{Ref: "main", SHA: "aaaa000"}
	stackRef := func(position int) *StackRef {
		return &StackRef{ID: "STK_142", Number: 142, Size: 5, Position: position, Base: base}
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	return Fixture{
		Owner: "acme",
		Repo:  "monolith",
		Stacks: []Stack{
			{
				ID:     "STK_142",
				Number: 142,
				Base:   base,
				Open:   true,
				PullRequests: []int{4810, 4812, 4815, 4816, 4820},
			},
		},
		PullRequests: []PullRequest{
			{Number: 4810, Title: "Tokenizer rewrite for query parser", State: "closed", HeadRef: "refactor/tokenizer", HeadSHA: "bbbb001", BaseRef: "main", UpdatedAt: now, Stack: stackRef(1)},
			{Number: 4812, Title: "BM25F ranker integration", State: "open", HeadRef: "refactor/bm25f-ranker", HeadSHA: "8f31c2d", BaseRef: "refactor/tokenizer", UpdatedAt: now, Stack: stackRef(2)},
			{Number: 4815, Title: "Relevance debug API endpoint", State: "open", HeadRef: "feat/relevance-debug", HeadSHA: "bbbb003", BaseRef: "refactor/bm25f-ranker", UpdatedAt: now, Stack: stackRef(3)},
			{Number: 4816, Title: "Results page rewiring", State: "open", HeadRef: "feat/results-rewire", HeadSHA: "bbbb004", BaseRef: "feat/relevance-debug", UpdatedAt: now, Stack: stackRef(4)},
			{Number: 4820, Title: "Relevance telemetry dashboards", State: "open", HeadRef: "feat/relevance-telemetry", HeadSHA: "bbbb005", BaseRef: "feat/results-rewire", UpdatedAt: now, Stack: stackRef(5)},
		},
	}
}
