package drift

import (
	"maps"
	"strings"
)

// pullRequestMergeabilityClasses maps both REST mergeable_state values and
// GraphQL PullRequest.mergeable values onto the cache's GraphQL enum contract.
// REST's policy/check-state distinctions (clean, blocked, and unstable) all
// mean that the head can be merged by Git, so they intentionally collapse to
// MERGEABLE for drift comparison only. The raw REST value remains available in
// gh.PullRequest and is not changed by this lossy mapping.
var pullRequestMergeabilityClasses = map[string]string{
	"unknown":     "UNKNOWN",
	"dirty":       "CONFLICTING",
	"conflicting": "CONFLICTING",
	"clean":       "MERGEABLE",
	"blocked":     "MERGEABLE",
	"unstable":    "MERGEABLE",
	"mergeable":   "MERGEABLE",
}

var pullRequestStates = map[string]string{
	"open":   "open",
	"closed": "closed",
	"merged": "merged",
}

var pullRequestReviewDecisions = map[string]string{
	"":                  "",
	"approved":          "APPROVED",
	"review_required":   "REVIEW_REQUIRED",
	"changes_requested": "CHANGES_REQUESTED",
}

func canonicalizePullRequestComparison(
	cached map[string]any,
	upstream map[string]any,
) (map[string]any, map[string]any) {
	cached = canonicalPullRequestSnapshot(cached)
	upstream = canonicalPullRequestSnapshot(upstream)

	// Missing or JSON-null comparison dimensions are unobserved and cannot
	// prove drift. Fail open for unknown-vs-absent on each enumerated field by
	// omitting that dimension from both snapshots. A present empty
	// review_decision remains GraphQL's explicit no-decision value; fullFetch
	// omits the key when neither GraphQL nor the REST extension observed it.
	for _, field := range []string{"mergeable_state", "review_decision"} {
		if !observedSemanticField(cached, field) ||
			!observedSemanticField(upstream, field) {
			delete(cached, field)
			delete(upstream, field)
		}
	}

	return cached, upstream
}

func canonicalPullRequestSnapshot(snapshot map[string]any) map[string]any {
	canonical := make(map[string]any, len(snapshot))
	maps.Copy(canonical, snapshot)

	// REST's merged bit disambiguates state=closed. Missing and false both
	// leave the state unchanged, so closed+merged=false remains real drift
	// against the cache's state=merged.
	if merged, ok := canonical["merged"].(bool); ok && merged {
		canonical["state"] = "merged"
	} else if state, ok := canonical["state"].(string); ok {
		if normalized, exists := pullRequestStates[strings.ToLower(state)]; exists {
			canonical["state"] = normalized
		}
	}
	// merged is a REST observation used only to disambiguate REST state=closed;
	// the cache contract represents the result directly as state=merged.
	delete(canonical, "merged")

	if mergeability, ok := canonical["mergeable_state"].(string); ok {
		key := strings.ToLower(mergeability)
		if normalized, exists := pullRequestMergeabilityClasses[key]; exists {
			canonical["mergeable_state"] = normalized
		}
	}
	if decision, ok := canonical["review_decision"].(string); ok {
		key := strings.ToLower(decision)
		if normalized, exists := pullRequestReviewDecisions[key]; exists {
			canonical["review_decision"] = normalized
		}
	}

	return canonical
}

func observedSemanticField(snapshot map[string]any, key string) bool {
	value, exists := snapshot[key]
	return exists && value != nil
}
