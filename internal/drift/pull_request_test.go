package drift

import (
	"strings"
	"testing"
)

func TestSemanticDiffNormalizesPullRequestMergeabilityMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cache    string
		upstream string
	}{
		{name: "unknown", cache: "UNKNOWN", upstream: "unknown"},
		{name: "conflicting", cache: "CONFLICTING", upstream: "dirty"},
		{name: "mergeable_clean", cache: "MERGEABLE", upstream: "clean"},
		{name: "mergeable_blocked", cache: "MERGEABLE", upstream: "blocked"},
		{name: "mergeable_unstable", cache: "MERGEABLE", upstream: "unstable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cache := encodeSnapshot(map[string]any{
				"mergeable_state": test.cache,
			})
			upstream := encodeSnapshot(map[string]any{
				"mergeable_state": test.upstream,
			})
			equal, diff, err := semanticDiff(
				"pull_request",
				cache,
				upstream,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !equal || string(diff) != "{}" {
				t.Fatalf("mergeability compare equal=%v diff=%s", equal, diff)
			}
		})
	}
}

func TestSemanticDiffDetectsDifferentPullRequestMergeability(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cache    string
		upstream string
	}{
		{name: "conflicting_vs_clean", cache: "CONFLICTING", upstream: "clean"},
		{name: "mergeable_vs_dirty", cache: "MERGEABLE", upstream: "dirty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cache := encodeSnapshot(map[string]any{
				"mergeable_state": test.cache,
			})
			upstream := encodeSnapshot(map[string]any{
				"mergeable_state": test.upstream,
			})
			equal, diff, err := semanticDiff("pull_request", cache, upstream)
			if err != nil {
				t.Fatal(err)
			}
			if equal || !strings.Contains(string(diff), "mergeable_state") {
				t.Fatalf("different mergeability equal=%v diff=%s", equal, diff)
			}
		})
	}
}

func TestSemanticDiffPullRequestMergeabilityAbsentSemantics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cache    []byte
		upstream []byte
	}{
		{
			name:     "cache_not_observed",
			cache:    []byte(`{}`),
			upstream: []byte(`{"mergeable_state":"clean"}`),
		},
		{
			name:     "upstream_not_observed",
			cache:    []byte(`{"mergeable_state":"UNKNOWN"}`),
			upstream: []byte(`{}`),
		},
		{
			name:     "cache_null_is_not_observed",
			cache:    []byte(`{"mergeable_state":null}`),
			upstream: []byte(`{"mergeable_state":"dirty"}`),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			equal, diff, err := semanticDiff(
				"pull_request",
				test.cache,
				test.upstream,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !equal || string(diff) != "{}" {
				t.Fatalf("absent mergeability equal=%v diff=%s", equal, diff)
			}
		})
	}
}

func TestSemanticDiffNormalizesMergedPullRequestState(t *testing.T) {
	t.Parallel()
	cache := []byte(`{"state":"merged"}`)
	upstream := []byte(`{"state":"closed","merged":true}`)
	equal, diff, err := semanticDiff("pull_request", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if !equal || string(diff) != "{}" {
		t.Fatalf("merged-state compare equal=%v diff=%s", equal, diff)
	}
}

func TestSemanticDiffDetectsUnmergedClosedPullRequestState(t *testing.T) {
	t.Parallel()
	cache := []byte(`{"state":"merged"}`)
	upstream := []byte(`{"state":"closed","merged":false}`)
	equal, diff, err := semanticDiff("pull_request", cache, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if equal || !strings.Contains(string(diff), "state") {
		t.Fatalf("unmerged closed state equal=%v diff=%s", equal, diff)
	}
}

func TestSemanticDiffPullRequestReviewDecisionAbsentSemantics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cache     []byte
		upstream  []byte
		wantEqual bool
	}{
		{
			name:      "cache_not_observed",
			cache:     []byte(`{}`),
			upstream:  []byte(`{"review_decision":"APPROVED"}`),
			wantEqual: true,
		},
		{
			name:      "upstream_not_observed",
			cache:     []byte(`{"review_decision":"APPROVED"}`),
			upstream:  []byte(`{}`),
			wantEqual: true,
		},
		{
			name:      "cache_null_is_not_observed",
			cache:     []byte(`{"review_decision":null}`),
			upstream:  []byte(`{"review_decision":"APPROVED"}`),
			wantEqual: true,
		},
		{
			name:      "present_no_decision_differs_from_approved",
			cache:     []byte(`{"review_decision":""}`),
			upstream:  []byte(`{"review_decision":"APPROVED"}`),
			wantEqual: false,
		},
		{
			name:      "known_decisions_differ",
			cache:     []byte(`{"review_decision":"APPROVED"}`),
			upstream:  []byte(`{"review_decision":"CHANGES_REQUESTED"}`),
			wantEqual: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			equal, _, err := semanticDiff(
				"pull_request",
				test.cache,
				test.upstream,
			)
			if err != nil {
				t.Fatal(err)
			}
			if equal != test.wantEqual {
				t.Fatalf("review-decision compare equal=%v, want %v", equal, test.wantEqual)
			}
		})
	}
}

func TestSemanticDiffPullRequestCanonicalizationIsFieldScoped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		field    string
		cache    string
		upstream string
	}{
		{
			name: "head_ref_case", field: "head_ref",
			cache: "Feature/Parser", upstream: "feature/parser",
		},
		{
			name: "head_sha_case", field: "head_sha",
			cache: "ABCDEF", upstream: "abcdef",
		},
		{
			name: "author_login_case", field: "author_login",
			cache: "OctoCat", upstream: "octocat",
		},
		{
			name: "mergeability_whitespace", field: "mergeable_state",
			cache: "MERGEABLE ", upstream: "clean",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cache := encodeSnapshot(map[string]any{test.field: test.cache})
			upstream := encodeSnapshot(map[string]any{test.field: test.upstream})
			equal, diff, err := semanticDiff("pull_request", cache, upstream)
			if err != nil {
				t.Fatal(err)
			}
			if equal || !strings.Contains(string(diff), test.field) {
				t.Fatalf("%s compare equal=%v diff=%s", test.field, equal, diff)
			}
		})
	}
}
