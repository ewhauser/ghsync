package fetch

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
)

func TestSortedPullRequestKeysUseImmutableRepositoryIdentity(t *testing.T) {
	t.Parallel()
	records := []store.PullRequestRecord{
		{
			Repository: store.RepositoryRecord{
				InstallationID: 1,
				GitHubID:       1001,
				FullName:       "renamed/monolith",
			},
			Number: 4820,
		},
		{
			Repository: store.RepositoryRecord{
				InstallationID: 1,
				GitHubID:       1001,
				FullName:       "acme/old-name",
			},
			Number: 4812,
		},
	}
	got := SortedPullRequestKeys(records)
	want := []string{"pr:1:1001:4812", "pr:1:1001:4820"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted immutable keys = %v, want %v", got, want)
	}
}

func TestParseEntityKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw  string
		kind string
		want entityKey
	}{
		{
			"pr:acme/monolith:4812",
			"pr",
			entityKey{
				Kind: "pr", Repo: "acme/monolith", Value: "4812", Number: 4812,
			},
		},
		{
			"checks:acme/monolith:abc123",
			"checks",
			entityKey{
				Kind: "checks", Repo: "acme/monolith", Value: "abc123",
			},
		},
		{
			"branch:acme/monolith:feature/one",
			"branch",
			entityKey{
				Kind: "branch", Repo: "acme/monolith", Value: "feature/one",
			},
		},
	}
	for _, test := range tests {
		got, err := parseEntityKey(test.raw, test.kind)
		if err != nil {
			t.Fatalf("%s: %v", test.raw, err)
		}
		if got != test.want {
			t.Fatalf("%s parsed as %+v, want %+v", test.raw, got, test.want)
		}
	}
	for _, raw := range []string{
		"pr:no-repo:1",
		"pr:acme/repo:no",
		"stack:acme/repo:0",
	} {
		if _, err := parseEntityKey(
			raw,
			strings.Split(raw, ":")[0],
		); err == nil {
			t.Fatalf("invalid key %q accepted", raw)
		}
	}
}

func TestStackOrderOnlyChangeSchedulesMovedPRs(t *testing.T) {
	t.Parallel()
	specs := stackFollowupSpecs("acme/monolith", &store.ApplyStackResult{
		Applied:  true,
		MovedPRs: []int{4812, 4815},
	})
	want := []queue.RefreshSpec{
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:4812",
		},
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:4815",
		},
	}
	if !reflect.DeepEqual(specs, want) {
		t.Fatalf("moved PR specs = %v, want %v", specs, want)
	}
}

func TestPullRequestFollowupsSkipStackForNonStackDomainChange(t *testing.T) {
	t.Parallel()
	stackNumber := 142
	titleOnly := pullRequestFollowupSpecs(
		"acme/monolith",
		store.ApplyPullRequestResult{
			DomainChanged:  true,
			OldStackNumber: &stackNumber,
			NewStackNumber: &stackNumber,
			OldHeadSHA:     "head-one",
			NewHeadSHA:     "head-one",
		},
	)
	if len(titleOnly) != 0 {
		t.Fatalf("title-only PR change followups = %v, want none", titleOnly)
	}

	stackChanged := pullRequestFollowupSpecs(
		"acme/monolith",
		store.ApplyPullRequestResult{
			DomainChanged:     true,
			StackStateChanged: true,
			OldStackNumber:    &stackNumber,
			NewStackNumber:    &stackNumber,
			OldHeadSHA:        "head-one",
			NewHeadSHA:        "head-two",
		},
	)
	want := []queue.RefreshSpec{
		{
			Kind: queue.KindRefreshChecks,
			Key:  "checks:acme/monolith:head-two",
		},
		{
			Kind: queue.KindRefreshStack,
			Key:  "stack:acme/monolith:142",
		},
	}
	if !reflect.DeepEqual(stackChanged, want) {
		t.Fatalf("stack-state PR followups = %v, want %v", stackChanged, want)
	}
}

func TestStackFollowupSpecsHaveDeterministicOrder(t *testing.T) {
	t.Parallel()
	result := store.ApplyStackResult{
		Applied:   true,
		JoinedPRs: []int{9, 2},
		LeftPRs:   []int{2},
		PriorStackByPR: map[int]int{
			9: 200,
			2: 100,
			7: 200,
		},
	}
	want := []queue.RefreshSpec{
		{Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:100"},
		{Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:200"},
		{Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:200"},
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:2",
		},
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:9",
		},
	}
	for iteration := range 100 {
		got := stackFollowupSpecs("acme/monolith", &result)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf(
				"iteration %d stack follow-up specs = %v, want %v",
				iteration,
				got,
				want,
			)
		}
	}
}

func TestPullBackfillCursorCarriesPageAndPassCount(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		page         int
		passNewCount int
		alternate    bool
	}{
		{page: 1, passNewCount: 0},
		{page: 2, passNewCount: 4, alternate: true},
		{page: 137, passNewCount: 901},
	} {
		encoded, err := encodePullBackfillCursor(
			test.page,
			test.passNewCount,
			test.alternate,
		)
		if err != nil {
			t.Fatal(err)
		}
		page, passNewCount, alternate, err := decodePullBackfillCursor(
			encoded,
		)
		if err != nil {
			t.Fatal(err)
		}
		if page != test.page ||
			passNewCount != test.passNewCount ||
			alternate != test.alternate {
			t.Fatalf(
				"cursor %d decoded page=%d pass_new_count=%d alternate=%t, want %d/%d/%t",
				encoded,
				page,
				passNewCount,
				alternate,
				test.page,
				test.passNewCount,
				test.alternate,
			)
		}
	}
}
