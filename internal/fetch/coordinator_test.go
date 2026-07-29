package fetch

import (
	"reflect"
	"strings"
	"testing"

	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
)

func TestSortedPullRequestKeysUseImmutableRepositoryIdentity(t *testing.T) {
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
	specs := stackFollowupSpecs("acme/monolith", store.ApplyStackResult{
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
