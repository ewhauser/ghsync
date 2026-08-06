package queue

import (
	"encoding/json"
	"testing"

	"github.com/riverqueue/river/rivertype"
)

func TestRefreshJobArgsArePointersOnly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		args rivertype.JobArgs
		kind string
	}{
		{NewRefreshPRArgs("pr:acme/monolith:4812"), KindRefreshPR},
		{
			NewRefreshRepositoryArgs("repo:acme/monolith:metadata"),
			KindRefreshRepository,
		},
		{
			NewRefreshRepoRulesArgs("repo_rules:acme/monolith:rules"),
			KindRefreshRepoRules,
		},
		{NewRefreshStackArgs("stack:acme/monolith:142"), KindRefreshStack},
		{NewRefreshChecksArgs("checks:acme/monolith:abc"), KindRefreshChecks},
		{NewRefreshBranchArgs("branch:acme/monolith:main"), KindRefreshBranch},
		{
			NewResolveStackMembershipArgs("pr:acme/monolith:4812"),
			KindResolveStackMembership,
		},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(test.args)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 2 ||
				fields["kind"] != test.kind ||
				fields["key"] == "" {
				t.Fatalf("args = %s, want only non-empty kind and key", encoded)
			}
			if test.args.Kind() != test.kind {
				t.Fatalf("River kind = %q, want %q", test.args.Kind(), test.kind)
			}
		})
	}
	headArgs := NewRefreshRepositoryHeadArgs("repo:acme/monolith:metadata")
	encoded, err := json.Marshal(headArgs)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 ||
		fields["kind"] != KindRefreshRepository ||
		fields["key"] == "" ||
		fields["observe_default_branch_head"] != true {
		t.Fatalf("branch-head args = %s, want pointer plus observation mode", encoded)
	}
}

func TestInstallationBackfillArgsCarryExpectedCursorOnly(t *testing.T) {
	t.Parallel()
	args := NewBackfillInstallationPageArgs(7, "repositories", 3)
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 ||
		fields["installation_id"] != float64(7) ||
		fields["phase"] != "repositories" ||
		fields["page"] != float64(3) {
		t.Fatalf("installation backfill args = %s", encoded)
	}
	if args.Kind() != KindBackfillInstallation {
		t.Fatalf("kind = %q", args.Kind())
	}
}

func TestBackfillArgsCarryExpectedCursorOnly(t *testing.T) {
	t.Parallel()
	args := NewBackfillRepoPageArgs(7, "acme/monolith", "pull_requests", 3)
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 4 ||
		fields["installation_id"] != float64(7) ||
		fields["repo"] != "acme/monolith" ||
		fields["phase"] != "pull_requests" ||
		fields["page"] != float64(3) {
		t.Fatalf("backfill args = %s", encoded)
	}
	if args.Kind() != KindBackfillRepoPage {
		t.Fatalf("kind = %q", args.Kind())
	}
}
