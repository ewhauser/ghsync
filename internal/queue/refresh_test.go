package queue

import (
	"encoding/json"
	"testing"

	"github.com/riverqueue/river/rivertype"
)

func TestRefreshJobArgsArePointersOnly(t *testing.T) {
	tests := []struct {
		args rivertype.JobArgs
		kind string
	}{
		{NewRefreshPRArgs("pr:acme/monolith:4812"), KindRefreshPR},
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
}

func TestBackfillArgsCarryExpectedCursorOnly(t *testing.T) {
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
