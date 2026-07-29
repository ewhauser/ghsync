package store

import (
	"reflect"
	"testing"
	"time"
)

func TestStackMembershipDiffIncludesOrderOnlyMoves(t *testing.T) {
	now := time.Now()
	oldEntries := []StackEntry{
		{Number: 1, UpdatedAt: now},
		{Number: 2, UpdatedAt: now},
		{Number: 3, UpdatedAt: now},
	}
	newEntries := []StackEntry{
		{Number: 2, UpdatedAt: now},
		{Number: 1, UpdatedAt: now},
		{Number: 3, UpdatedAt: now},
	}
	result := stackMembershipDiff(oldEntries, newEntries)
	if len(result.JoinedPRs) != 0 || len(result.LeftPRs) != 0 {
		t.Fatalf("order-only diff has set changes: %+v", result)
	}
	if !reflect.DeepEqual(result.MovedPRs, []int{1, 2}) {
		t.Fatalf("moved PRs = %v, want [1 2]", result.MovedPRs)
	}
}

func TestCheckSemanticVersionIsStableAndDomainSensitive(t *testing.T) {
	run := CheckRunRecord{
		Status: "queued", DetailsURL: "https://example.test/check/1",
	}
	first := checkSemanticVersion(run)
	second := checkSemanticVersion(run)
	if first == "" || first != second {
		t.Fatalf("stable semantic versions = %q, %q", first, second)
	}
	run.Status = "in_progress"
	if changed := checkSemanticVersion(run); changed == first {
		t.Fatal("status transition did not change semantic version")
	}
}
