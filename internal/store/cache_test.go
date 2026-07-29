package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStackMembershipDiffIncludesOrderOnlyMoves(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	run := CheckRunRecord{
		Status: "queued", DetailsURL: "https://example.test/check/1",
	}
	first := checkSemanticVersion(&run)
	second := checkSemanticVersion(&run)
	if first == "" || first != second {
		t.Fatalf("stable semantic versions = %q, %q", first, second)
	}
	run.Status = "in_progress"
	if changed := checkSemanticVersion(&run); changed == first {
		t.Fatal("status transition did not change semantic version")
	}
}

func TestObservedWritersRequireObservation(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/observed", 2300, at)
	pull := storeTestPull(&repository, at, "observed-head")
	stack := StackRecord{
		Repository:      repository,
		GitHubID:        7000,
		NodeID:          "stack-node-7",
		Number:          7,
		GitHubUpdatedAt: at,
		SyncedAt:        at,
		Source:          SyncSourceWebhook,
	}
	writer := &EntityWriter{
		now:      time.Now,
		observer: noopCacheObserver{},
	}
	tests := map[string]func() error{
		"apply repository": func() error {
			_, err := writer.ApplyRepositoryObserved(
				context.Background(),
				nil,
				repository,
				SyncSourceWebhook,
				"",
				at,
			)
			return err
		},
		"tombstone repository": func() error {
			_, err := writer.TombstoneRepositoryObserved(
				context.Background(),
				nil,
				repository,
				SyncSourceWebhook,
				at,
			)
			return err
		},
		"apply pull request": func() error {
			_, err := writer.ApplyPullRequestObserved(
				context.Background(),
				nil,
				pull,
				nil,
			)
			return err
		},
		"tombstone pull request": func() error {
			_, err := writer.TombstonePullRequestObserved(
				context.Background(),
				nil,
				repository,
				pull.Number,
				SyncSourceWebhook,
				at,
				nil,
			)
			return err
		},
		"apply stack": func() error {
			_, err := writer.ApplyStackObserved(
				context.Background(),
				nil,
				stack,
				nil,
			)
			return err
		},
		"tombstone stack": func() error {
			_, err := writer.TombstoneStackObserved(
				context.Background(),
				nil,
				repository,
				stack.Number,
				SyncSourceWebhook,
				at,
				nil,
			)
			return err
		},
		"apply checks": func() error {
			_, err := writer.ApplyChecksObserved(
				context.Background(),
				nil,
				ChecksRecord{
					Repository: repository,
					HeadSHA:    pull.HeadSHA,
					SyncedAt:   at,
					Source:     SyncSourceWebhook,
				},
			)
			return err
		},
		"apply repository rules": func() error {
			_, err := writer.ApplyRepoRulesObserved(
				context.Background(),
				nil,
				RepoRulesRecord{
					Repository: repository,
					SyncedAt:   at,
					Source:     SyncSourceWebhook,
				},
			)
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := run()
			if err == nil || !strings.Contains(err.Error(), "observation") {
				t.Fatalf("error = %v, want required observation", err)
			}
		})
	}
}
