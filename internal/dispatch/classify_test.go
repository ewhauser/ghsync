package dispatch

import (
	"reflect"
	"testing"

	"github.com/acme/frontier/internal/queue"
)

func TestDefaultClassifierHintCoverage(t *testing.T) {
	classifier := DefaultClassifier()
	tests := []struct {
		name  string
		event string
		body  string
		want  []Intent
	}{
		{
			name:  "loose pull request",
			event: "pull_request",
			body: `{
				"action":"opened",
				"number":4800,
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4800,"stack":null}
			}`,
			want: []Intent{
				{
					Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4800",
					Priority: PriorityEvent,
				},
				{
					Kind: queue.KindResolveStackMembership,
					Key:  "pr:acme/monolith:4800", Priority: PriorityEvent,
				},
			},
		},
		{
			name:  "stacked pull request escalates",
			event: "pull_request",
			body: `{
				"action":"synchronize",
				"number":4812,
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4812,"stack":{"number":142}}
			}`,
			want: []Intent{
				{
					Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:142",
					Priority: PriorityEvent,
				},
				{
					Kind: queue.KindResolveStackMembership,
					Key:  "pr:acme/monolith:4812", Priority: PriorityEvent,
				},
			},
		},
		{
			name:  "stacked action escalates",
			event: "pull_request",
			body: `{
				"action":"stacked",
				"number":4815,
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4815,"stack":{"number":142}}
			}`,
			want: []Intent{
				{
					Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:142",
					Priority: PriorityEvent,
				},
				{
					Kind: queue.KindResolveStackMembership,
					Key:  "pr:acme/monolith:4815", Priority: PriorityEvent,
				},
			},
		},
		{
			name:  "check run by SHA",
			event: "check_run",
			body: `{
				"action":"completed",
				"repository":{"full_name":"acme/monolith"},
				"check_run":{"head_sha":"8f31c2d"}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshChecks, Key: "checks:acme/monolith:8f31c2d",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "check suite by SHA",
			event: "check_suite",
			body: `{
				"action":"rerequested",
				"repository":{"full_name":"acme/monolith"},
				"check_suite":{"head_sha":"8f31c2d"}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshChecks, Key: "checks:acme/monolith:8f31c2d",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "branch push",
			event: "push",
			body: `{
				"ref":"refs/heads/refactor/bm25f-ranker",
				"repository":{"full_name":"acme/monolith"}
			}`,
			want: []Intent{{
				Kind:     queue.KindRefreshBranch,
				Key:      "branch:acme/monolith:refactor/bm25f-ranker",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "stack branch push escalates",
			event: "push",
			body: `{
				"ref":"refs/heads/refactor/bm25f-ranker",
				"repository":{"full_name":"acme/monolith"},
				"stack":{"number":142}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:142",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "tag push is not a branch hint",
			event: "push",
			body: `{
				"ref":"refs/tags/v1.0.0",
				"repository":{"full_name":"acme/monolith"}
			}`,
			want: []Intent{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := classifier.Classify(test.event, []byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("intents = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestUnknownEventIsProcessedWithoutParsing(t *testing.T) {
	intents, err := DefaultClassifier().Classify("mystery_event", []byte(`not-json`))
	if err != nil {
		t.Fatalf("unknown event returned error: %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("unknown event emitted %#v", intents)
	}
}

func TestKnownMalformedEventFailsClassification(t *testing.T) {
	if _, err := DefaultClassifier().Classify(
		"pull_request",
		[]byte(`not-json`),
	); err == nil {
		t.Fatal("malformed known event was accepted")
	}
}

func TestRuleTableControlsActions(t *testing.T) {
	classifier := NewClassifier([]Rule{{
		Event: "pull_request", Action: "opened", Target: TargetPullRequest,
	}})
	payload := []byte(`{
		"action":"synchronize",
		"number":4812,
		"repository":{"full_name":"acme/monolith"},
		"pull_request":{"number":4812}
	}`)
	intents, err := classifier.Classify("pull_request", payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 0 {
		t.Fatalf("action not present in rules emitted %#v", intents)
	}
}
