package dispatch

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
)

func TestDefaultClassifierHintCoverage(t *testing.T) {
	t.Parallel()
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
					Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4812",
					Priority: PriorityEvent,
				},
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
					Kind:     queue.KindRefreshPR,
					Key:      "pr:acme/monolith:4815",
					Priority: PriorityEvent,
				},
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
			name:  "pull request review",
			event: "pull_request_review",
			body: `{
				"action":"submitted",
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4812}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4812",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "edited pull request review",
			event: "pull_request_review",
			body: `{
				"action":"edited",
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4812}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4812",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "dismissed pull request review",
			event: "pull_request_review",
			body: `{
				"action":"dismissed",
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4812}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4812",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "pull request review comment",
			event: "pull_request_review_comment",
			body: `{
				"action":"created",
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4815}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4815",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "pull request review thread",
			event: "pull_request_review_thread",
			body: `{
				"action":"resolved",
				"repository":{"full_name":"acme/monolith"},
				"pull_request":{"number":4816}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4816",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "pull request issue comment",
			event: "issue_comment",
			body: `{
				"action":"created",
				"number":9999,
				"repository":{"full_name":"acme/monolith"},
				"issue":{"number":4816,"pull_request":{"url":"https://api.github.test/pulls/4816"}}
			}`,
			want: []Intent{{
				Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:4816",
				Priority: PriorityEvent,
			}},
		},
		{
			name:  "plain issue comment is a clean no-op",
			event: "issue_comment",
			body: `{
				"action":"created",
				"repository":{"full_name":"acme/monolith"},
				"issue":{"number":99,"pull_request":null}
			}`,
			want: []Intent{},
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
			want: []Intent{
				{
					Kind:     queue.KindRefreshBranch,
					Key:      "branch:acme/monolith:refactor/bm25f-ranker",
					Priority: PriorityEvent,
				},
			},
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
			t.Parallel()
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

func TestCodeownersPushRuleTargetsOnlyEffectiveDefaultBranchPaths(
	t *testing.T,
) {
	t.Parallel()
	classifier := NewClassifier([]Rule{{
		Event: "push", Action: ActionAny, Target: TargetCodeowners,
	}})
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "modified effective path",
			body: `{"ref":"refs/heads/main","repository":{"full_name":"acme/monolith","default_branch":"main"},"commits":[{"modified":[".github/CODEOWNERS"]}]}`,
			want: true,
		},
		{
			name: "removed fallback path",
			body: `{"ref":"refs/heads/main","repository":{"full_name":"acme/monolith","default_branch":"main"},"head_commit":{"removed":["docs/CODEOWNERS"]}}`,
			want: true,
		},
		{
			name: "added root path",
			body: `{"ref":"refs/heads/main","repository":{"full_name":"acme/monolith","default_branch":"main"},"commits":[{"added":["CODEOWNERS"]}]}`,
			want: true,
		},
		{
			name: "non default branch",
			body: `{"ref":"refs/heads/topic","repository":{"full_name":"acme/monolith","default_branch":"main"},"commits":[{"modified":["CODEOWNERS"]}]}`,
		},
		{
			name: "unrelated default branch file",
			body: `{"ref":"refs/heads/main","repository":{"full_name":"acme/monolith","default_branch":"main"},"commits":[{"modified":["src/CODEOWNERS"]}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			intents, err := classifier.Classify("push", []byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			if (len(intents) == 1) != test.want {
				t.Fatalf("intents = %#v, want emitted=%v", intents, test.want)
			}
		})
	}
}

func TestLoadRulesFileFailsClosedOnSchemaAndSemanticErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"unknown key": `rules:
  - event: pull_request
    action: "*"
    target: pull_request
    stacked_targte: stack
`,
		"trailing document": `rules:
  - event: push
    action: "*"
    target: branch
---
rules: []
`,
		"untrimmed action": `rules:
  - event: pull_request
    action: " opened "
    target: pull_request
`,
		"invalid target combination": `rules:
  - event: push
    action: "*"
    target: checks
`,
		"invalid stacked target combination": `rules:
  - event: check_run
    action: "*"
    target: checks
    stacked_target: stack
`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "rules.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadRulesFile(path); err == nil {
				t.Fatalf("invalid rules file accepted:\n%s", body)
			}
		})
	}
}

func TestUnknownEventIsProcessedWithoutParsing(t *testing.T) {
	t.Parallel()
	intents, err := DefaultClassifier().Classify("mystery_event", []byte(`not-json`))
	if err != nil {
		t.Fatalf("unknown event returned error: %v", err)
	}
	if len(intents) != 0 {
		t.Fatalf("unknown event emitted %#v", intents)
	}
}

func TestKnownMalformedEventFailsClassification(t *testing.T) {
	t.Parallel()
	if _, err := DefaultClassifier().Classify(
		"pull_request",
		[]byte(`not-json`),
	); err == nil {
		t.Fatal("malformed known event was accepted")
	}
}

func TestCompleteStackSummaryHintRequiresAuthoritativeResolver(t *testing.T) {
	t.Parallel()
	body := []byte(`{
		"action":"synchronize",
		"number":72787,
		"repository":{"full_name":"acme/monolith"},
		"pull_request":{
			"number":72787,
			"stack":{
				"id":46101,
				"number":72787,
				"base":{
					"ref":"main",
					"sha":"89850dd46b0e9edb77b61bf2ea8c376e58fc5aca"
				},
				"size":6,
				"position":1
			}
		}
	}`)
	result, err := DefaultClassifier().classify("pull_request", body)
	if err != nil {
		t.Fatal(err)
	}
	want := &stackSummaryHint{
		Repo:     "acme/monolith",
		PRNumber: 72787,
		ID:       46101,
		Number:   72787,
		Size:     6,
		Position: 1,
		BaseRef:  "main",
		BaseSHA:  "89850dd46b0e9edb77b61bf2ea8c376e58fc5aca",
	}
	if !reflect.DeepEqual(result.stackHint, want) {
		t.Fatalf("stack hint = %+v, want %+v", result.stackHint, want)
	}

	stackOnly := NewClassifier([]Rule{{
		Event:         "pull_request",
		Action:        ActionAny,
		Target:        TargetPullRequest,
		StackedTarget: TargetStack,
	}})
	result, err = stackOnly.classify("pull_request", body)
	if err != nil {
		t.Fatal(err)
	}
	if result.stackHint != nil {
		t.Fatalf(
			"stack hint without authoritative PR resolver = %+v",
			result.stackHint,
		)
	}
}

func TestUnknownStackBaseSHARetainsEagerStackFetch(t *testing.T) {
	t.Parallel()
	body := []byte(`{
		"action":"synchronize",
		"number":72787,
		"repository":{"full_name":"acme/monolith"},
		"pull_request":{
			"number":72787,
			"stack":{
				"id":46101,
				"number":72787,
				"base":{"ref":"deleted/historical-base","sha":null},
				"size":6,
				"position":1
			}
		}
	}`)
	result, err := DefaultClassifier().classify("pull_request", body)
	if err != nil {
		t.Fatalf("classify null stack base SHA: %v", err)
	}
	if result.stackHint != nil {
		t.Fatalf("unknown SHA produced suppression hint: %+v", result.stackHint)
	}
	want := []Intent{
		{
			Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:72787",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:72787",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:72787", Priority: PriorityEvent,
		},
	}
	if !reflect.DeepEqual(result.intents, want) {
		t.Fatalf("unknown-SHA intents = %#v, want %#v", result.intents, want)
	}
}

func TestHistoricalStackPositionRetainsEagerStackFetch(t *testing.T) {
	t.Parallel()
	body := []byte(`{
		"action":"synchronize",
		"number":72787,
		"repository":{"full_name":"acme/monolith"},
		"pull_request":{
			"number":72787,
			"stack":{
				"id":46101,
				"number":72787,
				"base":{"ref":"main","sha":"base-one"},
				"size":2,
				"position":5
			}
		}
	}`)
	result, err := DefaultClassifier().classify("pull_request", body)
	if err != nil {
		t.Fatalf("classify historical stack position: %v", err)
	}
	if result.stackHint != nil {
		t.Fatalf("historical position produced suppression hint: %+v", result.stackHint)
	}
	want := []Intent{
		{
			Kind: queue.KindRefreshPR, Key: "pr:acme/monolith:72787",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindRefreshStack, Key: "stack:acme/monolith:72787",
			Priority: PriorityEvent,
		},
		{
			Kind: queue.KindResolveStackMembership,
			Key:  "pr:acme/monolith:72787", Priority: PriorityEvent,
		},
	}
	if !reflect.DeepEqual(result.intents, want) {
		t.Fatalf("historical-position intents = %#v, want %#v", result.intents, want)
	}
}

func TestHistoricalStackPositionCannotIndexCurrentEntries(t *testing.T) {
	t.Parallel()
	matched, err := stackSummaryMatchesCache(
		t.Context(),
		nil,
		&stackSummaryHint{Size: 2, Position: 5},
	)
	if err != nil || matched {
		t.Fatalf("historical-position comparison = %v, %v, want false, nil", matched, err)
	}
}

func TestClassifierUsesStoredFormContentType(t *testing.T) {
	t.Parallel()
	jsonBody := []byte(`{
		"ref":"refs/heads/main",
		"repository":{"full_name":"acme/monolith"}
	}`)
	formBody := []byte(url.Values{
		"payload": {string(jsonBody)},
	}.Encode())
	headers, err := json.Marshal(struct {
		Headers http.Header `json:"headers"`
	}{
		Headers: http.Header{
			"Content-Type": {gh.WebhookFormContentType},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := DefaultClassifier().classifyStored(
		"push",
		formBody,
		headers,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []Intent{{
		Kind: queue.KindRefreshBranch, Key: "branch:acme/monolith:main",
		Priority: PriorityEvent,
	}}
	if !reflect.DeepEqual(got.intents, want) {
		t.Fatalf("intents = %#v, want %#v", got.intents, want)
	}
}

func TestClassifierRejectsMalformedStoredHeaders(t *testing.T) {
	t.Parallel()
	_, err := DefaultClassifier().classifyStored(
		"push",
		[]byte(`{}`),
		[]byte(`not-json`),
	)
	if err == nil {
		t.Fatal("malformed stored headers were accepted")
	}
}

func TestRuleTableControlsActions(t *testing.T) {
	t.Parallel()
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
