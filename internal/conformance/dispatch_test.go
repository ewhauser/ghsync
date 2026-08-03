package conformance_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/ewhauser/ghsync/internal/dispatch"
)

var updateDispatchGolden = flag.Bool(
	"update",
	false,
	"regenerate the dispatch conformance golden file",
)

const dispatchGoldenPath = "testdata/dispatch.golden.json"

type dispatchGoldenEntry struct {
	DefaultRules    dispatchGoldenOutcome `json:"default_rules"`
	DispatcherRules dispatchGoldenOutcome `json:"dispatcher_rules"`
}

type dispatchGoldenOutcome struct {
	Disposition string            `json:"disposition"`
	MatchedRule bool              `json:"matched_rule"`
	Intents     []dispatch.Intent `json:"intents"`
}

func TestDispatchCorpus(t *testing.T) {
	t.Parallel()
	defaultRules := dispatch.DefaultRules()
	configuredRules, err := dispatch.LoadRulesFile(
		"../../config/dispatcher-rules.yaml",
	)
	if err != nil {
		t.Fatalf("load shipped dispatcher rules: %v", err)
	}
	classifiers := []struct {
		name       string
		rules      []dispatch.Rule
		classifier dispatch.Classifier
	}{
		{
			name:       "default rules",
			rules:      defaultRules,
			classifier: dispatch.NewClassifier(defaultRules),
		},
		{
			name:       "dispatcher rules",
			rules:      configuredRules,
			classifier: dispatch.NewClassifier(configuredRules),
		},
	}

	actual := make(map[string]dispatchGoldenEntry)
	payloads := loadCorpusPayloads(t)
	payloads = append(payloads, pullRequestIssueCommentGoldenPayload(t, payloads))
	sort.Slice(payloads, func(i, j int) bool {
		return payloads[i].Filename < payloads[j].Filename
	})
	for _, payload := range payloads {
		var outcomes [2]dispatchGoldenOutcome
		formBody := []byte(url.Values{
			"payload": {string(payload.Body)},
		}.Encode())
		for index, classifier := range classifiers {
			outcomes[index] = classifyCorpusPayload(
				t,
				classifier.name,
				classifier.rules,
				classifier.classifier,
				payload,
				"application/json",
				payload.Body,
			)
			formOutcome := classifyCorpusPayload(
				t,
				classifier.name+" form",
				classifier.rules,
				classifier.classifier,
				payload,
				"application/x-www-form-urlencoded",
				formBody,
			)
			if !reflect.DeepEqual(formOutcome, outcomes[index]) {
				t.Fatalf(
					"%s form outcome for %s = %#v, want JSON outcome %#v",
					classifier.name,
					payload.Filename,
					formOutcome,
					outcomes[index],
				)
			}
		}
		actual[payload.Filename] = dispatchGoldenEntry{
			DefaultRules:    outcomes[0],
			DispatcherRules: outcomes[1],
		}
	}

	if *updateDispatchGolden {
		writeDispatchGolden(t, actual)
		return
	}
	expected := loadDispatchGolden(t)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf(
			"dispatch corpus differs from golden; run "+
				"`go test ./internal/conformance -run TestDispatchCorpus -update`\n"+
				"got:  %#v\nwant: %#v",
			actual,
			expected,
		)
	}
}

func pullRequestIssueCommentGoldenPayload(
	t *testing.T,
	payloads []corpusPayload,
) corpusPayload {
	t.Helper()
	for _, payload := range payloads {
		if payload.Event != "issue_comment" {
			continue
		}
		var envelope map[string]any
		if err := json.Unmarshal(payload.Body, &envelope); err != nil {
			t.Fatal(err)
		}
		issue, ok := envelope["issue"].(map[string]any)
		if !ok {
			t.Fatalf("%s has no issue object", payload.Filename)
		}
		issue["number"] = float64(4812)
		issue["pull_request"] = map[string]any{
			"url": "https://api.github.com/repos/Codertocat/Hello-World/pulls/4812",
		}
		body, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		return corpusPayload{
			Event:    "issue_comment",
			Filename: "issue_comment/pr-associated.created.synthetic.json",
			Body:     body,
		}
	}
	t.Fatal("issue_comment corpus has no payload")
	return corpusPayload{}
}

func classifyCorpusPayload(
	t *testing.T,
	classifierName string,
	rules []dispatch.Rule,
	classifier dispatch.Classifier,
	payload corpusPayload,
	contentType string,
	body []byte,
) dispatchGoldenOutcome {
	t.Helper()
	var envelope struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(payload.Body, &envelope); err != nil {
		t.Fatalf("decode %s action: %v", payload.Filename, err)
	}

	intents, err := classifyWithoutPanic(
		classifier,
		payload.Event,
		contentType,
		body,
	)
	if err != nil {
		t.Fatalf(
			"%s classify %s would park: %v",
			classifierName,
			payload.Filename,
			err,
		)
	}
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].Kind != intents[j].Kind {
			return intents[i].Kind < intents[j].Kind
		}
		if intents[i].Key != intents[j].Key {
			return intents[i].Key < intents[j].Key
		}
		return intents[i].Priority < intents[j].Priority
	})
	if intents == nil {
		intents = []dispatch.Intent{}
	}

	disposition := "no_op"
	if len(intents) > 0 {
		disposition = "dispatch"
	}
	matchedRule := matchesRule(rules, payload.Event, envelope.Action)
	if !matchedRule {
		t.Fatalf(
			"%s has no explicit rule for %s action %q",
			classifierName,
			payload.Event,
			envelope.Action,
		)
	}
	return dispatchGoldenOutcome{
		Disposition: disposition,
		MatchedRule: matchedRule,
		Intents:     intents,
	}
}

func classifyWithoutPanic(
	classifier dispatch.Classifier,
	event string,
	contentType string,
	body []byte,
) (intents []dispatch.Intent, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return classifier.ClassifyContent(event, contentType, body)
}

func matchesRule(rules []dispatch.Rule, event, action string) bool {
	for _, rule := range rules {
		if rule.Event == event &&
			(rule.Action == dispatch.ActionAny || rule.Action == action) {
			return true
		}
	}
	return false
}

func loadDispatchGolden(t *testing.T) map[string]dispatchGoldenEntry {
	t.Helper()
	body, err := os.ReadFile(dispatchGoldenPath)
	if err != nil {
		t.Fatalf("read dispatch golden: %v", err)
	}
	var golden map[string]dispatchGoldenEntry
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&golden); err != nil {
		t.Fatalf("decode dispatch golden: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("decode dispatch golden trailing data: %v", err)
	}
	if len(golden) == 0 {
		t.Fatal("dispatch golden has no corpus entries")
	}
	return golden
}

func writeDispatchGolden(
	t *testing.T,
	golden map[string]dispatchGoldenEntry,
) {
	t.Helper()
	body, err := json.MarshalIndent(golden, "", "  ")
	if err != nil {
		t.Fatalf("encode dispatch golden: %v", err)
	}
	body = append(body, '\n')
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatalf("create dispatch golden directory: %v", err)
	}
	if err := os.WriteFile(dispatchGoldenPath, body, 0o644); err != nil {
		t.Fatalf("write dispatch golden: %v", err)
	}
}
