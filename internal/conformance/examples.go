package conformance

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

//go:embed corpus/pull_request/*payload*.json corpus/pull_request/opened.with-null-body.json corpus/pull_request_review/*payload*.json corpus/pull_request_review_comment/*payload*.json corpus/pull_request_review_thread/*payload*.json corpus/check_run/*payload*.json corpus/check_suite/*payload*.json corpus/push/*payload*.json
var payloadExamples embed.FS

// PayloadExample is one vendored octokit webhook example.
type PayloadExample struct {
	Filename string
	Payload  map[string]any
}

// PayloadExamples returns fresh decoded payloads for one event family.
func PayloadExamples(event string) ([]PayloadExample, error) {
	directory := path.Join("corpus", event)
	entries, err := fs.ReadDir(payloadExamples, directory)
	if err != nil {
		return nil, fmt.Errorf("read %s examples: %w", event, err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	examples := make([]PayloadExample, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() ||
			!strings.HasSuffix(entry.Name(), ".json") ||
			strings.HasSuffix(entry.Name(), ".schema.json") {
			continue
		}
		filename := path.Join(directory, entry.Name())
		body, err := payloadExamples.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", filename, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("decode %s: %w", filename, err)
		}
		examples = append(examples, PayloadExample{
			Filename: event + "/" + entry.Name(),
			Payload:  payload,
		})
	}
	if len(examples) == 0 {
		return nil, fmt.Errorf("octokit corpus has no %s payload examples", event)
	}
	return examples, nil
}

// ExamplePayload returns a fresh copy of the canonical octokit payload example
// for an event and action. Push has no action and uses the empty string.
func ExamplePayload(event, action string) (map[string]any, error) {
	examples, err := PayloadExamples(event)
	if err != nil {
		return nil, err
	}
	for _, example := range examples {
		payloadAction, _ := example.Payload["action"].(string)
		if payloadAction == action {
			return example.Payload, nil
		}
	}
	return nil, fmt.Errorf(
		"octokit corpus has no %s payload example for action %q",
		event,
		action,
	)
}
