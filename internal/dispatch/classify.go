// Package dispatch classifies durable webhook hints and coalesces them into
// River refresh jobs. It never treats webhook payload data as cache truth.
package dispatch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/queue"
	"gopkg.in/yaml.v3"
)

const (
	ActionAny     = "*"
	PriorityEvent = queue.QueueEvent
)

// Intent is a dispatch decision, not an entity payload.
type Intent struct {
	Kind     string `json:"kind"`
	Key      string `json:"key"`
	Priority string `json:"priority"`
}

// Target names a declarative key extractor used by a Rule.
type Target string

const (
	TargetPullRequest Target = "pull_request"
	TargetStack       Target = "stack"
	TargetChecks      Target = "checks"
	TargetBranch      Target = "branch"
	// TargetResolveStackMembership carries only the PR key. Its M3 worker
	// consults cached membership and refreshes both the old and new stacks.
	TargetResolveStackMembership Target = "resolve_stack_membership"
)

// Rule is the config-driven event/action → refresh mapping. StackedTarget
// escalates a pull request event when its payload carries the stack preview
// object (SYNC_ENGINE §2.1).
type Rule struct {
	Event         string `json:"event" yaml:"event"`
	Action        string `json:"action" yaml:"action"`
	Target        Target `json:"target" yaml:"target"`
	StackedTarget Target `json:"stacked_target,omitempty" yaml:"stacked_target,omitempty"`
}

// DefaultRules is data rather than event-specific control flow so preview
// webhook changes can be accommodated by changing the table.
func DefaultRules() []Rule {
	return []Rule{
		{
			Event:         "pull_request",
			Action:        ActionAny,
			Target:        TargetPullRequest,
			StackedTarget: TargetStack,
		},
		{
			Event:  "pull_request",
			Action: ActionAny,
			Target: TargetResolveStackMembership,
		},
		{
			Event:  "pull_request_review",
			Action: ActionAny,
			Target: TargetPullRequest,
		},
		{
			Event:  "pull_request_review_comment",
			Action: ActionAny,
			Target: TargetPullRequest,
		},
		{
			Event:  "pull_request_review_thread",
			Action: ActionAny,
			Target: TargetPullRequest,
		},
		{Event: "check_run", Action: ActionAny, Target: TargetChecks},
		{Event: "check_suite", Action: ActionAny, Target: TargetChecks},
		{
			Event:         "push",
			Action:        ActionAny,
			Target:        TargetBranch,
			StackedTarget: TargetStack,
		},
	}
}

// Classifier applies an injected rule table.
type Classifier struct {
	rules []Rule
}

func NewClassifier(rules []Rule) Classifier {
	return Classifier{rules: append([]Rule(nil), rules...)}
}

func DefaultClassifier() Classifier {
	return NewClassifier(DefaultRules())
}

// LoadRulesFile makes observed-payload findings a reviewed data change rather
// than an event-specific code branch. YAML is a superset of the shipped JSON
// shape, so either format is accepted.
func LoadRulesFile(path string) ([]Rule, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dispatcher rules file: %w", err)
	}
	var document struct {
		Rules []Rule `json:"rules" yaml:"rules"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode dispatcher rules file: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf(
				"decode dispatcher rules file: trailing YAML document",
			)
		}
		return nil, fmt.Errorf(
			"decode dispatcher rules file trailing data: %w",
			err,
		)
	}
	if len(document.Rules) == 0 {
		return nil, fmt.Errorf("dispatcher rules file has no rules")
	}
	for index, rule := range document.Rules {
		if err := validateRule(rule); err != nil {
			return nil, fmt.Errorf("dispatcher rule %d: %w", index, err)
		}
	}
	return document.Rules, nil
}

func validateRule(rule Rule) error {
	if rule.Event == "" || strings.TrimSpace(rule.Event) != rule.Event {
		return fmt.Errorf("event must be non-empty and trimmed")
	}
	if rule.Action == "" || strings.TrimSpace(rule.Action) != rule.Action {
		return fmt.Errorf("action must be non-empty and trimmed")
	}
	if !validTarget(rule.Target) {
		return fmt.Errorf("unsupported target %q", rule.Target)
	}
	if rule.StackedTarget != "" && !validTarget(rule.StackedTarget) {
		return fmt.Errorf(
			"unsupported stacked target %q",
			rule.StackedTarget,
		)
	}
	switch rule.Target {
	case TargetPullRequest:
		if !isPullRequestEvent(rule.Event) {
			return fmt.Errorf(
				"target %q requires a pull-request event",
				rule.Target,
			)
		}
	case TargetResolveStackMembership:
		if rule.Event != "pull_request" {
			return fmt.Errorf(
				"target %q requires event pull_request",
				rule.Target,
			)
		}
	case TargetChecks:
		if rule.Event != "check_run" && rule.Event != "check_suite" {
			return fmt.Errorf(
				"target %q requires check_run or check_suite",
				rule.Target,
			)
		}
	case TargetBranch:
		if rule.Event != "push" {
			return fmt.Errorf("target %q requires event push", rule.Target)
		}
	case TargetStack:
		return fmt.Errorf(
			"target stack is only valid as stacked_target",
		)
	}
	if rule.StackedTarget != "" {
		if rule.StackedTarget != TargetStack {
			return fmt.Errorf("stacked_target must be stack")
		}
		if rule.Target != TargetPullRequest && rule.Target != TargetBranch {
			return fmt.Errorf(
				"stacked_target requires pull_request or branch target",
			)
		}
	}
	return nil
}

func isPullRequestEvent(event string) bool {
	switch event {
	case "pull_request",
		"pull_request_review",
		"pull_request_review_comment",
		"pull_request_review_thread":
		return true
	default:
		return false
	}
}

func validTarget(target Target) bool {
	switch target {
	case TargetPullRequest,
		TargetStack,
		TargetChecks,
		TargetBranch,
		TargetResolveStackMembership:
		return true
	default:
		return false
	}
}

type payloadEnvelope struct {
	Action     string `json:"action"`
	Number     int    `json:"number"`
	Ref        string `json:"ref"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest struct {
		Number int              `json:"number"`
		Stack  *payloadStackRef `json:"stack"`
	} `json:"pull_request"`
	// Stack is populated by synthetic M2 fixtures to model the branch-to-stack
	// relationship that M3 will resolve from the authoritative cache.
	Stack *struct {
		Number int `json:"number"`
	} `json:"stack"`
	CheckRun struct {
		HeadSHA string `json:"head_sha"`
	} `json:"check_run"`
	CheckSuite struct {
		HeadSHA string `json:"head_sha"`
	} `json:"check_suite"`
}

type payloadStackRef struct {
	ID       int64 `json:"id"`
	Number   int   `json:"number"`
	Size     int   `json:"size"`
	Position int   `json:"position"`
	Base     struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"base"`
}

type stackSummaryHint struct {
	Repo     string
	PRNumber int
	ID       int64
	Number   int
	Size     int
	Position int
	BaseRef  string
	BaseSHA  string
}

type classification struct {
	intents      []Intent
	matchedRules int
	stackHint    *stackSummaryHint
}

// Classify parses only events that have a configured rule. Unknown events are
// successful no-ops even when their bodies are malformed (C-I5).
func (c Classifier) Classify(event string, body []byte) ([]Intent, error) {
	result, err := c.classify(event, body)
	return result.intents, err
}

func (c Classifier) classify(event string, body []byte) (classification, error) {
	return c.classifyContent(event, gh.WebhookJSONContentType, body)
}

// ClassifyContent classifies a webhook body in either GitHub-supported wire
// format. It is primarily exposed for conformance tests; the dispatcher reads
// the content type captured by ingress before calling the same path.
func (c Classifier) ClassifyContent(
	event string,
	contentType string,
	body []byte,
) ([]Intent, error) {
	result, err := c.classifyContent(event, contentType, body)
	return result.intents, err
}

func (c Classifier) classifyContent(
	event string,
	contentType string,
	body []byte,
) (classification, error) {
	eventRules := make([]Rule, 0, 1)
	for _, rule := range c.rules {
		if rule.Event == event {
			eventRules = append(eventRules, rule)
		}
	}
	if len(eventRules) == 0 {
		return classification{}, nil
	}

	body, err := gh.DecodeWebhookPayload(contentType, body)
	if err != nil {
		return classification{}, fmt.Errorf("decode %s payload: %w", event, err)
	}
	var payload payloadEnvelope
	if err := json.Unmarshal(body, &payload); err != nil {
		return classification{}, fmt.Errorf("decode %s payload: %w", event, err)
	}

	matchedRules := 0
	seen := make(map[string]struct{}, len(eventRules))
	intents := make([]Intent, 0, len(eventRules))
	for _, rule := range eventRules {
		if rule.Action != ActionAny && rule.Action != payload.Action {
			continue
		}
		matchedRules++
		target := rule.Target
		if rule.StackedTarget != "" && payloadStack(&payload) != nil {
			target = rule.StackedTarget
		}
		key, emit, err := intentKey(target, event, &payload)
		if err != nil {
			return classification{}, err
		}
		if !emit {
			continue
		}
		kind, err := jobKind(target)
		if err != nil {
			return classification{}, err
		}
		identity := kind + "\x00" + key
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		intents = append(intents, Intent{
			Kind:     kind,
			Key:      key,
			Priority: PriorityEvent,
		})
	}
	result := classification{
		intents:      intents,
		matchedRules: matchedRules,
	}
	result.stackHint = completeStackSummaryHint(&payload, intents)
	return result, nil
}

func (c Classifier) classifyStored(
	event string,
	body []byte,
	storedHeaders []byte,
) (classification, error) {
	contentType, err := storedWebhookContentType(storedHeaders)
	if err != nil {
		return classification{}, err
	}
	return c.classifyContent(event, contentType, body)
}

func storedWebhookContentType(storedHeaders []byte) (string, error) {
	if len(bytes.TrimSpace(storedHeaders)) == 0 {
		return gh.WebhookJSONContentType, nil
	}
	var envelope struct {
		Headers http.Header `json:"headers"`
	}
	if err := json.Unmarshal(storedHeaders, &envelope); err != nil {
		return "", fmt.Errorf("decode stored webhook headers: %w", err)
	}
	contentType := envelope.Headers.Get("Content-Type")
	if contentType == "" {
		// Older fixtures and restored deliveries may predate the request
		// envelope. Their raw bodies have always been JSON.
		return gh.WebhookJSONContentType, nil
	}
	return contentType, nil
}

func intentKey(
	target Target,
	event string,
	payload *payloadEnvelope,
) (string, bool, error) {
	repo := payload.Repository.FullName
	if repo == "" {
		return "", false, fmt.Errorf("%s payload is missing repository.full_name", event)
	}
	switch target {
	case TargetPullRequest, TargetResolveStackMembership:
		number := payload.Number
		if number == 0 {
			number = payload.PullRequest.Number
		}
		if number <= 0 {
			return "", false, fmt.Errorf("pull_request payload is missing a PR number")
		}
		return "pr:" + repo + ":" + strconv.Itoa(number), true, nil
	case TargetStack:
		stack := payloadStack(payload)
		if stack == nil || stack.Number <= 0 {
			return "", false, fmt.Errorf("%s payload is missing stack.number", event)
		}
		return "stack:" + repo + ":" +
			strconv.Itoa(stack.Number), true, nil
	case TargetChecks:
		headSHA := payload.CheckRun.HeadSHA
		if event == "check_suite" {
			headSHA = payload.CheckSuite.HeadSHA
		}
		if headSHA == "" {
			return "", false, fmt.Errorf("%s payload is missing head_sha", event)
		}
		return "checks:" + repo + ":" + headSHA, true, nil
	case TargetBranch:
		const branchPrefix = "refs/heads/"
		if !strings.HasPrefix(payload.Ref, branchPrefix) {
			return "", false, nil
		}
		branch := strings.TrimPrefix(payload.Ref, branchPrefix)
		if branch == "" {
			return "", false, fmt.Errorf("push payload has an empty branch ref")
		}
		return "branch:" + repo + ":" + branch, true, nil
	default:
		return "", false, fmt.Errorf("unsupported dispatch target %q", target)
	}
}

func jobKind(target Target) (string, error) {
	switch target {
	case TargetPullRequest:
		return queue.KindRefreshPR, nil
	case TargetStack:
		return queue.KindRefreshStack, nil
	case TargetChecks:
		return queue.KindRefreshChecks, nil
	case TargetBranch:
		return queue.KindRefreshBranch, nil
	case TargetResolveStackMembership:
		return queue.KindResolveStackMembership, nil
	default:
		return "", fmt.Errorf("unsupported dispatch target %q", target)
	}
}

type stackPointer struct {
	Number int
}

func payloadStack(payload *payloadEnvelope) *stackPointer {
	if payload.PullRequest.Stack != nil {
		return &stackPointer{Number: payload.PullRequest.Stack.Number}
	}
	if payload.Stack != nil {
		return &stackPointer{Number: payload.Stack.Number}
	}
	return nil
}

func completeStackSummaryHint(
	payload *payloadEnvelope,
	intents []Intent,
) *stackSummaryHint {
	stack := payload.PullRequest.Stack
	prNumber := payload.Number
	if prNumber == 0 {
		prNumber = payload.PullRequest.Number
	}
	if stack == nil ||
		payload.Repository.FullName == "" ||
		prNumber <= 0 ||
		stack.ID <= 0 ||
		stack.Number <= 0 ||
		stack.Size <= 0 ||
		stack.Position <= 0 ||
		stack.Position > stack.Size ||
		stack.Base.Ref == "" ||
		stack.Base.SHA == "" {
		return nil
	}
	stackKey := "stack:" + payload.Repository.FullName + ":" +
		strconv.Itoa(stack.Number)
	prKey := "pr:" + payload.Repository.FullName + ":" +
		strconv.Itoa(prNumber)
	var refreshesStack, resolvesMembership bool
	for _, intent := range intents {
		if intent.Kind == queue.KindRefreshStack && intent.Key == stackKey {
			refreshesStack = true
		}
		if intent.Kind == queue.KindResolveStackMembership &&
			intent.Key == prKey {
			resolvesMembership = true
		}
	}
	if !refreshesStack || !resolvesMembership {
		return nil
	}
	return &stackSummaryHint{
		Repo:     payload.Repository.FullName,
		PRNumber: prNumber,
		ID:       stack.ID,
		Number:   stack.Number,
		Size:     stack.Size,
		Position: stack.Position,
		BaseRef:  stack.Base.Ref,
		BaseSHA:  stack.Base.SHA,
	}
}
