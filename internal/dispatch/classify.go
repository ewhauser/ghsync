// Package dispatch classifies durable webhook hints and coalesces them into
// River refresh jobs. It never treats webhook payload data as cache truth.
package dispatch

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/acme/frontier/internal/queue"
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
	Event         string
	Action        string
	Target        Target
	StackedTarget Target
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

type payloadEnvelope struct {
	Action     string `json:"action"`
	Number     int    `json:"number"`
	Ref        string `json:"ref"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	PullRequest struct {
		Number int `json:"number"`
		Stack  *struct {
			Number int `json:"number"`
		} `json:"stack"`
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

// Classify parses only events that have a configured rule. Unknown events are
// successful no-ops even when their bodies are malformed (C-I5).
func (c Classifier) Classify(event string, body []byte) ([]Intent, error) {
	eventRules := make([]Rule, 0, 1)
	for _, rule := range c.rules {
		if rule.Event == event {
			eventRules = append(eventRules, rule)
		}
	}
	if len(eventRules) == 0 {
		return nil, nil
	}

	var payload payloadEnvelope
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode %s payload: %w", event, err)
	}

	seen := make(map[string]struct{}, len(eventRules))
	intents := make([]Intent, 0, len(eventRules))
	for _, rule := range eventRules {
		if rule.Action != ActionAny && rule.Action != payload.Action {
			continue
		}
		target := rule.Target
		if rule.StackedTarget != "" && payloadStack(payload) != nil {
			target = rule.StackedTarget
		}
		key, emit, err := intentKey(target, event, payload)
		if err != nil {
			return nil, err
		}
		if !emit {
			continue
		}
		kind, err := jobKind(target)
		if err != nil {
			return nil, err
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
	return intents, nil
}

func intentKey(
	target Target,
	event string,
	payload payloadEnvelope,
) (string, bool, error) {
	repo := payload.Repository.FullName
	if repo == "" {
		return "", false, fmt.Errorf("%s payload is missing repository.full_name", event)
	}
	switch target {
	case TargetPullRequest:
		fallthrough
	case TargetResolveStackMembership:
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

func payloadStack(payload payloadEnvelope) *stackPointer {
	if payload.PullRequest.Stack != nil {
		return &stackPointer{Number: payload.PullRequest.Stack.Number}
	}
	if payload.Stack != nil {
		return &stackPointer{Number: payload.Stack.Number}
	}
	return nil
}
