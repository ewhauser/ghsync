//nolint:gocritic // Recording values are immutable snapshots; copies isolate validation and compilation.
package replay

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"time"
)

const RecordingVersion = 2

type Header struct {
	Type             string     `json:"type"`
	Version          int        `json:"version"`
	Repository       Repository `json:"repository"`
	Since            time.Time  `json:"since"`
	Until            time.Time  `json:"until"`
	StartedAt        time.Time  `json:"started_at"`
	SynthesizeStacks float64    `json:"synthesize_stacks_percent"`
	Seed             int64      `json:"seed"`
}

type Recording struct {
	Header Header
	Events []Event
}

type Repository struct {
	ID               int64     `json:"id"`
	NodeID           string    `json:"node_id"`
	Owner            string    `json:"owner"`
	Name             string    `json:"name"`
	DefaultBranch    string    `json:"default_branch"`
	DefaultBranchSHA string    `json:"default_branch_sha"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (r Repository) FullName() string {
	return r.Owner + "/" + r.Name
}

type Branch struct {
	Ref        string `json:"ref"`
	SHA        string `json:"sha"`
	Repository string `json:"repository,omitempty"`
}

type PullRequest struct {
	ID             int64      `json:"id"`
	NodeID         string     `json:"node_id"`
	Number         int        `json:"number"`
	Title          string     `json:"title"`
	State          string     `json:"state"`
	Draft          bool       `json:"draft"`
	Merged         bool       `json:"merged"`
	AuthorLogin    string     `json:"author_login"`
	ReviewDecision string     `json:"review_decision,omitempty"`
	MergeableState string     `json:"mergeable_state,omitempty"`
	Head           Branch     `json:"head"`
	Base           Branch     `json:"base"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ClosedAt       *time.Time `json:"closed_at,omitempty"`
	MergedAt       *time.Time `json:"merged_at,omitempty"`
}

type Review struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	State       string    `json:"state"`
	Body        string    `json:"body,omitempty"`
	AuthorLogin string    `json:"author_login"`
	CommitSHA   string    `json:"commit_sha,omitempty"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type ReviewThread struct {
	ID         string          `json:"id"`
	IsResolved bool            `json:"is_resolved"`
	IsOutdated bool            `json:"is_outdated"`
	Path       string          `json:"path"`
	Line       *int            `json:"line,omitempty"`
	Comments   []ReviewComment `json:"comments"`
}

type ReviewComment struct {
	ID          int64     `json:"id"`
	NodeID      string    `json:"node_id"`
	ReviewID    int64     `json:"review_id,omitempty"`
	Body        string    `json:"body,omitempty"`
	Path        string    `json:"path"`
	Line        *int      `json:"line,omitempty"`
	AuthorLogin string    `json:"author_login"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CheckSuite struct {
	ID         int64     `json:"id"`
	NodeID     string    `json:"node_id"`
	HeadSHA    string    `json:"head_sha"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion,omitempty"`
	AppSlug    string    `json:"app_slug,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CheckRun struct {
	ID          int64      `json:"id"`
	NodeID      string     `json:"node_id"`
	HeadSHA     string     `json:"head_sha"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion,omitempty"`
	DetailsURL  string     `json:"details_url"`
	AppSlug     string     `json:"app_slug,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type Commit struct {
	SHA               string    `json:"sha"`
	ParentSHA         string    `json:"parent_sha,omitempty"`
	Ref               string    `json:"ref"`
	CommittedAt       time.Time `json:"committed_at"`
	PullRequestNumber int       `json:"pull_request_number,omitempty"`
	DefaultBranch     bool      `json:"default_branch,omitempty"`
}

type Push struct {
	Ref           string    `json:"ref"`
	Before        string    `json:"before"`
	After         string    `json:"after"`
	Forced        bool      `json:"forced,omitempty"`
	DefaultBranch bool      `json:"default_branch,omitempty"`
	PushedAt      time.Time `json:"pushed_at"`
}

type Stack struct {
	ID                int64         `json:"id"`
	Number            int           `json:"number"`
	Base              Branch        `json:"base"`
	PullRequests      []int         `json:"pull_requests"`
	PullRequestStates []PullRequest `json:"pull_request_states"`
	Synthetic         bool          `json:"synthetic,omitempty"`
}

type Event struct {
	Seq          int64          `json:"seq"`
	AtMS         int64          `json:"at_ms"`
	Kind         string         `json:"kind"`
	Action       string         `json:"action,omitempty"`
	Repository   *Repository    `json:"repository,omitempty"`
	PullRequest  *PullRequest   `json:"pull_request,omitempty"`
	Review       *Review        `json:"review,omitempty"`
	Thread       *ReviewThread  `json:"review_thread,omitempty"`
	Comment      *ReviewComment `json:"review_comment,omitempty"`
	CheckSuite   *CheckSuite    `json:"check_suite,omitempty"`
	CheckRun     *CheckRun      `json:"check_run,omitempty"`
	Commit       *Commit        `json:"commit,omitempty"`
	Push         *Push          `json:"push,omitempty"`
	Stack        *Stack         `json:"stack,omitempty"`
	PreviousBase *Branch        `json:"previous_base,omitempty"`
}

func Read(reader io.Reader) (Recording, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return Recording{}, fmt.Errorf("read recording header: %w", err)
		}
		return Recording{}, fmt.Errorf("recording is empty")
	}
	var header Header
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return Recording{}, fmt.Errorf("decode recording header: %w", err)
	}
	if header.Type != "recording" || header.Version != RecordingVersion {
		return Recording{}, fmt.Errorf(
			"unsupported recording header type=%q version=%d",
			header.Type,
			header.Version,
		)
	}
	if err := validateHeader(header); err != nil {
		return Recording{}, fmt.Errorf("invalid recording header: %w", err)
	}
	recording := Recording{Header: header}
	var previousSeq int64
	var previousAt int64
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return Recording{}, fmt.Errorf(
				"decode recording event after seq %d: %w",
				previousSeq,
				err,
			)
		}
		if event.Seq != previousSeq+1 {
			return Recording{}, fmt.Errorf(
				"recording seq %d follows %d",
				event.Seq,
				previousSeq,
			)
		}
		if event.AtMS < previousAt {
			return Recording{}, fmt.Errorf(
				"recording seq %d moves backward from %dms to %dms",
				event.Seq,
				previousAt,
				event.AtMS,
			)
		}
		if previousSeq == 0 && event.AtMS != 0 {
			return Recording{}, fmt.Errorf(
				"first recording event starts at %dms, want 0ms",
				event.AtMS,
			)
		}
		if err := validateEvent(event); err != nil {
			return Recording{}, fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		if err := validateEventHeader(event, header); err != nil {
			return Recording{}, fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		recording.Events = append(recording.Events, event)
		previousSeq = event.Seq
		previousAt = event.AtMS
	}
	if err := scanner.Err(); err != nil {
		return Recording{}, fmt.Errorf("read recording events: %w", err)
	}
	return recording, nil
}

func Write(writer io.Writer, recording Recording) error {
	if recording.Header.Type == "" {
		recording.Header.Type = "recording"
	}
	if recording.Header.Version == 0 {
		recording.Header.Version = RecordingVersion
	}
	if err := validateHeader(recording.Header); err != nil {
		return fmt.Errorf("invalid recording header: %w", err)
	}
	buffered := bufio.NewWriter(writer)
	encoder := json.NewEncoder(buffered)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(recording.Header); err != nil {
		return fmt.Errorf("encode recording header: %w", err)
	}
	var previousAt int64
	for index, event := range recording.Events {
		if event.Seq != int64(index+1) {
			return fmt.Errorf(
				"recording event index %d has seq %d",
				index,
				event.Seq,
			)
		}
		if err := validateEvent(event); err != nil {
			return fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		if err := validateEventHeader(event, recording.Header); err != nil {
			return fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		if index == 0 && event.AtMS != 0 {
			return fmt.Errorf(
				"first recording event starts at %dms, want 0ms",
				event.AtMS,
			)
		}
		if index > 0 && event.AtMS < previousAt {
			return fmt.Errorf(
				"recording seq %d moves backward from %dms to %dms",
				event.Seq,
				previousAt,
				event.AtMS,
			)
		}
		if err := encoder.Encode(event); err != nil {
			return fmt.Errorf("encode recording seq %d: %w", event.Seq, err)
		}
		previousAt = event.AtMS
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("flush recording: %w", err)
	}
	return nil
}

func NormalizeEvents(events []TimedEvent) ([]Event, time.Time) {
	sort.SliceStable(events, func(i, j int) bool {
		if !events[i].At.Equal(events[j].At) {
			return events[i].At.Before(events[j].At)
		}
		leftOrder := eventOrder(events[i].Event)
		rightOrder := eventOrder(events[j].Event)
		if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		if events[i].Event.Kind != events[j].Event.Kind {
			return events[i].Event.Kind < events[j].Event.Kind
		}
		if events[i].Event.Action != events[j].Event.Action {
			return events[i].Event.Action < events[j].Event.Action
		}
		if pullNumber(events[i].Event) != pullNumber(events[j].Event) {
			return pullNumber(events[i].Event) < pullNumber(events[j].Event)
		}
		return events[i].StableKey < events[j].StableKey
	})
	if len(events) == 0 {
		return nil, time.Time{}
	}
	startedAt := events[0].At.UTC()
	normalized := make([]Event, 0, len(events))
	for index := range events {
		event := events[index].Event
		event.Seq = int64(index + 1)
		event.AtMS = events[index].At.Sub(startedAt).Milliseconds()
		normalized = append(normalized, event)
	}
	return normalized, startedAt
}

type TimedEvent struct {
	At        time.Time
	StableKey string
	Event     Event
}

func validateEvent(event Event) error {
	if event.Seq <= 0 {
		return fmt.Errorf("seq must be positive")
	}
	if event.AtMS < 0 {
		return fmt.Errorf("at_ms must not be negative")
	}
	switch event.Kind {
	case "repository":
		if event.Repository == nil ||
			event.Repository.ID <= 0 ||
			event.Repository.NodeID == "" {
			return fmt.Errorf("repository identity is incomplete")
		}
	case "pull_request":
		if err := validatePullRequest(event.PullRequest); err != nil {
			return err
		}
		if !oneOf(event.Action, "opened", "synchronize", "edited", "closed", "reopened") {
			return fmt.Errorf("unsupported pull_request action %q", event.Action)
		}
	case "pull_request_review":
		if err := validatePullRequest(event.PullRequest); err != nil {
			return err
		}
		if event.Review == nil || event.Review.ID <= 0 ||
			event.Review.NodeID == "" ||
			event.Review.SubmittedAt.IsZero() {
			return fmt.Errorf("review identity or timestamp is incomplete")
		}
		if !oneOf(event.Action, "submitted", "dismissed") {
			return fmt.Errorf(
				"unsupported pull_request_review action %q",
				event.Action,
			)
		}
	case "review_thread":
		if err := validatePullRequest(event.PullRequest); err != nil {
			return err
		}
		if event.Thread == nil || event.Thread.ID == "" {
			return fmt.Errorf("review thread identity is incomplete")
		}
		if !oneOf(event.Action, "resolved", "unresolved") {
			return fmt.Errorf("unsupported review_thread action %q", event.Action)
		}
	case "review_comment":
		if err := validatePullRequest(event.PullRequest); err != nil {
			return err
		}
		if err := validateReviewComment(event.Comment); err != nil {
			return err
		}
		if !oneOf(event.Action, "created", "edited") {
			return fmt.Errorf("unsupported review_comment action %q", event.Action)
		}
	case "check_suite":
		if event.CheckSuite == nil || event.CheckSuite.ID <= 0 ||
			event.CheckSuite.NodeID == "" ||
			event.CheckSuite.HeadSHA == "" ||
			event.CheckSuite.CreatedAt.IsZero() ||
			event.CheckSuite.UpdatedAt.IsZero() {
			return fmt.Errorf("check suite identity or timestamp is incomplete")
		}
		if !oneOf(event.Action, "requested", "completed") {
			return fmt.Errorf("unsupported check_suite action %q", event.Action)
		}
	case "check_run":
		if event.CheckRun == nil || event.CheckRun.ID <= 0 ||
			event.CheckRun.NodeID == "" ||
			event.CheckRun.HeadSHA == "" ||
			event.CheckRun.Name == "" {
			return fmt.Errorf("check run identity is incomplete")
		}
		if !oneOf(event.Action, "created", "completed") {
			return fmt.Errorf("unsupported check_run action %q", event.Action)
		}
	case "commit":
		if event.Commit == nil || event.Commit.SHA == "" ||
			event.Commit.Ref == "" ||
			event.Commit.CommittedAt.IsZero() {
			return fmt.Errorf("commit identity or timestamp is incomplete")
		}
	case "push":
		if event.Push == nil || event.Push.Ref == "" ||
			event.Push.Before == "" ||
			event.Push.After == "" ||
			event.Push.PushedAt.IsZero() {
			return fmt.Errorf("push identity or timestamp is incomplete")
		}
	case "stack":
		if event.Stack == nil || event.Stack.ID <= 0 ||
			event.Stack.Number <= 0 ||
			len(event.Stack.PullRequests) < 2 {
			return fmt.Errorf("stack identity or members are incomplete")
		}
		if len(event.Stack.PullRequestStates) !=
			len(event.Stack.PullRequests) {
			return fmt.Errorf(
				"stack has %d members but %d member states",
				len(event.Stack.PullRequests),
				len(event.Stack.PullRequestStates),
			)
		}
		seen := make(map[int]struct{}, len(event.Stack.PullRequests))
		for index, pull := range event.Stack.PullRequestStates {
			if err := validatePullRequest(&pull); err != nil {
				return fmt.Errorf(
					"stack member %d: %w",
					index,
					err,
				)
			}
			if pull.Number != event.Stack.PullRequests[index] {
				return fmt.Errorf(
					"stack member %d number %d does not match member list %d",
					index,
					pull.Number,
					event.Stack.PullRequests[index],
				)
			}
			if _, duplicate := seen[pull.Number]; duplicate {
				return fmt.Errorf(
					"stack repeats pull request %d",
					pull.Number,
				)
			}
			seen[pull.Number] = struct{}{}
		}
	default:
		return fmt.Errorf("unknown kind %q", event.Kind)
	}
	return nil
}

func validatePullRequest(pull *PullRequest) error {
	if pull == nil || pull.ID <= 0 || pull.NodeID == "" ||
		pull.Number <= 0 || pull.Head.Ref == "" ||
		pull.Head.SHA == "" || pull.Base.Ref == "" ||
		pull.CreatedAt.IsZero() || pull.UpdatedAt.IsZero() {
		return fmt.Errorf("pull request identity or timestamp is incomplete")
	}
	return nil
}

func validateReviewComment(comment *ReviewComment) error {
	if comment == nil || comment.ID <= 0 || comment.NodeID == "" ||
		comment.Path == "" || comment.CreatedAt.IsZero() ||
		comment.UpdatedAt.IsZero() {
		return fmt.Errorf("review comment identity or timestamp is incomplete")
	}
	return nil
}

func oneOf(value string, allowed ...string) bool {
	return slices.Contains(allowed, value)
}

func validateHeader(header Header) error {
	if header.Type != "recording" || header.Version != RecordingVersion {
		return fmt.Errorf(
			"unsupported type=%q version=%d",
			header.Type,
			header.Version,
		)
	}
	if header.Repository.ID <= 0 ||
		header.Repository.NodeID == "" ||
		header.Repository.Owner == "" ||
		header.Repository.Name == "" ||
		header.Repository.DefaultBranch == "" {
		return fmt.Errorf("repository identity is incomplete")
	}
	if header.Since.IsZero() || header.Until.IsZero() ||
		!header.Until.After(header.Since) {
		return fmt.Errorf("recording window is invalid")
	}
	if header.StartedAt.IsZero() ||
		header.StartedAt.Before(header.Since) ||
		!header.StartedAt.Before(header.Until) {
		return fmt.Errorf("started_at is outside the recording window")
	}
	if header.SynthesizeStacks < 0 || header.SynthesizeStacks > 100 ||
		math.IsNaN(header.SynthesizeStacks) ||
		math.IsInf(header.SynthesizeStacks, 0) {
		return fmt.Errorf("synthesize_stacks_percent is outside [0,100]")
	}
	return nil
}

func validateEventHeader(event Event, header Header) error {
	if event.Repository != nil && *event.Repository != header.Repository {
		return fmt.Errorf("repository event disagrees with recording header")
	}
	return nil
}

func eventOrder(event Event) int {
	switch event.Kind {
	case "repository":
		return 0
	case "commit":
		return 10
	case "push":
		return 20
	case "pull_request":
		return 30
	case "pull_request_review":
		return 40
	case "review_comment":
		return 50
	case "review_thread":
		return 60
	case "check_suite":
		if event.Action == "completed" {
			return 100
		}
		return 70
	case "check_run":
		if event.Action == "completed" {
			return 90
		}
		return 80
	case "stack":
		return 110
	default:
		return 100
	}
}

func pullNumber(event Event) int {
	if event.PullRequest == nil {
		return 0
	}
	return event.PullRequest.Number
}
