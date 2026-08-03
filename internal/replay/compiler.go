package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ewhauser/ghsync/internal/conformance"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
)

const DefaultWebhookSecret = "ghsync-replay-secret"

type CompileOptions struct {
	Speed         float64
	Copies        int
	Loop          bool
	WebhookSecret []byte
}

type FixtureMutation struct {
	Kind          string         `json:"kind"`
	Action        string         `json:"action,omitempty"`
	Repository    Repository     `json:"repository"`
	PullRequest   *PullRequest   `json:"pull_request,omitempty"`
	Review        *Review        `json:"review,omitempty"`
	IssueComment  *IssueComment  `json:"issue_comment,omitempty"`
	ReviewThread  *ReviewThread  `json:"review_thread,omitempty"`
	ReviewComment *ReviewComment `json:"review_comment,omitempty"`
	CheckSuite    *CheckSuite    `json:"check_suite,omitempty"`
	CheckRun      *CheckRun      `json:"check_run,omitempty"`
	Commit        *Commit        `json:"commit,omitempty"`
	Push          *Push          `json:"push,omitempty"`
	Stack         *Stack         `json:"stack,omitempty"`
}

type Delivery struct {
	GUID      string          `json:"guid"`
	Event     string          `json:"event"`
	Action    string          `json:"action,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Signature string          `json:"signature"`
}

type Step struct {
	Seq        uint64          `json:"seq"`
	SourceSeq  int64           `json:"source_seq"`
	AtMS       int64           `json:"at_ms"`
	Copy       int             `json:"copy"`
	Lap        uint64          `json:"lap"`
	Mutation   FixtureMutation `json:"mutation"`
	Deliveries []Delivery      `json:"deliveries"`
}

type Program struct {
	recording Recording
	options   CompileOptions
	nextLap   uint64
	idStride  int64
	numStride int
	lapSpanMS int64
	exhausted bool
	validator *conformance.WebhookSchemaValidator
}

func Compile(recording Recording, options CompileOptions) (*Program, error) {
	if options.Speed == 0 {
		options.Speed = 1
	}
	if options.Speed <= 0 || math.IsNaN(options.Speed) ||
		math.IsInf(options.Speed, 0) {
		return nil, fmt.Errorf("replay speed must be a finite positive number")
	}
	if options.Copies == 0 {
		options.Copies = 1
	}
	if options.Copies < 1 {
		return nil, fmt.Errorf("replay copies must be positive")
	}
	if len(options.WebhookSecret) == 0 {
		options.WebhookSecret = []byte(DefaultWebhookSecret)
	} else {
		options.WebhookSecret = append([]byte(nil), options.WebhookSecret...)
	}
	if len(recording.Events) == 0 {
		return nil, fmt.Errorf("recording has no events")
	}
	if err := validateHeader(recording.Header); err != nil {
		return nil, fmt.Errorf("invalid recording header: %w", err)
	}
	var previousAt int64
	for index, event := range recording.Events {
		if event.Seq != int64(index+1) {
			return nil, fmt.Errorf(
				"recording event index %d has seq %d",
				index,
				event.Seq,
			)
		}
		if (index == 0 && event.AtMS != 0) ||
			(index > 0 && event.AtMS < previousAt) {
			return nil, fmt.Errorf(
				"recording seq %d has unordered at_ms %d",
				event.Seq,
				event.AtMS,
			)
		}
		if err := validateEvent(event); err != nil {
			return nil, fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		if err := validateEventHeader(event, recording.Header); err != nil {
			return nil, fmt.Errorf("recording seq %d: %w", event.Seq, err)
		}
		previousAt = event.AtMS
	}
	var maximumID int64
	var maximumNumber int
	for _, event := range recording.Events {
		maximumID = max(maximumID, eventMaximumID(event))
		maximumNumber = max(maximumNumber, eventMaximumNumber(event))
	}
	maximumID = max(maximumID, recording.Header.Repository.ID)
	idStride, err := positiveStride(maximumID)
	if err != nil {
		return nil, err
	}
	numberStride := maximumNumber + 1
	if numberStride <= 0 {
		return nil, fmt.Errorf("recording entity number range overflows")
	}
	lastAt := recording.Events[len(recording.Events)-1].AtMS
	scaledLastAt := scaleAtMS(lastAt, options.Speed)
	if scaledLastAt < 0 || scaledLastAt == math.MaxInt64 {
		return nil, fmt.Errorf("scaled replay timeline overflows")
	}
	lapSpan := scaledLastAt + 1
	return &Program{
		recording: recording,
		options:   options,
		idStride:  idStride,
		numStride: numberStride,
		lapSpanMS: max(lapSpan, 1),
		validator: conformance.NewWebhookSchemaValidator(),
	}, nil
}

func (p *Program) NextLap() ([]Step, error) {
	if p.exhausted {
		return nil, io.EOF
	}
	lap := p.nextLap
	if lap == math.MaxUint64 {
		return nil, fmt.Errorf("replay lap overflows")
	}
	lapOffset, err := checkedLapOffset(lap, p.lapSpanMS)
	if err != nil {
		return nil, err
	}
	var steps []Step
	for _, event := range p.recording.Events {
		for copyIndex := 0; copyIndex < p.options.Copies; copyIndex++ {
			namespace, err := replayNamespace(lap, p.options.Copies, copyIndex)
			if err != nil {
				return nil, err
			}
			replayRepository, err := p.renumberRepository(lap, copyIndex)
			if err != nil {
				return nil, err
			}
			renumbered, err := p.renumberEvent(
				normalizeEventForReplay(event),
				namespace,
				replayRepository,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"renumber recording seq %d copy %d lap %d: %w",
					event.Seq,
					copyIndex,
					lap,
					err,
				)
			}
			deliveries, err := compileDeliveries(
				replayRepository,
				renumbered,
				namespace,
				lap,
				copyIndex,
				p.options.WebhookSecret,
				p.validator,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"compile recording seq %d copy %d lap %d: %w",
					event.Seq,
					copyIndex,
					lap,
					err,
				)
			}
			scaledAt := scaleAtMS(event.AtMS, p.options.Speed)
			if scaledAt > math.MaxInt64-lapOffset {
				return nil, fmt.Errorf("replay step time overflows")
			}
			steps = append(steps, Step{
				SourceSeq: event.Seq,
				AtMS:      scaledAt + lapOffset,
				Copy:      copyIndex,
				Lap:       lap,
				Mutation: FixtureMutation{
					Kind:          renumbered.Kind,
					Action:        renumbered.Action,
					Repository:    replayRepository,
					PullRequest:   renumbered.PullRequest,
					Review:        renumbered.Review,
					IssueComment:  renumbered.IssueComment,
					ReviewThread:  renumbered.Thread,
					ReviewComment: renumbered.Comment,
					CheckSuite:    renumbered.CheckSuite,
					CheckRun:      renumbered.CheckRun,
					Commit:        renumbered.Commit,
					Push:          renumbered.Push,
					Stack:         renumbered.Stack,
				},
				Deliveries: deliveries,
			})
		}
	}
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].AtMS != steps[j].AtMS {
			return steps[i].AtMS < steps[j].AtMS
		}
		if steps[i].SourceSeq != steps[j].SourceSeq {
			return steps[i].SourceSeq < steps[j].SourceSeq
		}
		return steps[i].Copy < steps[j].Copy
	})
	if uint64(len(steps)) > 0 && lap > math.MaxUint64/uint64(len(steps)) {
		return nil, fmt.Errorf("replay step sequence overflows")
	}
	baseSeq := lap * uint64(len(steps))
	for index := range steps {
		steps[index].Seq = baseSeq + uint64(index+1)
	}
	p.nextLap++
	if !p.options.Loop {
		p.exhausted = true
	}
	return steps, nil
}

func checkedLapOffset(lap uint64, span int64) (int64, error) {
	if span <= 0 || lap > uint64(math.MaxInt64/span) {
		return 0, fmt.Errorf("replay lap time overflows")
	}
	return int64(lap) * span, nil
}

func scaleAtMS(atMS int64, speed float64) int64 {
	return int64(math.Round(float64(atMS) / speed))
}

func positiveStride(maximum int64) (int64, error) {
	if maximum < 0 || maximum == math.MaxInt64 {
		return 0, fmt.Errorf("recording entity ID range overflows")
	}
	return maximum + 1, nil
}

func replayNamespace(lap uint64, copies, copyIndex int) (uint64, error) {
	if lap > math.MaxUint64/uint64(copies) {
		return 0, fmt.Errorf("replay namespace overflows")
	}
	namespace := lap*uint64(copies) + uint64(copyIndex)
	if namespace > math.MaxInt64 {
		return 0, fmt.Errorf("replay namespace exceeds signed ID space")
	}
	return namespace, nil
}

func (p *Program) renumberRepository(
	lap uint64,
	copyIndex int,
) (Repository, error) {
	namespace, err := replayNamespace(lap, p.options.Copies, copyIndex)
	if err != nil {
		return Repository{}, err
	}
	repository := p.recording.Header.Repository
	if namespace == 0 {
		return repository, nil
	}
	offset, err := checkedOffset(p.idStride, namespace)
	if err != nil {
		return Repository{}, err
	}
	repository.ID, err = addID(repository.ID, offset)
	if err != nil {
		return Repository{}, err
	}
	repository.NodeID = renumberNodeID(repository.NodeID, namespace)
	repository.Name = renumberRepositoryName(repository.Name, namespace)
	repository.DefaultBranch = renumberBranchRef(
		repository.DefaultBranch,
		namespace,
	)
	repository.DefaultBranchSHA = renumberSHA(
		repository.DefaultBranchSHA,
		namespace,
	)
	return repository, nil
}

func (p *Program) renumberEvent(
	event Event,
	namespace uint64,
	repository Repository,
) (Event, error) {
	if namespace == 0 {
		return cloneEvent(event)
	}
	result, err := cloneEvent(event)
	if err != nil {
		return Event{}, err
	}
	offsetID, err := checkedOffset(p.idStride, namespace)
	if err != nil {
		return Event{}, err
	}
	offsetNumber64, err := checkedOffset(int64(p.numStride), namespace)
	if err != nil || offsetNumber64 > math.MaxInt {
		return Event{}, fmt.Errorf("entity number offset overflows")
	}
	offsetNumber := int(offsetNumber64)
	originalRepository := p.recording.Header.Repository
	if result.Repository != nil {
		repositorySnapshot := repository
		result.Repository = &repositorySnapshot
	}
	if result.PullRequest != nil {
		if err := renumberPullRequest(
			result.PullRequest,
			offsetID,
			offsetNumber,
			namespace,
			originalRepository,
			repository,
		); err != nil {
			return Event{}, err
		}
	}
	if result.PreviousBase != nil {
		branch := renumberBranch(
			*result.PreviousBase,
			namespace,
			originalRepository,
			repository,
		)
		result.PreviousBase = &branch
	}
	if result.Review != nil {
		result.Review.ID, err = addID(result.Review.ID, offsetID)
		if err != nil {
			return Event{}, err
		}
		result.Review.NodeID = renumberNodeID(result.Review.NodeID, namespace)
		result.Review.CommitSHA = renumberSHA(result.Review.CommitSHA, namespace)
	}
	if result.IssueComment != nil {
		result.IssueComment.ID, err = addID(result.IssueComment.ID, offsetID)
		if err != nil {
			return Event{}, err
		}
		result.IssueComment.NodeID = renumberNodeID(
			result.IssueComment.NodeID,
			namespace,
		)
		result.IssueComment.AuthorNodeID = renumberNodeID(
			result.IssueComment.AuthorNodeID,
			namespace,
		)
	}
	if result.Thread != nil {
		result.Thread.ID = renumberNodeID(result.Thread.ID, namespace)
		for index := range result.Thread.Comments {
			if err := renumberComment(
				&result.Thread.Comments[index],
				offsetID,
				namespace,
			); err != nil {
				return Event{}, err
			}
		}
	}
	if result.Comment != nil {
		if err := renumberComment(result.Comment, offsetID, namespace); err != nil {
			return Event{}, err
		}
	}
	if result.CheckSuite != nil {
		result.CheckSuite.ID, err = addID(result.CheckSuite.ID, offsetID)
		if err != nil {
			return Event{}, err
		}
		result.CheckSuite.NodeID = renumberNodeID(
			result.CheckSuite.NodeID,
			namespace,
		)
		result.CheckSuite.HeadSHA = renumberSHA(
			result.CheckSuite.HeadSHA,
			namespace,
		)
	}
	if result.CheckRun != nil {
		result.CheckRun.ID, err = addID(result.CheckRun.ID, offsetID)
		if err != nil {
			return Event{}, err
		}
		result.CheckRun.NodeID = renumberNodeID(
			result.CheckRun.NodeID,
			namespace,
		)
		result.CheckRun.HeadSHA = renumberSHA(
			result.CheckRun.HeadSHA,
			namespace,
		)
	}
	if result.Commit != nil {
		result.Commit.SHA = renumberSHA(result.Commit.SHA, namespace)
		result.Commit.ParentSHA = renumberSHA(
			result.Commit.ParentSHA,
			namespace,
		)
		result.Commit.Ref = renumberRef(result.Commit.Ref, namespace)
		if result.Commit.PullRequestNumber > 0 {
			result.Commit.PullRequestNumber, err = addNumber(
				result.Commit.PullRequestNumber,
				offsetNumber,
			)
			if err != nil {
				return Event{}, err
			}
		}
	}
	if result.Push != nil {
		result.Push.Ref = renumberRef(result.Push.Ref, namespace)
		result.Push.Before = renumberSHA(result.Push.Before, namespace)
		result.Push.After = renumberSHA(result.Push.After, namespace)
	}
	if result.Stack != nil {
		result.Stack.ID, err = addID(result.Stack.ID, offsetID)
		if err != nil {
			return Event{}, err
		}
		result.Stack.Number, err = addNumber(
			result.Stack.Number,
			offsetNumber,
		)
		if err != nil {
			return Event{}, err
		}
		result.Stack.Base = renumberBranch(
			result.Stack.Base,
			namespace,
			originalRepository,
			repository,
		)
		for index := range result.Stack.PullRequests {
			result.Stack.PullRequests[index], err = addNumber(
				result.Stack.PullRequests[index],
				offsetNumber,
			)
			if err != nil {
				return Event{}, err
			}
		}
		for index := range result.Stack.PullRequestStates {
			if err := renumberPullRequest(
				&result.Stack.PullRequestStates[index],
				offsetID,
				offsetNumber,
				namespace,
				originalRepository,
				repository,
			); err != nil {
				return Event{}, err
			}
		}
	}
	return result, nil
}

func renumberPullRequest(
	pull *PullRequest,
	offsetID int64,
	offsetNumber int,
	namespace uint64,
	originalRepository Repository,
	repository Repository,
) error {
	var err error
	pull.ID, err = addID(pull.ID, offsetID)
	if err != nil {
		return err
	}
	pull.Number, err = addNumber(pull.Number, offsetNumber)
	if err != nil {
		return err
	}
	pull.NodeID = renumberNodeID(pull.NodeID, namespace)
	pull.Head = renumberBranch(
		pull.Head,
		namespace,
		originalRepository,
		repository,
	)
	pull.Base = renumberBranch(
		pull.Base,
		namespace,
		originalRepository,
		repository,
	)
	return nil
}

func compileDeliveries(
	repository Repository,
	event Event,
	namespace uint64,
	lap uint64,
	copyIndex int,
	secret []byte,
	validator *conformance.WebhookSchemaValidator,
) ([]Delivery, error) {
	payload, webhookEvent, err := buildPayload(repository, event)
	if err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s payload: %w", webhookEvent, err)
	}
	if err := validator.Validate(webhookEvent, body); err != nil {
		return nil, fmt.Errorf(
			"compiled %s/%s payload does not match vendored schema: %w",
			webhookEvent,
			event.Action,
			err,
		)
	}
	guid := deliveryGUID(
		repository.FullName(),
		event.Seq,
		namespace,
		lap,
		copyIndex,
		0,
	)
	return []Delivery{{
		GUID:      guid,
		Event:     webhookEvent,
		Action:    event.Action,
		Payload:   body,
		Signature: gh.SignBody(secret, body),
	}}, nil
}

func buildPayload(
	repository Repository,
	event Event,
) (map[string]any, string, error) {
	fixture := fixtureForEvent(repository, event)
	fake := fakegithub.New(fixture, DefaultWebhookSecret)
	var (
		payload map[string]any
		err     error
	)
	switch event.Kind {
	case "repository", "commit", "stack":
		return nil, "", nil
	case "pull_request":
		action := event.Action
		if action == "edited" {
			payload, err = fake.PullRequestWebhookPayload(
				"synchronize",
				event.PullRequest.Number,
			)
			if err == nil {
				payload["action"] = "edited"
				delete(payload, "before")
				delete(payload, "after")
				previous := event.PullRequest.Base
				if event.PreviousBase != nil {
					previous = *event.PreviousBase
				}
				payload["changes"] = map[string]any{
					"base": map[string]any{
						"ref": map[string]any{"from": previous.Ref},
						"sha": map[string]any{"from": previous.SHA},
					},
				}
			}
		} else {
			payload, err = fake.PullRequestWebhookPayload(
				action,
				event.PullRequest.Number,
			)
		}
		if err == nil {
			overlayPullRequestState(
				payload,
				*event.PullRequest,
				repository,
			)
		}
		return payload, "pull_request", err
	case "pull_request_review":
		payload, err = fake.PullRequestReviewWebhookPayload(
			event.Action,
			event.PullRequest.Number,
		)
		if err == nil {
			overlayReview(payload, *event.Review)
		}
		return payload, "pull_request_review", err
	case "review_comment":
		payload, err = fake.PullRequestReviewCommentWebhookPayload(
			event.Action,
			event.PullRequest.Number,
		)
		if err == nil {
			overlayReviewComment(payload, *event.Comment)
		}
		return payload, "pull_request_review_comment", err
	case "issue_comment":
		switch event.Action {
		case "created":
			payload, err = fake.IssueCommentCreatedWebhookPayload(
				event.PullRequest.Number,
				event.IssueComment.ID,
			)
		case "edited":
			payload, err = fake.IssueCommentEditedWebhookPayload(
				event.PullRequest.Number,
				event.IssueComment.ID,
			)
		case "deleted":
			payload, err = fake.IssueCommentDeletedWebhookPayload(
				event.PullRequest.Number,
				event.IssueComment.ID,
			)
		default:
			return nil, "", fmt.Errorf(
				"unsupported issue_comment action %q",
				event.Action,
			)
		}
		if err == nil {
			overlayIssueComment(payload, *event.IssueComment)
		}
		return payload, "issue_comment", err
	case "review_thread":
		payload, err = fake.PullRequestReviewThreadWebhookPayload(
			event.Action,
			event.PullRequest.Number,
		)
		if err == nil {
			overlayReviewThread(payload, *event.Thread)
		}
		return payload, "pull_request_review_thread", err
	case "check_suite":
		payload, err = fake.CheckSuiteWebhookPayload(
			event.Action,
			event.CheckSuite.HeadSHA,
		)
		if err == nil {
			overlayCheckSuite(payload, *event.CheckSuite)
		}
		return payload, "check_suite", err
	case "check_run":
		payload, err = fake.CheckRunWebhookPayload(
			event.Action,
			event.CheckRun.ID,
		)
		return payload, "check_run", err
	case "push":
		payload, err = fake.PushWebhookPayload(
			event.Push.Ref,
			event.Push.Before,
			event.Push.After,
		)
		if err == nil {
			overlayPush(payload, *event.Push, repository)
		}
		return payload, "push", err
	default:
		return nil, "", fmt.Errorf("unsupported recording kind %q", event.Kind)
	}
}

func fixtureForEvent(repository Repository, event Event) fakegithub.Fixture {
	when := repository.UpdatedAt
	if when.IsZero() {
		when = time.Unix(0, 0).UTC()
	}
	wireRepository := fakegithub.Repository{
		ID:               repository.ID,
		NodeID:           repository.NodeID,
		Owner:            repository.Owner,
		Name:             repository.Name,
		FullName:         repository.FullName(),
		DefaultBranch:    repository.DefaultBranch,
		DefaultBranchSHA: repository.DefaultBranchSHA,
		UpdatedAt:        when,
		PushedAt:         when,
	}
	fixture := fakegithub.Fixture{
		Owner:        repository.Owner,
		Repo:         repository.Name,
		Repository:   wireRepository,
		Repositories: []fakegithub.Repository{wireRepository},
	}
	if event.PullRequest != nil {
		pull := event.PullRequest
		fixture.PullRequests = append(
			fixture.PullRequests,
			fakegithub.PullRequest{
				ID:             pull.ID,
				NodeID:         pull.NodeID,
				Number:         pull.Number,
				Title:          pull.Title,
				State:          pull.State,
				Draft:          pull.Draft,
				AuthorLogin:    pull.AuthorLogin,
				ReviewDecision: pull.ReviewDecision,
				MergeableState: pull.MergeableState,
				Head: fakegithub.PullRequestBranch{
					Ref: pull.Head.Ref,
					SHA: pull.Head.SHA,
				},
				Base: fakegithub.Base{
					Ref: pull.Base.Ref,
					SHA: pull.Base.SHA,
				},
				CreatedAt: pull.CreatedAt,
				UpdatedAt: pull.UpdatedAt,
			},
		)
	}
	if event.IssueComment != nil && len(fixture.PullRequests) == 1 {
		comment := event.IssueComment
		fixture.PullRequests[0].Comments = append(
			fixture.PullRequests[0].Comments,
			fakegithub.IssueComment{
				ID:     comment.ID,
				NodeID: comment.NodeID,
				Author: fakegithub.Actor{
					Kind:   comment.AuthorKind,
					NodeID: comment.AuthorNodeID,
					Login:  comment.AuthorLogin,
				},
				Body:      comment.Body,
				CreatedAt: comment.CreatedAt,
				UpdatedAt: comment.UpdatedAt,
			},
		)
	}
	if event.CheckRun != nil {
		run := event.CheckRun
		fixture.CheckRuns = append(fixture.CheckRuns, fakegithub.CheckRun{
			ID:          run.ID,
			NodeID:      run.NodeID,
			HeadSHA:     run.HeadSHA,
			Name:        run.Name,
			Status:      run.Status,
			Conclusion:  run.Conclusion,
			DetailsURL:  run.DetailsURL,
			AppSlug:     run.AppSlug,
			StartedAt:   run.StartedAt,
			CompletedAt: run.CompletedAt,
		})
	}
	if event.CheckSuite != nil {
		suite := event.CheckSuite
		fixture.CheckRuns = append(fixture.CheckRuns, fakegithub.CheckRun{
			ID:         max(suite.ID, 1),
			NodeID:     suite.NodeID + "_suite-run",
			HeadSHA:    suite.HeadSHA,
			Name:       "suite",
			Status:     suite.Status,
			Conclusion: suite.Conclusion,
			DetailsURL: "https://github.com/" + repository.FullName() + "/actions",
			AppSlug:    suite.AppSlug,
		})
	}
	return fixture
}

func overlayPullRequestState(
	payload map[string]any,
	pull PullRequest,
	repository Repository,
) {
	wire := object(payload, "pull_request")
	if wire == nil {
		return
	}
	wire["merged"] = pull.Merged
	wire["closed_at"] = nullableRFC3339(pull.ClosedAt)
	wire["merged_at"] = nullableRFC3339(pull.MergedAt)
	if !pull.Merged {
		wire["merged_by"] = nil
	}
	overlayBranchRepository(
		object(wire, "head"),
		pull.Head.Repository,
		repository,
	)
	overlayBranchRepository(
		object(wire, "base"),
		pull.Base.Repository,
		repository,
	)
}

func overlayBranchRepository(
	branch map[string]any,
	fullName string,
	defaultRepository Repository,
) {
	if branch == nil || fullName == "" ||
		fullName == defaultRepository.FullName() {
		return
	}
	wire := object(branch, "repo")
	if wire == nil {
		return
	}
	owner, name, ok := strings.Cut(fullName, "/")
	if !ok {
		return
	}
	wire["name"] = name
	wire["full_name"] = fullName
	if ownerWire := object(wire, "owner"); ownerWire != nil {
		ownerWire["login"] = owner
	}
}

func overlayReview(payload map[string]any, review Review) {
	wire := object(payload, "review")
	if wire == nil {
		return
	}
	wire["id"] = review.ID
	wire["node_id"] = review.NodeID
	if review.Body == "" {
		wire["body"] = nil
	} else {
		wire["body"] = review.Body
	}
	wire["commit_id"] = review.CommitSHA
	wire["submitted_at"] = review.SubmittedAt.UTC().Format(time.RFC3339)
	wire["state"] = strings.ToLower(review.State)
	if user := object(wire, "user"); user != nil {
		user["login"] = review.AuthorLogin
	}
}

func overlayReviewComment(payload map[string]any, comment ReviewComment) {
	wire := object(payload, "comment")
	if wire == nil {
		return
	}
	overlayCommentObject(wire, comment)
}

func overlayIssueComment(payload map[string]any, comment IssueComment) {
	wire := object(payload, "comment")
	if wire == nil {
		return
	}
	wire["id"] = comment.ID
	wire["node_id"] = comment.NodeID
	wire["body"] = comment.Body
	wire["created_at"] = comment.CreatedAt.UTC().Format(time.RFC3339)
	wire["updated_at"] = comment.UpdatedAt.UTC().Format(time.RFC3339)
	if user := object(wire, "user"); user != nil {
		user["login"] = comment.AuthorLogin
		user["node_id"] = comment.AuthorNodeID
	}
}

func overlayReviewThread(payload map[string]any, thread ReviewThread) {
	wire := object(payload, "thread")
	if wire == nil {
		return
	}
	wire["node_id"] = thread.ID
	templateComments, _ := wire["comments"].([]any)
	if len(thread.Comments) == 0 || len(templateComments) == 0 {
		return
	}
	comments := make([]any, 0, len(thread.Comments))
	for _, comment := range thread.Comments {
		commentWire := cloneMap(templateComments[0].(map[string]any))
		overlayCommentObject(commentWire, comment)
		comments = append(comments, commentWire)
	}
	wire["comments"] = comments
}

func overlayCommentObject(wire map[string]any, comment ReviewComment) {
	wire["id"] = comment.ID
	wire["node_id"] = comment.NodeID
	wire["pull_request_review_id"] = comment.ReviewID
	wire["body"] = comment.Body
	wire["path"] = comment.Path
	wire["created_at"] = comment.CreatedAt.UTC().Format(time.RFC3339)
	wire["updated_at"] = comment.UpdatedAt.UTC().Format(time.RFC3339)
	if comment.Line == nil {
		wire["line"] = nil
	} else {
		wire["line"] = *comment.Line
	}
	if user := object(wire, "user"); user != nil {
		user["login"] = comment.AuthorLogin
	}
}

func overlayCheckSuite(payload map[string]any, suite CheckSuite) {
	wire := object(payload, "check_suite")
	if wire == nil {
		return
	}
	wire["id"] = suite.ID
	wire["node_id"] = suite.NodeID
	wire["head_sha"] = suite.HeadSHA
	wire["status"] = suite.Status
	conclusion := suite.Conclusion
	// GitHub GraphQL can report SKIPPED for a suite, while the GitHub-owned
	// webhook schema permits SKIPPED only for check runs. The corresponding
	// suite delivery uses NEUTRAL and retains SKIPPED in fixture truth.
	if conclusion == "skipped" {
		conclusion = "neutral"
	}
	if conclusion == "" {
		wire["conclusion"] = nil
	} else {
		wire["conclusion"] = conclusion
	}
	wire["created_at"] = suite.CreatedAt.UTC().Format(time.RFC3339)
	wire["updated_at"] = suite.UpdatedAt.UTC().Format(time.RFC3339)
	if app := object(wire, "app"); app != nil && suite.AppSlug != "" {
		app["slug"] = suite.AppSlug
	}
}

func overlayPush(
	payload map[string]any,
	push Push,
	repository Repository,
) {
	payload["forced"] = push.Forced
	payload["created"] = isZeroSHA(push.Before) && !isZeroSHA(push.After)
	payload["deleted"] = isZeroSHA(push.After)
	payload["compare"] = fmt.Sprintf(
		"https://github.com/compare/%s...%s",
		push.Before,
		push.After,
	)
	if isZeroSHA(push.After) {
		payload["commits"] = []any{}
		payload["head_commit"] = nil
		return
	}
	actor := map[string]any{
		"name":     "ghsync replay",
		"email":    nil,
		"username": "ghsync-replay",
	}
	commit := map[string]any{
		"id":        push.After,
		"tree_id":   push.After,
		"distinct":  true,
		"message":   "Recorded commit " + shortSHA(push.After),
		"timestamp": push.PushedAt.UTC().Format(time.RFC3339),
		"url": "https://api.github.com/repos/" +
			repository.FullName() + "/commits/" + push.After,
		"author":    cloneMap(actor),
		"committer": cloneMap(actor),
		"added":     []any{},
		"removed":   []any{},
		"modified":  []any{},
	}
	payload["commits"] = []any{commit}
	payload["head_commit"] = cloneMap(commit)
}

func isZeroSHA(value string) bool {
	return value != "" && strings.Trim(value, "0") == ""
}

func shortSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func object(parent map[string]any, key string) map[string]any {
	value, _ := parent[key].(map[string]any)
	return value
}

func cloneMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		switch value := value.(type) {
		case map[string]any:
			result[key] = cloneMap(value)
		case []any:
			items := make([]any, len(value))
			for index, item := range value {
				if itemMap, ok := item.(map[string]any); ok {
					items[index] = cloneMap(itemMap)
				} else {
					items[index] = item
				}
			}
			result[key] = items
		default:
			result[key] = value
		}
	}
	return result
}

func nullableRFC3339(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}

func deliveryGUID(
	repository string,
	sourceSeq int64,
	namespace uint64,
	lap uint64,
	copyIndex int,
	deliveryIndex int,
) string {
	sum := sha256.Sum256(fmt.Appendf(nil,
		"%s:%d:%d:%d:%d:%d",
		repository,
		sourceSeq,
		namespace,
		lap,
		copyIndex,
		deliveryIndex,
	))
	hexValue := hex.EncodeToString(sum[:16])
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hexValue[:8],
		hexValue[8:12],
		hexValue[12:16],
		hexValue[16:20],
		hexValue[20:],
	)
}

func checkedOffset(stride int64, namespace uint64) (int64, error) {
	if stride <= 0 || namespace > uint64(math.MaxInt64/stride) {
		return 0, fmt.Errorf("entity ID offset overflows")
	}
	return stride * int64(namespace), nil
}

func addID(value, offset int64) (int64, error) {
	if value < 0 || offset > math.MaxInt64-value {
		return 0, fmt.Errorf("entity ID overflows")
	}
	return value + offset, nil
}

func addNumber(value, offset int) (int, error) {
	if value < 0 || offset > math.MaxInt-value {
		return 0, fmt.Errorf("entity number overflows")
	}
	return value + offset, nil
}

func renumberComment(
	comment *ReviewComment,
	offset int64,
	namespace uint64,
) error {
	var err error
	comment.ID, err = addID(comment.ID, offset)
	if err != nil {
		return err
	}
	if comment.ReviewID > 0 {
		comment.ReviewID, err = addID(comment.ReviewID, offset)
		if err != nil {
			return err
		}
	}
	comment.NodeID = renumberNodeID(comment.NodeID, namespace)
	return nil
}

func renumberBranch(
	branch Branch,
	namespace uint64,
	originalRepository Repository,
	repository Repository,
) Branch {
	branch.Ref = renumberBranchRef(branch.Ref, namespace)
	branch.SHA = renumberSHA(branch.SHA, namespace)
	if branch.Repository == "" ||
		branch.Repository == originalRepository.FullName() {
		branch.Repository = repository.FullName()
	} else {
		branch.Repository = renumberFullName(branch.Repository, namespace)
	}
	return branch
}

func renumberBranchRef(ref string, namespace uint64) string {
	return fmt.Sprintf("replay/%d/%s", namespace, ref)
}

func renumberRef(ref string, namespace uint64) string {
	const heads = "refs/heads/"
	if after, ok := strings.CutPrefix(ref, heads); ok {
		return heads + fmt.Sprintf(
			"replay/%d/%s",
			namespace,
			after,
		)
	}
	return fmt.Sprintf("refs/replay/%d/%s", namespace, strings.TrimPrefix(ref, "refs/"))
}

func renumberSHA(value string, namespace uint64) string {
	if value == "" || isZeroSHA(value) {
		return value
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s", namespace, value))
	return hex.EncodeToString(sum[:20])
}

func renumberNodeID(value string, namespace uint64) string {
	if value == "" {
		return ""
	}
	return fmt.Sprintf("%s_replay_%d", value, namespace)
}

func renumberRepositoryName(value string, namespace uint64) string {
	return fmt.Sprintf("%s-replay-%d", value, namespace)
}

func renumberFullName(value string, namespace uint64) string {
	owner, name, ok := strings.Cut(value, "/")
	if !ok {
		return renumberRepositoryName(value, namespace)
	}
	return owner + "/" + renumberRepositoryName(name, namespace)
}

func normalizeEventForReplay(event Event) Event {
	if event.CheckSuite != nil &&
		strings.EqualFold(event.CheckSuite.Conclusion, "skipped") {
		checkSuite := *event.CheckSuite
		checkSuite.Conclusion = "neutral"
		event.CheckSuite = &checkSuite
	}
	return event
}

func cloneEvent(event Event) (Event, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return Event{}, fmt.Errorf("clone event: %w", err)
	}
	var result Event
	if err := json.Unmarshal(body, &result); err != nil {
		return Event{}, fmt.Errorf("clone event: %w", err)
	}
	return result, nil
}

func eventMaximumID(event Event) int64 {
	var maximum int64
	if event.PullRequest != nil {
		maximum = max(maximum, event.PullRequest.ID)
	}
	if event.Review != nil {
		maximum = max(maximum, event.Review.ID)
	}
	if event.IssueComment != nil {
		maximum = max(maximum, event.IssueComment.ID)
	}
	if event.Thread != nil {
		for _, comment := range event.Thread.Comments {
			maximum = max(maximum, comment.ID, comment.ReviewID)
		}
	}
	if event.Comment != nil {
		maximum = max(maximum, event.Comment.ID, event.Comment.ReviewID)
	}
	if event.CheckSuite != nil {
		maximum = max(maximum, event.CheckSuite.ID)
	}
	if event.CheckRun != nil {
		maximum = max(maximum, event.CheckRun.ID)
	}
	if event.Stack != nil {
		maximum = max(maximum, event.Stack.ID)
		for _, pull := range event.Stack.PullRequestStates {
			maximum = max(maximum, pull.ID)
		}
	}
	return maximum
}

func eventMaximumNumber(event Event) int {
	var maximum int
	if event.PullRequest != nil {
		maximum = max(maximum, event.PullRequest.Number)
	}
	if event.Stack != nil {
		maximum = max(maximum, event.Stack.Number)
		for _, number := range event.Stack.PullRequests {
			maximum = max(maximum, number)
		}
		for _, pull := range event.Stack.PullRequestStates {
			maximum = max(maximum, pull.Number)
		}
	}
	return maximum
}

func EncodeSteps(writer io.Writer, steps []Step) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for _, step := range steps {
		if err := encoder.Encode(step); err != nil {
			return fmt.Errorf("encode replay step %d: %w", step.Seq, err)
		}
	}
	return nil
}

func FirstLap(recording Recording, options CompileOptions) ([]Step, error) {
	program, err := Compile(recording, options)
	if err != nil {
		return nil, err
	}
	steps, err := program.NextLap()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return steps, err
}
