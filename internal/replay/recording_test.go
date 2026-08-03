package replay_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/conformance"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/replay"
)

func TestRecordingRoundTripIsByteIdentical(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	var first bytes.Buffer
	if err := replay.Write(&first, recording); err != nil {
		t.Fatal(err)
	}
	decoded, err := replay.Read(bytes.NewReader(first.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	if err := replay.Write(&second, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("recording bytes changed across a read/write round trip")
	}
}

func TestNormalizeEventsUsesInstantsAcrossTimeZones(t *testing.T) {
	t.Parallel()
	mountain := time.FixedZone("MDT", -6*60*60)
	start := time.Date(2026, 7, 1, 6, 0, 0, 0, mountain)
	repository := testRepository()
	events, startedAt := replay.NormalizeEvents([]replay.TimedEvent{
		{
			At: start.Add(1500 * time.Millisecond),
			Event: replay.Event{
				Kind:       "repository",
				Repository: &repository,
			},
			StableKey: "second",
		},
		{
			At: start.UTC(),
			Event: replay.Event{
				Kind:       "repository",
				Repository: &repository,
			},
			StableKey: "first",
		},
	})
	if !startedAt.Equal(start) || startedAt.Location() != time.UTC {
		t.Fatalf("started_at = %v, want UTC instant %v", startedAt, start.UTC())
	}
	if len(events) != 2 || events[0].AtMS != 0 || events[1].AtMS != 1500 {
		t.Fatalf("relative timestamps = %+v, want [0 1500]ms", events)
	}
}

func TestNormalizeEventsUsesCausalTieOrdering(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	pull := testPull(1, "feature", "main")
	comment := replay.ReviewComment{
		ID: 1, NodeID: "comment", Path: "main.go",
		CreatedAt: at, UpdatedAt: at,
	}
	thread := replay.ReviewThread{ID: "thread", Comments: []replay.ReviewComment{
		comment,
	}}
	run := replay.CheckRun{
		ID: 2, NodeID: "run", HeadSHA: pull.Head.SHA,
		Name: "unit", Status: "completed",
	}
	suite := replay.CheckSuite{
		ID: 3, NodeID: "suite", HeadSHA: pull.Head.SHA,
		Status: "completed", CreatedAt: at, UpdatedAt: at,
	}
	events, _ := replay.NormalizeEvents([]replay.TimedEvent{
		{
			At: at, StableKey: "suite",
			Event: replay.Event{
				Kind: "check_suite", Action: "completed", CheckSuite: &suite,
			},
		},
		{
			At: at, StableKey: "thread",
			Event: replay.Event{
				Kind: "review_thread", Action: "resolved",
				PullRequest: &pull, Thread: &thread,
			},
		},
		{
			At: at, StableKey: "run",
			Event: replay.Event{
				Kind: "check_run", Action: "completed", CheckRun: &run,
			},
		},
		{
			At: at, StableKey: "comment",
			Event: replay.Event{
				Kind: "review_comment", Action: "created",
				PullRequest: &pull, Comment: &comment,
			},
		},
	})
	var kinds []string
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	want := []string{
		"review_comment",
		"review_thread",
		"check_run",
		"check_suite",
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("same-time event order = %v, want %v", kinds, want)
	}
}

func TestWriteRejectsUnorderedRecording(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	recording.Events[2].AtMS = recording.Events[1].AtMS - 1
	var output bytes.Buffer
	if err := replay.Write(&output, recording); err == nil ||
		!strings.Contains(err.Error(), "moves backward") {
		t.Fatalf("Write error = %v, want backward-time error", err)
	}
}

func TestCompilerDeterminismAndSchemaValidity(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	options := replay.CompileOptions{
		Speed:         8,
		Copies:        2,
		WebhookSecret: []byte("compiler-test-secret"),
	}
	first, err := replay.FirstLap(recording, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replay.FirstLap(recording, options)
	if err != nil {
		t.Fatal(err)
	}
	var firstBytes, secondBytes bytes.Buffer
	if err := replay.EncodeSteps(&firstBytes, first); err != nil {
		t.Fatal(err)
	}
	if err := replay.EncodeSteps(&secondBytes, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes.Bytes(), secondBytes.Bytes()) {
		t.Fatal("compiler output is not byte-identical")
	}
	validator := conformance.NewWebhookSchemaValidator()
	var priorAt int64
	for _, step := range first {
		if step.AtMS < priorAt {
			t.Fatalf("step time moved backward: %d after %d", step.AtMS, priorAt)
		}
		priorAt = step.AtMS
		for _, delivery := range step.Deliveries {
			if !gh.VerifySignature(
				options.WebhookSecret,
				delivery.Payload,
				delivery.Signature,
			) {
				t.Fatalf("delivery %s signature is invalid", delivery.GUID)
			}
			if err := validator.Validate(
				delivery.Event,
				delivery.Payload,
			); err != nil {
				t.Fatalf(
					"source seq %d %s/%s: %v\n%s",
					step.SourceSeq,
					delivery.Event,
					delivery.Action,
					err,
					delivery.Payload,
				)
			}
			assertDeliveryMatchesMutation(t, step, delivery)
		}
	}
}

func TestCompilerPreservesUnknownStackBaseSHAAcrossRenumbering(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	var stackSeq int64
	for index := range recording.Events {
		event := &recording.Events[index]
		if event.Stack == nil {
			continue
		}
		stackSeq = event.Seq
		event.Stack.Base.SHA = ""
		if len(event.Stack.PullRequestStates) == 0 {
			t.Fatal("test stack has no full member snapshots")
		}
		event.Stack.PullRequestStates[0].Base.SHA = ""
		break
	}
	if stackSeq == 0 {
		t.Fatal("test recording has no stack mutation")
	}
	steps, err := replay.FirstLap(recording, replay.CompileOptions{
		Copies:        2,
		WebhookSecret: []byte("unknown-stack-base"),
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, step := range steps {
		if step.SourceSeq != stackSeq {
			continue
		}
		seen++
		if step.Mutation.Stack == nil ||
			step.Mutation.Stack.Base.SHA != "" ||
			len(step.Mutation.Stack.PullRequestStates) == 0 ||
			step.Mutation.Stack.PullRequestStates[0].Base.SHA != "" {
			t.Fatalf("renumbered unknown stack base = %+v", step.Mutation.Stack)
		}
	}
	if seen != 2 {
		t.Fatalf("renumbered stack copies = %d, want 2", seen)
	}
}

func assertDeliveryMatchesMutation(
	t *testing.T,
	step replay.Step,
	delivery replay.Delivery,
) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(delivery.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	repository := requiredMap(t, payload, "repository")
	if jsonID(t, repository, "id") != step.Mutation.Repository.ID ||
		jsonString(t, repository, "node_id") !=
			step.Mutation.Repository.NodeID ||
		jsonString(t, repository, "full_name") !=
			step.Mutation.Repository.FullName() {
		t.Fatalf(
			"delivery repository %#v disagrees with mutation %+v",
			repository,
			step.Mutation.Repository,
		)
	}
	if step.Mutation.PullRequest != nil {
		pull := requiredMap(t, payload, "pull_request")
		mutation := step.Mutation.PullRequest
		if jsonID(t, pull, "id") != mutation.ID ||
			int(jsonID(t, pull, "number")) != mutation.Number ||
			jsonString(t, requiredMap(t, pull, "head"), "sha") !=
				mutation.Head.SHA ||
			jsonString(t, requiredMap(t, pull, "base"), "ref") !=
				mutation.Base.Ref {
			t.Fatalf(
				"delivery pull request %#v disagrees with mutation %+v",
				pull,
				*mutation,
			)
		}
	}
	switch delivery.Event {
	case "pull_request_review":
		review := requiredMap(t, payload, "review")
		if jsonID(t, review, "id") != step.Mutation.Review.ID ||
			jsonString(t, review, "node_id") !=
				step.Mutation.Review.NodeID ||
			jsonString(t, review, "state") !=
				step.Mutation.Review.State {
			t.Fatalf("delivery review %#v disagrees with mutation", review)
		}
	case "pull_request_review_comment":
		comment := requiredMap(t, payload, "comment")
		if jsonID(t, comment, "id") != step.Mutation.ReviewComment.ID ||
			jsonString(t, comment, "node_id") !=
				step.Mutation.ReviewComment.NodeID {
			t.Fatalf("delivery comment %#v disagrees with mutation", comment)
		}
	case "pull_request_review_thread":
		thread := requiredMap(t, payload, "thread")
		if jsonString(t, thread, "node_id") !=
			step.Mutation.ReviewThread.ID {
			t.Fatalf("delivery thread %#v disagrees with mutation", thread)
		}
	case "check_suite":
		suite := requiredMap(t, payload, "check_suite")
		if jsonID(t, suite, "id") != step.Mutation.CheckSuite.ID ||
			jsonString(t, suite, "node_id") !=
				step.Mutation.CheckSuite.NodeID ||
			jsonString(t, suite, "head_sha") !=
				step.Mutation.CheckSuite.HeadSHA ||
			jsonOptionalString(suite, "conclusion") !=
				step.Mutation.CheckSuite.Conclusion {
			t.Fatalf("delivery check suite %#v disagrees with mutation", suite)
		}
	case "check_run":
		run := requiredMap(t, payload, "check_run")
		if jsonID(t, run, "id") != step.Mutation.CheckRun.ID ||
			jsonString(t, run, "node_id") !=
				step.Mutation.CheckRun.NodeID ||
			jsonString(t, run, "head_sha") !=
				step.Mutation.CheckRun.HeadSHA ||
			jsonOptionalString(run, "conclusion") !=
				step.Mutation.CheckRun.Conclusion {
			t.Fatalf("delivery check run %#v disagrees with mutation", run)
		}
	case "push":
		if jsonString(t, payload, "ref") != step.Mutation.Push.Ref ||
			jsonString(t, payload, "before") != step.Mutation.Push.Before ||
			jsonString(t, payload, "after") != step.Mutation.Push.After ||
			jsonBool(t, payload, "forced") != step.Mutation.Push.Forced {
			t.Fatalf("delivery push %#v disagrees with mutation", payload)
		}
		head := requiredMap(t, payload, "head_commit")
		if jsonString(t, head, "id") != step.Mutation.Push.After ||
			jsonString(t, head, "timestamp") !=
				step.Mutation.Push.PushedAt.UTC().Format(time.RFC3339) {
			t.Fatalf("delivery head commit %#v disagrees with mutation", head)
		}
		commits, ok := payload["commits"].([]any)
		if !ok || len(commits) != 1 {
			t.Fatalf("delivery commits = %#v, want one", payload["commits"])
		}
		commit, ok := commits[0].(map[string]any)
		if !ok ||
			jsonString(t, commit, "id") != step.Mutation.Push.After {
			t.Fatalf("delivery commit %#v disagrees with mutation", commits[0])
		}
	}
}

func requiredMap(
	t *testing.T,
	parent map[string]any,
	key string,
) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%q = %#v, want object", key, parent[key])
	}
	return value
}

func jsonID(t *testing.T, parent map[string]any, key string) int64 {
	t.Helper()
	value, ok := parent[key].(float64)
	if !ok || value != float64(int64(value)) {
		t.Fatalf("%q = %#v, want integer", key, parent[key])
	}
	return int64(value)
}

func jsonString(t *testing.T, parent map[string]any, key string) string {
	t.Helper()
	value, ok := parent[key].(string)
	if !ok {
		t.Fatalf("%q = %#v, want string", key, parent[key])
	}
	return value
}

func jsonOptionalString(parent map[string]any, key string) string {
	value, _ := parent[key].(string)
	return value
}

func jsonBool(t *testing.T, parent map[string]any, key string) bool {
	t.Helper()
	value, ok := parent[key].(bool)
	if !ok {
		t.Fatalf("%q = %#v, want boolean", key, parent[key])
	}
	return value
}

func TestSpeedCompressionPreservesSourceOrdering(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	for index := range recording.Events {
		recording.Events[index].AtMS = int64(index)
	}
	steps, err := replay.FirstLap(recording, replay.CompileOptions{
		Speed:  2,
		Copies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var previousAt int64
	var previousSource int64
	var previousCopy int
	for index, step := range steps {
		if index > 0 && step.AtMS < previousAt {
			t.Fatalf("step %d moved backward after speed compression", step.Seq)
		}
		if step.AtMS == previousAt &&
			(step.SourceSeq < previousSource ||
				(step.SourceSeq == previousSource &&
					step.Copy < previousCopy)) {
			t.Fatalf(
				"step order changed at %dms: source=%d copy=%d after source=%d copy=%d",
				step.AtMS,
				step.SourceSeq,
				step.Copy,
				previousSource,
				previousCopy,
			)
		}
		previousAt = step.AtMS
		previousSource = step.SourceSeq
		previousCopy = step.Copy
	}
}

func TestCompilerPreservesGitHubInt64IDs(t *testing.T) {
	t.Parallel()
	const largeID = int64(9_007_199_254_740_993)
	recording := testRecording()
	for index := range recording.Events {
		if recording.Events[index].CheckRun != nil {
			recording.Events[index].CheckRun.ID = largeID
		}
	}
	steps, err := replay.FirstLap(recording, replay.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if step.Mutation.CheckRun == nil {
			continue
		}
		if step.Mutation.CheckRun.ID != largeID {
			t.Fatalf(
				"mutation check run ID = %d, want %d",
				step.Mutation.CheckRun.ID,
				largeID,
			)
		}
		var payload struct {
			CheckRun struct {
				ID int64 `json:"id"`
			} `json:"check_run"`
		}
		if err := json.Unmarshal(step.Deliveries[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.CheckRun.ID != largeID {
			t.Fatalf(
				"payload check run ID = %d, want %d",
				payload.CheckRun.ID,
				largeID,
			)
		}
	}
}

func TestCompilerNormalizesCheckSuiteTruthAndDeliveryTogether(t *testing.T) {
	t.Parallel()
	recording := testRecording()
	for index := range recording.Events {
		suite := recording.Events[index].CheckSuite
		if suite != nil && recording.Events[index].Action == "completed" {
			suite.Conclusion = "skipped"
		}
	}
	steps, err := replay.FirstLap(recording, replay.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if step.Mutation.CheckSuite == nil ||
			step.Mutation.Action != "completed" {
			continue
		}
		if step.Mutation.CheckSuite.Conclusion != "neutral" {
			t.Fatalf(
				"compiled truth conclusion = %q, want neutral",
				step.Mutation.CheckSuite.Conclusion,
			)
		}
		var payload struct {
			CheckSuite struct {
				Conclusion string `json:"conclusion"`
			} `json:"check_suite"`
		}
		if err := json.Unmarshal(step.Deliveries[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.CheckSuite.Conclusion !=
			step.Mutation.CheckSuite.Conclusion {
			t.Fatalf(
				"delivery conclusion = %q, truth = %q",
				payload.CheckSuite.Conclusion,
				step.Mutation.CheckSuite.Conclusion,
			)
		}
	}
}

func TestCopiesUseDisjointEntitySpaces(t *testing.T) {
	t.Parallel()
	steps, err := replay.FirstLap(testRecording(), replay.CompileOptions{
		Copies: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	spaces := make(map[int]entitySpace)
	for _, step := range steps {
		space := spaces[step.Copy]
		space.addRepository(step.Mutation.Repository)
		if step.Mutation.PullRequest != nil {
			space.addPull(*step.Mutation.PullRequest)
		}
		if step.Mutation.Review != nil {
			space.addID(step.Mutation.Review.ID)
			space.addNode(step.Mutation.Review.NodeID)
			space.addSHA(step.Mutation.Review.CommitSHA)
		}
		if step.Mutation.ReviewThread != nil {
			space.addNode(step.Mutation.ReviewThread.ID)
			for _, comment := range step.Mutation.ReviewThread.Comments {
				space.addComment(comment)
			}
		}
		if step.Mutation.ReviewComment != nil {
			space.addComment(*step.Mutation.ReviewComment)
		}
		if step.Mutation.CheckSuite != nil {
			space.addID(step.Mutation.CheckSuite.ID)
			space.addNode(step.Mutation.CheckSuite.NodeID)
			space.addSHA(step.Mutation.CheckSuite.HeadSHA)
		}
		if step.Mutation.CheckRun != nil {
			space.checkRunIDs[step.Mutation.CheckRun.ID] = true
			space.addID(step.Mutation.CheckRun.ID)
			space.addNode(step.Mutation.CheckRun.NodeID)
			space.addSHA(step.Mutation.CheckRun.HeadSHA)
		}
		if step.Mutation.Commit != nil {
			space.addSHA(step.Mutation.Commit.SHA)
			space.addSHA(step.Mutation.Commit.ParentSHA)
			if step.Mutation.Commit.PullRequestNumber > 0 {
				space.numbers[step.Mutation.Commit.PullRequestNumber] = true
			}
		}
		if step.Mutation.Push != nil {
			space.addSHA(step.Mutation.Push.Before)
			space.addSHA(step.Mutation.Push.After)
		}
		spaces[step.Copy] = space
	}
	if len(spaces) != 3 {
		t.Fatalf("entity spaces = %d, want 3", len(spaces))
	}
	for left := range 3 {
		for right := left + 1; right < 3; right++ {
			assertDisjoint(t, "IDs", spaces[left].ids, spaces[right].ids)
			assertDisjoint(
				t,
				"repository IDs",
				spaces[left].repositoryIDs,
				spaces[right].repositoryIDs,
			)
			assertDisjoint(
				t,
				"PR numbers",
				spaces[left].numbers,
				spaces[right].numbers,
			)
			assertDisjoint(
				t,
				"node IDs",
				spaces[left].nodes,
				spaces[right].nodes,
			)
			assertDisjoint(
				t,
				"check run IDs",
				spaces[left].checkRunIDs,
				spaces[right].checkRunIDs,
			)
			assertDisjoint(
				t,
				"SHAs",
				spaces[left].shas,
				spaces[right].shas,
			)
		}
	}
}

type entitySpace struct {
	ids           map[int64]bool
	repositoryIDs map[int64]bool
	numbers       map[int]bool
	nodes         map[string]bool
	checkRunIDs   map[int64]bool
	shas          map[string]bool
}

func (s *entitySpace) initialize() {
	if s.ids == nil {
		s.ids = make(map[int64]bool)
		s.repositoryIDs = make(map[int64]bool)
		s.numbers = make(map[int]bool)
		s.nodes = make(map[string]bool)
		s.checkRunIDs = make(map[int64]bool)
		s.shas = make(map[string]bool)
	}
}

func (s *entitySpace) addRepository(repository replay.Repository) {
	s.initialize()
	s.ids[repository.ID] = true
	s.repositoryIDs[repository.ID] = true
	s.addNode(repository.NodeID)
	s.addSHA(repository.DefaultBranchSHA)
}

func (s *entitySpace) addPull(pull replay.PullRequest) {
	s.initialize()
	s.addID(pull.ID)
	s.numbers[pull.Number] = true
	s.addNode(pull.NodeID)
	s.addSHA(pull.Head.SHA)
	s.addSHA(pull.Base.SHA)
}

func (s *entitySpace) addComment(comment replay.ReviewComment) {
	s.addID(comment.ID)
	if comment.ReviewID > 0 {
		s.addID(comment.ReviewID)
	}
	s.addNode(comment.NodeID)
}

func (s *entitySpace) addID(value int64) {
	s.initialize()
	if value > 0 {
		s.ids[value] = true
	}
}

func (s *entitySpace) addNode(value string) {
	s.initialize()
	if value != "" {
		s.nodes[value] = true
	}
}

func (s *entitySpace) addSHA(value string) {
	s.initialize()
	if value != "" && value != strings.Repeat("0", 40) {
		s.shas[value] = true
	}
}

func assertDisjoint[K comparable](
	t *testing.T,
	label string,
	left map[K]bool,
	right map[K]bool,
) {
	t.Helper()
	for value := range left {
		if right[value] {
			t.Fatalf("%s overlap at %v", label, value)
		}
	}
}

func TestLoopRenumbersEveryLap(t *testing.T) {
	t.Parallel()
	program, err := replay.Compile(testRecording(), replay.CompileOptions{
		Copies: 2,
		Loop:   true,
		Speed:  100,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := program.NextLap()
	if err != nil {
		t.Fatal(err)
	}
	second, err := program.NextLap()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("compiled replay lap is empty")
	}
	firstIDs := mutationPullIDs(first)
	secondIDs := mutationPullIDs(second)
	for id := range firstIDs {
		if secondIDs[id] {
			t.Fatalf("pull ID %d was reused on the next lap", id)
		}
	}
	if second[0].AtMS <= first[len(first)-1].AtMS {
		t.Fatalf(
			"second lap starts at %d, first ends at %d",
			second[0].AtMS,
			first[len(first)-1].AtMS,
		)
	}
	firstSpace := collectEntitySpace(first)
	secondSpace := collectEntitySpace(second)
	assertDisjoint(t, "loop IDs", firstSpace.ids, secondSpace.ids)
	assertDisjoint(t, "loop PR numbers", firstSpace.numbers, secondSpace.numbers)
	assertDisjoint(t, "loop node IDs", firstSpace.nodes, secondSpace.nodes)
	assertDisjoint(t, "loop SHAs", firstSpace.shas, secondSpace.shas)
	firstGUIDs := make(map[string]bool)
	for _, step := range first {
		for _, delivery := range step.Deliveries {
			firstGUIDs[delivery.GUID] = true
		}
	}
	for _, step := range second {
		if step.Seq <= first[len(first)-1].Seq {
			t.Fatalf("loop step sequence %d overlaps first lap", step.Seq)
		}
		for _, delivery := range step.Deliveries {
			if firstGUIDs[delivery.GUID] {
				t.Fatalf("delivery GUID %s was reused on the next lap", delivery.GUID)
			}
		}
	}
}

func TestDeriveStacksFromBaseRefChains(t *testing.T) {
	t.Parallel()
	repository := testRepository()
	pulls := []replay.PullRequest{
		testPull(11, "one", "main"),
		testPull(12, "two", "one"),
		testPull(13, "three", "two"),
		testPull(20, "independent", "main"),
	}
	stacks := replay.DeriveStacks(repository, pulls)
	if len(stacks) != 1 {
		t.Fatalf("stacks = %+v, want one", stacks)
	}
	if !reflect.DeepEqual(stacks[0].PullRequests, []int{11, 12, 13}) {
		t.Fatalf(
			"stack members = %v, want [11 12 13]",
			stacks[0].PullRequests,
		)
	}
	if len(stacks[0].PullRequestStates) != 3 {
		t.Fatalf(
			"stack member states = %+v, want all three full pulls",
			stacks[0].PullRequestStates,
		)
	}
	for index, number := range stacks[0].PullRequests {
		if stacks[0].PullRequestStates[index] != pulls[index] ||
			stacks[0].PullRequestStates[index].Number != number {
			t.Fatalf(
				"stack member state %d = %+v, want %+v",
				index,
				stacks[0].PullRequestStates[index],
				pulls[index],
			)
		}
	}
	if stacks[0].Base.Ref != "main" {
		t.Fatalf("stack base = %+v, want main", stacks[0].Base)
	}
}

func TestDeriveStacksIgnoresForkDefaultBranchHeads(t *testing.T) {
	t.Parallel()
	repository := testRepository()
	pulls := []replay.PullRequest{
		testPull(11, "feature", "main"),
		testPull(12, "main", "main"),
	}
	pulls[0].Head.Repository = repository.FullName()
	pulls[0].Base.Repository = repository.FullName()
	pulls[1].Head.Repository = "contributor/ghsync"
	pulls[1].Base.Repository = repository.FullName()
	if stacks := replay.DeriveStacks(repository, pulls); len(stacks) != 0 {
		t.Fatalf("fork default-branch head created stacks: %+v", stacks)
	}
}

func TestDeriveStacksDisambiguatesReusedHeadBranchesBySHA(t *testing.T) {
	t.Parallel()
	repository := testRepository()
	old := testPull(10, "feature", "main")
	old.State = "closed"
	old.Head.SHA = "1111111111111111111111111111111111111111"
	current := testPull(11, "feature", "main")
	current.Head.SHA = "2222222222222222222222222222222222222222"
	child := testPull(12, "child", "feature")
	child.Base.SHA = current.Head.SHA
	stacks := replay.DeriveStacks(
		repository,
		[]replay.PullRequest{current, child, old},
	)
	if len(stacks) != 1 ||
		!reflect.DeepEqual(stacks[0].PullRequests, []int{11, 12}) {
		t.Fatalf(
			"stack from reused branch = %+v, want [11 12]",
			stacks,
		)
	}
}

func TestDeriveStacksUnknownBaseSHAFallsBackWithoutEmptySHAIdentity(
	t *testing.T,
) {
	t.Parallel()
	repository := testRepository()
	old := testPull(10, "feature", "main")
	old.State = "closed"
	old.UpdatedAt = old.UpdatedAt.Add(time.Hour)
	current := testPull(11, "feature", "main")
	child := testPull(12, "child", "feature")
	child.Base.SHA = ""
	stacks := replay.DeriveStacks(
		repository,
		[]replay.PullRequest{old, child, current},
	)
	if len(stacks) != 1 ||
		!reflect.DeepEqual(stacks[0].PullRequests, []int{11, 12}) {
		t.Fatalf(
			"unknown-SHA reused branch stack = %+v, want open fallback [11 12]",
			stacks,
		)
	}
}

func TestStackSynthesisIsSeededAndDeterministic(t *testing.T) {
	t.Parallel()
	repository := testRepository()
	pulls := []replay.PullRequest{
		testPull(1, "one", "main"),
		testPull(2, "two", "main"),
		testPull(3, "three", "main"),
		testPull(4, "four", "main"),
	}
	first, firstMembers := replay.SynthesizeStackBases(
		repository,
		pulls,
		75,
		42,
	)
	second, secondMembers := replay.SynthesizeStackBases(
		repository,
		pulls,
		75,
		42,
	)
	if !reflect.DeepEqual(first, second) ||
		!reflect.DeepEqual(firstMembers, secondMembers) {
		t.Fatal("same synthesis seed produced different stacks")
	}
	if len(replay.DeriveStacks(repository, first)) == 0 {
		t.Fatal("synthesis did not create a base-ref chain")
	}
}

func TestStackSynthesisUsesEntireEvenTarget(t *testing.T) {
	t.Parallel()
	repository := testRepository()
	pulls := []replay.PullRequest{
		testPull(1, "one", "main"),
		testPull(2, "two", "main"),
		testPull(3, "three", "main"),
		testPull(4, "four", "main"),
	}
	synthesized, members := replay.SynthesizeStackBases(
		repository,
		pulls,
		100,
		7,
	)
	if len(members) != len(pulls) {
		t.Fatalf(
			"synthetic members = %v, want all %d pulls",
			members,
			len(pulls),
		)
	}
	stacks := replay.DeriveStacks(repository, synthesized)
	var stacked int
	for _, stack := range stacks {
		stacked += len(stack.PullRequests)
	}
	if stacked != len(pulls) {
		t.Fatalf("synthesized stacks = %+v, want all pulls stacked", stacks)
	}
}

func mutationPullIDs(steps []replay.Step) map[int64]bool {
	result := make(map[int64]bool)
	for _, step := range steps {
		if step.Mutation.PullRequest != nil {
			result[step.Mutation.PullRequest.ID] = true
		}
	}
	return result
}

func collectEntitySpace(steps []replay.Step) entitySpace {
	var space entitySpace
	for _, step := range steps {
		space.addRepository(step.Mutation.Repository)
		if step.Mutation.PullRequest != nil {
			space.addPull(*step.Mutation.PullRequest)
		}
		if step.Mutation.Review != nil {
			space.addID(step.Mutation.Review.ID)
			space.addNode(step.Mutation.Review.NodeID)
			space.addSHA(step.Mutation.Review.CommitSHA)
		}
		if step.Mutation.ReviewThread != nil {
			space.addNode(step.Mutation.ReviewThread.ID)
			for _, comment := range step.Mutation.ReviewThread.Comments {
				space.addComment(comment)
			}
		}
		if step.Mutation.ReviewComment != nil {
			space.addComment(*step.Mutation.ReviewComment)
		}
		if step.Mutation.CheckSuite != nil {
			space.addID(step.Mutation.CheckSuite.ID)
			space.addNode(step.Mutation.CheckSuite.NodeID)
			space.addSHA(step.Mutation.CheckSuite.HeadSHA)
		}
		if step.Mutation.CheckRun != nil {
			space.addID(step.Mutation.CheckRun.ID)
			space.addNode(step.Mutation.CheckRun.NodeID)
			space.addSHA(step.Mutation.CheckRun.HeadSHA)
		}
		if step.Mutation.Commit != nil {
			space.addSHA(step.Mutation.Commit.SHA)
			space.addSHA(step.Mutation.Commit.ParentSHA)
			if step.Mutation.Commit.PullRequestNumber > 0 {
				space.numbers[step.Mutation.Commit.PullRequestNumber] = true
			}
		}
		if step.Mutation.Push != nil {
			space.addSHA(step.Mutation.Push.Before)
			space.addSHA(step.Mutation.Push.After)
		}
		if step.Mutation.Stack != nil {
			space.addID(step.Mutation.Stack.ID)
			space.numbers[step.Mutation.Stack.Number] = true
			for _, number := range step.Mutation.Stack.PullRequests {
				space.numbers[number] = true
			}
			for _, pull := range step.Mutation.Stack.PullRequestStates {
				space.addPull(pull)
			}
		}
	}
	return space
}

func testRecording() replay.Recording {
	start := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	repository := testRepository()
	pull := testPull(11, "feature", "main")
	pull.ID = 1011
	pull.NodeID = "PR_node_11"
	pull.Title = "Recorded pull request"
	pull.AuthorLogin = "octocat"
	pull.CreatedAt = start
	pull.UpdatedAt = start
	closedAt := start.Add(11 * time.Second)
	review := replay.Review{
		ID: 2011, NodeID: "PRR_node_2011", State: "approved",
		Body: "Looks good.", AuthorLogin: "reviewer",
		CommitSHA: pull.Head.SHA, SubmittedAt: start.Add(3 * time.Second),
	}
	line := 8
	comment := replay.ReviewComment{
		ID: 3011, NodeID: "PRRC_node_3011", ReviewID: review.ID,
		Body: "Please add a test.", Path: "main.go", Line: &line,
		AuthorLogin: "reviewer", CreatedAt: start.Add(4 * time.Second),
		UpdatedAt: start.Add(4 * time.Second),
	}
	thread := replay.ReviewThread{
		ID: "PRRT_node_1", IsResolved: true, Path: "main.go",
		Line: &line, Comments: []replay.ReviewComment{comment},
	}
	suiteCreated := start.Add(6 * time.Second)
	suiteCompleted := start.Add(9 * time.Second)
	runStarted := start.Add(7 * time.Second)
	runCompleted := start.Add(8 * time.Second)
	events := []replay.Event{
		{Seq: 1, AtMS: 0, Kind: "repository", Repository: &repository},
		{Seq: 2, AtMS: 1000, Kind: "pull_request", Action: "opened", PullRequest: &pull},
		{
			Seq: 3, AtMS: 2000, Kind: "pull_request", Action: "edited",
			PullRequest: &pull,
			PreviousBase: &replay.Branch{
				Ref: "release", SHA: "1111111111111111111111111111111111111111",
			},
		},
		{
			Seq: 4, AtMS: 3000, Kind: "pull_request_review",
			Action: "submitted", PullRequest: &pull, Review: &review,
		},
		{
			Seq: 5, AtMS: 4000, Kind: "review_comment",
			Action: "created", PullRequest: &pull, Comment: &comment,
		},
		{
			Seq: 6, AtMS: 5000, Kind: "review_thread",
			Action: "resolved", PullRequest: &pull, Thread: &thread,
		},
		{
			Seq: 7, AtMS: 6000, Kind: "check_suite", Action: "requested",
			CheckSuite: &replay.CheckSuite{
				ID: 4011, NodeID: "CS_node_4011", HeadSHA: pull.Head.SHA,
				Status: "queued", AppSlug: "github-actions",
				CreatedAt: suiteCreated, UpdatedAt: suiteCreated,
			},
		},
		{
			Seq: 8, AtMS: 7000, Kind: "check_run", Action: "created",
			CheckRun: &replay.CheckRun{
				ID: 5011, NodeID: "CR_node_5011", HeadSHA: pull.Head.SHA,
				Name: "unit", Status: "queued",
				DetailsURL: "https://github.com/ewhauser/ghsync/actions/runs/5011",
				AppSlug:    "github-actions", StartedAt: &runStarted,
			},
		},
		{
			Seq: 9, AtMS: 8000, Kind: "check_run", Action: "completed",
			CheckRun: &replay.CheckRun{
				ID: 5011, NodeID: "CR_node_5011", HeadSHA: pull.Head.SHA,
				Name: "unit", Status: "completed", Conclusion: "success",
				DetailsURL: "https://github.com/ewhauser/ghsync/actions/runs/5011",
				AppSlug:    "github-actions", StartedAt: &runStarted,
				CompletedAt: &runCompleted,
			},
		},
		{
			Seq: 10, AtMS: 9000, Kind: "check_suite", Action: "completed",
			CheckSuite: &replay.CheckSuite{
				ID: 4011, NodeID: "CS_node_4011", HeadSHA: pull.Head.SHA,
				Status: "completed", Conclusion: "success",
				AppSlug: "github-actions", CreatedAt: suiteCreated,
				UpdatedAt: suiteCompleted,
			},
		},
		{
			Seq: 11, AtMS: 10000, Kind: "commit",
			Commit: &replay.Commit{
				SHA: pull.Head.SHA, Ref: "refs/heads/feature",
				ParentSHA:         "2222222222222222222222222222222222222222",
				CommittedAt:       start.Add(10 * time.Second),
				PullRequestNumber: pull.Number,
			},
		},
		{
			Seq: 12, AtMS: 10000, Kind: "push",
			Push: &replay.Push{
				Ref:      "refs/heads/feature",
				Before:   "2222222222222222222222222222222222222222",
				After:    pull.Head.SHA,
				PushedAt: start.Add(10 * time.Second),
			},
		},
	}
	closed := pull
	closed.State = "closed"
	closed.ClosedAt = &closedAt
	closed.UpdatedAt = closedAt
	events = append(events, replay.Event{
		Seq: 13, AtMS: 11000, Kind: "pull_request", Action: "closed",
		PullRequest: &closed,
	})
	stacks := replay.DeriveStacks(
		repository,
		[]replay.PullRequest{
			testPull(10, "parent", "main"),
			testPull(11, "feature", "parent"),
		},
	)
	if len(stacks) == 0 {
		panic("test recording produced no derived stack")
	}
	stack := stacks[0]
	events = append(events, replay.Event{
		Seq: 14, AtMS: 12000, Kind: "stack", Stack: &stack,
	})
	return replay.Recording{
		Header: replay.Header{
			Type: "recording", Version: replay.RecordingVersion,
			Repository: repository, Since: start, Until: start.Add(time.Hour),
			StartedAt: start, Seed: 1,
		},
		Events: events,
	}
}

func testRepository() replay.Repository {
	return replay.Repository{
		ID: 1001, NodeID: "R_node_1001", Owner: "acme", Name: "ghsync",
		DefaultBranch:    "main",
		DefaultBranchSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		UpdatedAt:        time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}
}

func testPull(number int, head, base string) replay.PullRequest {
	return replay.PullRequest{
		ID: int64(number), NodeID: "PR", Number: number,
		Title: "Pull", State: "open", AuthorLogin: "octocat",
		Head: replay.Branch{
			Ref: head,
			SHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		Base: replay.Branch{
			Ref: base,
			SHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		CreatedAt: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}
}

func TestProgramStopsWithoutLoop(t *testing.T) {
	t.Parallel()
	program, err := replay.Compile(testRecording(), replay.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := program.NextLap(); err != nil {
		t.Fatal(err)
	}
	if _, err := program.NextLap(); !errors.Is(err, io.EOF) {
		t.Fatalf("second lap error = %v, want EOF", err)
	}
}

func TestStepJSONContainsLogicalMutationNotBarePayload(t *testing.T) {
	t.Parallel()
	steps, err := replay.FirstLap(testRecording(), replay.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) < 2 {
		t.Fatalf("compiled steps = %d, want at least 2", len(steps))
	}
	body, err := json.Marshal(steps[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"mutation"`)) ||
		!bytes.Contains(body, []byte(`"pull_request"`)) ||
		!bytes.Contains(body, []byte(`"deliveries"`)) {
		t.Fatalf("compiled step is missing truth or deliveries: %s", body)
	}
}

func TestCommittedRecordingCompilesToSchemaValidDeliveries(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	if !reflect.DeepEqual(
		names,
		[]string{"README.md", "workerd-week.ndjson"},
	) {
		t.Fatalf("committed replay artifacts = %v, want README and one recording", names)
	}
	readme, err := os.ReadFile("testdata/README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"go run ./cmd/ghrecord",
		"--repo cloudflare/workerd",
		"--since 2026-07-21",
		"--until 2026-07-27",
		"--synthesize-stacks=20",
		"--seed=1",
	} {
		if !bytes.Contains(readme, []byte(fragment)) {
			t.Errorf("recording README is missing %q", fragment)
		}
	}
	info, err := os.Stat("testdata/workerd-week.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 1<<20 {
		t.Fatalf("committed recording is oversized: %d bytes", info.Size())
	}
	file, err := os.Open("testdata/workerd-week.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = file.Close()
	}()
	recording, err := replay.Read(file)
	if err != nil {
		t.Fatal(err)
	}
	if recording.Header.Repository.FullName() != "cloudflare/workerd" ||
		!recording.Header.Since.Equal(
			time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		) ||
		!recording.Header.Until.Equal(
			time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		) ||
		recording.Header.SynthesizeStacks != 20 ||
		recording.Header.Seed != 1 {
		t.Fatalf(
			"committed recording header disagrees with README: %+v",
			recording.Header,
		)
	}
	if len(recording.Events) < 1500 || len(recording.Events) > 2500 {
		t.Fatalf(
			"committed recording has %d events, want about 2000",
			len(recording.Events),
		)
	}
	var hasCommit bool
	var stackHasCompleteTruth bool
	for _, event := range recording.Events {
		hasCommit = hasCommit || event.Kind == "commit"
		if event.Kind == "stack" &&
			len(event.Stack.PullRequestStates) ==
				len(event.Stack.PullRequests) {
			for _, pull := range event.Stack.PullRequestStates {
				stackHasCompleteTruth = stackHasCompleteTruth ||
					pull.Number == 6823
			}
		}
	}
	if !hasCommit {
		t.Fatal("committed recording has no explicit commit events")
	}
	if !stackHasCompleteTruth {
		t.Fatal("committed stack lacks full fixture truth for member 6823")
	}
	options := replay.CompileOptions{
		Speed: 1000,
	}
	steps, err := replay.FirstLap(recording, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replay.FirstLap(recording, options)
	if err != nil {
		t.Fatal(err)
	}
	var firstBytes, secondBytes bytes.Buffer
	if err := replay.EncodeSteps(&firstBytes, steps); err != nil {
		t.Fatal(err)
	}
	if err := replay.EncodeSteps(&secondBytes, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes.Bytes(), secondBytes.Bytes()) {
		t.Fatal("committed recording does not compile byte-identically")
	}
	validator := conformance.NewWebhookSchemaValidator()
	for _, step := range steps {
		for _, delivery := range step.Deliveries {
			if err := validator.Validate(
				delivery.Event,
				delivery.Payload,
			); err != nil {
				t.Fatalf(
					"seq %d %s/%s: %v",
					step.SourceSeq,
					delivery.Event,
					delivery.Action,
					err,
				)
			}
		}
	}
}
