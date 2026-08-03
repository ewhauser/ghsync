package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/replay"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestValidateConfigRejectsUnboundedAndUnownedChaos(t *testing.T) {
	t.Parallel()
	cfg := config{
		engineURL:      "http://engine",
		fakeGitHubURL:  "http://fake",
		databaseURL:    "postgres://database",
		installationID: 1,
		recordingPath:  "recording.ndjson",
		speed:          1,
		copies:         1,
		targetRate:     1,
		trafficTimeout: time.Minute,
		drainTimeout:   time.Minute,
		scrapeInterval: time.Second,
		fake429Retry:   time.Second,
	}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cfg.loop = true
	if err := validateConfig(cfg); err == nil {
		t.Fatal("unbounded loop was accepted")
	}
	cfg.loop = false
	cfg.maxSteps = 10
	if err := validateConfig(cfg); err == nil {
		t.Fatal("partial one-shot replay was accepted")
	}
	cfg.maxSteps = 0
	cfg.reorderWindow = 1
	if err := validateConfig(cfg); err == nil {
		t.Fatal("no-op reorder window was accepted")
	}
	cfg.reorderWindow = 0
	cfg.restartAfter = time.Second
	if err := validateConfig(cfg); err == nil {
		t.Fatal("restart without owned engine was accepted")
	}
	cfg.engineCmd = "ghsyncd serve --roles=all"
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("owned restart rejected: %v", err)
	}
}

func TestEngineProcessSIGKILLRestart(t *testing.T) {
	t.Parallel()
	process := &engineProcess{command: "sleep 30"}
	if err := process.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	firstPID := process.cmd.Process.Pid
	if err := process.Restart(t.Context()); err != nil {
		t.Fatal(err)
	}
	secondPID := process.cmd.Process.Pid
	if firstPID == secondPID {
		t.Fatalf("engine restart retained PID %d", firstPID)
	}
}

func TestReplayPlanCountsDropsDuplicatesAndBoundedReordering(t *testing.T) {
	t.Parallel()
	steps := testReplaySteps(6)
	plan, err := newReplayPlan(steps, replayChaos{
		duplicateEvery: 2,
		reorderWindow:  3,
		dropEvery:      4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.deliveryCount != 6 || plan.duplicates != 3 ||
		plan.dropped != 1 {
		t.Fatalf(
			"plan counts = deliveries %d duplicates %d dropped %d",
			plan.deliveryCount,
			plan.duplicates,
			plan.dropped,
		)
	}
	got := reorderedSequences(8, 3)
	want := []int{3, 2, 1, 6, 5, 4, 8, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reordered sequence = %v, want %v", got, want)
	}
	for outputIndex, sequence := range got {
		if displacement := outputIndex + 1 - sequence; displacement >= 3 ||
			displacement <= -3 {
			t.Fatalf(
				"sequence %d displacement %d exceeds window",
				sequence,
				displacement,
			)
		}
	}
}

func TestReplayPlanRejectsVacuousOracleAndNoOpChaos(t *testing.T) {
	t.Parallel()
	if _, err := newReplayPlan(
		[]replay.Step{testReplayStep(1)},
		replayChaos{},
	); err == nil || !strings.Contains(err.Error(), "incomplete non-vacuous") {
		t.Fatalf("partial oracle plan error = %v", err)
	}
	steps := testReplaySteps(1)
	for _, chaos := range []replayChaos{
		{duplicateEvery: 2},
		{dropEvery: 2},
		{reorderWindow: 1},
	} {
		if _, err := newReplayPlan(steps, chaos); err == nil {
			t.Fatalf("no-op chaos %+v was accepted", chaos)
		}
	}
}

func TestOracleRecordSortingUsesCanonicalKeys(t *testing.T) {
	t.Parallel()
	assertSameOrder := func(name string, left, right any) {
		t.Helper()
		if !reflect.DeepEqual(left, right) {
			t.Fatalf("%s canonical order differs\nleft=%+v\nright=%+v", name, left, right)
		}
	}

	leftPulls := []oraclePull{{Number: 2}, {Number: 1}}
	rightPulls := []oraclePull{{Number: 1}, {Number: 2}}
	sortOraclePulls(leftPulls)
	sortOraclePulls(rightPulls)
	assertSameOrder("pull requests", leftPulls, rightPulls)

	leftStacks := []oracleStack{{Number: 2}, {Number: 1}}
	rightStacks := []oracleStack{{Number: 1}, {Number: 2}}
	sortOracleStacks(leftStacks)
	sortOracleStacks(rightStacks)
	assertSameOrder("stacks", leftStacks, rightStacks)

	leftChecks := []oracleCheck{{ID: 2}, {ID: 1}}
	rightChecks := []oracleCheck{{ID: 1}, {ID: 2}}
	sortOracleChecks(leftChecks)
	sortOracleChecks(rightChecks)
	assertSameOrder("check runs", leftChecks, rightChecks)

	const (
		hyphenatedID = "PRRT_kwDOIANE6c6SwE-z"
		letterID     = "PRRT_kwDOIANE6c6SwEWD"
	)
	leftThreads := []oracleThread{{ID: letterID}, {ID: hyphenatedID}}
	rightThreads := []oracleThread{{ID: hyphenatedID}, {ID: letterID}}
	sortOracleThreads(leftThreads)
	sortOracleThreads(rightThreads)
	assertSameOrder("review threads", leftThreads, rightThreads)
	if leftThreads[0].ID != hyphenatedID {
		t.Fatalf("review-thread byte order = %q first, want %q", leftThreads[0].ID, hyphenatedID)
	}
}

func TestExecuteReplayAppliesTruthAndDuplicatesDelivery(t *testing.T) {
	t.Parallel()
	var received atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			received.Add(1)
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer target.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), "secret")
	fakeServer := httptest.NewServer(fake)
	defer fakeServer.Close()
	cfg := config{
		engineURL:     target.URL,
		fakeGitHubURL: fakeServer.URL,
		httpClient:    fakeServer.Client(),
	}
	step := testReplayStep(1)
	plan, err := newReplayPlan(
		append([]replay.Step{step}, testOracleCoverageSteps()...),
		replayChaos{duplicateEvery: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	result := executeReplay(
		context.Background(),
		cfg,
		plan,
		time.Now(),
	)
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.deliveries != 1 || result.duplicates != 1 ||
		received.Load() != 2 {
		t.Fatalf(
			"replay result=%+v received=%d",
			result,
			received.Load(),
		)
	}
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		fakeServer.URL+fakegithub.ControlTruthPath,
		http.NoBody,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := fakeServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	var truth fakegithub.TruthSnapshot
	if err := json.NewDecoder(response.Body).Decode(&truth); err != nil {
		t.Fatal(err)
	}
	if len(truth.Repositories) != 1 ||
		truth.Repositories[0].PullRequests[1].Title != "Replay PR 4812" {
		t.Fatalf("truth after replay = %+v", truth)
	}
}

func TestExecuteReplayDropIsRecordedWithoutIngress(t *testing.T) {
	t.Parallel()
	var received atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			received.Add(1)
			w.WriteHeader(http.StatusAccepted)
		},
	))
	defer target.Close()
	fake := fakegithub.New(fakegithub.DefaultFixture(), "secret")
	fakeServer := httptest.NewServer(fake)
	defer fakeServer.Close()
	cfg := config{
		engineURL:     target.URL,
		fakeGitHubURL: fakeServer.URL,
		httpClient:    fakeServer.Client(),
	}
	plan, err := newReplayPlan(
		testReplaySteps(1),
		replayChaos{dropEvery: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	result := executeReplay(
		context.Background(),
		cfg,
		plan,
		time.Now(),
	)
	if result.err != nil {
		t.Fatal(result.err)
	}
	deliveries := fake.Deliveries()
	if received.Load() != 0 || result.dropped != 1 ||
		len(deliveries) != 1 || deliveries[0].Status != "DROPPED" {
		t.Fatalf(
			"drop result=%+v received=%d deliveries=%+v",
			result,
			received.Load(),
			deliveries,
		)
	}
}

func TestChaosKnobsPassStrictEndToEndAssertions(t *testing.T) {
	if raceEnabled {
		t.Skip("external-process chaos integration is covered by the non-race suite")
	}
	if testing.Short() {
		t.Skip("strict chaos integration test")
	}
	database := testdb.New(t)
	recording := chaosTestRecording()
	recordingPath := filepath.Join(t.TempDir(), "chaos-recording.ndjson")
	recordingFile, err := os.Create(recordingPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Write(recordingFile, recording); err != nil {
		_ = recordingFile.Close()
		t.Fatal(err)
	}
	if err := recordingFile.Close(); err != nil {
		t.Fatal(err)
	}

	const (
		webhookSecret = "loadgen-chaos-secret"
		appToken      = "fake-installation-loadgen-chaos-token"
		copies        = 10
	)
	fixtures := emptyRecordingFixtures(t, recording, copies)
	fake := fakegithub.New(
		fixtures[0],
		webhookSecret,
		fakegithub.WithAppBearerToken(appToken),
		fakegithub.WithAdditionalFixtures(fixtures[1:]...),
	)
	fakeServer := httptest.NewServer(fake)
	defer fakeServer.Close()

	repositoryRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	engineBinary := filepath.Join(t.TempDir(), "ghsyncd")
	build := exec.CommandContext(
		t.Context(),
		"go",
		"build",
		"-o",
		engineBinary,
		"./cmd/ghsyncd",
	)
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build ghsyncd: %v\n%s", err, output)
	}
	engineAddress := reserveLoopbackAddress(t)
	t.Setenv("DATABASE_URL", database.URL)
	t.Setenv("GITHUB_BASE_URL", fakeServer.URL)
	t.Setenv("GITHUB_TOKEN", appToken)
	t.Setenv("GITHUB_WEBHOOK_SECRET", webhookSecret)
	t.Setenv("GITHUB_INSTALLATION_ID", "1")
	t.Setenv("GITHUB_ORG_ID", "1")
	t.Setenv("HTTP_ADDR", engineAddress)
	t.Setenv(
		"DISPATCH_RULES_FILE",
		filepath.Join(repositoryRoot, "config", "dispatcher-rules.yaml"),
	)
	for key, value := range map[string]string{
		"DISPATCH_DEBOUNCE":              "50ms",
		"DISPATCH_POLL_INTERVAL":         "20ms",
		"FETCH_BATCH_WINDOW":             "2ms",
		"BUDGET_LEASE_TTL":               "6s",
		"BUDGET_LEASE_RENEW_INTERVAL":    "2s",
		"GAP_HEAL_PERIOD":                "250ms",
		"DRIFT_PERIOD":                   "500ms",
		"SWEEP_OPEN_STACK_MAX_STALENESS": "45s",
		"SWEEP_OPEN_PR_MAX_STALENESS":    "45s",
		"STREAM_WATERMARK_REFRESH":       "25ms",
		"DERIVER_POLL_INTERVAL":          "25ms",
	} {
		t.Setenv(key, value)
	}
	engineCommand := engineBinary + " serve --roles=all"
	bootstrap := &engineProcess{command: engineCommand}
	if err := bootstrap.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := waitHealthy(
		t.Context(),
		&http.Client{Timeout: time.Second},
		"http://"+engineAddress+"/healthz",
		15*time.Second,
	); err != nil {
		bootstrap.Close()
		t.Fatalf("bootstrap engine health: %v", err)
	}
	backfill := exec.CommandContext(t.Context(), engineBinary, "backfill")
	backfill.Dir = repositoryRoot
	backfill.Env = os.Environ()
	if output, err := backfill.CombinedOutput(); err != nil {
		bootstrap.Close()
		t.Fatalf("start backfill: %v\n%s", err, output)
	}
	seedConfig := config{
		installationID: 1,
		copies:         copies,
		drainTimeout:   20 * time.Second,
	}
	if err := waitForCacheSeed(
		t.Context(),
		seedConfig,
		database.Pool,
	); err != nil {
		bootstrap.Close()
		t.Fatal(err)
	}
	bootstrap.Close()

	result, err := run(context.Background(), config{
		engineURL:      "http://" + engineAddress,
		fakeGitHubURL:  fakeServer.URL,
		databaseURL:    database.URL,
		installationID: 1,
		recordingPath:  recordingPath,
		speed:          1,
		copies:         copies,
		targetRate:     0.2,
		trafficTimeout: 60 * time.Second,
		drainTimeout:   45 * time.Second,
		scrapeInterval: 100 * time.Millisecond,
		duplicateEvery: 3,
		reorderWindow:  3,
		dropEvery:      4,
		fake500Burst:   1,
		fake429Burst:   1,
		fake429Retry:   time.Second,
		engineCmd:      engineCommand,
		restartAfter:   7 * time.Second,
		httpClient:     &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.deliveries != 110 || result.dropped != 27 ||
		result.duplicates != 36 || result.streamEvents == 0 ||
		result.streamRestartAt == 0 || result.observed500s < 1 ||
		result.observed429s < 1 || result.gapHeals < 27 ||
		result.engineRestarts != 1 || result.cq2P95 > 20 ||
		result.cq2P99 > 60 {
		t.Fatalf("strict chaos result = %+v", result)
	}
}

func emptyRecordingFixtures(
	t *testing.T,
	recording replay.Recording,
	copies int,
) []fakegithub.Fixture {
	t.Helper()
	steps, err := replay.FirstLap(recording, replay.CompileOptions{
		Copies: copies,
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, copies)
	fixtures := make([]fakegithub.Fixture, 0, copies)
	for _, step := range steps {
		repository := step.Mutation.Repository
		fullName := repository.FullName()
		if _, exists := seen[fullName]; exists {
			continue
		}
		seen[fullName] = struct{}{}
		fixtures = append(fixtures, fakegithub.EmptyFixture(fakegithub.Repository{
			ID:               repository.ID,
			NodeID:           repository.NodeID,
			Owner:            repository.Owner,
			Name:             repository.Name,
			FullName:         fullName,
			DefaultBranch:    repository.DefaultBranch,
			DefaultBranchSHA: repository.DefaultBranchSHA,
			UpdatedAt:        repository.UpdatedAt,
			PushedAt:         repository.UpdatedAt,
		}))
	}
	if len(fixtures) != copies {
		t.Fatalf("recording fixtures = %d, want %d", len(fixtures), copies)
	}
	return fixtures
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(
		t.Context(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func chaosTestRecording() replay.Recording {
	start := time.Date(2026, 7, 30, 18, 0, 0, 0, time.UTC)
	repository := replay.Repository{
		ID:               21001,
		NodeID:           "R_loadgen_chaos",
		Owner:            "acme",
		Name:             "loadgen-chaos",
		DefaultBranch:    "main",
		DefaultBranchSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		UpdatedAt:        start,
	}
	pull := func(id int64, number int, branch string) replay.PullRequest {
		return replay.PullRequest{
			ID:          id,
			NodeID:      fmt.Sprintf("PR_loadgen_chaos_%d", number),
			Number:      number,
			Title:       fmt.Sprintf("Chaos pull %d", number),
			State:       "open",
			AuthorLogin: "octocat",
			Head: replay.Branch{
				Ref: branch,
				SHA: fmt.Sprintf("%040d", number+1),
			},
			Base: replay.Branch{
				Ref:        "main",
				SHA:        repository.DefaultBranchSHA,
				Repository: repository.FullName(),
			},
			CreatedAt: start,
			UpdatedAt: start,
		}
	}
	first := pull(22001, 1, "feature-one")
	second := pull(22002, 2, "feature-two")
	second.Base = replay.Branch{
		Ref:        first.Head.Ref,
		SHA:        first.Head.SHA,
		Repository: repository.FullName(),
	}
	review := replay.Review{
		ID:          23001,
		NodeID:      "REV_loadgen_chaos",
		State:       "approved",
		AuthorLogin: "reviewer",
		CommitSHA:   first.Head.SHA,
		SubmittedAt: start.Add(1500 * time.Millisecond),
	}
	line := 12
	comment := replay.ReviewComment{
		ID:          24001,
		NodeID:      "C_loadgen_chaos",
		ReviewID:    review.ID,
		Body:        "please keep this convergent",
		Path:        "chaos.go",
		Line:        &line,
		AuthorLogin: "reviewer",
		CreatedAt:   start.Add(1750 * time.Millisecond),
		UpdatedAt:   start.Add(1750 * time.Millisecond),
	}
	thread := replay.ReviewThread{
		ID:         "T_loadgen_chaos",
		IsResolved: true,
		Path:       "chaos.go",
		Line:       &line,
		Comments:   []replay.ReviewComment{comment},
	}
	runStarted := start.Add(time.Second)
	runCompleted := start.Add(2750 * time.Millisecond)
	suite := replay.CheckSuite{
		ID:        25001,
		NodeID:    "CS_loadgen_chaos",
		HeadSHA:   first.Head.SHA,
		Status:    "queued",
		AppSlug:   "github-actions",
		CreatedAt: start.Add(time.Second),
		UpdatedAt: start.Add(time.Second),
	}
	check := replay.CheckRun{
		ID:         26001,
		NodeID:     "CR_loadgen_chaos",
		HeadSHA:    first.Head.SHA,
		Name:       "unit",
		Status:     "queued",
		DetailsURL: "https://example.test/check/26001",
		AppSlug:    "github-actions",
		StartedAt:  &runStarted,
	}
	stack := replay.Stack{
		ID:     27001,
		Number: 7,
		Base: replay.Branch{
			Ref:        "main",
			SHA:        repository.DefaultBranchSHA,
			Repository: repository.FullName(),
		},
		PullRequests:      []int{first.Number, second.Number},
		PullRequestStates: []replay.PullRequest{first, second},
	}
	events := []replay.Event{
		{Seq: 1, AtMS: 0, Kind: "repository", Repository: &repository},
		{
			Seq: 2, AtMS: 250, Kind: "pull_request", Action: "opened",
			PullRequest: &first,
		},
		{
			Seq: 3, AtMS: 500, Kind: "pull_request", Action: "opened",
			PullRequest: &second,
		},
		{Seq: 4, AtMS: 750, Kind: "stack", Stack: &stack},
		{
			Seq: 5, AtMS: 1000, Kind: "check_suite", Action: "requested",
			CheckSuite: &suite,
		},
		{
			Seq: 6, AtMS: 1250, Kind: "check_run", Action: "created",
			CheckRun: &check,
		},
		{
			Seq: 7, AtMS: 1500, Kind: "pull_request_review",
			Action: "submitted", PullRequest: &first, Review: &review,
		},
		{
			Seq: 8, AtMS: 1750, Kind: "review_comment",
			Action: "created", PullRequest: &first, Comment: &comment,
		},
		{
			Seq: 9, AtMS: 2000, Kind: "review_thread",
			Action: "resolved", PullRequest: &first, Thread: &thread,
		},
		{
			Seq: 10, AtMS: 12000, Kind: "push",
			Push: &replay.Push{
				Ref:      "refs/heads/feature-one",
				Before:   repository.DefaultBranchSHA,
				After:    first.Head.SHA,
				PushedAt: start.Add(5 * time.Second),
			},
		},
	}
	first.Title = "Chaos pull 1 after restart"
	first.UpdatedAt = start.Add(5500 * time.Millisecond)
	events = append(events, replay.Event{
		Seq: 11, AtMS: 12500, Kind: "pull_request", Action: "synchronize",
		PullRequest: &first,
	})
	check.Status = "completed"
	check.Conclusion = "success"
	check.CompletedAt = &runCompleted
	events = append(events, replay.Event{
		Seq: 12, AtMS: 12750, Kind: "check_run", Action: "completed",
		CheckRun: &check,
	})
	second.Title = "Chaos pull 2 final"
	second.UpdatedAt = start.Add(6500 * time.Millisecond)
	events = append(events, replay.Event{
		Seq: 13, AtMS: 13000, Kind: "pull_request", Action: "edited",
		PullRequest: &second,
		PreviousBase: &replay.Branch{
			Ref: second.Base.Ref,
			SHA: second.Base.SHA,
		},
	})
	return replay.Recording{
		Header: replay.Header{
			Type:       "recording",
			Version:    replay.RecordingVersion,
			Repository: repository,
			Since:      start.Add(-time.Minute),
			Until:      start.Add(time.Hour),
			StartedAt:  start,
			Seed:       1,
		},
		Events: events,
	}
}

func testReplayStep(sequence int) replay.Step {
	fixture := fakegithub.DefaultFixture()
	pull := fixture.PullRequests[1]
	pull.Title = "Replay PR 4812"
	replayPull := testReplayPullRequest(pull)
	payload := json.RawMessage(`{
		"action":"synchronize",
		"repository":{"id":1001,"full_name":"acme/monolith"},
		"pull_request":{"number":4812,"stack":null}
	}`)
	return replay.Step{
		Seq:       uint64(sequence),
		AtMS:      0,
		Copy:      0,
		Lap:       0,
		SourceSeq: int64(sequence),
		Mutation: replay.FixtureMutation{
			Kind: "pull_request",
			Repository: replay.Repository{
				ID:               fixture.Repository.ID,
				NodeID:           fixture.Repository.NodeID,
				Owner:            fixture.Repository.Owner,
				Name:             fixture.Repository.Name,
				DefaultBranch:    fixture.Repository.DefaultBranch,
				DefaultBranchSHA: fixture.Repository.DefaultBranchSHA,
				UpdatedAt:        fixture.Repository.UpdatedAt,
			},
			PullRequest: &replayPull,
		},
		Deliveries: []replay.Delivery{{
			GUID:    "loadgen-test-" + strconv.Itoa(sequence),
			Event:   "pull_request",
			Action:  "synchronize",
			Payload: payload,
		}},
	}
}

func testReplayPullRequest(pull fakegithub.PullRequest) replay.PullRequest {
	return replay.PullRequest{
		ID:             pull.ID,
		NodeID:         pull.NodeID,
		Number:         pull.Number,
		Title:          pull.Title,
		State:          pull.State,
		Draft:          pull.Draft,
		Merged:         pull.MergedAt != nil,
		AuthorLogin:    pull.AuthorLogin,
		ReviewDecision: pull.ReviewDecision,
		MergeableState: pull.MergeableState,
		Head: replay.Branch{
			Ref: pull.Head.Ref,
			SHA: pull.Head.SHA,
		},
		Base: replay.Branch{
			Ref: pull.Base.Ref,
			SHA: pull.Base.SHA,
		},
		CreatedAt: pull.CreatedAt,
		UpdatedAt: pull.UpdatedAt,
		MergedAt:  pull.MergedAt,
	}
}

func testReplaySteps(deliveries int) []replay.Step {
	steps := make([]replay.Step, 0, deliveries+3)
	for sequence := 1; sequence <= deliveries; sequence++ {
		steps = append(steps, testReplayStep(sequence))
	}
	return append(steps, testOracleCoverageSteps()...)
}

func testOracleCoverageSteps() []replay.Step {
	fixture := fakegithub.DefaultFixture()
	repository := replay.Repository{
		ID:               fixture.Repository.ID,
		NodeID:           fixture.Repository.NodeID,
		Owner:            fixture.Repository.Owner,
		Name:             fixture.Repository.Name,
		DefaultBranch:    fixture.Repository.DefaultBranch,
		DefaultBranchSHA: fixture.Repository.DefaultBranchSHA,
		UpdatedAt:        fixture.Repository.UpdatedAt,
	}
	pull := fixture.PullRequests[1]
	stackPulls := []replay.PullRequest{
		testReplayPullRequest(fixture.PullRequests[0]),
		testReplayPullRequest(fixture.PullRequests[1]),
	}
	return []replay.Step{
		{
			Seq: 1001,
			Mutation: replay.FixtureMutation{
				Kind:       "stack",
				Repository: repository,
				Stack: &replay.Stack{
					ID:                fixture.Stacks[0].ID,
					Number:            fixture.Stacks[0].Number,
					Base:              replay.Branch{Ref: "main", SHA: "aaaa000"},
					PullRequests:      []int{4810, 4812},
					PullRequestStates: stackPulls,
				},
			},
		},
		{
			Seq: 1002,
			Mutation: replay.FixtureMutation{
				Kind:       "check_run",
				Repository: repository,
				CheckRun: &replay.CheckRun{
					ID:         fixture.CheckRuns[0].ID,
					NodeID:     fixture.CheckRuns[0].NodeID,
					HeadSHA:    fixture.CheckRuns[0].HeadSHA,
					Name:       fixture.CheckRuns[0].Name,
					Status:     fixture.CheckRuns[0].Status,
					DetailsURL: fixture.CheckRuns[0].DetailsURL,
				},
			},
		},
		{
			Seq: 1003,
			Mutation: replay.FixtureMutation{
				Kind:        "review_thread",
				Repository:  repository,
				PullRequest: testReplayStep(1).Mutation.PullRequest,
				ReviewThread: &replay.ReviewThread{
					ID:   pull.ReviewThreads[0].ID,
					Path: pull.ReviewThreads[0].Path,
				},
			},
		},
	}
}

func TestMetricValueSumsLabeledStarvationSeries(t *testing.T) {
	t.Parallel()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE ghsync_c_b3_starvations_total counter
ghsync_c_b3_starvations_total 0
ghsync_c_b3_starvations_total{class="event",resource="rest"} 3
ghsync_c_b3_starvations_total{class="sweep",resource="graphql"} 2
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := metricValue(
		families,
		"ghsync_c_b3_starvations_total",
		nil,
	); got != 5 {
		t.Fatalf("unfiltered starvation total = %v, want 5", got)
	}
	families, err = parser.TextToMetricFamilies(strings.NewReader(`
# TYPE ghsync_c_b1_github_requests_total counter
ghsync_c_b1_github_requests_total{class="event",resource="rest",status="500"} 3
ghsync_c_b1_github_requests_total{class="sweep",resource="rest",status="500"} 2
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := githubStatusResponses(families, 500); got != 5 {
		t.Fatalf("partially labeled HTTP 500 total = %v, want 5", got)
	}
}

func TestHistogramDeltasProduceRunScopedConstraintQuantiles(t *testing.T) {
	t.Parallel()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	priorFamilies, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE ghsync_c_q2_event_to_cache_latency_seconds histogram
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="20"} 90
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="60"} 99
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="+Inf"} 100
ghsync_c_q2_event_to_cache_latency_seconds_sum 1000
ghsync_c_q2_event_to_cache_latency_seconds_count 100
`))
	if err != nil {
		t.Fatal(err)
	}
	currentFamilies, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE ghsync_c_q2_event_to_cache_latency_seconds histogram
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="20"} 100
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="60"} 109
ghsync_c_q2_event_to_cache_latency_seconds_bucket{le="+Inf"} 110
ghsync_c_q2_event_to_cache_latency_seconds_sum 1100
ghsync_c_q2_event_to_cache_latency_seconds_count 110
`))
	if err != nil {
		t.Fatal(err)
	}
	prior := histogramState(
		priorFamilies["ghsync_c_q2_event_to_cache_latency_seconds"],
	)
	current := histogramState(
		currentFamilies["ghsync_c_q2_event_to_cache_latency_seconds"],
	)
	delta, err := subtractHistogram(current, prior)
	if err != nil {
		t.Fatal(err)
	}
	p95, err := histogramQuantileSnapshot(delta, 0.95)
	if err != nil {
		t.Fatal(err)
	}
	if delta.count != 10 || delta.buckets[20] != 10 || p95 != 20 {
		t.Fatalf("histogram delta=%+v p95=%v", delta, p95)
	}
}

func TestC_R1StalenessAssertionUsesEntityBounds(t *testing.T) {
	t.Parallel()
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(`
# TYPE ghsync_c_r1_cache_staleness_seconds gauge
ghsync_c_r1_cache_staleness_seconds{entity_class="open_pr"} 9
# TYPE ghsync_c_r1_staleness_bound_seconds gauge
ghsync_c_r1_staleness_bound_seconds{entity_class="open_pr"} 10
`))
	if err != nil {
		t.Fatal(err)
	}
	samples, err := assertStalenessWithinBounds(families)
	if err != nil || samples != 1 {
		t.Fatalf("within-bound result = %d, %v", samples, err)
	}
	staleness := families["ghsync_c_r1_cache_staleness_seconds"]
	if staleness == nil || len(staleness.Metric) == 0 ||
		staleness.Metric[0] == nil || staleness.Metric[0].Gauge == nil {
		t.Fatal("parsed staleness metric is incomplete")
	}
	staleness.Metric[0].Gauge.Value = new(11.0)
	if _, err := assertStalenessWithinBounds(families); err == nil {
		t.Fatal("C-R1 accepted staleness beyond the configured bound")
	}
}

func TestDroppedDeliveryHealingRequiresProcessedRedeliveryWithinBound(
	t *testing.T,
) {
	t.Parallel()
	database := testdb.New(t)
	ctx := context.Background()
	droppedAt := time.Now().UTC().Add(-time.Second)
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO webhook_deliveries (
		    delivery_guid, event, raw_body, headers, received_at, status
		)
		VALUES ('dropped-guid', 'pull_request', '{}', '{}', $1, 'processed')
	`, droppedAt.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	drops := map[string]time.Time{"dropped-guid": droppedAt}
	if err := assertDroppedDeliveriesHealed(
		ctx,
		database.Pool,
		drops,
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if err := assertDroppedDeliveriesHealed(
		ctx,
		database.Pool,
		drops,
		100*time.Millisecond,
	); err == nil {
		t.Fatal("late dropped-delivery healing passed a tighter C-R1 bound")
	}
}

func TestLoadStreamConsumerAppliesExactlyOnceAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	database := testdb.New(t)

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	consumer, err := newLoadStreamConsumer(ctx, database.Pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.cleanup(ctx)
	if err := consumer.start(ctx); err != nil {
		t.Fatal(err)
	}
	insertLoadStreamEvents(t, ctx, database.Pool, "before", 2)
	waitForLoadStreamConsumer(t, ctx, consumer, 2)
	if err := consumer.restart(ctx); err != nil {
		t.Fatal(err)
	}
	insertLoadStreamEvents(t, ctx, database.Pool, "after", 2)
	waitForLoadStreamConsumer(t, ctx, consumer, 4)
	if err := consumer.stop(ctx); err != nil {
		t.Fatal(err)
	}
	counts, err := consumer.assertFinal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.total != 4 || counts.distinct != 4 || counts.expected != 4 {
		t.Fatalf("stream counts = %+v, want four exactly once", counts)
	}
}

func insertLoadStreamEvents(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	prefix string,
	count int,
) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
	}()
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var maxSeq int64
	for index := range count {
		if err := tx.QueryRow(ctx, `
			INSERT INTO change_events (
			    stream, kind, entity_key, payload
			)
			VALUES (
			    'entities', 'pull_request.changed', $1, '{"version":1}'
			)
			RETURNING seq
		`, prefix+"-"+strconv.Itoa(index)).Scan(&maxSeq); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE stream_watermark
		SET safe_seq = GREATEST(safe_seq, $1),
		    updated_at = clock_timestamp()
		WHERE singleton
	`, maxSeq); err != nil {
		t.Fatal(err)
	}
}

func waitForLoadStreamConsumer(
	t *testing.T,
	ctx context.Context,
	consumer *loadStreamConsumer,
	want int64,
) {
	t.Helper()
	for {
		if err := consumer.check(); err != nil {
			t.Fatal(err)
		}
		counts, err := consumer.counts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts.total == want &&
			counts.distinct == want &&
			counts.expected == want &&
			counts.cursor == counts.maxSeq {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"stream consumer did not reach %d applications: %+v (%v)",
				want,
				counts,
				ctx.Err(),
			)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func TestFullFixtureOracleComparesEveryCacheFamily(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	line := 17
	stackNumber := 9
	stackPosition := 1
	repository := fakegithub.Repository{
		ID:               7001,
		NodeID:           "R_oracle",
		Owner:            "acme",
		Name:             "oracle",
		FullName:         "acme/oracle",
		DefaultBranch:    "main",
		DefaultBranchSHA: "base",
		UpdatedAt:        now,
		PushedAt:         now,
	}
	comments := []store.ReviewCommentRecord{{
		ID:          "C1",
		Body:        "oracle comment",
		UpdatedAt:   now,
		AuthorLogin: "reviewer",
	}}
	entries := []store.StackEntry{{
		Number:    42,
		State:     "open",
		Draft:     false,
		UpdatedAt: now,
		HeadRef:   "feature",
		HeadSHA:   "head",
	}}
	encodedComments, _ := json.Marshal(comments)
	encodedEntries, _ := json.Marshal(entries)
	checkTruth := fakegithub.TruthCheckRunSnapshot{
		ID:          10001,
		NodeID:      "CR_oracle",
		HeadSHA:     "head",
		Name:        "unit",
		Status:      "completed",
		Conclusion:  "success",
		DetailsURL:  "https://example.test/check",
		AppSlug:     "actions",
		StartedAt:   &now,
		CompletedAt: &now,
	}
	checkSemantic := checkSemanticVersion(checkTruth)
	ctx := context.Background()
	var repoID int64
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO repos (
		    installation_id, org_id, gh_id, node_id, owner, name,
		    full_name, default_branch, gh_updated_at, head_sha,
		    synced_at, last_checked_at, sync_source
		)
		VALUES (1, 1, 7001, 'R_oracle', 'acme', 'oracle',
		        'acme/oracle', 'main', $1, 'base', $1, $1, 'webhook')
		RETURNING id
	`, now).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO pull_requests (
		    repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, stack_number, stack_position,
		    gh_updated_at, synced_at, last_checked_at, sync_source
		)
		VALUES (
		    $1, 8001, 'PR_oracle', 42, 'Oracle PR', 'open', false,
		    'author', 'feature', 'head', 'main', '',
		    'APPROVED', 'MERGEABLE', 9, 1, $2, $2, $2, 'webhook'
		)
	`, repoID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO stacks (
		    repo_id, gh_id, node_id, number, base_ref, base_sha, open,
		    entries, gh_updated_at, head_sha, synced_at, last_checked_at,
		    sync_source
		)
		VALUES (
		    $1, 9001, 'S_oracle', 9, 'main', '', true,
		    $2, $3, 'head', $3, $3, 'webhook'
		)
	`, repoID, encodedEntries, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO review_threads (
		    id, repo_id, pr_number, is_resolved, is_outdated, path, line,
		    comments, gh_updated_at, head_sha, synced_at, last_checked_at,
		    sync_source
		)
		VALUES (
		    'T1', $1, 42, true, false, 'file.go', 17,
		    $2, $3, 'head', $3, $3, 'webhook'
		)
	`, repoID, encodedComments, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, conclusion,
		    details_url, app_slug, started_at, completed_at,
		    gh_updated_at, semantic_version, head_sha, synced_at, last_checked_at,
		    sync_source
		)
		VALUES (
		    10001, $1, 'CR_oracle', 'unit', 'completed', 'success',
		    'https://example.test/check', 'actions', $2, $2,
		    $2, $3, 'head', $2, $2, 'webhook'
		)
	`, repoID, now, checkSemantic); err != nil {
		t.Fatal(err)
	}
	truth := fakegithub.TruthFixtureSnapshot{
		Repository: repository,
		PullRequests: []fakegithub.TruthPullRequestSnapshot{{
			ID:             8001,
			NodeID:         "PR_oracle",
			Number:         42,
			Title:          "Oracle PR",
			State:          "open",
			Draft:          false,
			AuthorLogin:    "author",
			ReviewDecision: "APPROVED",
			MergeableState: "MERGEABLE",
			Head:           fakegithub.PullRequestBranch{Ref: "feature", SHA: "head"},
			Base:           fakegithub.Base{Ref: "main", SHA: ""},
			UpdatedAt:      now,
			Stack: &fakegithub.StackRef{
				ID:       9001,
				Number:   stackNumber,
				Size:     1,
				Position: stackPosition,
				Base:     fakegithub.Base{Ref: "main", SHA: ""},
			},
		}},
		Stacks: []fakegithub.Stack{{
			ID:        9001,
			NodeID:    "S_oracle",
			Number:    9,
			Base:      fakegithub.Base{Ref: "main", SHA: ""},
			Open:      true,
			UpdatedAt: now,
			PullRequests: []fakegithub.StackPullRequest{{
				Number:    42,
				State:     "open",
				Draft:     false,
				UpdatedAt: now,
				Head:      fakegithub.PullRequestBranch{Ref: "feature", SHA: "head"},
			}},
		}},
		CheckRuns: []fakegithub.TruthCheckRunSnapshot{checkTruth},
		ReviewThreads: []fakegithub.TruthReviewThreadSnapshot{{
			PullRequest: 42,
			ID:          "T1",
			IsResolved:  true,
			Path:        "file.go",
			Line:        &line,
			Comments: []fakegithub.ReviewComment{{
				ID:          "C1",
				Body:        "oracle comment",
				UpdatedAt:   now,
				AuthorLogin: "reviewer",
			}},
			UpdatedAt: now,
		}},
	}
	if err := assertFixtureConverged(ctx, database.Pool, truth); err != nil {
		t.Fatal(err)
	}
	truth.Stacks[0].UpdatedAt = now.Add(time.Second)
	if err := assertFixtureConverged(ctx, database.Pool, truth); err == nil {
		t.Fatal("oracle accepted a mismatched stack timestamp")
	}
	truth.Stacks[0].UpdatedAt = now
	truth.CheckRuns[0].DetailsURL = "https://example.test/wrong"
	if err := assertFixtureConverged(ctx, database.Pool, truth); err == nil {
		t.Fatal("oracle accepted a mismatched check-run field")
	}
	truth.CheckRuns[0].DetailsURL = "https://example.test/check"
	truth.ReviewThreads[0].UpdatedAt = now.Add(time.Second)
	if err := assertFixtureConverged(ctx, database.Pool, truth); err == nil {
		t.Fatal("oracle accepted a mismatched review-thread timestamp")
	}
	truth.ReviewThreads[0].UpdatedAt = now
	truth.PullRequests[0].Title = "wrong"
	if err := assertFixtureConverged(ctx, database.Pool, truth); err == nil {
		t.Fatal("oracle accepted a mismatched pull-request title")
	}
	truth = fakegithub.TruthFixtureSnapshot{Repository: repository}
	if err := assertFixtureConverged(ctx, database.Pool, truth); err == nil {
		t.Fatal("oracle accepted four empty entity sets")
	}
}

func TestControlFaultRequestEncodingUsesDurationNanoseconds(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]any{
		"retry_after": time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`1000000000`)) {
		t.Fatalf("fault request encoding = %s", body)
	}
}
