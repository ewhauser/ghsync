package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/ingress"
	"github.com/ewhauser/ghsync/internal/replay"
)

type replayChaos struct {
	duplicateEvery int
	reorderWindow  int
	dropEvery      int
}

type plannedDelivery struct {
	atMS     int64
	sequence int
	value    fakegithub.ControlDelivery
}

type replayPlan struct {
	steps         []replay.Step
	chaos         replayChaos
	deliveryCount int
	dropped       int
	duplicates    int
	guids         []string
	expected      map[string]expectedEntityKeys
	droppedAt     map[string]time.Time
	progress      atomic.Int64
}

type expectedEntityKeys struct {
	pulls   map[int]struct{}
	stacks  map[int]struct{}
	checks  map[int64]struct{}
	threads map[string]struct{}
}

type replayResult struct {
	deliveries int
	dropped    int
	duplicates int
	err        error
}

func newReplayPlan(
	steps []replay.Step,
	chaos replayChaos,
) (*replayPlan, error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("replay plan has no steps")
	}
	if chaos.duplicateEvery < 0 || chaos.reorderWindow < 0 ||
		chaos.dropEvery < 0 {
		return nil, fmt.Errorf("replay chaos counts cannot be negative")
	}
	if chaos.reorderWindow == 1 {
		return nil, fmt.Errorf("reorder window must be zero (off) or at least two")
	}
	plan := &replayPlan{
		steps:     append([]replay.Step(nil), steps...),
		chaos:     chaos,
		expected:  make(map[string]expectedEntityKeys),
		droppedAt: make(map[string]time.Time),
	}
	seen := make(map[string]struct{})
	sequence := 0
	for _, step := range steps {
		if err := plan.recordExpected(step.Mutation); err != nil {
			return nil, fmt.Errorf(
				"compiled step %d truth coverage: %w",
				step.Seq,
				err,
			)
		}
		for _, delivery := range step.Deliveries {
			sequence++
			if delivery.GUID == "" || delivery.Event == "" ||
				len(delivery.Payload) == 0 {
				return nil, fmt.Errorf(
					"compiled delivery %d is incomplete",
					sequence,
				)
			}
			if _, duplicate := seen[delivery.GUID]; duplicate {
				return nil, fmt.Errorf(
					"compiled delivery GUID %q is not unique",
					delivery.GUID,
				)
			}
			seen[delivery.GUID] = struct{}{}
			plan.guids = append(plan.guids, delivery.GUID)
			if every(sequence, chaos.dropEvery) {
				plan.dropped++
			}
			if every(sequence, chaos.duplicateEvery) {
				plan.duplicates++
			}
		}
	}
	plan.deliveryCount = sequence
	if plan.deliveryCount == 0 {
		return nil, fmt.Errorf("compiled replay has no deliveries")
	}
	if chaos.duplicateEvery > 0 && plan.duplicates == 0 {
		return nil, fmt.Errorf(
			"--duplicate-every=%d injects no duplicate into %d deliveries",
			chaos.duplicateEvery,
			plan.deliveryCount,
		)
	}
	if chaos.dropEvery > 0 && plan.dropped == 0 {
		return nil, fmt.Errorf(
			"--drop-every=%d injects no drop into %d deliveries",
			chaos.dropEvery,
			plan.deliveryCount,
		)
	}
	if chaos.reorderWindow > 1 && plan.deliveryCount < 2 {
		return nil, fmt.Errorf(
			"--reorder-window=%d cannot reorder %d delivery",
			chaos.reorderWindow,
			plan.deliveryCount,
		)
	}
	if err := plan.validateExpectedCoverage(); err != nil {
		return nil, err
	}
	return plan, nil
}

func (p *replayPlan) recordExpected(
	mutation replay.FixtureMutation,
) error {
	repository := mutation.Repository.FullName()
	if repository == "/" {
		return fmt.Errorf("mutation repository is incomplete")
	}
	keys, ok := p.expected[repository]
	if !ok {
		keys = expectedEntityKeys{
			pulls:   make(map[int]struct{}),
			stacks:  make(map[int]struct{}),
			checks:  make(map[int64]struct{}),
			threads: make(map[string]struct{}),
		}
	}
	switch mutation.Kind {
	case "repository", "commit", "push", "check_suite":
	case "pull_request", "pull_request_review", "review_comment":
		if mutation.PullRequest == nil {
			return fmt.Errorf(
				"%s mutation is missing pull request",
				mutation.Kind,
			)
		}
		keys.pulls[mutation.PullRequest.Number] = struct{}{}
	case "review_thread":
		if mutation.PullRequest == nil || mutation.ReviewThread == nil {
			return fmt.Errorf("review-thread mutation is incomplete")
		}
		keys.pulls[mutation.PullRequest.Number] = struct{}{}
		keys.threads[mutation.ReviewThread.ID] = struct{}{}
	case "check_run":
		if mutation.CheckRun == nil {
			return fmt.Errorf("check-run mutation is missing check run")
		}
		keys.checks[mutation.CheckRun.ID] = struct{}{}
	case "stack":
		if mutation.Stack == nil {
			return fmt.Errorf("stack mutation is missing stack")
		}
		keys.stacks[mutation.Stack.Number] = struct{}{}
		for _, pull := range mutation.Stack.PullRequestStates {
			keys.pulls[pull.Number] = struct{}{}
		}
	default:
		return fmt.Errorf("unsupported mutation kind %q", mutation.Kind)
	}
	p.expected[repository] = keys
	return nil
}

func (p *replayPlan) validateExpectedCoverage() error {
	if len(p.expected) == 0 {
		return fmt.Errorf("compiled replay contains no repository truth")
	}
	for repository, keys := range p.expected {
		if len(keys.pulls) == 0 || len(keys.stacks) == 0 ||
			len(keys.checks) == 0 || len(keys.threads) == 0 {
			return fmt.Errorf(
				"repository %s has incomplete non-vacuous oracle coverage "+
					"(pull_requests=%d stacks=%d check_runs=%d review_threads=%d)",
				repository,
				len(keys.pulls),
				len(keys.stacks),
				len(keys.checks),
				len(keys.threads),
			)
		}
	}
	return nil
}

func every(sequence, interval int) bool {
	return interval > 0 && sequence%interval == 0
}

func replayedProgress(plan *replayPlan) int {
	if plan == nil {
		return 0
	}
	return int(plan.progress.Load())
}

func executeReplay(
	ctx context.Context,
	cfg config,
	plan *replayPlan,
	startedAt time.Time,
) replayResult {
	if plan.chaos.reorderWindow > 1 {
		return executeReorderedReplay(ctx, cfg, plan, startedAt)
	}
	deliverySequence := 0
	for _, step := range plan.steps {
		if err := waitUntil(
			ctx,
			startedAt.Add(time.Duration(step.AtMS)*time.Millisecond),
		); err != nil {
			return replayResult{
				deliveries: deliverySequence,
				err: fmt.Errorf(
					"traffic deadline reached after %d/%d deliveries: %w",
					deliverySequence,
					plan.deliveryCount,
					err,
				),
			}
		}
		deliveries := make([]fakegithub.ControlDelivery, 0, len(step.Deliveries)*2)
		for _, delivery := range step.Deliveries {
			deliverySequence++
			drop := every(deliverySequence, plan.chaos.dropEvery)
			control := controlDelivery(
				delivery,
				drop,
			)
			deliveries = append(deliveries, control)
			if drop {
				plan.droppedAt[delivery.GUID] = time.Now()
			}
			if every(deliverySequence, plan.chaos.duplicateEvery) {
				deliveries = append(deliveries, control)
			}
		}
		if err := control(
			ctx,
			cfg,
			&step.Mutation,
			deliveries,
		); err != nil {
			return replayResult{
				deliveries: deliverySequence - len(step.Deliveries),
				err: fmt.Errorf(
					"apply replay step %d: %w",
					step.Seq,
					err,
				),
			}
		}
		plan.progress.Add(int64(len(step.Deliveries)))
	}
	return replayResult{
		deliveries: deliverySequence,
		dropped:    plan.dropped,
		duplicates: plan.duplicates,
	}
}

func executeReorderedReplay(
	ctx context.Context,
	cfg config,
	plan *replayPlan,
	startedAt time.Time,
) replayResult {
	window := make([]plannedDelivery, 0, plan.chaos.reorderWindow)
	deliverySequence := 0
	completed := 0
	flush := func() error {
		for _, v := range slices.Backward(window) {
			item := v
			if item.value.Drop {
				plan.droppedAt[item.value.GUID] = time.Now()
			}
			if err := control(
				ctx,
				cfg,
				nil,
				[]fakegithub.ControlDelivery{item.value},
			); err != nil {
				return fmt.Errorf(
					"emit reordered delivery %d: %w",
					item.sequence,
					err,
				)
			}
			if every(item.sequence, plan.chaos.duplicateEvery) {
				if err := control(
					ctx,
					cfg,
					nil,
					[]fakegithub.ControlDelivery{item.value},
				); err != nil {
					return fmt.Errorf(
						"emit duplicate delivery %d: %w",
						item.sequence,
						err,
					)
				}
			}
			completed++
			plan.progress.Add(1)
		}
		window = window[:0]
		return nil
	}
	for _, step := range plan.steps {
		if err := waitUntil(
			ctx,
			startedAt.Add(time.Duration(step.AtMS)*time.Millisecond),
		); err != nil {
			return replayResult{
				deliveries: completed,
				err: fmt.Errorf(
					"traffic deadline reached after %d/%d deliveries: %w",
					completed,
					plan.deliveryCount,
					err,
				),
			}
		}
		if err := control(ctx, cfg, &step.Mutation, nil); err != nil {
			return replayResult{
				deliveries: completed,
				err: fmt.Errorf(
					"apply replay mutation %d: %w",
					step.Seq,
					err,
				),
			}
		}
		for _, delivery := range step.Deliveries {
			deliverySequence++
			window = append(window, plannedDelivery{
				atMS:     step.AtMS,
				sequence: deliverySequence,
				value: controlDelivery(
					delivery,
					every(deliverySequence, plan.chaos.dropEvery),
				),
			})
			if len(window) == plan.chaos.reorderWindow {
				if err := flush(); err != nil {
					return replayResult{deliveries: completed, err: err}
				}
			}
		}
	}
	if err := flush(); err != nil {
		return replayResult{deliveries: completed, err: err}
	}
	return replayResult{
		deliveries: completed,
		dropped:    plan.dropped,
		duplicates: plan.duplicates,
	}
}

func controlDelivery(
	delivery replay.Delivery,
	drop bool,
) fakegithub.ControlDelivery {
	return fakegithub.ControlDelivery{
		Event:   delivery.Event,
		GUID:    delivery.GUID,
		Payload: delivery.Payload,
		Drop:    drop,
	}
}

func control(
	ctx context.Context,
	cfg config,
	mutation *replay.FixtureMutation,
	deliveries []fakegithub.ControlDelivery,
) error {
	requestBody, err := json.Marshal(struct {
		TargetURL             string                       `json:"target_url,omitempty"`
		Mutation              *replay.FixtureMutation      `json:"mutation,omitempty"`
		Deliveries            []fakegithub.ControlDelivery `json:"deliveries,omitempty"`
		AllowDeliveryFailures bool                         `json:"allow_delivery_failures,omitempty"`
	}{
		TargetURL:             cfg.engineURL + ingress.WebhookPath,
		Mutation:              mutation,
		Deliveries:            deliveries,
		AllowDeliveryFailures: cfg.restartAfter > 0,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		cfg.fakeGitHubURL+fakegithub.ControlEmitPath,
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"fake control returned %d: %s",
			response.StatusCode,
			string(bytes.TrimSpace(message)),
		)
	}
	return nil
}

func configureFaults(ctx context.Context, cfg config) error {
	requestBody, err := json.Marshal(map[string]any{
		"internal_errors": cfg.fake500Burst,
		"rate_limits":     cfg.fake429Burst,
		"retry_after":     cfg.fake429Retry,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		cfg.fakeGitHubURL+fakegithub.ControlFaultPath,
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := cfg.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("configure fake faults: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusNoContent {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"configure fake faults status %d: %s",
			response.StatusCode,
			string(bytes.TrimSpace(message)),
		)
	}
	return nil
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// reorderedSequences is kept small and deterministic so unit tests can prove
// the bounded-reordering contract without timing or HTTP.
func reorderedSequences(count, window int) []int {
	values := make([]int, count)
	for index := range values {
		values[index] = index + 1
	}
	if window <= 1 {
		return values
	}
	for start := 0; start < len(values); start += window {
		end := min(start+window, len(values))
		sort.Sort(sort.Reverse(sort.IntSlice(values[start:end])))
	}
	return values
}
