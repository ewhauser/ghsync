package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/outbox"
	streammaint "github.com/ewhauser/ghsync/internal/stream"
	"github.com/ewhauser/ghsync/internal/testdb"
	"github.com/ewhauser/ghsync/pkg/streamclient"
)

var exampleAPITestID atomic.Int64

const (
	testContextTimeout = 45 * time.Second
	testWaitTimeout    = 15 * time.Second
)

func TestExampleAPIHelpLabelsReferenceExample(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	_, err := parseConfig(
		[]string{"--help"},
		func(string) string { return "" },
		&output,
	)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseConfig --help error = %v", err)
	}
	help := output.String()
	if !strings.Contains(help, "reference example") ||
		!strings.Contains(help, "not a production service") {
		t.Fatalf("help output does not label the example:\n%s", help)
	}
}

func TestExampleAPIRejectsUnboundedBuffers(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"DATABASE_URL":  "postgres://example.invalid/example",
		"API_RING_SIZE": strconv.Itoa(maximumRingSize + 1),
	}
	_, err := parseConfig(
		nil,
		func(name string) string { return values[name] },
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "API_RING_SIZE must be between") {
		t.Fatalf("parseConfig oversized ring error = %v", err)
	}
}

func TestEventHubDoesNotDeliverAtOrBelowResumeSequence(t *testing.T) {
	t.Parallel()
	hub := newEventHub(4, 4)
	hub.initialize(1)
	hub.mu.Lock()
	hub.updateKnownSafeLocked(5)
	hub.mu.Unlock()
	current, replay, status := hub.subscribeFromRing(5)
	if status != subscriptionCovered || current == nil || len(replay) != 0 {
		t.Fatalf("subscription status=%d current=%v replay=%v", status, current, replay)
	}
	defer hub.unsubscribe(current)
	hub.publish(materializedEvent{Seq: 3, Kind: "test"})
	select {
	case event := <-current.events:
		t.Fatalf("delivered pre-resume event %#v", event)
	default:
	}
	hub.publish(materializedEvent{Seq: 6, Kind: "test"})
	select {
	case event := <-current.events:
		if event.Seq != 6 {
			t.Fatalf("delivered seq %d, want 6", event.Seq)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("post-resume event was not delivered")
	}
}

func TestEventHubDropsSlowSubscriberWithResync(t *testing.T) {
	t.Parallel()
	hub := newEventHub(4, 1)
	hub.initialize(0)
	current, _, status := hub.subscribeFromRing(0)
	if status != subscriptionCovered || current == nil {
		t.Fatal("initial ring subscription was not covered")
	}
	hub.publish(materializedEvent{Seq: 1, Kind: "test"})
	hub.publish(materializedEvent{Seq: 2, Kind: "test"})
	select {
	case control := <-current.control:
		if control.resync == nil || control.resync.Reason != "slow_client" {
			t.Fatalf("slow-client control = %#v", control)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("slow subscriber did not receive a resync advisory")
	}
}

func TestEventHubSnapshotHandoffReplaysConcurrentEventExactlyOnce(t *testing.T) {
	t.Parallel()
	hub := newEventHub(4, 4)
	hub.initialize(5)
	hub.publish(materializedEvent{Seq: 6, Kind: "test"})
	current, replay, status := hub.subscribeAfterSnapshot(5)
	if status != subscriptionCovered || current == nil {
		t.Fatal("snapshot handoff was not covered by the ring")
	}
	defer hub.unsubscribe(current)
	if len(replay) != 1 || replay[0].Seq != 6 {
		t.Fatalf("snapshot handoff replay = %#v", replay)
	}
	hub.publish(materializedEvent{Seq: 6, Kind: "test"})
	hub.publish(materializedEvent{Seq: 7, Kind: "test"})
	select {
	case event := <-current.events:
		if event.Seq != 7 {
			t.Fatalf("live handoff event seq = %d, want 7", event.Seq)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("live handoff event was not delivered")
	}
	select {
	case duplicate := <-current.events:
		t.Fatalf("snapshot handoff duplicated an event: %#v", duplicate)
	default:
	}
}

func TestEventHubCircularRingPreservesSequenceOrder(t *testing.T) {
	t.Parallel()
	hub := newEventHub(3, 4)
	hub.initialize(0)
	for sequence := int64(1); sequence <= 5; sequence++ {
		hub.publish(materializedEvent{Seq: sequence, Kind: "test"})
	}
	current, replay, status := hub.subscribeFromRing(2)
	if status != subscriptionCovered || current == nil {
		t.Fatalf("wrapped ring subscription status=%d", status)
	}
	defer hub.unsubscribe(current)
	if len(replay) != 3 {
		t.Fatalf("wrapped ring replay = %#v", replay)
	}
	for index, want := range []int64{3, 4, 5} {
		if replay[index].Seq != want {
			t.Fatalf("wrapped ring replay[%d] = %d, want %d", index, replay[index].Seq, want)
		}
	}
}

func TestEventHubConcurrentSubscriberChurn(t *testing.T) {
	t.Parallel()
	hub := newEventHub(32, 4)
	hub.initialize(0)
	const subscriberWorkers = 16
	const subscriptionsPerWorker = 100
	var subscribers sync.WaitGroup
	subscribers.Add(subscriberWorkers)
	for range subscriberWorkers {
		go func() {
			defer subscribers.Done()
			for range subscriptionsPerWorker {
				hub.mu.Lock()
				from := hub.throughSeq
				hub.mu.Unlock()
				current, _, status := hub.subscribeFromRing(from)
				if status == subscriptionCovered {
					hub.unsubscribe(current)
				}
			}
		}()
	}
	for sequence := int64(1); sequence <= 1_000; sequence++ {
		hub.publish(materializedEvent{Seq: sequence, Kind: "test"})
	}
	subscribers.Wait()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if len(hub.subscribers) != 0 {
		t.Fatalf("subscriber registry retained %d entries", len(hub.subscribers))
	}
}

func TestRESTEndpointsReadSeededMirrorEntities(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	advanceWatermark(t, ctx, database.Pool, fixture.safeSeq)
	api := startTestAPI(t, database.Pool, 8)

	assertHealth(t, api.server.URL+"/healthz")
	var pullList struct {
		PullRequests []map[string]any `json:"pull_requests"`
	}
	getJSON(t, api.server.URL+"/v1/pull-requests?limit=1", &pullList)
	if len(pullList.PullRequests) != 1 ||
		pullList.PullRequests[0]["title"] != "initial title" {
		t.Fatalf("pull request list = %#v", pullList.PullRequests)
	}
	var pull map[string]any
	getJSON(t, api.server.URL+"/v1/pull-requests/7", &pull)
	if pull["head_sha"] != fixture.headSHA {
		t.Fatalf("pull request = %#v", pull)
	}
	var stackList struct {
		Stacks []map[string]any `json:"stacks"`
	}
	getJSON(t, api.server.URL+"/v1/stacks", &stackList)
	if len(stackList.Stacks) != 1 || stackList.Stacks[0]["number"] != float64(3) {
		t.Fatalf("stack list = %#v", stackList.Stacks)
	}
	var stack map[string]any
	getJSON(t, api.server.URL+"/v1/stacks/3", &stack)
	if stack["head_sha"] != fixture.headSHA {
		t.Fatalf("stack = %#v", stack)
	}
	var checks struct {
		Checks []map[string]any `json:"checks"`
	}
	getJSON(t, api.server.URL+"/v1/checks/"+fixture.headSHA, &checks)
	if len(checks.Checks) != 1 || checks.Checks[0]["name"] != "test" {
		t.Fatalf("checks = %#v", checks.Checks)
	}
	assertOneConsumerCursor(t, database.Pool, api.consumer)
}

func TestMaterializeChangedEntityToleratesTombstonedAndAbsentRows(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	event := streamclient.Event{
		Seq:       fixture.safeSeq + 1,
		Stream:    entityStream,
		Kind:      "pull_request.changed",
		EntityKey: "pr:1:1001:7",
	}

	modifyPullRequestWithoutEvent(t, ctx, database.Pool, `
		UPDATE pull_requests
		SET tombstoned_at = clock_timestamp()
		WHERE repo_id = 101 AND number = 7
	`)
	materialized, supported, err := materializeChange(ctx, database.Pool, &event)
	if err != nil || !supported || !materialized.Tombstone {
		t.Fatalf(
			"tombstoned point read supported=%t event=%#v err=%v",
			supported,
			materialized,
			err,
		)
	}

	modifyPullRequestWithoutEvent(t, ctx, database.Pool, `
		DELETE FROM pull_requests
		WHERE repo_id = 101 AND number = 7
	`)
	materialized, supported, err = materializeChange(ctx, database.Pool, &event)
	if err != nil || !supported || !materialized.Tombstone {
		t.Fatalf(
			"absent point read supported=%t event=%#v err=%v",
			supported,
			materialized,
			err,
		)
	}
}

func TestMaterializeSupportedEntityKinds(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	tests := []struct {
		kind          string
		entityKey     string
		wantTombstone bool
	}{
		{kind: "repository.changed", entityKey: "repo:1:1001"},
		{kind: "pull_request.changed", entityKey: "pr:1:1001:7"},
		{kind: "stack.changed", entityKey: "stack:1:1001:3"},
		{
			kind:      "checks.changed",
			entityKey: "checks:1:1001:" + fixture.headSHA,
		},
		{
			kind:          "repo_rules.changed",
			entityKey:     "repo_rules:1:1001",
			wantTombstone: true,
		},
	}
	for index, test := range tests {
		event := streamclient.Event{
			Seq:       fixture.safeSeq + int64(index) + 1,
			Stream:    entityStream,
			Kind:      test.kind,
			EntityKey: test.entityKey,
		}
		materialized, supported, err := materializeChange(
			ctx,
			database.Pool,
			&event,
		)
		if err != nil || !supported {
			t.Fatalf("materialize %s supported=%t err=%v", test.kind, supported, err)
		}
		if materialized.Tombstone != test.wantTombstone {
			t.Fatalf(
				"materialize %s tombstone=%t, want %t",
				test.kind,
				materialized.Tombstone,
				test.wantTombstone,
			)
		}
		if !test.wantTombstone && len(materialized.Entity) == 0 {
			t.Fatalf("materialize %s returned no entity", test.kind)
		}
	}
}

func TestRunContextGracefulShutdown(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	consumer := fmt.Sprintf(
		"example-api-shutdown-%d",
		exampleAPITestID.Add(1),
	)
	cfg := &apiConfig{
		databaseURL:      database.URL,
		databaseAuth:     config.DatabaseAuthPassword,
		addr:             "127.0.0.1:0",
		consumerName:     consumer,
		ringSize:         4,
		subscriberBuffer: 4,
		replayLimit:      16,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- runContext(ctx, cfg)
	}()
	waitForConsumerCursorRow(t, database.Pool, consumer)
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("graceful shutdown = %v", err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("example API did not shut down")
	}
}

func TestWatchSnapshotThenLiveHonorsSafeWatermark(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	advanceWatermark(t, ctx, database.Pool, fixture.safeSeq)
	api := startTestAPI(t, database.Pool, 8)
	stream := openSSE(t, api.server.URL+"/v1/watch", "")
	defer stream.close()

	var sawPull bool
	for {
		event := nextSSE(t, stream.events)
		if event.name == "snapshot" {
			if event.id != "" {
				t.Fatalf("snapshot row reused resume id %q", event.id)
			}
			var payload materializedEvent
			decodeSSEData(t, event, &payload)
			if payload.Kind == "pull_request.snapshot" {
				if !bytes.Contains(payload.Entity, []byte("initial title")) {
					t.Fatalf("pull request snapshot = %s", payload.Entity)
				}
				sawPull = true
			}
		}
		if event.name == "snapshot-complete" {
			if event.id != strconv.FormatInt(fixture.safeSeq, 10) {
				t.Fatalf("snapshot completion id = %q", event.id)
			}
			break
		}
	}
	if !sawPull {
		t.Fatal("snapshot did not include the seeded pull request")
	}
	assertOneConsumerCursor(t, database.Pool, api.consumer)

	heldSeq := updatePullRequest(
		t,
		ctx,
		database.Pool,
		"held above watermark",
		time.Now().UTC(),
	)
	if cursor := readConsumerCursor(t, database.Pool, api.consumer); cursor != fixture.safeSeq {
		t.Fatalf("cursor advanced above watermark to %d, want %d", cursor, fixture.safeSeq)
	}

	advanceWatermark(t, ctx, database.Pool, heldSeq)
	event := nextSSE(t, stream.events)
	if event.name != "change" || event.id != strconv.FormatInt(heldSeq, 10) {
		t.Fatalf("live event = %#v, want change id %d", event, heldSeq)
	}
	var payload materializedEvent
	decodeSSEData(t, event, &payload)
	if payload.Seq != heldSeq ||
		!bytes.Contains(payload.Entity, []byte("held above watermark")) {
		t.Fatalf("live payload = %#v entity=%s", payload, payload.Entity)
	}
	waitConsumerCursor(t, database.Pool, api.consumer, heldSeq)
}

func TestWatchResumeUsesRingAndDatabaseFallbackExactlyOnce(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	advanceWatermark(t, ctx, database.Pool, fixture.safeSeq)
	api := startTestAPI(t, database.Pool, 1)

	firstSeq := updatePullRequest(
		t,
		ctx,
		database.Pool,
		"ring replay",
		time.Now().UTC(),
	)
	advanceWatermark(t, ctx, database.Pool, firstSeq)
	waitConsumerCursor(t, database.Pool, api.consumer, firstSeq)
	ring := openSSE(
		t,
		api.server.URL+"/v1/watch",
		strconv.FormatInt(fixture.safeSeq, 10),
	)
	event := nextSSE(t, ring.events)
	if event.name != "change" || event.id != strconv.FormatInt(firstSeq, 10) {
		t.Fatalf("ring replay = %#v", event)
	}
	ring.close()

	secondSeq := updatePullRequest(
		t,
		ctx,
		database.Pool,
		"fallback second",
		time.Now().UTC(),
	)
	advanceWatermark(t, ctx, database.Pool, secondSeq)
	waitConsumerCursor(t, database.Pool, api.consumer, secondSeq)
	thirdSeq := updatePullRequest(
		t,
		ctx,
		database.Pool,
		"fallback third",
		time.Now().UTC(),
	)
	advanceWatermark(t, ctx, database.Pool, thirdSeq)
	waitConsumerCursor(t, database.Pool, api.consumer, thirdSeq)

	fallback := openSSE(
		t,
		api.server.URL+"/v1/watch",
		strconv.FormatInt(fixture.safeSeq, 10),
	)
	defer fallback.close()
	want := []int64{firstSeq, secondSeq, thirdSeq}
	for _, seq := range want {
		event := nextSSE(t, fallback.events)
		if event.name != "change" || event.id != strconv.FormatInt(seq, 10) {
			t.Fatalf("database replay event = %#v, want id %d", event, seq)
		}
		var payload materializedEvent
		decodeSSEData(t, event, &payload)
		if payload.Seq != seq || !bytes.Contains(payload.Entity, []byte("fallback third")) {
			t.Fatalf("database replay payload = %#v entity=%s", payload, payload.Entity)
		}
	}
	fourthSeq := updatePullRequest(
		t,
		ctx,
		database.Pool,
		"live after fallback",
		time.Now().UTC(),
	)
	advanceWatermark(t, ctx, database.Pool, fourthSeq)
	next := nextSSE(t, fallback.events)
	if next.name != "change" || next.id != strconv.FormatInt(fourthSeq, 10) {
		t.Fatalf("post-fallback event = %#v, want id %d", next, fourthSeq)
	}
	assertOneConsumerCursor(t, database.Pool, api.consumer)
}

func TestWatchResyncsBelowPrunedHorizon(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(
		t,
		ctx,
		database.Pool,
		time.Now().UTC().Add(-8*24*time.Hour),
	)
	advanceWatermark(t, ctx, database.Pool, fixture.safeSeq)
	retention, err := streammaint.NewRetention(
		database.Pool,
		streammaint.RetentionOptions{
			Age:       7 * 24 * time.Hour,
			Period:    time.Hour,
			BatchSize: 100,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := retention.Prune(ctx)
	if err != nil || deleted == 0 {
		t.Fatalf("retention deleted=%d err=%v", deleted, err)
	}
	api := startTestAPI(t, database.Pool, 1)
	stream := openSSE(t, api.server.URL+"/v1/watch", "0")
	defer stream.close()

	advisoryEvent := nextSSE(t, stream.events)
	if advisoryEvent.name != "resync" || advisoryEvent.id != "" {
		t.Fatalf("resync advisory = %#v", advisoryEvent)
	}
	var advisory resyncAdvisory
	decodeSSEData(t, advisoryEvent, &advisory)
	if advisory.Reason != "pruned_horizon" ||
		advisory.PrunedThroughSeq < fixture.safeSeq {
		t.Fatalf("resync payload = %#v", advisory)
	}
	var sawFreshPull bool
	for {
		event := nextSSE(t, stream.events)
		if event.name == "snapshot" {
			var payload materializedEvent
			decodeSSEData(t, event, &payload)
			if payload.Kind == "pull_request.snapshot" &&
				bytes.Contains(payload.Entity, []byte("initial title")) {
				sawFreshPull = true
			}
		}
		if event.name == "snapshot-complete" {
			if event.id != strconv.FormatInt(fixture.safeSeq, 10) {
				t.Fatalf("fresh snapshot id = %q", event.id)
			}
			break
		}
	}
	if !sawFreshPull {
		t.Fatal("fresh snapshot did not contain the current pull request")
	}
}

func TestWatchPreservesTombstoneAcrossDeleteAndRecreate(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), testContextTimeout)
	defer cancel()
	fixture := seedMirror(t, ctx, database.Pool, time.Now().UTC())
	advanceWatermark(t, ctx, database.Pool, fixture.safeSeq)
	api := startTestAPI(t, database.Pool, 8)
	waitConsumerCursor(t, database.Pool, api.consumer, fixture.safeSeq)
	stream := openSSE(
		t,
		api.server.URL+"/v1/watch",
		strconv.FormatInt(fixture.safeSeq, 10),
	)
	defer stream.close()

	tombstoneSeq, recreatedSeq := tombstoneAndRecreatePullRequest(
		t,
		ctx,
		database.Pool,
	)
	advanceWatermark(t, ctx, database.Pool, recreatedSeq)
	tombstone := nextSSE(t, stream.events)
	if tombstone.name != "change" ||
		tombstone.id != strconv.FormatInt(tombstoneSeq, 10) {
		t.Fatalf("tombstone event = %#v", tombstone)
	}
	var tombstonePayload materializedEvent
	decodeSSEData(t, tombstone, &tombstonePayload)
	if !tombstonePayload.Tombstone || len(tombstonePayload.Entity) != 0 {
		t.Fatalf("tombstone payload = %#v", tombstonePayload)
	}
	recreated := nextSSE(t, stream.events)
	if recreated.name != "change" ||
		recreated.id != strconv.FormatInt(recreatedSeq, 10) ||
		recreatedSeq <= tombstoneSeq {
		t.Fatalf("recreated event = %#v", recreated)
	}
	var recreatedPayload materializedEvent
	decodeSSEData(t, recreated, &recreatedPayload)
	if recreatedPayload.Tombstone ||
		!bytes.Contains(recreatedPayload.Entity, []byte("recreated title")) {
		t.Fatalf(
			"recreated payload = %#v entity=%s",
			recreatedPayload,
			recreatedPayload.Entity,
		)
	}
}

func TestWatchDisconnectRemovesSubscriber(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	api := startTestAPI(t, database.Pool, 4)
	stream := openSSE(t, api.server.URL+"/v1/watch", "")
	complete := nextSSE(t, stream.events)
	if complete.name != "snapshot-complete" {
		stream.close()
		t.Fatalf("first empty-snapshot event = %#v", complete)
	}
	waitSubscriberCount(t, api.hub, 1)
	stream.close()
	waitSubscriberCount(t, api.hub, 0)
}

type mirrorFixture struct {
	safeSeq int64
	headSHA string
}

func seedMirror(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	occurredAt time.Time,
) mirrorFixture {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	const headSHA = "0123456789abcdef0123456789abcdef01234567"
	statements := []string{
		`INSERT INTO repos (
		    id, installation_id, org_id, gh_id, node_id, owner, name,
		    full_name, default_branch, archived, gh_updated_at, head_sha,
		    synced_at, etag, sync_source, last_checked_at
		)
		VALUES (
		    101, 1, 1, 1001, 'R_1001', 'acme', 'rocket',
		    'acme/rocket', 'main', false, clock_timestamp(), $1,
		    clock_timestamp(), '', 'manual', clock_timestamp()
		)`,
		`INSERT INTO pull_requests (
		    id, repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, gh_updated_at, synced_at,
		    etag, sync_source, last_checked_at
		)
		VALUES (
		    201, 101, 2001, 'PR_2001', 7, 'initial title', 'open', false,
		    'octocat', 'feature', $1, 'main', 'base-sha', 'approved',
		    'clean', clock_timestamp(), clock_timestamp(), '', 'manual',
		    clock_timestamp()
		)`,
		`INSERT INTO stacks (
		    id, repo_id, gh_id, node_id, number, base_ref, base_sha, open,
		    entries, gh_updated_at, head_sha, synced_at, etag, sync_source,
		    last_checked_at
		)
		VALUES (
		    301, 101, 3001, 'S_3001', 3, 'main', 'base-sha', true,
		    '[{"number":7}]', clock_timestamp(), $1, clock_timestamp(),
		    '', 'manual', clock_timestamp()
		)`,
		`INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, conclusion, details_url,
		    app_slug, gh_updated_at, head_sha, synced_at, etag, sync_source,
		    semantic_version, last_checked_at
		)
		VALUES (
		    4001, 101, 'CR_4001', 'test', 'completed', 'success',
		    'https://example.test/check', 'ci', clock_timestamp(), $1,
		    clock_timestamp(), '', 'manual', 'v1', clock_timestamp()
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement, headSHA); err != nil {
			t.Fatal(err)
		}
	}
	events := []struct {
		kind string
		key  string
	}{
		{kind: "repository.changed", key: "repo:1:1001"},
		{kind: "pull_request.changed", key: "pr:1:1001:7"},
		{kind: "stack.changed", key: "stack:1:1001:3"},
		{kind: "checks.changed", key: "checks:1:1001:" + headSHA},
	}
	var safeSeq int64
	for _, event := range events {
		if err := tx.QueryRow(ctx, `
			INSERT INTO change_events (
			    stream, kind, entity_key, occurred_at, payload
			)
			VALUES ('entities', $1, $2, $3, '{"version":1}')
			RETURNING seq
		`, event.kind, event.key, occurredAt).Scan(&safeSeq); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return mirrorFixture{safeSeq: safeSeq, headSHA: headSHA}
}

func updatePullRequest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	title string,
	occurredAt time.Time,
) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE pull_requests
		SET title = $1,
		    synced_at = clock_timestamp(),
		    last_checked_at = clock_timestamp()
		WHERE repo_id = 101 AND number = 7
	`, title); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES (
		    'entities', 'pull_request.changed', 'pr:1:1001:7', $1,
		    '{"version":1}'
		)
		RETURNING seq
	`, occurredAt).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return seq
}

func tombstoneAndRecreatePullRequest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) (int64, int64) {
	t.Helper()
	tombstoneSeq := changePullRequestAndEmit(
		t,
		ctx,
		pool,
		`UPDATE pull_requests
		 SET tombstoned_at = clock_timestamp(),
		     synced_at = clock_timestamp(),
		     last_checked_at = clock_timestamp()
		 WHERE repo_id = 101 AND number = 7`,
		"pull_request.tombstoned",
	)
	recreatedSeq := changePullRequestAndEmit(
		t,
		ctx,
		pool,
		`UPDATE pull_requests
		 SET tombstoned_at = NULL,
		     title = 'recreated title',
		     synced_at = clock_timestamp(),
		     last_checked_at = clock_timestamp()
		 WHERE repo_id = 101 AND number = 7`,
		"pull_request.changed",
	)
	return tombstoneSeq, recreatedSeq
}

func changePullRequestAndEmit(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	updateStatement string,
	eventKind string,
) int64 {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, updateStatement); err != nil {
		t.Fatal(err)
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		VALUES (
		    'entities', $1, 'pr:1:1001:7', clock_timestamp(),
		    '{"version":1}'
		)
		RETURNING seq
	`, eventKind).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return sequence
}

func modifyPullRequestWithoutEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	statement string,
) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

type runningTestAPI struct {
	server   *httptest.Server
	consumer string
	hub      *eventHub
}

func startTestAPI(
	t *testing.T,
	pool *pgxpool.Pool,
	ringSize int,
) runningTestAPI {
	t.Helper()
	stream, err := streamclient.New(pool, streamclient.Config{
		BatchSize:    32,
		PollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	hub := newEventHub(ringSize, 8)
	consumer := fmt.Sprintf(
		"example-api-test-%d",
		exampleAPITestID.Add(1),
	)
	tailer := newEntityTailer(stream, hub, consumer)
	ctx, cancel := context.WithCancel(context.Background())
	if err := tailer.start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	api := newAPIServer(pool, hub, tailer, 32)
	server := httptest.NewServer(api.routes())
	t.Cleanup(func() {
		server.Close()
		hub.close()
		cancel()
		select {
		case err := <-tailer.done():
			if err != nil {
				t.Errorf("tailer shutdown = %v", err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("tailer did not stop")
		}
	})
	return runningTestAPI{server: server, consumer: consumer, hub: hub}
}

func advanceWatermark(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	target int64,
) {
	t.Helper()
	watermarker, err := streammaint.NewWatermarker(
		pool,
		streammaint.WatermarkOptions{
			RefreshInterval: 10 * time.Millisecond,
			LeaseTTL:        time.Second,
			Owner: fmt.Sprintf(
				"example-api-watermarker-%d",
				exampleAPITestID.Add(1),
			),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watermarker.Close(context.WithoutCancel(ctx)) //nolint:errcheck // deferred cleanup cannot change the primary operation result
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		progress, err := watermarker.Step(ctx)
		if err != nil && !errors.Is(err, streammaint.ErrLeaseHeld) {
			t.Fatal(err)
		}
		if err == nil && progress.SafeSeq >= target {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("watermark did not advance through %d: %v", target, ctx.Err())
		case <-deadline.C:
			t.Fatalf("watermark did not advance through %d", target)
		case <-poll.C:
		}
	}
}

func waitConsumerCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
	want int64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	var lastSequence int64
	var lastErr error
	for {
		lastSequence, lastErr = queryConsumerCursor(ctx, pool, consumer)
		if lastErr == nil && lastSequence == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"consumer cursor did not reach %d (last=%d err=%v)",
				want,
				lastSequence,
				lastErr,
			)
		case <-poll.C:
		}
	}
}

func readConsumerCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	sequence, err := queryConsumerCursor(ctx, pool, consumer)
	if err != nil {
		t.Fatal(err)
	}
	return sequence
}

func queryConsumerCursor(
	ctx context.Context,
	pool *pgxpool.Pool,
	consumer string,
) (int64, error) {
	var sequence int64
	err := pool.QueryRow(ctx, `
		SELECT seq
		FROM consumer_cursors
		WHERE consumer = $1 AND stream = 'entities'
	`, consumer).Scan(&sequence)
	return sequence, err
}

func waitForConsumerCursorRow(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	var lastErr error
	for {
		var exists bool
		lastErr = pool.QueryRow(ctx, `
			SELECT EXISTS (
			    SELECT 1
			    FROM consumer_cursors
			    WHERE consumer = $1 AND stream = 'entities'
			)
		`, consumer).Scan(&exists)
		if lastErr == nil && exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("consumer cursor row was not created: %v", lastErr)
		case <-poll.C:
		}
	}
}

func assertOneConsumerCursor(
	t *testing.T,
	pool *pgxpool.Pool,
	consumer string,
) {
	t.Helper()
	var total, matching int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*), count(*) FILTER (WHERE consumer = $1 AND stream = 'entities')
		FROM consumer_cursors
	`, consumer).Scan(&total, &matching); err != nil {
		t.Fatal(err)
	}
	if total != 1 || matching != 1 {
		t.Fatalf("consumer cursor rows total=%d matching=%d", total, matching)
	}
}

func waitSubscriberCount(t *testing.T, hub *eventHub, want int) {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		hub.mu.Lock()
		count := len(hub.subscribers)
		hub.mu.Unlock()
		if count == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("subscriber count = %d, want %d", count, want)
		case <-poll.C:
		}
	}
}

func assertHealth(t *testing.T, url string) {
	t.Helper()
	response, err := http.Get(url) //nolint:noctx // test request is bounded by the owning test context and local server cleanup
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck // test cleanup cannot change the assertion result
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("health status=%d body=%s", response.StatusCode, body)
	}
}

func getJSON(t *testing.T, url string, destination any) {
	t.Helper()
	response, err := http.Get(url) //nolint:noctx // test request is bounded by the owning test context and local server cleanup
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close() //nolint:errcheck // test cleanup cannot change the assertion result
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s status=%d body=%s", url, response.StatusCode, body)
	}
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatal(err)
	}
}

type sseEvent struct {
	name string
	id   string
	data []byte
	err  error
}

type sseStream struct {
	body   io.ReadCloser
	events <-chan sseEvent
}

func openSSE(t *testing.T, url, lastEventID string) sseStream {
	t.Helper()
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		url,
		http.NoBody,
	)
	if err != nil {
		t.Fatal(err)
	}
	if lastEventID != "" {
		request.Header.Set("Last-Event-ID", lastEventID)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close() //nolint:errcheck // test failure cleanup
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("watch status=%d body=%s", response.StatusCode, body)
	}
	events := make(chan sseEvent, 32)
	go scanSSE(response.Body, events)
	return sseStream{body: response.Body, events: events}
}

func (s sseStream) close() {
	_ = s.body.Close()
}

func scanSSE(body io.Reader, events chan<- sseEvent) {
	defer close(events)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var current sseEvent
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if current.name != "" || current.id != "" || len(current.data) > 0 {
				events <- current
				current = sseEvent{}
			}
			continue
		}
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "event":
			current.name = value
		case "id":
			current.id = value
		case "data":
			if len(current.data) > 0 {
				current.data = append(current.data, '\n')
			}
			current.data = append(current.data, value...)
		}
	}
	if err := scanner.Err(); err != nil {
		events <- sseEvent{err: err}
	}
}

func nextSSE(t *testing.T, events <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("SSE stream closed")
		}
		if event.err != nil {
			t.Fatalf("read SSE stream: %v", event.err)
		}
		return event
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for SSE event")
	}
	return sseEvent{}
}

func decodeSSEData(t *testing.T, event sseEvent, destination any) {
	t.Helper()
	if err := json.Unmarshal(event.data, destination); err != nil {
		t.Fatalf("decode SSE data %q: %v", event.data, err)
	}
}
