package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/pkg/streamclient"
)

const entityStream = "entities"

type materializedEvent struct {
	Seq       int64           `json:"seq,omitempty"`
	Kind      string          `json:"kind"`
	EntityKey string          `json:"entity_key"`
	Entity    json.RawMessage `json:"entity,omitempty"`
	Tombstone bool            `json:"tombstone,omitempty"`
}

type resyncAdvisory struct {
	Reason           string `json:"reason"`
	FromSeq          int64  `json:"from_seq,omitempty"`
	PrunedThroughSeq int64  `json:"pruned_through_seq,omitempty"`
	Limit            string `json:"limit,omitempty"`
	Maximum          int    `json:"maximum,omitempty"`
}

type subscriberControl struct {
	resync *resyncAdvisory
}

type subscriber struct {
	events   chan materializedEvent
	control  chan subscriberControl
	afterSeq int64
}

type subscriptionStatus uint8

const (
	subscriptionNotCovered subscriptionStatus = iota
	subscriptionCovered
	subscriptionClosed
)

type eventHub struct {
	mu               sync.Mutex
	ring             []materializedEvent
	ringStart        int
	ringCapacity     int
	subscriberBuffer int
	subscribers      map[*subscriber]struct{}
	baseSeq          int64
	throughSeq       int64
	knownSafeSeq     int64
	closed           bool
}

func newEventHub(ringCapacity, subscriberBuffer int) *eventHub {
	return &eventHub{
		ring:             make([]materializedEvent, 0, ringCapacity),
		ringCapacity:     ringCapacity,
		subscriberBuffer: subscriberBuffer,
		subscribers:      make(map[*subscriber]struct{}),
	}
}

func (h *eventHub) initialize(safeSeq int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ring = h.ring[:0]
	h.ringStart = 0
	h.baseSeq = safeSeq
	h.throughSeq = safeSeq
	h.knownSafeSeq = safeSeq
}

func (h *eventHub) publish(event materializedEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || event.Seq <= h.throughSeq {
		return
	}
	if len(h.ring) == h.ringCapacity {
		h.baseSeq = h.ring[h.ringStart].Seq
		h.ring[h.ringStart] = event
		h.ringStart = (h.ringStart + 1) % h.ringCapacity
	} else {
		h.ring = append(h.ring, event)
	}
	h.throughSeq = event.Seq
	if event.Seq > h.knownSafeSeq {
		h.knownSafeSeq = event.Seq
	}
	for current := range h.subscribers {
		if event.Seq <= current.afterSeq {
			continue
		}
		select {
		case current.events <- event:
			current.afterSeq = event.Seq
		default:
			delete(h.subscribers, current)
			current.control <- subscriberControl{
				resync: &resyncAdvisory{
					Reason: "slow_client",
				},
			}
		}
	}
}

func (h *eventHub) subscribeFromRing(
	from int64,
) (*subscriber, []materializedEvent, subscriptionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.subscribeFromRingLocked(from)
}

// subscribeAfterSnapshot pairs a committed database snapshot with ring replay
// and live registration. The database transaction must be committed before
// calling it so the tailer never waits on this mutex while the caller performs
// database work.
func (h *eventHub) subscribeAfterSnapshot(
	safeSeq int64,
) (*subscriber, []materializedEvent, subscriptionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.updateKnownSafeLocked(safeSeq)
	return h.subscribeFromRingLocked(safeSeq)
}

func (h *eventHub) subscribeFromRingLocked(
	from int64,
) (*subscriber, []materializedEvent, subscriptionStatus) {
	if h.closed {
		return nil, nil, subscriptionClosed
	}
	if from > h.knownSafeSeq {
		return nil, nil, subscriptionNotCovered
	}
	if h.throughSeq > from && from < h.baseSeq {
		return nil, nil, subscriptionNotCovered
	}
	current := &subscriber{
		events:   make(chan materializedEvent, h.subscriberBuffer),
		control:  make(chan subscriberControl, 1),
		afterSeq: max(from, h.throughSeq),
	}
	h.subscribers[current] = struct{}{}
	replay := make([]materializedEvent, 0, len(h.ring))
	for offset := range len(h.ring) {
		index := (h.ringStart + offset) % len(h.ring)
		event := h.ring[index]
		if event.Seq > from {
			replay = append(replay, event)
		}
	}
	return current, replay, subscriptionCovered
}

func (h *eventHub) updateKnownSafeLocked(safeSeq int64) {
	if safeSeq > h.knownSafeSeq {
		h.knownSafeSeq = safeSeq
	}
}

func (h *eventHub) unsubscribe(current *subscriber) {
	if current == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subscribers, current)
}

func (h *eventHub) resetForResync(
	safeSeq int64,
	advisory resyncAdvisory,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	control := subscriberControl{
		resync: &advisory,
	}
	for current := range h.subscribers {
		delete(h.subscribers, current)
		current.control <- control
	}
	h.ring = h.ring[:0]
	h.ringStart = 0
	h.baseSeq = safeSeq
	h.throughSeq = safeSeq
	h.knownSafeSeq = safeSeq
}

func (h *eventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for current := range h.subscribers {
		delete(h.subscribers, current)
		current.control <- subscriberControl{}
	}
}

type entityTailer struct {
	client       *streamclient.Client
	hub          *eventHub
	consumerName string
	running      atomic.Bool
	result       chan error
}

func newEntityTailer(
	client *streamclient.Client,
	hub *eventHub,
	consumerName string,
) *entityTailer {
	return &entityTailer{
		client:       client,
		hub:          hub,
		consumerName: consumerName,
		result:       make(chan error, 1),
	}
}

func (t *entityTailer) start(ctx context.Context) error {
	safeSeq, err := t.bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("initialize entity tailer: %w", err)
	}
	t.hub.initialize(safeSeq)
	t.running.Store(true)
	go func() {
		err := t.run(ctx)
		t.running.Store(false)
		t.result <- err
	}()
	return nil
}

func (t *entityTailer) done() <-chan error {
	return t.result
}

func (t *entityTailer) isRunning() bool {
	return t.running.Load()
}

func (t *entityTailer) run(ctx context.Context) error {
	for {
		err := t.client.Tail(
			ctx,
			t.consumerName,
			entityStream,
			t.handleEvent,
		)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // caller cancellation is a clean tailer shutdown
		}
		var resync *streamclient.ErrResyncRequired
		if errors.As(err, &resync) {
			safeSeq, bootstrapErr := t.bootstrap(ctx)
			if bootstrapErr != nil {
				return fmt.Errorf("resync entity tailer: %w", bootstrapErr)
			}
			advisory := resyncAdvisory{Reason: "pruned_horizon"}
			if resync != nil {
				advisory.FromSeq = resync.Cursor
				advisory.PrunedThroughSeq = resync.PrunedThrough
			}
			t.hub.resetForResync(safeSeq, advisory)
			continue
		}
		if !streamclient.IsRetryable(err) {
			if err == nil {
				return fmt.Errorf("entity stream stopped unexpectedly")
			}
			return fmt.Errorf("tail entity changes: %w", err)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (t *entityTailer) bootstrap(ctx context.Context) (safeSeq int64, resultErr error) {
	snapshot, err := t.client.Bootstrap(
		ctx,
		t.consumerName,
		entityStream,
	)
	if err != nil {
		return 0, fmt.Errorf("bootstrap entity stream: %w", err)
	}
	defer func() {
		if closeErr := snapshot.CloseContext(ctx); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close tailer bootstrap: %w", closeErr)
		}
	}()
	if err := snapshot.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit entity bootstrap: %w", err)
	}
	return snapshot.SafeSeq, nil
}

func (t *entityTailer) handleEvent(
	ctx context.Context,
	tx pgx.Tx,
	event streamclient.Event, //nolint:gocritic // streamclient.Handler passes Event by value
) error {
	materialized, supported, err := materializeChange(ctx, tx, &event)
	if err != nil {
		return fmt.Errorf("materialize entity change: %w", err)
	}
	if supported {
		// publish never waits for a subscriber: every queue is bounded and a
		// full queue is disconnected. The hub also suppresses replayed seqs, so
		// a later cursor-commit failure can safely redeliver this durable event.
		t.hub.publish(materialized)
	}
	return nil
}

func materializeChange(
	ctx context.Context,
	query interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	event *streamclient.Event,
) (materializedEvent, bool, error) {
	result := materializedEvent{
		Seq:       event.Seq,
		Kind:      event.Kind,
		EntityKey: event.EntityKey,
	}
	var entity json.RawMessage
	var row pgx.Row
	switch event.Kind {
	case "repository.tombstoned":
		if _, _, parseErr := parseRepositoryKey(event.EntityKey); parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		result.Tombstone = true
		return result, true, nil
	case "repository.changed":
		installationID, repoID, parseErr := parseRepositoryKey(event.EntityKey)
		if parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		row = query.QueryRow(ctx, `
			SELECT to_jsonb(repos)
			FROM repos
			WHERE installation_id = $1
			  AND gh_id = $2
			  AND tombstoned_at IS NULL
		`, installationID, repoID)
	case "pull_request.tombstoned":
		if _, _, _, parseErr := parseNumberedKey(
			event.EntityKey,
			"pr",
		); parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		result.Tombstone = true
		return result, true, nil
	case "pull_request.changed":
		installationID, repoID, number, parseErr := parseNumberedKey(
			event.EntityKey,
			"pr",
		)
		if parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		row = query.QueryRow(ctx, `
			SELECT to_jsonb(pull_requests)
			FROM pull_requests
			JOIN repos ON repos.id = pull_requests.repo_id
			WHERE repos.installation_id = $1
			  AND repos.gh_id = $2
			  AND repos.tombstoned_at IS NULL
			  AND pull_requests.number = $3
			  AND pull_requests.tombstoned_at IS NULL
		`, installationID, repoID, number)
	case "stack.tombstoned":
		if _, _, _, parseErr := parseNumberedKey(
			event.EntityKey,
			"stack",
		); parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		result.Tombstone = true
		return result, true, nil
	case "stack.changed":
		installationID, repoID, number, parseErr := parseNumberedKey(
			event.EntityKey,
			"stack",
		)
		if parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		row = query.QueryRow(ctx, `
			SELECT to_jsonb(stacks)
			FROM stacks
			JOIN repos ON repos.id = stacks.repo_id
			WHERE repos.installation_id = $1
			  AND repos.gh_id = $2
			  AND repos.tombstoned_at IS NULL
			  AND stacks.number = $3
			  AND stacks.tombstoned_at IS NULL
		`, installationID, repoID, number)
	case "checks.changed":
		installationID, repoID, headSHA, parseErr := parseChecksKey(
			event.EntityKey,
		)
		if parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		row = query.QueryRow(ctx, `
			SELECT jsonb_agg(
			    to_jsonb(check_runs)
			    ORDER BY check_runs.name, check_runs.gh_id
			)
			FROM check_runs
			JOIN repos ON repos.id = check_runs.repo_id
			WHERE repos.installation_id = $1
			  AND repos.gh_id = $2
			  AND repos.tombstoned_at IS NULL
			  AND check_runs.head_sha = $3
			  AND check_runs.tombstoned_at IS NULL
			HAVING count(*) > 0
		`, installationID, repoID, headSHA)
	case "repo_rules.changed":
		installationID, repoID, parseErr := parseRepositoryRulesKey(
			event.EntityKey,
		)
		if parseErr != nil {
			return materializedEvent{}, true, parseErr
		}
		row = query.QueryRow(ctx, `
			SELECT jsonb_agg(to_jsonb(repo_rules) ORDER BY rule_key)
			FROM repo_rules
			JOIN repos ON repos.id = repo_rules.repo_id
			WHERE repos.installation_id = $1
			  AND repos.gh_id = $2
			  AND repos.tombstoned_at IS NULL
			  AND repo_rules.tombstoned_at IS NULL
			HAVING count(*) > 0
		`, installationID, repoID)
	default:
		return materializedEvent{}, false, nil
	}
	err := row.Scan(&entity)
	if errors.Is(err, pgx.ErrNoRows) {
		result.Tombstone = true
		return result, true, nil
	}
	if err != nil {
		return materializedEvent{}, true, fmt.Errorf(
			"materialize %s %q: %w",
			event.Kind,
			event.EntityKey,
			err,
		)
	}
	result.Entity = append(json.RawMessage(nil), entity...)
	return result, true, nil
}

func parseRepositoryKey(key string) (int64, int64, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 || parts[0] != "repo" {
		return 0, 0, fmt.Errorf("invalid repository entity key %q", key)
	}
	installationID, err := parsePositiveKeyPart(parts[1], key)
	if err != nil {
		return 0, 0, err
	}
	repoID, err := parsePositiveKeyPart(parts[2], key)
	if err != nil {
		return 0, 0, err
	}
	return installationID, repoID, nil
}

func parseRepositoryRulesKey(key string) (int64, int64, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 || parts[0] != "repo_rules" {
		return 0, 0, fmt.Errorf("invalid repository-rules entity key %q", key)
	}
	installationID, err := parsePositiveKeyPart(parts[1], key)
	if err != nil {
		return 0, 0, err
	}
	repoID, err := parsePositiveKeyPart(parts[2], key)
	if err != nil {
		return 0, 0, err
	}
	return installationID, repoID, nil
}

func parseNumberedKey(
	key string,
	prefix string,
) (int64, int64, int, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 4 || parts[0] != prefix {
		return 0, 0, 0, fmt.Errorf("invalid %s entity key %q", prefix, key)
	}
	installationID, err := parsePositiveKeyPart(parts[1], key)
	if err != nil {
		return 0, 0, 0, err
	}
	repoID, err := parsePositiveKeyPart(parts[2], key)
	if err != nil {
		return 0, 0, 0, err
	}
	number, err := strconv.Atoi(parts[3])
	if err != nil || number <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid numbered entity key %q", key)
	}
	return installationID, repoID, number, nil
}

func parseChecksKey(key string) (int64, int64, string, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 4 || parts[0] != "checks" || parts[3] == "" {
		return 0, 0, "", fmt.Errorf("invalid checks entity key %q", key)
	}
	installationID, err := parsePositiveKeyPart(parts[1], key)
	if err != nil {
		return 0, 0, "", err
	}
	repoID, err := parsePositiveKeyPart(parts[2], key)
	if err != nil {
		return 0, 0, "", err
	}
	return installationID, repoID, parts[3], nil
}

func parsePositiveKeyPart(value, key string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid entity key %q", key)
	}
	return parsed, nil
}
