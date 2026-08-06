package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const (
	// MaxDebounce is C-Q2's hard event-to-cache debounce ceiling.
	MaxDebounce                    = 15 * time.Second
	classificationRetryBaseBackoff = time.Second
	classificationRetryMaxBackoff  = time.Minute
	// DefaultBranchReconcilePageSize matches the GraphQL nodes batch ceiling.
	DefaultBranchReconcilePageSize = 25
)

// Config controls dispatcher batching, poison tolerance, and bounded debounce.
type Config struct {
	BatchSize      int
	MaxAttempts    int
	Debounce       time.Duration
	PollInterval   time.Duration
	Now            func() time.Time
	Classifier     Classifier
	Observer       Observer
	Tracer         trace.Tracer
	BranchPageSize int
}

// Observer is M6's C-P2/C-I5 observability seam. Implementations run only
// after the delivery batch and its River pointers commit.
type Observer interface {
	DispatchBatch(context.Context, int)
}

// UnmatchedEventObserver is an optional coverage-gap signal. Dispatcher also
// logs every committed delivery that matched zero configured rules so the
// signal remains visible when the batch observer does not implement this
// interface.
type UnmatchedEventObserver interface {
	DispatchUnmatchedEvent(context.Context, string)
}

// BranchBulkObserver distinguishes local branch hint application from remote
// entity refresh work without attaching repository or entity keys as tags.
type BranchBulkObserver interface {
	BranchBulkApplied(
		context.Context,
		int,
		int,
		int,
		int,
		int64,
	)
}

type noopObserver struct{}

func (noopObserver) DispatchBatch(context.Context, int) {}

// Dispatcher owns the delivery → River transaction boundary (C-P2).
type Dispatcher struct {
	pool   *pgxpool.Pool
	river  *river.Client[pgx.Tx]
	config Config
	writer *store.EntityWriter

	dispatchBatch func(context.Context) (int, error)
	retryDelay    func(time.Duration) time.Duration
	afterClaim    func()
}

// New validates config and constructs a delivery dispatcher.
func New(
	pool *pgxpool.Pool,
	riverClient *river.Client[pgx.Tx],
	config Config, //nolint:gocritic // constructor copies validated options into owned dispatcher state
) (*Dispatcher, error) {
	if pool == nil || riverClient == nil {
		return nil, fmt.Errorf("dispatcher requires Postgres and River clients")
	}
	if config.BatchSize <= 0 || config.MaxAttempts <= 0 ||
		config.Debounce <= 0 || config.PollInterval <= 0 {
		return nil, fmt.Errorf("dispatcher sizes and durations must be positive")
	}
	if config.Debounce > MaxDebounce {
		return nil, fmt.Errorf(
			"dispatcher debounce must not exceed %s",
			MaxDebounce,
		)
	}
	if config.BranchPageSize == 0 {
		config.BranchPageSize = DefaultBranchReconcilePageSize
	}
	if config.BranchPageSize < 0 ||
		config.BranchPageSize > DefaultBranchReconcilePageSize {
		return nil, fmt.Errorf(
			"branch reconciliation page size must be in [1,%d]",
			DefaultBranchReconcilePageSize,
		)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if len(config.Classifier.rules) == 0 {
		return nil, fmt.Errorf("dispatcher classifier rules are required")
	}
	if config.Observer == nil {
		config.Observer = noopObserver{}
	}
	if config.Tracer == nil {
		config.Tracer = noop.NewTracerProvider().Tracer(
			"github.com/ewhauser/ghsync/internal/dispatch",
		)
	}
	dispatcher := &Dispatcher{
		pool:   pool,
		river:  riverClient,
		config: config,
		writer: store.NewEntityWriter(pool),
	}
	dispatcher.dispatchBatch = dispatcher.DispatchBatch
	dispatcher.retryDelay = jitteredRetryDelay
	return dispatcher, nil
}

// Run continuously drains available batches and polls when idle.
func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		count, err := d.dispatchBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // cancellation is a graceful dispatcher shutdown
			}
			if !retryableDispatchError(err) {
				return err
			}
			delay := d.retryDelay(d.config.PollInterval)
			slog.WarnContext(
				ctx,
				"retryable dispatcher batch error",
				"error", err,
				"retry_in", delay,
			)
			if !waitForDispatch(ctx, delay) {
				return nil
			}
			continue
		}
		if count > 0 {
			continue
		}
		if !waitForDispatch(ctx, d.config.PollInterval) {
			return nil
		}
	}
}

func retryableDispatchError(err error) bool {
	if errors.Is(err, queue.ErrRefreshGenerationContention) {
		return true
	}
	var sqlState interface{ SQLState() string }
	if errors.As(err, &sqlState) {
		code := sqlState.SQLState()
		if code == "40001" ||
			code == "40P01" ||
			strings.HasPrefix(code, "08") {
			return true
		}
	}
	var connectError *pgconn.ConnectError
	if errors.As(err, &connectError) ||
		errors.Is(err, pgconn.ErrConnClosed) ||
		pgconn.SafeToRetry(err) ||
		pgconn.Timeout(err) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func jitteredRetryDelay(interval time.Duration) time.Duration {
	if interval <= time.Nanosecond {
		return interval
	}
	return interval/2 + time.Duration(rand.Int64N(int64(interval)))
}

func waitForDispatch(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// DispatchBatch claims, classifies, enqueues, and finishes one batch. Each
// attempt uses one pgx transaction shared by sqlc and River; a contended
// generation lock rolls that attempt back before retrying the complete batch.
func (d *Dispatcher) DispatchBatch(
	ctx context.Context,
) (count int, resultErr error) {
	afterClaim := d.afterClaim
	for {
		count, err := d.dispatchBatchAttempt(ctx, afterClaim)
		afterClaim = nil
		if !errors.Is(err, queue.ErrRefreshGenerationContention) {
			return count, err
		}
		if !waitForDispatch(ctx, d.retryDelay(d.config.PollInterval)) {
			return 0, fmt.Errorf(
				"retry contended refresh generation batch: %w",
				ctx.Err(),
			)
		}
	}
}

func (d *Dispatcher) dispatchBatchAttempt(
	ctx context.Context,
	afterClaim func(),
) (count int, resultErr error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin dispatch batch: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	queries := dbgen.New(tx)
	deliveries, err := queries.ClaimWebhookDeliveries(
		ctx,
		int32(d.config.BatchSize),
	)
	if err != nil {
		return 0, fmt.Errorf("claim webhook deliveries: %w", err)
	}
	if len(deliveries) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty dispatch batch: %w", err)
		}
		return 0, nil
	}
	ctx, span := d.config.Tracer.Start(
		ctx,
		"ghsync.dispatch.batch",
		trace.WithAttributes(attribute.Int(
			"ghsync.dispatch.delivery_count",
			len(deliveries),
		)),
	)
	defer func() {
		if resultErr != nil {
			span.RecordError(resultErr)
			span.SetStatus(codes.Error, resultErr.Error())
		}
		span.End()
	}()
	addDeliveryTraceLinks(ctx, span, deliveries)
	if afterClaim != nil {
		afterClaim()
	}

	type deliveryResult struct {
		DeliveryGUID      string `json:"delivery_guid"`
		Status            string `json:"status"`
		LastError         string `json:"last_error"`
		RetryDelaySeconds *int32 `json:"retry_delay_seconds,omitempty"`
	}
	results := make([]deliveryResult, 0, len(deliveries))
	intents := make([]Intent, 0, len(deliveries))
	intentReceivedAt := make(map[Intent]time.Time, len(deliveries))
	type pendingBranchPush struct {
		hint         branchPushHint
		deliveryGUID string
		receivedAt   time.Time
	}
	branchPushes := make([]pendingBranchPush, 0)
	unmatchedEvents := make([]string, 0)
	for index := range deliveries {
		delivery := &deliveries[index]
		result, classifyErr := d.config.Classifier.classifyStored(
			delivery.Event,
			delivery.RawBody,
			delivery.Headers,
		)
		if classifyErr != nil {
			status := "pending"
			var retryDelaySeconds *int32
			if int(delivery.Attempts) >= d.config.MaxAttempts {
				status = "parked"
			} else {
				delay := int32(
					classificationRetryBackoff(delivery.Attempts) /
						time.Second,
				)
				retryDelaySeconds = &delay
			}
			results = append(results, deliveryResult{
				DeliveryGUID:      delivery.DeliveryGuid,
				Status:            status,
				LastError:         classifyErr.Error(),
				RetryDelaySeconds: retryDelaySeconds,
			})
			continue
		}
		if result.matchedRules == 0 {
			unmatchedEvents = append(unmatchedEvents, delivery.Event)
		}
		classified := result.intents
		if result.branchHint != nil {
			branchPushes = append(branchPushes, pendingBranchPush{
				hint:         *result.branchHint,
				deliveryGUID: delivery.DeliveryGuid,
				receivedAt:   delivery.ReceivedAt.Time,
			})
			classified = withoutBranchRefresh(classified)
		}
		if result.stackHint != nil {
			matches, matchErr := stackSummaryMatchesCache(
				ctx,
				queries,
				result.stackHint,
			)
			if matchErr != nil {
				return 0, fmt.Errorf(
					"compare webhook stack summary: %w",
					matchErr,
				)
			}
			if matches {
				classified = withoutMatchingStackRefresh(
					classified,
					result.stackHint,
				)
			}
		}
		intents = append(intents, classified...)
		for _, intent := range classified {
			receivedAt := delivery.ReceivedAt.Time
			prior, exists := intentReceivedAt[intent]
			if !exists || receivedAt.Before(prior) {
				intentReceivedAt[intent] = receivedAt
			}
		}
		results = append(results, deliveryResult{
			DeliveryGUID: delivery.DeliveryGuid,
			Status:       "processed",
		})
	}

	type branchBulkObservation struct {
		result store.BranchBulkResult
		pages  int
	}
	branchObservations := make([]branchBulkObservation, 0, len(branchPushes))
	if len(branchPushes) > 0 {
		fenceObserver, _ := d.config.Observer.(outbox.FenceObserver)
		fencedTx, fenceErr := outbox.AcquireObservedWriterFence(
			ctx, tx, fenceObserver,
		)
		if fenceErr != nil {
			return 0, fenceErr
		}
		tx = fencedTx
		queries = dbgen.New(tx)
		for _, pending := range branchPushes {
			bulk, err := d.writer.ApplyBranchPushTx(
				ctx,
				tx,
				&store.BranchPushHint{
					RepoFullName:    pending.hint.Repo,
					Branch:          pending.hint.Branch,
					BeforeSHA:       pending.hint.BeforeSHA,
					AfterSHA:        pending.hint.AfterSHA,
					TransitionKnown: pending.hint.TransitionKnown,
					Deleted:         pending.hint.Deleted,
					Forced:          pending.hint.Forced,
					DeliveryGUID:    pending.deliveryGUID,
					ReceivedAt:      pending.receivedAt,
				},
			)
			if err != nil {
				if errors.Is(
					err, store.ErrBranchRefreshGenerationContention,
				) {
					return 0, fmt.Errorf(
						"bulk apply branch push: %s: %w",
						err.Error(),
						queue.ErrRefreshGenerationContention,
					)
				}
				return 0, fmt.Errorf("bulk apply branch push: %w", err)
			}
			if bulk.RepoID == 0 {
				continue
			}
			pageParams, pageCounts, err := branchReconciliationPages(
				pending.hint,
				&bulk,
				d.config.BranchPageSize,
			)
			if err != nil {
				return 0, err
			}
			if err := d.writer.RecordBranchReconciliationPagesTx(
				ctx,
				tx,
				bulk.RepoID,
				pending.hint.Branch,
				bulk.Generation,
				pageCounts,
			); err != nil {
				return 0, err
			}
			if len(pageParams) > 0 {
				inserted, err := d.river.InsertManyTx(ctx, tx, pageParams)
				if err != nil {
					return 0, fmt.Errorf(
						"insert branch reconciliation pages: %w", err,
					)
				}
				if len(inserted) != len(pageParams) {
					return 0, fmt.Errorf(
						"inserted %d branch pages for %d specs",
						len(inserted), len(pageParams),
					)
				}
			}
			// A default-branch hint also changes the repository head. Exact
			// before-SHA CAS prevents most stale writes, but two causally linked
			// pushes can arrive in reverse order (new1->new2, then old->new1).
			// One constant-size bulk-lane repository observation closes that gap;
			// it is not dependent fanout and its generation was claimed above.
			if bulk.RepositoryRefreshKey != "" {
				if _, err := d.river.InsertTx(
					ctx,
					tx,
					queue.NewRefreshRepositoryHeadArgs(bulk.RepositoryRefreshKey),
					queue.NewRefreshInsertOptsForQueue(queue.QueueBulk, time.Time{}),
				); err != nil {
					return 0, fmt.Errorf(
						"insert default-branch repository reconciliation: %w", err,
					)
				}
			}
			branchObservations = append(
				branchObservations,
				branchBulkObservation{result: bulk, pages: len(pageParams)},
			)
		}
	}

	if len(intents) > 0 {
		intents = dedupeIntents(intents)
		generationKeys := make([]queue.RefreshGenerationKey, 0, len(intents))
		for _, intent := range intents {
			generationKeys = append(generationKeys, queue.RefreshGenerationKey{
				Kind: intent.Kind,
				Key:  intent.Key,
			})
		}
		if err := queue.TryLockRefreshIntentGenerationsTx(
			ctx,
			tx,
			generationKeys,
		); err != nil {
			return 0, fmt.Errorf("lock refresh intent generations: %w", err)
		}
		encodedIntents, err := json.Marshal(
			intentGenerationPointers(intents, intentReceivedAt),
		)
		if err != nil {
			return 0, fmt.Errorf("encode refresh generation pointers: %w", err)
		}
		generations, err := queries.BumpRefreshIntentGenerations(ctx, encodedIntents)
		if err != nil {
			return 0, fmt.Errorf("bump refresh intent generations: %w", err)
		}
		if len(generations) != len(intents) {
			return 0, fmt.Errorf(
				"bump refresh intent generations: got %d of %d rows",
				len(generations),
				len(intents),
			)
		}
		params, err := d.insertParams(intents)
		if err != nil {
			return 0, err
		}
		_, err = d.river.InsertManyTx(ctx, tx, params)
		if err != nil {
			return 0, fmt.Errorf("insert refresh intents: %w", err)
		}
	}

	encodedResults, err := json.Marshal(results)
	if err != nil {
		return 0, fmt.Errorf("encode delivery results: %w", err)
	}
	updated, err := queries.SetWebhookDeliveryResults(ctx, encodedResults)
	if err != nil {
		return 0, fmt.Errorf("finish webhook delivery batch: %w", err)
	}
	if updated != int64(len(deliveries)) {
		return 0, fmt.Errorf(
			"finish webhook delivery batch: updated %d of %d rows",
			updated,
			len(deliveries),
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit dispatch batch: %w", err)
	}
	d.config.Observer.DispatchBatch(ctx, len(deliveries))
	if observer, ok := d.config.Observer.(BranchBulkObserver); ok {
		for _, observation := range branchObservations {
			observer.BranchBulkApplied(
				ctx,
				observation.result.Repositories,
				observation.result.PullRequests,
				observation.result.Stacks,
				observation.pages,
				observation.result.SupersededPages,
			)
		}
	}
	for _, event := range unmatchedEvents {
		slog.WarnContext(
			ctx,
			"webhook event matched zero dispatcher rules",
			"event", event,
		)
		if observer, ok := d.config.Observer.(UnmatchedEventObserver); ok {
			observer.DispatchUnmatchedEvent(ctx, event)
		}
	}
	return len(deliveries), nil
}

func addDeliveryTraceLinks(
	ctx context.Context,
	span trace.Span,
	deliveries []dbgen.WebhookDelivery,
) {
	seen := make(map[string]struct{}, len(deliveries))
	for index := range deliveries {
		delivery := &deliveries[index]
		traceparent := ""
		if delivery.Traceparent.Valid {
			traceparent = delivery.Traceparent.String
		}
		tracestate := ""
		if delivery.Tracestate.Valid {
			tracestate = delivery.Tracestate.String
		}
		carrier := propagation.MapCarrier{
			"traceparent": traceparent,
			"tracestate":  tracestate,
		}
		extracted := propagation.TraceContext{}.Extract(
			ctx,
			carrier,
		)
		spanContext := trace.SpanContextFromContext(extracted)
		if !spanContext.IsValid() {
			continue
		}
		key := spanContext.TraceID().String() + ":" +
			spanContext.SpanID().String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		span.AddLink(trace.Link{
			SpanContext: spanContext,
			Attributes: []attribute.KeyValue{
				attribute.String(
					"ghsync.webhook.event",
					delivery.Event,
				),
			},
		})
	}
}

func stackSummaryMatchesCache(
	ctx context.Context,
	queries *dbgen.Queries,
	hint *stackSummaryHint,
) (bool, error) {
	// A historical ordinal beyond the current size cannot identify a member in
	// the cached current entries, so it never suppresses the eager stack fetch.
	if hint.Position > hint.Size {
		return false, nil
	}
	// Suppression requires positive knowledge of the base commit. Treat an
	// unknown payload SHA as a mismatch even if the cache is also unknown.
	if hint.BaseSHA == "" {
		return false, nil
	}
	stack, err := queries.GetStackByKey(
		ctx,
		dbgen.GetStackByKeyParams{
			RepoFullName: hint.Repo,
			StackNumber:  int32(hint.Number),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stack.TombstonedAt.Valid ||
		!stack.GhID.Valid ||
		stack.GhID.Int64 != hint.ID ||
		stack.BaseRef != hint.BaseRef ||
		stack.BaseSha == "" ||
		stack.BaseSha != hint.BaseSHA {
		return false, nil
	}
	var entries []struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(stack.Entries, &entries); err != nil {
		return false, fmt.Errorf("decode cached stack entries: %w", err)
	}
	return len(entries) == hint.Size &&
		hint.Position <= len(entries) &&
		entries[hint.Position-1].Number == hint.PRNumber, nil
}

func withoutMatchingStackRefresh(
	intents []Intent,
	hint *stackSummaryHint,
) []Intent {
	stackKey := fmt.Sprintf("stack:%s:%d", hint.Repo, hint.Number)
	filtered := make([]Intent, 0, len(intents)-1)
	for _, intent := range intents {
		if intent.Kind == queue.KindRefreshStack && intent.Key == stackKey {
			continue
		}
		filtered = append(filtered, intent)
	}
	return filtered
}

func withoutBranchRefresh(intents []Intent) []Intent {
	filtered := make([]Intent, 0, len(intents))
	for _, intent := range intents {
		if intent.Kind == queue.KindRefreshBranch {
			continue
		}
		filtered = append(filtered, intent)
	}
	return filtered
}

func branchReconciliationPages(
	hint branchPushHint,
	bulk *store.BranchBulkResult,
	pageSize int,
) ([]river.InsertManyParams, []int, error) {
	if pageSize <= 0 {
		return nil, nil, fmt.Errorf(
			"branch reconciliation page size must be positive",
		)
	}
	params := make(
		[]river.InsertManyParams,
		0,
		(len(bulk.Targets)+pageSize-1)/pageSize,
	)
	counts := make([]int, 0, cap(params))
	for start := 0; start < len(bulk.Targets); start += pageSize {
		end := min(start+pageSize, len(bulk.Targets))
		targets := make([]queue.BranchReconcileTarget, 0, end-start)
		for _, target := range bulk.Targets[start:end] {
			if target.RefreshKind != queue.KindRefreshPR &&
				target.RefreshKind != queue.KindRefreshStack {
				return nil, nil, fmt.Errorf(
					"unsupported branch reconciliation target kind %q",
					target.RefreshKind,
				)
			}
			targets = append(targets, queue.BranchReconcileTarget{
				Kind:       target.RefreshKind,
				Key:        target.RefreshKey,
				EntityKey:  target.EntityKey,
				Generation: target.RefreshGeneration,
			})
		}
		page := len(params) + 1
		params = append(params, river.InsertManyParams{
			Args: queue.NewBranchReconcilePageArgs(
				bulk.RepoID,
				hint.Repo,
				hint.Branch,
				bulk.Generation,
				page,
				targets,
			),
			InsertOpts: queue.NewBackfillInsertOptsForQueue(queue.QueueBulk),
		})
		counts = append(counts, len(targets))
	}
	return params, counts, nil
}

func (d *Dispatcher) insertParams(
	intents []Intent,
) ([]river.InsertManyParams, error) {
	scheduledAt := d.config.Now().Add(d.config.Debounce)
	params := make([]river.InsertManyParams, 0, len(intents))
	for _, intent := range intents {
		args, err := refreshArgs(intent)
		if err != nil {
			return nil, err
		}
		params = append(params, river.InsertManyParams{
			Args:       args,
			InsertOpts: queue.NewRefreshInsertOpts(scheduledAt),
		})
	}
	return params, nil
}

func refreshArgs(intent Intent) (rivertype.JobArgs, error) {
	if intent.Priority != PriorityEvent {
		return nil, fmt.Errorf("unsupported refresh priority %q", intent.Priority)
	}
	switch intent.Kind {
	case queue.KindRefreshPR:
		return queue.NewRefreshPRArgs(intent.Key), nil
	case queue.KindRefreshStack:
		return queue.NewRefreshStackArgs(intent.Key), nil
	case queue.KindRefreshChecks:
		return queue.NewRefreshChecksArgs(intent.Key), nil
	case queue.KindRefreshBranch:
		return queue.NewRefreshBranchArgs(intent.Key), nil
	case queue.KindResolveStackMembership:
		return queue.NewResolveStackMembershipArgs(intent.Key), nil
	default:
		return nil, fmt.Errorf("unsupported refresh kind %q", intent.Kind)
	}
}

func classificationRetryBackoff(attempt int32) time.Duration {
	delay := classificationRetryBaseBackoff
	for current := int32(1); current < attempt; current++ {
		if delay >= classificationRetryMaxBackoff/2 {
			return classificationRetryMaxBackoff
		}
		delay *= 2
	}
	return delay
}

func dedupeIntents(intents []Intent) []Intent {
	// River dedupes against existing rows, but duplicate unique keys within one
	// bulk INSERT would conflict with each other. Coalesce each batch first.
	seen := make(map[Intent]struct{}, len(intents))
	deduped := make([]Intent, 0, len(intents))
	for _, intent := range intents {
		if _, duplicate := seen[intent]; duplicate {
			continue
		}
		seen[intent] = struct{}{}
		deduped = append(deduped, intent)
	}
	// Match the SQL upsert's conflict-row lock order across dispatcher replicas.
	sort.Slice(deduped, func(i, j int) bool {
		if deduped[i].Kind != deduped[j].Kind {
			return deduped[i].Kind < deduped[j].Kind
		}
		if deduped[i].Key != deduped[j].Key {
			return deduped[i].Key < deduped[j].Key
		}
		return deduped[i].Priority < deduped[j].Priority
	})
	return deduped
}

type intentGenerationPointer struct {
	Kind            string `json:"kind"`
	RefreshKey      string `json:"refresh_key"`
	EventReceivedAt string `json:"event_received_at,omitempty"`
}

func intentGenerationPointers(
	intents []Intent,
	receivedAt map[Intent]time.Time,
) []intentGenerationPointer {
	pointers := make([]intentGenerationPointer, 0, len(intents))
	for _, intent := range intents {
		pointer := intentGenerationPointer{
			Kind:       intent.Kind,
			RefreshKey: intent.Key,
		}
		if at := receivedAt[intent]; !at.IsZero() {
			pointer.EventReceivedAt = at.UTC().Format(time.RFC3339Nano)
		}
		pointers = append(pointers, pointer)
	}
	return pointers
}
