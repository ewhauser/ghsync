package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/pkg/streamclient"
)

const (
	sseWriteTimeout            = 5 * time.Second
	maximumSnapshotEntities    = 100_000
	maximumSnapshotEntityBytes = 64 << 20
	maximumConcurrentSnapshots = 2
)

type apiServer struct {
	pool                *pgxpool.Pool
	hub                 *eventHub
	tailer              *entityTailer
	replayLimit         int
	snapshotSlots       chan struct{}
	snapshotEntityLimit int
	snapshotBytesLimit  int
}

func newAPIServer(
	pool *pgxpool.Pool,
	hub *eventHub,
	tailer *entityTailer,
	replayLimit int,
) *apiServer {
	return &apiServer{
		pool:        pool,
		hub:         hub,
		tailer:      tailer,
		replayLimit: replayLimit,
		snapshotSlots: make(
			chan struct{},
			maximumConcurrentSnapshots,
		),
		snapshotEntityLimit: maximumSnapshotEntities,
		snapshotBytesLimit:  maximumSnapshotEntityBytes,
	}
}

func (s *apiServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /v1/pull-requests", s.listPullRequests)
	mux.HandleFunc("GET /v1/pull-requests/{number}", s.getPullRequest)
	mux.HandleFunc("GET /v1/stacks", s.listStacks)
	mux.HandleFunc("GET /v1/stacks/{number}", s.getStack)
	mux.HandleFunc("GET /v1/checks/{head_sha}", s.getChecks)
	mux.HandleFunc("GET /v1/watch", s.watch)
	return mux
}

func (s *apiServer) health(response http.ResponseWriter, request *http.Request) {
	if !s.tailer.isRunning() {
		writeJSONError(response, http.StatusServiceUnavailable, "tailer is not running")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		writeJSONError(response, http.StatusServiceUnavailable, "database is unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *apiServer) listPullRequests(
	response http.ResponseWriter,
	request *http.Request,
) {
	limit, err := requestLimit(request)
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := queryJSONRows(request.Context(), s.pool, `
		SELECT to_jsonb(pull_requests)
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE pull_requests.state = 'open'
		  AND pull_requests.tombstoned_at IS NULL
		  AND repos.tombstoned_at IS NULL
		ORDER BY pull_requests.repo_id, pull_requests.number
		LIMIT $1
	`, limit)
	if err != nil {
		s.writeDatabaseError(response, "list pull requests", err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"pull_requests": rows})
}

func (s *apiServer) getPullRequest(
	response http.ResponseWriter,
	request *http.Request,
) {
	number, err := positivePathInt(request.PathValue("number"))
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, "pull request number must be positive")
		return
	}
	rows, err := queryJSONRows(request.Context(), s.pool, `
		SELECT to_jsonb(pull_requests)
		FROM pull_requests
		JOIN repos ON repos.id = pull_requests.repo_id
		WHERE pull_requests.number = $1
		  AND pull_requests.tombstoned_at IS NULL
		  AND repos.tombstoned_at IS NULL
		  AND ($2 = '' OR repos.full_name = $2)
		ORDER BY pull_requests.repo_id
		LIMIT 2
	`, number, request.URL.Query().Get("repo"))
	if err != nil {
		s.writeDatabaseError(response, "get pull request", err)
		return
	}
	s.writeSingularEntity(response, rows, "pull request")
}

func (s *apiServer) listStacks(
	response http.ResponseWriter,
	request *http.Request,
) {
	limit, err := requestLimit(request)
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := queryJSONRows(request.Context(), s.pool, `
		SELECT to_jsonb(stacks)
		FROM stacks
		JOIN repos ON repos.id = stacks.repo_id
		WHERE stacks.tombstoned_at IS NULL
		  AND repos.tombstoned_at IS NULL
		  AND (stacks.open OR stacks.display_until > clock_timestamp())
		ORDER BY stacks.open DESC, stacks.repo_id, stacks.number
		LIMIT $1
	`, limit)
	if err != nil {
		s.writeDatabaseError(response, "list stacks", err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"stacks": rows})
}

func (s *apiServer) getStack(
	response http.ResponseWriter,
	request *http.Request,
) {
	number, err := positivePathInt(request.PathValue("number"))
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, "stack number must be positive")
		return
	}
	rows, err := queryJSONRows(request.Context(), s.pool, `
		SELECT to_jsonb(stacks)
		FROM stacks
		JOIN repos ON repos.id = stacks.repo_id
		WHERE stacks.number = $1
		  AND stacks.tombstoned_at IS NULL
		  AND repos.tombstoned_at IS NULL
		  AND ($2 = '' OR repos.full_name = $2)
		ORDER BY stacks.repo_id
		LIMIT 2
	`, number, request.URL.Query().Get("repo"))
	if err != nil {
		s.writeDatabaseError(response, "get stack", err)
		return
	}
	s.writeSingularEntity(response, rows, "stack")
}

func (s *apiServer) getChecks(
	response http.ResponseWriter,
	request *http.Request,
) {
	headSHA := strings.TrimSpace(request.PathValue("head_sha"))
	if headSHA == "" {
		writeJSONError(response, http.StatusBadRequest, "head SHA is required")
		return
	}
	limit, err := requestLimit(request)
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := queryJSONRows(request.Context(), s.pool, `
		SELECT to_jsonb(check_runs)
		FROM check_runs
		JOIN repos ON repos.id = check_runs.repo_id
		WHERE check_runs.head_sha = $1
		  AND check_runs.tombstoned_at IS NULL
		  AND repos.tombstoned_at IS NULL
		ORDER BY check_runs.repo_id, check_runs.name, check_runs.gh_id
		LIMIT $2
	`, headSHA, limit)
	if err != nil {
		s.writeDatabaseError(response, "get checks", err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"checks": rows})
}

func (s *apiServer) writeSingularEntity(
	response http.ResponseWriter,
	rows []json.RawMessage,
	entityName string,
) {
	switch len(rows) {
	case 0:
		writeJSONError(response, http.StatusNotFound, entityName+" not found")
	case 1:
		writeRawJSON(response, http.StatusOK, rows[0])
	default:
		writeJSONError(
			response,
			http.StatusConflict,
			entityName+" number is ambiguous; add ?repo=owner/name",
		)
	}
}

func (s *apiServer) watch(response http.ResponseWriter, request *http.Request) {
	flusher, ok := response.(http.Flusher)
	if !ok {
		writeJSONError(response, http.StatusInternalServerError, "streaming is unavailable")
		return
	}
	from, resume, err := resumeSequence(request)
	if err != nil {
		writeJSONError(response, http.StatusBadRequest, err.Error())
		return
	}
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)
	flusher.Flush()
	if !resume {
		s.serveFreshSnapshot(response, flusher, request)
		return
	}
	current, replay, status := s.hub.subscribeFromRing(from)
	if status == subscriptionClosed {
		return
	}
	if status == subscriptionCovered {
		s.serveSubscription(response, flusher, request, current, replay)
		return
	}
	current, replay, advisory, err := s.subscribeFromDatabase(
		request.Context(),
		from,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		slog.Error("resume watch", "error", err)
		_ = writeSSE(response, flusher, "resync", nil, resyncAdvisory{
			Reason:  "server_error",
			FromSeq: from,
		})
		return
	}
	if advisory != nil {
		if err := writeSSE(response, flusher, "resync", nil, advisory); err != nil {
			return
		}
		s.serveFreshSnapshot(response, flusher, request)
		return
	}
	s.serveSubscription(response, flusher, request, current, replay)
}

func (s *apiServer) serveFreshSnapshot(
	response http.ResponseWriter,
	flusher http.Flusher,
	request *http.Request,
) {
	safeSeq, err := s.writeSnapshot(
		request.Context(),
		response,
		flusher,
	)
	if err != nil {
		if request.Context().Err() != nil {
			return
		}
		var limitErr *snapshotLimitError
		if errors.As(err, &limitErr) {
			slog.Warn("watch snapshot rejected", "error", err)
			_ = writeSSE(response, flusher, "resync", nil, resyncAdvisory{
				Reason:  "snapshot_limit_exceeded",
				Limit:   limitErr.kind,
				Maximum: limitErr.maximum,
			})
			return
		}
		slog.Error("read watch snapshot", "error", err)
		_ = writeSSE(response, flusher, "resync", nil, resyncAdvisory{
			Reason: "server_error",
		})
		return
	}
	if err := writeSSE(
		response,
		flusher,
		"snapshot-complete",
		&safeSeq,
		map[string]int64{"seq": safeSeq},
	); err != nil {
		return
	}
	current, replay, status := s.hub.subscribeAfterSnapshot(safeSeq)
	if status == subscriptionClosed {
		return
	}
	if status == subscriptionCovered {
		s.serveSubscription(
			response,
			flusher,
			request,
			current,
			replay,
		)
		return
	}
	_ = writeSSE(
		response,
		flusher,
		"resync",
		nil,
		resyncAdvisory{
			Reason:  "catchup_window_exceeded",
			FromSeq: safeSeq,
		},
	)
}

func (s *apiServer) serveSubscription(
	response http.ResponseWriter,
	flusher http.Flusher,
	request *http.Request,
	current *subscriber,
	replay []materializedEvent,
) {
	if current == nil {
		return
	}
	defer s.hub.unsubscribe(current)
	for _, event := range replay {
		select {
		case <-request.Context().Done():
			return
		case control := <-current.control:
			if control.resync != nil {
				_ = writeSSE(response, flusher, "resync", nil, control.resync)
			}
			return
		default:
		}
		seq := event.Seq
		if err := writeSSE(response, flusher, "change", &seq, event); err != nil {
			return
		}
	}
	for {
		select {
		case <-request.Context().Done():
			return
		case control := <-current.control:
			if control.resync != nil {
				_ = writeSSE(response, flusher, "resync", nil, control.resync)
			}
			return
		case event := <-current.events:
			seq := event.Seq
			if err := writeSSE(
				response,
				flusher,
				"change",
				&seq,
				event,
			); err != nil {
				return
			}
		}
	}
}

func (s *apiServer) writeSnapshot(
	ctx context.Context,
	response http.ResponseWriter,
	flusher http.Flusher,
) (safeSeq int64, resultErr error) {
	select {
	case s.snapshotSlots <- struct{}{}:
		defer func() { <-s.snapshotSlots }()
	case <-ctx.Done():
		return 0, fmt.Errorf("wait for watch snapshot capacity: %w", ctx.Err())
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("begin watch snapshot: %w", err)
	}
	defer func() {
		rollbackErr := rollbackReadTransaction(ctx, tx)
		if resultErr == nil && rollbackErr != nil {
			resultErr = fmt.Errorf("roll back watch snapshot: %w", rollbackErr)
		}
	}()
	if err := tx.QueryRow(ctx, `
		SELECT safe_seq
		FROM stream_watermark
		WHERE singleton
	`).Scan(&safeSeq); err != nil {
		return 0, fmt.Errorf("read watch snapshot watermark: %w", err)
	}
	entities, err := loadSnapshotEntities(
		ctx,
		tx,
		s.snapshotEntityLimit,
		s.snapshotBytesLimit,
	)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit watch snapshot: %w", err)
	}
	// C-C6: the repeatable-read snapshot is fully materialized and committed
	// before the first potentially slow client write. The bounded entity/byte
	// limits in loadSnapshotEntities keep this example from trading a database
	// resource leak for unbounded process memory.
	for _, event := range entities {
		if err := writeSSE(response, flusher, "snapshot", nil, event); err != nil {
			return 0, fmt.Errorf("write watch snapshot entity: %w", err)
		}
	}
	return safeSeq, nil
}

func (s *apiServer) subscribeFromDatabase(
	ctx context.Context,
	from int64,
) (
	current *subscriber,
	events []materializedEvent,
	advisory *resyncAdvisory,
	resultErr error,
) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("begin watch replay: %w", err)
	}
	defer func() {
		rollbackErr := rollbackReadTransaction(ctx, tx)
		if resultErr == nil && rollbackErr != nil {
			resultErr = fmt.Errorf("roll back watch replay: %w", rollbackErr)
		}
	}()
	var prunedThrough, safeSeq int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE((
		    SELECT pruned_through_seq
		    FROM stream_horizons
		    WHERE stream = $1
		), 0), safe_seq
		FROM stream_watermark
		WHERE singleton
	`, entityStream).Scan(&prunedThrough, &safeSeq); err != nil {
		return nil, nil, nil, fmt.Errorf("read watch replay bounds: %w", err)
	}
	if from < prunedThrough {
		return nil, nil, &resyncAdvisory{
			Reason:           "pruned_horizon",
			FromSeq:          from,
			PrunedThroughSeq: prunedThrough,
		}, nil
	}
	if from > safeSeq {
		return nil, nil, &resyncAdvisory{
			Reason:  "future_sequence",
			FromSeq: from,
		}, nil
	}
	streamEvents, err := queryStreamEvents(
		ctx,
		tx,
		from,
		safeSeq,
		s.replayLimit+1,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read bounded watch replay: %w", err)
	}
	if len(streamEvents) > s.replayLimit {
		return nil, nil, &resyncAdvisory{
			Reason:  "replay_limit_exceeded",
			FromSeq: from,
		}, nil
	}
	events = make([]materializedEvent, 0, len(streamEvents))
	for _, event := range streamEvents {
		materialized, supported, err := materializeChange(ctx, tx, &event)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"materialize watch replay event %d: %w",
				event.Seq,
				err,
			)
		}
		if supported {
			events = append(events, materialized)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, nil, fmt.Errorf("commit watch replay: %w", err)
	}
	current, ringReplay, status := s.hub.subscribeAfterSnapshot(safeSeq)
	if status == subscriptionClosed {
		return nil, nil, nil, context.Canceled
	}
	if status != subscriptionCovered {
		if ctx.Err() != nil {
			return nil, nil, nil, fmt.Errorf(
				"register watch replay: %w",
				ctx.Err(),
			)
		}
		return nil, nil, &resyncAdvisory{
			Reason:  "catchup_window_exceeded",
			FromSeq: from,
		}, nil
	}
	events = append(events, ringReplay...)
	return current, events, nil, nil
}

func rollbackReadTransaction(ctx context.Context, tx pgx.Tx) error {
	rollbackCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		time.Second,
	)
	defer cancel()
	err := tx.Rollback(rollbackCtx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}

func queryStreamEvents(
	ctx context.Context,
	tx pgx.Tx,
	from int64,
	through int64,
	limit int,
) ([]streamclient.Event, error) {
	rows, err := tx.Query(ctx, `
		SELECT seq, stream, kind, entity_key, occurred_at, payload
		FROM change_events
		WHERE stream = $1
		  AND seq > $2
		  AND seq <= $3
		ORDER BY seq
		LIMIT $4
	`, entityStream, from, through, limit)
	if err != nil {
		return nil, fmt.Errorf("query watch replay: %w", err)
	}
	defer rows.Close()
	events := make([]streamclient.Event, 0)
	for rows.Next() {
		var event streamclient.Event
		if err := rows.Scan(
			&event.Seq,
			&event.Stream,
			&event.Kind,
			&event.EntityKey,
			&event.OccurredAt,
			&event.Payload,
		); err != nil {
			return nil, fmt.Errorf("scan watch replay: %w", err)
		}
		event.Payload = append(json.RawMessage(nil), event.Payload...)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate watch replay: %w", err)
	}
	return events, nil
}

const snapshotEntitiesCTE = `
	WITH snapshot_entities AS (
		    SELECT 'repository.snapshot'::text AS kind,
		           'repo:' || installation_id || ':' || gh_id AS entity_key,
		           to_jsonb(repos) AS entity
		    FROM repos
		    WHERE tombstoned_at IS NULL
		    UNION ALL
		    SELECT 'repo_rules.snapshot',
		           'repo_rules:' || repos.installation_id || ':' || repos.gh_id,
		           jsonb_agg(to_jsonb(repo_rules) ORDER BY rule_key)
		    FROM repo_rules
		    JOIN repos ON repos.id = repo_rules.repo_id
		    WHERE repo_rules.tombstoned_at IS NULL
		      AND repos.tombstoned_at IS NULL
		    GROUP BY repos.installation_id, repos.gh_id
		    UNION ALL
		    SELECT 'pull_request.snapshot',
		           'pr:' || repos.installation_id || ':' || repos.gh_id || ':' || pull_requests.number,
		           to_jsonb(pull_requests)
		    FROM pull_requests
		    JOIN repos ON repos.id = pull_requests.repo_id
		    WHERE pull_requests.tombstoned_at IS NULL
		      AND repos.tombstoned_at IS NULL
		    UNION ALL
		    SELECT 'stack.snapshot',
		           'stack:' || repos.installation_id || ':' || repos.gh_id || ':' || stacks.number,
		           to_jsonb(stacks)
		    FROM stacks
		    JOIN repos ON repos.id = stacks.repo_id
		    WHERE stacks.tombstoned_at IS NULL
		      AND repos.tombstoned_at IS NULL
		    UNION ALL
		    SELECT 'checks.snapshot',
		           'checks:' || repos.installation_id || ':' || repos.gh_id || ':' || check_runs.head_sha,
		           jsonb_agg(to_jsonb(check_runs) ORDER BY check_runs.name, check_runs.gh_id)
		    FROM check_runs
		    JOIN repos ON repos.id = check_runs.repo_id
		    WHERE check_runs.tombstoned_at IS NULL
		      AND repos.tombstoned_at IS NULL
		    GROUP BY repos.installation_id, repos.gh_id, check_runs.head_sha
	)
`

func loadSnapshotEntities(
	ctx context.Context,
	tx pgx.Tx,
	entityLimit int,
	bytesLimit int,
) ([]materializedEvent, error) {
	var entityCount, totalBytes int64
	if err := tx.QueryRow(ctx, snapshotEntitiesCTE+`
		SELECT count(*), COALESCE(sum(
		    octet_length(kind) +
		    octet_length(entity_key) +
		    octet_length(entity::text)
		), 0)
		FROM snapshot_entities
	`).Scan(&entityCount, &totalBytes); err != nil {
		return nil, fmt.Errorf("measure watch snapshot: %w", err)
	}
	if entityCount > int64(entityLimit) {
		return nil, &snapshotLimitError{
			kind:    "entities",
			maximum: entityLimit,
		}
	}
	if totalBytes > int64(bytesLimit) {
		return nil, &snapshotLimitError{
			kind:    "bytes",
			maximum: bytesLimit,
		}
	}

	rows, err := tx.Query(ctx, snapshotEntitiesCTE+`
		SELECT kind, entity_key, entity
		FROM snapshot_entities
		ORDER BY kind, entity_key
	`)
	if err != nil {
		return nil, fmt.Errorf("query watch snapshot: %w", err)
	}
	defer rows.Close()
	result := make([]materializedEvent, 0, int(entityCount))
	materializedBytes := 0
	for rows.Next() {
		var event materializedEvent
		var entity json.RawMessage
		if err := rows.Scan(&event.Kind, &event.EntityKey, &entity); err != nil {
			return nil, fmt.Errorf("scan watch snapshot: %w", err)
		}
		event.Entity = append(json.RawMessage(nil), entity...)
		materializedBytes += len(event.Kind) + len(event.EntityKey) + len(event.Entity)
		if materializedBytes > bytesLimit {
			return nil, &snapshotLimitError{
				kind:    "bytes",
				maximum: bytesLimit,
			}
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate watch snapshot: %w", err)
	}
	return result, nil
}

type snapshotLimitError struct {
	kind    string
	maximum int
}

func (e *snapshotLimitError) Error() string {
	return fmt.Sprintf(
		"watch snapshot exceeds maximum %s (%d)",
		e.kind,
		e.maximum,
	)
}

func resumeSequence(request *http.Request) (int64, bool, error) {
	value := ""
	if request.URL.Query().Has("from") {
		value = request.URL.Query().Get("from")
	} else {
		value = request.Header.Get("Last-Event-ID")
	}
	if strings.TrimSpace(value) == "" {
		if request.URL.Query().Has("from") {
			return 0, false, fmt.Errorf("from must be a non-negative sequence")
		}
		return 0, false, nil
	}
	seq, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seq < 0 {
		return 0, false, fmt.Errorf("resume sequence must be a non-negative integer")
	}
	return seq, true, nil
}

func requestLimit(request *http.Request) (int, error) {
	value := request.URL.Query().Get("limit")
	if value == "" {
		return defaultListLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 || limit > maximumListLimit {
		return 0, fmt.Errorf(
			"limit must be between 1 and %d",
			maximumListLimit,
		)
	}
	return limit, nil
}

func positivePathInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("value must be positive")
	}
	return parsed, nil
}

func queryJSONRows(
	ctx context.Context,
	query interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	statement string,
	args ...any,
) ([]json.RawMessage, error) {
	rows, err := query.Query(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("query JSON rows: %w", err)
	}
	defer rows.Close()
	result := make([]json.RawMessage, 0)
	for rows.Next() {
		var encoded json.RawMessage
		if err := rows.Scan(&encoded); err != nil {
			return nil, fmt.Errorf("scan JSON row: %w", err)
		}
		result = append(result, append(json.RawMessage(nil), encoded...))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate JSON rows: %w", err)
	}
	return result, nil
}

func writeSSE(
	response http.ResponseWriter,
	flusher http.Flusher,
	eventName string,
	id *int64,
	payload any,
) error {
	controller := http.NewResponseController(response)
	if err := controller.SetWriteDeadline(time.Now().Add(sseWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("set SSE write deadline: %w", err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode SSE event: %w", err)
	}
	if _, err := fmt.Fprintf(response, "event: %s\n", eventName); err != nil {
		return fmt.Errorf("write SSE event name: %w", err)
	}
	if id != nil {
		if _, err := fmt.Fprintf(response, "id: %d\n", *id); err != nil {
			return fmt.Errorf("write SSE event id: %w", err)
		}
	}
	if _, err := fmt.Fprintf(response, "data: %s\n\n", encoded); err != nil {
		return fmt.Errorf("write SSE event data: %w", err)
	}
	flusher.Flush()
	return nil
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(payload); err != nil {
		slog.Debug("write JSON response", "error", err)
	}
}

func writeRawJSON(response http.ResponseWriter, status int, payload []byte) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	if _, err := response.Write(append(payload, '\n')); err != nil {
		slog.Debug("write JSON response", "error", err)
	}
}

func writeJSONError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}

func (s *apiServer) writeDatabaseError(
	response http.ResponseWriter,
	operation string,
	err error,
) {
	slog.Error(operation, "error", err)
	writeJSONError(response, http.StatusInternalServerError, "database query failed")
}
