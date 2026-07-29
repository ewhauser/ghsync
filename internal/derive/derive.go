// Package derive owns M5's pure derivation seam and C-P5 dirty-set drain loop.
// Classification policy remains outside the sync engine.
package derive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/acme/frontier/internal/outbox"
)

const (
	defaultDirtyCap     = 500
	defaultPollInterval = 500 * time.Millisecond
	dirtyNotifyChannel  = "frontier_derivation_dirty"
)

// Deriver is the pure C-D1 seam. Implementations may inspect only Snapshot
// and must perform no I/O.
type Deriver interface {
	Derive(Snapshot) []ScopeResult
}

// NoopDeriver is the default M5 implementation. It proves the drain loop and
// leaves classification to the future derivation project.
type NoopDeriver struct{}

// Derive returns one empty, scope-owned result per input and performs no I/O.
func (NoopDeriver) Derive(snapshot Snapshot) []ScopeResult {
	results := make([]ScopeResult, 0, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		results = append(results, ScopeResult{ScopeKey: scope.ScopeKey})
	}
	return results
}

// Snapshot is one snapshot-consistent cache view for an entire claimed dirty
// set (C-D2/C-P5).
type Snapshot struct {
	Scopes []ScopeSnapshot
}

// ScopeSnapshot contains a dirty scope and its cache rows encoded as one
// stable JSON document for the pure deriver.
type ScopeSnapshot struct {
	ScopeKey string
	OrgID    int64
	RepoID   int64
	Data     json.RawMessage
}

// WorkItem is the minimal derived value persisted by M5.
type WorkItem struct {
	IdentityKey string          `json:"identity_key"`
	OrgID       int64           `json:"org_id"`
	Payload     json.RawMessage `json:"payload"`
}

// ScopeResult is the complete derived output owned by one claimed C-D2 scope.
// Returning an empty WorkItems set removes every prior item for that scope.
type ScopeResult struct {
	ScopeKey  string     `json:"scope_key"`
	WorkItems []WorkItem `json:"work_items"`
}

// StackIdentity returns the stable C-D3 identity for a repository stack.
func StackIdentity(repositoryGitHubID int64, stackNumber int) string {
	return outbox.StackWorkItemKey(repositoryGitHubID, stackNumber)
}

// PullRequestIdentity returns the stable C-D3 identity for a loose pull
// request.
func PullRequestIdentity(repositoryGitHubID int64, pullNumber int) string {
	return outbox.PullRequestWorkItemKey(repositoryGitHubID, pullNumber)
}

// Options configures the dirty-set loop.
type Options struct {
	Pool         *pgxpool.Pool
	Deriver      Deriver
	DirtyCap     int
	PollInterval time.Duration
	Observer     Observer
}

// Observer is M6's C-P5 pass-duration seam.
type Observer interface {
	DeriverPass(context.Context, int, time.Duration, error)
}

type noopObserver struct{}

func (noopObserver) DeriverPass(context.Context, int, time.Duration, error) {}

// Service drains dirty scopes and applies each derivation batch atomically.
type Service struct {
	pool         *pgxpool.Pool
	deriver      Deriver
	loader       SnapshotLoader
	dirtyCap     int
	pollInterval time.Duration
	observer     Observer
}

// New constructs a C-D2/C-P5 derivation service. NoopDeriver is wired when no
// implementation is supplied.
func New(options Options) (*Service, error) {
	if options.Pool == nil {
		return nil, fmt.Errorf("deriver requires Postgres")
	}
	if options.Deriver == nil {
		options.Deriver = NoopDeriver{}
	}
	if options.DirtyCap == 0 {
		options.DirtyCap = defaultDirtyCap
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.DirtyCap < 0 {
		return nil, fmt.Errorf("deriver dirty cap must be positive")
	}
	if options.PollInterval < 0 {
		return nil, fmt.Errorf("deriver poll interval must be positive")
	}
	if options.Observer == nil {
		options.Observer = noopObserver{}
	}
	return &Service{
		pool:         options.Pool,
		deriver:      options.Deriver,
		loader:       SnapshotLoader{},
		dirtyCap:     options.DirtyCap,
		pollInterval: options.PollInterval,
		observer:     options.Observer,
	}, nil
}

// RunOnce claims the entire currently available dirty set up to DirtyCap,
// loads one cache snapshot, calls the pure deriver once, and writes work items,
// work_items events, and dirty-row deletes in one transaction (C-P5).
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	startedAt := time.Now()
	count, err := s.runOnce(ctx)
	s.observer.DeriverPass(ctx, count, time.Since(startedAt), err)
	return count, err
}

func (s *Service) runOnce(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin derivation pass: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT scope_key
		FROM derivation_dirty
		ORDER BY marked_at, scope_key
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, s.dirtyCap)
	if err != nil {
		return 0, fmt.Errorf("claim derivation dirty set: %w", err)
	}
	scopeKeys, err := pgx.CollectRows(
		rows,
		func(row pgx.CollectableRow) (string, error) {
			var key string
			if err := row.Scan(&key); err != nil {
				return "", err
			}
			return key, nil
		},
	)
	if err != nil {
		return 0, fmt.Errorf("scan derivation dirty set: %w", err)
	}
	if len(scopeKeys) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty derivation pass: %w", err)
		}
		return 0, nil
	}

	snapshot, err := s.loader.Load(ctx, tx, scopeKeys)
	if err != nil {
		return 0, err
	}
	results := s.deriver.Derive(snapshot)
	items, err := validateScopeResults(snapshot, results)
	if err != nil {
		return 0, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].IdentityKey < items[j].IdentityKey
	})
	encoded, err := json.Marshal(items)
	if err != nil {
		return 0, fmt.Errorf("encode derived work items: %w", err)
	}
	if err := outbox.AcquireWriterFence(ctx, tx); err != nil {
		return 0, err
	}

	// C-P5: each claimed scope's complete prior set is reconciled with the
	// returned set. Changed and removed references share this transaction.
	eventRows, err := tx.Query(ctx, `
			WITH claimed(scope_key) AS (
			    SELECT unnest($1::text[])
			),
			input AS (
			    SELECT scope_key, identity_key, org_id, payload
			    FROM jsonb_to_recordset($2::jsonb) AS item(
			        scope_key text,
			        identity_key text,
			        org_id bigint,
			        payload jsonb
			    )
			),
			upserted AS (
			    INSERT INTO work_items (
			        scope_key, identity_key, org_id, payload, updated_at
			    )
			    SELECT scope_key, identity_key, org_id, payload,
			           clock_timestamp()
			    FROM input
			    ON CONFLICT (identity_key) DO UPDATE
			    SET scope_key = EXCLUDED.scope_key,
			        org_id = EXCLUDED.org_id,
			        payload = EXCLUDED.payload,
			        updated_at = EXCLUDED.updated_at
			    WHERE ROW(
			            work_items.scope_key,
			            work_items.org_id,
			            work_items.payload
			        )
			        IS DISTINCT FROM
			        ROW(
			            EXCLUDED.scope_key,
			            EXCLUDED.org_id,
			            EXCLUDED.payload
			        )
			    RETURNING scope_key, identity_key
			),
			removed AS (
			    DELETE FROM work_items AS prior
			    USING claimed
			    WHERE prior.scope_key = claimed.scope_key
			      AND NOT EXISTS (
			          SELECT 1
			          FROM input
			          WHERE input.identity_key = prior.identity_key
			      )
			    RETURNING prior.scope_key, prior.identity_key
			),
			events AS (
			    SELECT scope_key, identity_key, $4::text AS kind
			    FROM upserted
			    UNION ALL
			    SELECT scope_key, identity_key, $5::text AS kind
			    FROM removed
			)
			INSERT INTO change_events (
			    stream, kind, entity_key, occurred_at, payload
			)
			SELECT $3, kind, identity_key, clock_timestamp(),
			       jsonb_build_object(
			           'version', 1,
			           'identity_key', identity_key,
			           'scope_key', scope_key
			       )
			FROM events
			ORDER BY identity_key, kind
			RETURNING seq
		`,
		scopeKeys,
		encoded,
		outbox.WorkItemsStream,
		outbox.WorkItemChangedKind,
		outbox.WorkItemRemovedKind,
	)
	if err != nil {
		return 0, fmt.Errorf("apply derived work-item batch: %w", err)
	}
	eventSeqs, err := pgx.CollectRows(
		eventRows,
		func(row pgx.CollectableRow) (int64, error) {
			var seq int64
			if err := row.Scan(&seq); err != nil {
				return 0, err
			}
			return seq, nil
		},
	)
	if err != nil {
		return 0, fmt.Errorf("scan derived change sequences: %w", err)
	}
	for _, seq := range eventSeqs {
		if err := outbox.AfterSequenceAllocated(
			ctx,
			outbox.DeriverOrigin,
			seq,
		); err != nil {
			return 0, fmt.Errorf(
				"after derived change sequence allocation: %w", err,
			)
		}
	}

	// A writer that marks a claimed key during Derive waits on its row lock.
	// After this delete commits, that upsert creates a fresh mark, so work
	// arriving mid-pass survives (C-D2).
	if _, err := tx.Exec(ctx, `
		DELETE FROM derivation_dirty
		WHERE scope_key = ANY($1::text[])
	`, scopeKeys); err != nil {
		return 0, fmt.Errorf("clear derived dirty set: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit derivation pass: %w", err)
	}
	return len(scopeKeys), nil
}

// Run drains full batches immediately, then uses dirty-set NOTIFY as a latency
// hint with interval polling as the correctness path.
func (s *Service) Run(ctx context.Context) error {
	listener, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire deriver listener: %w", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, "LISTEN "+dirtyNotifyChannel); err != nil {
		return fmt.Errorf("listen for dirty scopes: %w", err)
	}

	for {
		count, err := s.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if count == s.dirtyCap {
			continue
		}
		waitCtx, cancel := context.WithTimeout(ctx, s.pollInterval)
		_, waitErr := listener.Conn().WaitForNotification(waitCtx)
		cancel()
		switch {
		case waitErr == nil:
		case errors.Is(waitErr, context.DeadlineExceeded):
		case ctx.Err() != nil:
			return nil
		default:
			return fmt.Errorf("wait for dirty notification: %w", waitErr)
		}
	}
}

type ownedWorkItem struct {
	ScopeKey    string          `json:"scope_key"`
	IdentityKey string          `json:"identity_key"`
	OrgID       int64           `json:"org_id"`
	Payload     json.RawMessage `json:"payload"`
}

func validateScopeResults(
	snapshot Snapshot,
	results []ScopeResult,
) ([]ownedWorkItem, error) {
	claimed := make(map[string]ScopeSnapshot, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		claimed[scope.ScopeKey] = scope
	}
	seenScopes := make(map[string]struct{}, len(results))
	seenItems := make(map[string]struct{})
	items := make([]ownedWorkItem, 0)
	for _, result := range results {
		scope, ok := claimed[result.ScopeKey]
		if !ok {
			return nil, fmt.Errorf(
				"deriver returned unclaimed scope %q", result.ScopeKey,
			)
		}
		if _, duplicate := seenScopes[result.ScopeKey]; duplicate {
			return nil, fmt.Errorf(
				"deriver returned duplicate scope %q", result.ScopeKey,
			)
		}
		seenScopes[result.ScopeKey] = struct{}{}
		parsed, err := parseScope(result.ScopeKey)
		if err != nil {
			return nil, err
		}
		expectedIdentity := identityForScope(parsed)
		for _, item := range result.WorkItems {
			if item.IdentityKey == "" {
				return nil, fmt.Errorf("derived work item identity is required")
			}
			if item.IdentityKey != expectedIdentity {
				return nil, fmt.Errorf(
					"derived work item identity %q is not owned by scope %q; want %q",
					item.IdentityKey,
					result.ScopeKey,
					expectedIdentity,
				)
			}
			if item.OrgID <= 0 || item.OrgID != scope.OrgID {
				return nil, fmt.Errorf(
					"derived work item %q has org ID %d, scope %q owns org %d",
					item.IdentityKey,
					item.OrgID,
					result.ScopeKey,
					scope.OrgID,
				)
			}
			if len(item.Payload) == 0 || !json.Valid(item.Payload) {
				return nil, fmt.Errorf(
					"derived work item %q has invalid JSON payload",
					item.IdentityKey,
				)
			}
			if _, duplicate := seenItems[item.IdentityKey]; duplicate {
				return nil, fmt.Errorf(
					"deriver returned duplicate identity %q",
					item.IdentityKey,
				)
			}
			seenItems[item.IdentityKey] = struct{}{}
			items = append(items, ownedWorkItem{
				ScopeKey:    result.ScopeKey,
				IdentityKey: item.IdentityKey,
				OrgID:       item.OrgID,
				Payload:     item.Payload,
			})
		}
	}
	for scopeKey := range claimed {
		if _, ok := seenScopes[scopeKey]; !ok {
			return nil, fmt.Errorf(
				"deriver omitted claimed scope %q", scopeKey,
			)
		}
	}
	return items, nil
}

func identityForScope(scope parsedScope) string {
	if scope.Kind == "stack" {
		return StackIdentity(scope.RepositoryID, scope.Number)
	}
	return PullRequestIdentity(scope.RepositoryID, scope.Number)
}

type parsedScope struct {
	ScopeKey     string `json:"scope_key"`
	Kind         string `json:"kind"`
	Installation int64  `json:"installation_id"`
	RepositoryID int64  `json:"repo_id"`
	Number       int    `json:"number"`
}

func parseScope(key string) (parsedScope, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 4 || (parts[0] != "stack" && parts[0] != "pr") {
		return parsedScope{}, fmt.Errorf(
			"invalid derivation scope key %q", key,
		)
	}
	installation, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || installation <= 0 {
		return parsedScope{}, fmt.Errorf(
			"invalid derivation installation in %q", key,
		)
	}
	repositoryID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || repositoryID <= 0 {
		return parsedScope{}, fmt.Errorf(
			"invalid derivation repository in %q", key,
		)
	}
	number, err := strconv.Atoi(parts[3])
	if err != nil || number <= 0 {
		return parsedScope{}, fmt.Errorf(
			"invalid derivation number in %q", key,
		)
	}
	return parsedScope{
		ScopeKey:     key,
		Kind:         parts[0],
		Installation: installation,
		RepositoryID: repositoryID,
		Number:       number,
	}, nil
}
