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
)

const (
	defaultDirtyCap     = 500
	defaultPollInterval = 500 * time.Millisecond
	dirtyNotifyChannel  = "frontier_derivation_dirty"
	workItemsStream     = "work_items"
	workItemChangedKind = "work_item.changed"
)

// Deriver is the pure C-D1 seam. Implementations may inspect only Snapshot
// and must perform no I/O.
type Deriver interface {
	Derive(Snapshot) []WorkItem
}

// NoopDeriver is the default M5 implementation. It proves the drain loop and
// leaves classification to the future derivation project.
type NoopDeriver struct{}

// Derive returns no work items and performs no I/O.
func (NoopDeriver) Derive(Snapshot) []WorkItem { return nil }

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

// StackIdentity returns the stable C-D3 identity for a repository stack.
func StackIdentity(repositoryID int64, stackNumber int) string {
	return "repo:" + strconv.FormatInt(repositoryID, 10) +
		":stack:" + strconv.Itoa(stackNumber)
}

// PullRequestIdentity returns the stable C-D3 identity for a loose pull
// request.
func PullRequestIdentity(repositoryID int64, pullNumber int) string {
	return "repo:" + strconv.FormatInt(repositoryID, 10) +
		":pr:" + strconv.Itoa(pullNumber)
}

// Options configures the dirty-set loop.
type Options struct {
	Pool         *pgxpool.Pool
	Deriver      Deriver
	DirtyCap     int
	PollInterval time.Duration
}

// Service drains dirty scopes and applies each derivation batch atomically.
type Service struct {
	pool         *pgxpool.Pool
	deriver      Deriver
	loader       SnapshotLoader
	dirtyCap     int
	pollInterval time.Duration
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
	return &Service{
		pool:         options.Pool,
		deriver:      options.Deriver,
		loader:       SnapshotLoader{},
		dirtyCap:     options.DirtyCap,
		pollInterval: options.PollInterval,
	}, nil
}

// RunOnce claims the entire currently available dirty set up to DirtyCap,
// loads one cache snapshot, calls the pure deriver once, and writes work items,
// work_items events, and dirty-row deletes in one transaction (C-P5).
func (s *Service) RunOnce(ctx context.Context) (int, error) {
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
	items := s.deriver.Derive(snapshot)
	if items == nil {
		items = []WorkItem{}
	}
	if err := validateWorkItems(items); err != nil {
		return 0, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].IdentityKey < items[j].IdentityKey
	})
	encoded, err := json.Marshal(items)
	if err != nil {
		return 0, fmt.Errorf("encode derived work items: %w", err)
	}

	// C-P5: every changed work item and its reference event are set-at-a-time
	// inside the same dirty-batch transaction.
	if _, err := tx.Exec(ctx, `
		WITH input AS (
		    SELECT identity_key, org_id, payload
		    FROM jsonb_to_recordset($1::jsonb) AS item(
		        identity_key text,
		        org_id bigint,
		        payload jsonb
		    )
		),
		upserted AS (
		    INSERT INTO work_items (
		        identity_key, org_id, payload, updated_at
		    )
		    SELECT identity_key, org_id, payload, clock_timestamp()
		    FROM input
		    ON CONFLICT (identity_key) DO UPDATE
		    SET org_id = EXCLUDED.org_id,
		        payload = EXCLUDED.payload,
		        updated_at = EXCLUDED.updated_at
		    WHERE ROW(work_items.org_id, work_items.payload)
		        IS DISTINCT FROM
		        ROW(EXCLUDED.org_id, EXCLUDED.payload)
		    RETURNING identity_key
		)
		INSERT INTO change_events (
		    stream, kind, entity_key, occurred_at, payload
		)
		SELECT $2, $3, identity_key, clock_timestamp(),
		       jsonb_build_object(
		           'version', 1,
		           'identity_key', identity_key
		       )
		FROM upserted
		ORDER BY identity_key
	`, encoded, workItemsStream, workItemChangedKind); err != nil {
		return 0, fmt.Errorf("apply derived work-item batch: %w", err)
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

func validateWorkItems(items []WorkItem) error {
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.IdentityKey == "" {
			return fmt.Errorf("derived work item identity is required")
		}
		if item.OrgID <= 0 {
			return fmt.Errorf(
				"derived work item %q has invalid org ID",
				item.IdentityKey,
			)
		}
		if len(item.Payload) == 0 || !json.Valid(item.Payload) {
			return fmt.Errorf(
				"derived work item %q has invalid JSON payload",
				item.IdentityKey,
			)
		}
		if _, duplicate := seen[item.IdentityKey]; duplicate {
			return fmt.Errorf(
				"deriver returned duplicate identity %q",
				item.IdentityKey,
			)
		}
		seen[item.IdentityKey] = struct{}{}
	}
	return nil
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
