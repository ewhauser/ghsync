package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// BranchPushHint is the narrow local fact proven by a push payload. It is not
// an authoritative pull-request, stack, or repository observation.
type BranchPushHint struct {
	RepoFullName    string
	Branch          string
	BeforeSHA       string
	AfterSHA        string
	TransitionKnown bool
	Deleted         bool
	Forced          bool
	DeliveryGUID    string
	ReceivedAt      time.Time
}

// BranchReconciliationTarget is one pointer in a bounded branch page. The
// bulk transaction advances and claims its entity generation locally; any
// later direct intent advances it again and therefore supersedes this target.
type BranchReconciliationTarget struct {
	RefreshKind       string `json:"kind"`
	RefreshKey        string `json:"key"`
	EntityKey         string `json:"entity_key"`
	RefreshGeneration int64  `json:"generation"`
}

// BranchRefreshGenerationKey is one direct-refresh generation advanced by a
// branch transition. Bulk apply locks these keys nonblockingly after its
// entity-row transition, using the same advisory/row protocol as ordinary
// refresh producers without inverting transactional entity follow-up hooks.
type BranchRefreshGenerationKey struct {
	Kind string `json:"kind"`
	Key  string `json:"refresh_key"`
}

// ErrBranchRefreshGenerationContention asks dispatch to roll back and retry
// the complete batch after an already-open entity transaction releases a
// generation row. The bulk transaction has made no externally visible change
// until that retry commits.
var ErrBranchRefreshGenerationContention = errors.New(
	"branch refresh generation contention",
)

// BranchBulkResult describes one committed-ready local transition and the
// bounded remote work that must follow it.
type BranchBulkResult struct {
	RepoID               int64
	Generation           int64
	Targets              []BranchReconciliationTarget
	RepositoryRefreshKey string
	Repositories         int
	PullRequests         int
	Stacks               int
	SupersededPages      int64
}

// ApplyBranchPushTx advances the branch generation, applies only exact
// before-SHA matches, retains dependent observations behind read-time SHA
// fences, marks derivation dirty, and emits reference events. The caller owns
// tx and must acquire the C-S2 writer fence before calling this method. No
// method on this path performs network I/O (C-C6).
func (w *EntityWriter) ApplyBranchPushTx(
	ctx context.Context,
	tx pgx.Tx,
	hint *BranchPushHint,
) (BranchBulkResult, error) {
	if tx == nil || hint.RepoFullName == "" || hint.Branch == "" ||
		hint.DeliveryGUID == "" || hint.ReceivedAt.IsZero() {
		return BranchBulkResult{}, fmt.Errorf("invalid branch push hint")
	}
	appliedAt, err := databaseClock(ctx, tx)
	if err != nil {
		return BranchBulkResult{}, err
	}
	queries := dbgen.New(tx)
	state, err := queries.BeginBranchReconciliation(
		ctx,
		dbgen.BeginBranchReconciliationParams{
			RepoFullName:    hint.RepoFullName,
			Branch:          hint.Branch,
			BeforeSha:       hint.BeforeSHA,
			AfterSha:        hint.AfterSHA,
			TransitionKnown: hint.TransitionKnown,
			Deleted:         hint.Deleted,
			Forced:          hint.Forced,
			DeliveryGuid:    hint.DeliveryGUID,
			ReceivedAt:      timestamp(hint.ReceivedAt),
			AppliedAt:       timestamp(appliedAt),
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return BranchBulkResult{}, nil
	}
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf(
			"begin branch reconciliation: %w", err,
		)
	}
	events, err := queries.ApplyBranchHintTransition(
		ctx,
		dbgen.ApplyBranchHintTransitionParams{
			RepoFullName:    hint.RepoFullName,
			AfterSha:        hint.AfterSHA,
			TransitionKnown: hint.TransitionKnown,
			Branch:          hint.Branch,
			BeforeSha:       hint.BeforeSHA,
			AppliedAt:       timestamp(appliedAt),
		},
	)
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf("apply branch SHA hint: %w", err)
	}
	result := BranchBulkResult{
		RepoID:          state.RepoID,
		Generation:      state.Generation,
		SupersededPages: state.SupersededPages,
	}
	for _, event := range events {
		switch event.Kind {
		case outbox.RepositoryChangedKind:
			result.Repositories++
		case outbox.PullRequestChangedKind:
			result.PullRequests++
		case outbox.StackChangedKind:
			result.Stacks++
		default:
			return BranchBulkResult{}, fmt.Errorf(
				"apply branch SHA hint returned unknown event kind %q",
				event.Kind,
			)
		}
		if err := outbox.AfterSequenceAllocated(
			ctx, outbox.EntityWriterOrigin, event.Seq,
		); err != nil {
			return BranchBulkResult{}, fmt.Errorf(
				"after branch event sequence allocation: %w", err,
			)
		}
	}
	generationKeys, err := w.BranchReconciliationGenerationKeysTx(
		ctx, tx, hint.RepoFullName, hint.Branch,
	)
	if err != nil {
		return BranchBulkResult{}, err
	}
	for _, key := range generationKeys {
		if key.Kind == "refresh_repository" {
			result.RepositoryRefreshKey = key.Key
			break
		}
	}
	encodedGenerationKeys, err := json.Marshal(generationKeys)
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf(
			"encode branch generation row locks: %w", err,
		)
	}
	advisoryLocks, err := queries.TryAcquireRefreshIntentGenerationLocks(
		ctx, encodedGenerationKeys,
	)
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf(
			"lock branch generation advisories: %w", err,
		)
	}
	if !advisoryLocks.Acquired {
		return BranchBulkResult{}, fmt.Errorf(
			"lock branch generation advisories: acquired %d of %d: %w",
			advisoryLocks.AcquiredCount,
			advisoryLocks.KeyCount,
			ErrBranchRefreshGenerationContention,
		)
	}
	rowLocks, err := queries.TryAcquireRefreshIntentGenerationRowLocks(
		ctx, encodedGenerationKeys,
	)
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf(
			"lock branch generation rows: %w", err,
		)
	}
	if !rowLocks.Acquired {
		return BranchBulkResult{}, fmt.Errorf(
			"lock branch generation rows: acquired %d of %d: %w",
			rowLocks.AcquiredCount,
			rowLocks.KeyCount,
			ErrBranchRefreshGenerationContention,
		)
	}
	targetRows, err := queries.AdvanceBranchReconciliationTargetGenerations(
		ctx,
		dbgen.AdvanceBranchReconciliationTargetGenerationsParams{
			RepoFullName: hint.RepoFullName,
			Branch:       hint.Branch,
			AppliedAt:    timestamp(appliedAt),
		},
	)
	if err != nil {
		return BranchBulkResult{}, fmt.Errorf(
			"list branch reconciliation targets: %w", err,
		)
	}
	result.Targets = make(
		[]BranchReconciliationTarget, 0, len(targetRows),
	)
	for _, target := range targetRows {
		result.Targets = append(result.Targets, BranchReconciliationTarget{
			RefreshKind:       target.RefreshKind,
			RefreshKey:        target.RefreshKey,
			EntityKey:         target.EntityKey,
			RefreshGeneration: target.RefreshGeneration,
		})
	}
	sort.Slice(result.Targets, func(i, j int) bool {
		if result.Targets[i].RefreshKind != result.Targets[j].RefreshKind {
			return result.Targets[i].RefreshKind < result.Targets[j].RefreshKind
		}
		return result.Targets[i].RefreshKey < result.Targets[j].RefreshKey
	})
	return result, nil
}

// BranchReconciliationGenerationKeysTx returns the direct entity generation
// keys that the corresponding bulk transaction will advance. ApplyBranchPushTx
// acquires their standard refresh-generation locks only after changing entity
// rows so it composes with entity writers whose transactional hooks produce
// refresh generations.
func (w *EntityWriter) BranchReconciliationGenerationKeysTx(
	ctx context.Context,
	tx pgx.Tx,
	repoFullName string,
	branch string,
) ([]BranchRefreshGenerationKey, error) {
	if tx == nil || repoFullName == "" || branch == "" {
		return nil, fmt.Errorf("invalid branch generation key request")
	}
	rows, err := dbgen.New(tx).ListBranchReconciliationGenerationKeys(
		ctx,
		dbgen.ListBranchReconciliationGenerationKeysParams{
			RepoFullName: repoFullName,
			Branch:       branch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list branch generation keys: %w", err)
	}
	keys := make([]BranchRefreshGenerationKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, BranchRefreshGenerationKey{
			Kind: row.RefreshKind,
			Key:  row.RefreshKey,
		})
	}
	return keys, nil
}

// RecordBranchReconciliationPagesTx makes the durable page manifest part of
// the same transaction that inserts its River jobs.
func (w *EntityWriter) RecordBranchReconciliationPagesTx(
	ctx context.Context,
	tx pgx.Tx,
	repoID int64,
	branch string,
	generation int64,
	pageTargetCounts []int,
) error {
	type pageRecord struct {
		PageNumber  int `json:"page_number"`
		TargetCount int `json:"target_count"`
	}
	pages := make([]pageRecord, 0, len(pageTargetCounts))
	total := 0
	for index, count := range pageTargetCounts {
		if count <= 0 {
			return fmt.Errorf("branch reconciliation page must not be empty")
		}
		pages = append(pages, pageRecord{
			PageNumber:  index + 1,
			TargetCount: count,
		})
		total += count
	}
	encoded, err := json.Marshal(pages)
	if err != nil {
		return fmt.Errorf("encode branch reconciliation pages: %w", err)
	}
	createdAt, err := databaseClock(ctx, tx)
	if err != nil {
		return err
	}
	if err := dbgen.New(tx).RecordBranchReconciliationPages(
		ctx,
		dbgen.RecordBranchReconciliationPagesParams{
			TargetCount: int32(total),
			PageCount:   int32(len(pages)),
			CreatedAt:   timestamp(createdAt),
			RepoID:      repoID,
			Branch:      branch,
			Generation:  generation,
			Pages:       encoded,
		},
	); err != nil {
		return fmt.Errorf("record branch reconciliation pages: %w", err)
	}
	return nil
}

// BranchTargets returns the derivation scopes affected by a branch change.
func (w *EntityWriter) BranchTargets(
	ctx context.Context,
	repoFullName string,
	branch string,
) ([]string, error) {
	queries := dbgen.New(w.pool)
	prs, err := queries.ListPRsAffectedByBranch(
		ctx,
		dbgen.ListPRsAffectedByBranchParams{
			RepoFullName: repoFullName,
			Branch:       branch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list branch PRs: %w", err)
	}
	stacks, err := queries.ListStacksAffectedByBranch(
		ctx,
		dbgen.ListStacksAffectedByBranchParams{
			RepoFullName: repoFullName,
			Branch:       branch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("list branch stacks: %w", err)
	}
	targets := make([]string, 0, len(prs)+len(stacks))
	seenStacks := make(map[int]struct{}, len(stacks))
	for _, number := range stacks {
		seenStacks[int(number)] = struct{}{}
		targets = append(
			targets,
			fmt.Sprintf("stack:%s:%d", repoFullName, number),
		)
	}
	for _, pr := range prs {
		if pr.StackNumber.Valid {
			number := int(pr.StackNumber.Int32)
			if _, seen := seenStacks[number]; !seen {
				seenStacks[number] = struct{}{}
				targets = append(
					targets,
					fmt.Sprintf("stack:%s:%d", repoFullName, number),
				)
			}
		}
		targets = append(
			targets,
			fmt.Sprintf("pr:%s:%d", repoFullName, pr.Number),
		)
	}
	sort.Strings(targets)
	return targets, nil
}
