// Package drift implements C-O3's sampled, full-fetch semantic validation.
package drift

import (
	"context"
	"crypto/md5" //nolint:gosec // non-security content identity
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/opsstate"
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store"
	"github.com/acme/frontier/internal/store/dbgen"
)

const jobKindDetect = "drift_detect"

var driftEntityKinds = []string{
	"repository",
	"pull_request",
	"stack",
	"repo_rules",
	"review_threads",
	"checks",
}

type Config struct {
	InstallationID     int64
	Period             time.Duration
	SampleSize         int
	PageSize           int
	ResolvedRetention  time.Duration
	RetentionBatchSize int
	Now                func() time.Time
	Observer           Observer
}

type Observer interface {
	Divergence(context.Context, dbgen.DriftFinding)
	PersistentDivergence(context.Context, dbgen.DriftFinding)
}

type LogObserver struct{}

func (LogObserver) Divergence(
	_ context.Context,
	finding dbgen.DriftFinding,
) {
	slog.Error(
		"C-O3 semantic cache drift detected",
		"finding_id", finding.ID,
		"entity_kind", finding.EntityKind,
		"entity_key", finding.EntityKey,
		"diff", string(finding.Diff),
	)
}

func (LogObserver) PersistentDivergence(
	_ context.Context,
	finding dbgen.DriftFinding,
) {
	slog.Error(
		"C-O3 semantic cache drift persisted after self-heal",
		"finding_id", finding.ID,
		"entity_kind", finding.EntityKind,
		"entity_key", finding.EntityKey,
		"occurrences", finding.OccurrenceCount,
		"diff", string(finding.Diff),
	)
}

type noopObserver struct{}

func (noopObserver) Divergence(context.Context, dbgen.DriftFinding) {}
func (noopObserver) PersistentDivergence(
	context.Context,
	dbgen.DriftFinding,
) {
}

type Observers []Observer

func (observers Observers) Divergence(
	ctx context.Context,
	finding dbgen.DriftFinding,
) {
	for _, observer := range observers {
		if observer != nil {
			observer.Divergence(ctx, finding)
		}
	}
}

func (observers Observers) PersistentDivergence(
	ctx context.Context,
	finding dbgen.DriftFinding,
) {
	for _, observer := range observers {
		if observer != nil {
			observer.PersistentDivergence(ctx, finding)
		}
	}
}

type Options struct {
	Pool    *pgxpool.Pool
	REST    *gh.RESTClient
	GraphQL *gh.GraphQLClient
	Config  Config
}

type Service struct {
	pool    *pgxpool.Pool
	rest    *gh.RESTClient
	graphQL *gh.GraphQLClient
	config  Config

	riverMu sync.RWMutex
	river   *river.Client[pgx.Tx]
}

func New(options Options) (*Service, error) {
	if options.Pool == nil || options.REST == nil || options.GraphQL == nil {
		return nil, fmt.Errorf(
			"drift detector requires Postgres, REST, and GraphQL",
		)
	}
	if options.Config.InstallationID <= 0 ||
		options.Config.Period <= 0 ||
		options.Config.SampleSize < len(driftEntityKinds) ||
		options.Config.PageSize <= 0 ||
		options.Config.PageSize > 100 ||
		options.Config.ResolvedRetention <= 0 ||
		options.Config.RetentionBatchSize <= 0 {
		return nil, fmt.Errorf("invalid drift detector configuration")
	}
	if options.Config.Now == nil {
		options.Config.Now = time.Now
	}
	if options.Config.Observer == nil {
		options.Config.Observer = noopObserver{}
	}
	return &Service{
		pool:    options.Pool,
		rest:    options.REST,
		graphQL: options.GraphQL,
		config:  options.Config,
	}, nil
}

func (s *Service) SetRiverClient(client *river.Client[pgx.Tx]) {
	s.riverMu.Lock()
	s.river = client
	s.riverMu.Unlock()
}

func (s *Service) riverClient() *river.Client[pgx.Tx] {
	s.riverMu.RLock()
	defer s.riverMu.RUnlock()
	return s.river
}

type DetectArgs struct {
	InstallationID int64 `json:"installation_id"`
	SampleSize     int   `json:"sample_size"`
}

func (DetectArgs) Kind() string { return jobKindDetect }

type worker struct {
	river.WorkerDefaults[DetectArgs]
	service *Service
}

func (w *worker) Work(
	ctx context.Context,
	job *river.Job[DetectArgs],
) error {
	_, err := w.service.Detect(ctx, job.Args)
	return err
}

func (s *Service) RegisterWorker(workers *river.Workers) {
	river.AddWorker(workers, &worker{service: s})
}

func (s *Service) PeriodicJobs() []*river.PeriodicJob {
	return []*river.PeriodicJob{
		river.NewPeriodicJob(
			river.PeriodicInterval(s.config.Period),
			func() (river.JobArgs, *river.InsertOpts) {
				return DetectArgs{
						InstallationID: s.config.InstallationID,
						SampleSize:     s.config.SampleSize,
					},
					&river.InsertOpts{
						Queue:    queue.QueueDrift,
						Priority: 1,
					}
			},
			&river.PeriodicJobOpts{
				ID:         "frontier_drift_detect",
				RunOnStart: true,
			},
		),
	}
}

func PeriodicJobs(config Config) []*river.PeriodicJob {
	return (&Service{config: config}).PeriodicJobs()
}

func (s *Service) Detect(
	ctx context.Context,
	args DetectArgs,
) ([]dbgen.DriftFinding, error) {
	if args.InstallationID != s.config.InstallationID {
		return nil, nil
	}
	if args.SampleSize < len(driftEntityKinds) {
		return nil, fmt.Errorf(
			"drift sample size must cover all %d entity classes",
			len(driftEntityKinds),
		)
	}
	var eventPipelineBusy bool
	if err := s.pool.QueryRow(ctx, `
		SELECT
		    EXISTS (
		        SELECT 1
		        FROM webhook_deliveries
		        WHERE status IN ('pending', 'processing')
		    )
		    OR EXISTS (
		        SELECT 1
		        FROM refresh_intent_generations
		        WHERE completed_generation < generation
		    )
		    OR EXISTS (
		        SELECT 1
		        FROM river_job
		        WHERE queue IN (
		            'interactive', 'event', 'sweep', 'reconcile'
		        )
		          AND state IN (
		              'available', 'pending', 'retryable',
		              'running', 'scheduled'
		          )
		    )
		    OR EXISTS (
		        SELECT 1
		        FROM installation_backfill_cursors
		        WHERE phase <> 'done'
		    )
		    OR NOT EXISTS (
		        SELECT 1
		        FROM installation_backfill_cursors
		        WHERE installation_id = $1
		          AND phase = 'done'
		          AND completed_at IS NOT NULL
		    )
		    OR EXISTS (
		        SELECT 1
		        FROM backfill_cursors
		        WHERE phase <> 'done'
		    )
	`, args.InstallationID).Scan(&eventPipelineBusy); err != nil {
		return nil, fmt.Errorf("check drift event-pipeline quiescence: %w", err)
	}
	if eventPipelineBusy {
		return nil, nil
	}
	queries := dbgen.New(s.pool)
	findings := make([]dbgen.DriftFinding, 0)
	var inspected int64
	baseQuota := args.SampleSize / len(driftEntityKinds)
	extra := args.SampleSize % len(driftEntityKinds)
	for index, kind := range driftEntityKinds {
		quota := baseQuota
		if index < extra {
			quota++
		}
		var afterID int64
		afterID, err := queries.GetDriftSampleCursor(
			ctx,
			dbgen.GetDriftSampleCursorParams{
				InstallationID: args.InstallationID,
				EntityKind:     kind,
			},
		)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf(
				"read %s drift cursor: %w",
				kind,
				err,
			)
		}
		rows, err := queries.SampleCachedEntitiesByKind(
			ctx,
			dbgen.SampleCachedEntitiesByKindParams{
				InstallationID: args.InstallationID,
				EntityKind:     kind,
				AfterSourceID:  afterID,
				SampleSize:     int32(quota),
			},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"sample cached %s entities: %w",
				kind,
				err,
			)
		}
		inspected += int64(len(rows))
		for _, row := range rows {
			finding, recorded, err := s.inspectSample(
				ctx,
				driftSample{
					EntityKind:    row.EntityKind,
					SourceID:      row.SourceID,
					EntityKey:     row.EntityKey,
					LockKey:       row.LockKey,
					CacheSnapshot: row.CacheSnapshot,
				},
			)
			if err != nil {
				return nil, err
			}
			if recorded {
				findings = append(findings, finding)
			}
		}
		if len(rows) > 0 {
			if err := queries.UpsertDriftSampleCursor(
				ctx,
				dbgen.UpsertDriftSampleCursorParams{
					InstallationID: args.InstallationID,
					EntityKind:     kind,
					SourceID:       rows[len(rows)-1].SourceID,
				},
			); err != nil {
				return nil, fmt.Errorf(
					"advance %s drift cursor: %w",
					kind,
					err,
				)
			}
		}
	}
	if _, err := queries.PruneResolvedDriftFindingBatch(
		ctx,
		dbgen.PruneResolvedDriftFindingBatchParams{
			Cutoff: timestamptz(
				s.config.Now().Add(-s.config.ResolvedRetention),
			),
			BatchSize: int32(s.config.RetentionBatchSize),
		},
	); err != nil {
		return nil, fmt.Errorf("prune resolved drift findings: %w", err)
	}
	if err := opsstate.RecordSuccessN(
		ctx,
		s.pool,
		args.InstallationID,
		"drift",
		"detector",
		inspected,
	); err != nil {
		return nil, err
	}
	return findings, nil
}

type driftSample struct {
	EntityKind    string
	SourceID      int64
	EntityKey     string
	LockKey       string
	CacheSnapshot []byte
}

func (s *Service) inspectSample(
	ctx context.Context,
	sample driftSample,
) (dbgen.DriftFinding, bool, error) {
	observation, err := store.NewEntityWriter(s.pool).BeginObservation(
		ctx,
		sample.LockKey,
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, fmt.Errorf(
			"lock drift sample %s: %w",
			sample.EntityKey,
			err,
		)
	}
	defer observation.Close() //nolint:errcheck
	current, err := dbgen.New(s.pool).GetCachedEntitySnapshot(
		ctx,
		dbgen.GetCachedEntitySnapshotParams{
			InstallationID: s.config.InstallationID,
			EntityKind:     sample.EntityKind,
			EntityKey:      sample.EntityKey,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return dbgen.DriftFinding{}, false, nil
	}
	if err != nil {
		return dbgen.DriftFinding{}, false, fmt.Errorf(
			"reread locked drift sample %s: %w",
			sample.EntityKey,
			err,
		)
	}
	upstream, spec, err := s.fullFetch(
		ctx,
		current.EntityKind,
		current.EntityKey,
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, err
	}
	equal, diff, err := semanticDiff(
		current.CacheSnapshot,
		upstream,
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, fmt.Errorf(
			"compare drift entity %s: %w",
			current.EntityKey,
			err,
		)
	}
	if equal {
		if _, err := dbgen.New(s.pool).ResolveOpenDriftFindings(
			ctx,
			dbgen.ResolveOpenDriftFindingsParams{
				ResolvedAt:     timestamptz(s.config.Now()),
				InstallationID: s.config.InstallationID,
				EntityKind:     current.EntityKind,
				EntityKey:      current.EntityKey,
			},
		); err != nil {
			return dbgen.DriftFinding{}, false, fmt.Errorf(
				"resolve drift findings: %w",
				err,
			)
		}
		return dbgen.DriftFinding{}, false, nil
	}
	finding, first, escalated, err := s.recordAndHeal(
		ctx,
		driftSample{
			EntityKind:    current.EntityKind,
			SourceID:      current.SourceID,
			EntityKey:     current.EntityKey,
			LockKey:       current.LockKey,
			CacheSnapshot: current.CacheSnapshot,
		},
		upstream,
		diff,
		spec,
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, err
	}
	if first {
		s.config.Observer.Divergence(ctx, finding)
	}
	if escalated {
		s.config.Observer.PersistentDivergence(ctx, finding)
	}
	return finding, true, nil
}

func (s *Service) recordAndHeal(
	ctx context.Context,
	sample driftSample,
	upstream []byte,
	diff []byte,
	spec queue.RefreshSpec,
) (dbgen.DriftFinding, bool, bool, error) {
	client := s.riverClient()
	if client == nil {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"drift River client is not configured",
		)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"begin drift finding: %w",
			err,
		)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	now := s.config.Now().UTC()
	// Match the upgrade backfill in migration 0011. This is a content
	// identity for deduplication, not a security boundary.
	hash := fmt.Sprintf("%x", md5.Sum(diff)) //nolint:gosec
	queries := dbgen.New(tx)
	finding, err := queries.GetOpenDriftFindingByHash(
		ctx,
		dbgen.GetOpenDriftFindingByHashParams{
			InstallationID: s.config.InstallationID,
			EntityKind:     sample.EntityKind,
			EntityKey:      sample.EntityKey,
			DiffHash:       hash,
		},
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"read open drift finding: %w",
			err,
		)
	}
	if err == nil {
		state, stateErr := queries.GetRefreshIntentState(
			ctx,
			dbgen.GetRefreshIntentStateParams{
				Kind:       spec.Kind,
				RefreshKey: spec.Key,
			},
		)
		if stateErr != nil && !errors.Is(stateErr, pgx.ErrNoRows) {
			return dbgen.DriftFinding{}, false, false, fmt.Errorf(
				"read drift heal generation: %w",
				stateErr,
			)
		}
		escalated := finding.HealGeneration > 0 &&
			state.CompletedGeneration >= finding.HealGeneration &&
			!finding.EscalatedAt.Valid
		if escalated {
			result, updateErr := queries.EscalateOpenDriftFinding(
				ctx,
				dbgen.EscalateOpenDriftFindingParams{
					LastSeenAt:       timestamptz(now),
					CacheSnapshot:    sample.CacheSnapshot,
					UpstreamSnapshot: upstream,
					Diff:             diff,
					EscalatedAt:      timestamptz(now),
					ID:               finding.ID,
				},
			)
			if updateErr != nil {
				return dbgen.DriftFinding{}, false, false, fmt.Errorf(
					"escalate drift finding: %w",
					updateErr,
				)
			}
			finding = result
		} else {
			finding, err = queries.TouchOpenDriftFinding(
				ctx,
				dbgen.TouchOpenDriftFindingParams{
					LastSeenAt:       timestamptz(now),
					CacheSnapshot:    sample.CacheSnapshot,
					UpstreamSnapshot: upstream,
					Diff:             diff,
					ID:               finding.ID,
				},
			)
			if err != nil {
				return dbgen.DriftFinding{}, false, false, fmt.Errorf(
					"touch drift finding: %w",
					err,
				)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return dbgen.DriftFinding{}, false, false, fmt.Errorf(
				"commit persistent drift finding: %w",
				err,
			)
		}
		return finding, false, escalated, nil
	}
	finding, err = queries.InsertDriftFinding(
		ctx,
		dbgen.InsertDriftFindingParams{
			InstallationID:    s.config.InstallationID,
			EntityKind:        sample.EntityKind,
			EntityKey:         sample.EntityKey,
			DetectedAt:        timestamptz(now),
			CacheSnapshot:     sample.CacheSnapshot,
			UpstreamSnapshot:  upstream,
			Diff:              diff,
			DiffHash:          hash,
			RefreshEnqueuedAt: timestamptz(now),
		},
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"insert drift finding: %w",
			err,
		)
	}
	generations, err := queue.InsertRefreshesTxReturning(
		ctx,
		tx,
		client,
		[]queue.RefreshSpec{spec},
		queue.QueueSweep,
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, false, err
	}
	if len(generations) != 1 {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"drift heal inserted %d generations",
			len(generations),
		)
	}
	finding, err = queries.SetDriftFindingHealGeneration(
		ctx,
		dbgen.SetDriftFindingHealGenerationParams{
			HealGeneration: generations[0].Generation,
			ID:             finding.ID,
		},
	)
	if err != nil {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"record drift heal generation: %w",
			err,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.DriftFinding{}, false, false, fmt.Errorf(
			"commit drift finding: %w",
			err,
		)
	}
	return finding, true, false, nil
}

func (s *Service) fullFetch(
	ctx context.Context,
	kind string,
	key string,
) ([]byte, queue.RefreshSpec, error) {
	switch kind {
	case "repository":
		repo, err := repoFromKey(key, "repo:", ":metadata")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		value, _, err := s.rest.GetRepository(
			ctx,
			budget.Sweep,
			owner,
			name,
			"",
		)
		spec := queue.RefreshSpec{
			Kind: queue.KindRefreshRepository,
			Key:  key,
		}
		if isNotFound(err) {
			return tombstoneSnapshot(), spec, nil
		}
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch repository %s: %w",
				repo,
				err,
			)
		}
		return encodeSnapshot(map[string]any{
			"id":             value.ID,
			"node_id":        value.NodeID,
			"owner":          value.Owner,
			"name":           value.Name,
			"full_name":      value.FullName,
			"default_branch": value.DefaultBranch,
			"archived":       value.Archived,
		}), spec, nil
	case "pull_request":
		repo, number, err := numberedKey(key, "pr:")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		pull, _, err := s.rest.GetPull(
			ctx,
			budget.Sweep,
			owner,
			name,
			number,
			"",
		)
		spec := queue.RefreshSpec{Kind: queue.KindRefreshPR, Key: key}
		if isNotFound(err) {
			return tombstoneSnapshot(), spec, nil
		}
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch pull request %s: %w",
				key,
				err,
			)
		}
		var stackNumber any
		var stackPosition any
		if pull.Stack != nil {
			stackNumber = pull.Stack.Number
			stackPosition = pull.Stack.Position
		}
		return encodeSnapshot(map[string]any{
			"id":              pull.GetID(),
			"node_id":         pull.GetNodeID(),
			"number":          pull.GetNumber(),
			"title":           pull.GetTitle(),
			"state":           pull.GetState(),
			"draft":           pull.GetDraft(),
			"author_login":    pull.GetUser().GetLogin(),
			"head_ref":        pull.GetHead().GetRef(),
			"head_sha":        pull.GetHead().GetSHA(),
			"base_ref":        pull.GetBase().GetRef(),
			"base_sha":        pull.GetBase().GetSHA(),
			"review_decision": pull.ReviewDecision,
			"mergeable_state": pull.GetMergeableState(),
			"stack_number":    stackNumber,
			"stack_position":  stackPosition,
		}), spec, nil
	case "stack":
		repo, number, err := numberedKey(key, "stack:")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		stack, _, err := s.rest.GetStack(
			ctx,
			budget.Sweep,
			owner,
			name,
			number,
			"",
		)
		spec := queue.RefreshSpec{Kind: queue.KindRefreshStack, Key: key}
		if isNotFound(err) {
			return tombstoneSnapshot(), spec, nil
		}
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch stack %s: %w",
				key,
				err,
			)
		}
		entries := make([]map[string]any, 0, len(stack.PullRequests))
		for _, pull := range stack.PullRequests {
			entry := map[string]any{
				"number":     pull.Number,
				"state":      pull.State,
				"draft":      pull.Draft,
				"updated_at": pull.UpdatedAt,
				"head_ref":   pull.Head.Ref,
				"head_sha":   pull.Head.SHA,
			}
			if pull.MergedAt != nil {
				entry["merged_at"] = pull.MergedAt
			}
			entries = append(entries, entry)
		}
		return encodeSnapshot(map[string]any{
			"id":       stack.ID,
			"node_id":  stack.NodeID,
			"number":   stack.Number,
			"base_ref": stack.Base.Ref,
			"base_sha": stack.Base.SHA,
			"open":     stack.Open,
			"entries":  entries,
		}), spec, nil
	case "repo_rules":
		repo, err := repoFromKey(key, "repo_rules:", ":rules")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		rules, _, err := s.rest.ListRepositoryRules(
			ctx,
			budget.Sweep,
			owner,
			name,
			"",
		)
		spec := queue.RefreshSpec{
			Kind: queue.KindRefreshRepoRules,
			Key:  key,
		}
		if isNotFound(err) {
			return tombstoneSnapshot(), spec, nil
		}
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch repository rules %s: %w",
				repo,
				err,
			)
		}
		semanticRules := make(map[string]any, len(rules))
		for _, rule := range rules {
			var semantic any
			if err := json.Unmarshal(rule.Raw, &semantic); err != nil {
				return nil, spec, fmt.Errorf(
					"decode upstream repository rule: %w",
					err,
				)
			}
			semanticRules[strconv.FormatInt(rule.ID, 10)] = semantic
		}
		return encodeSnapshot(map[string]any{
			"rules_by_id": semanticRules,
		}), spec, nil
	case "review_threads":
		repo, number, err := numberedKey(key, "review_threads:")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		pull, _, err := s.rest.GetPull(
			ctx,
			budget.Sweep,
			owner,
			name,
			number,
			"",
		)
		spec := queue.RefreshSpec{
			Kind: queue.KindRefreshPR,
			Key:  fmt.Sprintf("pr:%s:%d", repo, number),
		}
		if isNotFound(err) {
			return tombstoneSnapshot(), spec, nil
		}
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch review-thread PR %s: %w",
				key,
				err,
			)
		}
		nodes, _, err := s.graphQL.BatchPullRequests(
			ctx,
			budget.Sweep,
			[]string{pull.GetNodeID()},
		)
		if err != nil {
			return nil, spec, fmt.Errorf(
				"drift fetch review threads %s: %w",
				key,
				err,
			)
		}
		if len(nodes) != 1 || nodes[0] == nil {
			return tombstoneSnapshot(), spec, nil
		}
		return encodeSnapshot(map[string]any{
			"threads": semanticReviewThreads(nodes[0]),
		}), spec, nil
	case "checks":
		repo, headSHA, err := stringKey(key, "checks:")
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		owner, name, err := splitRepo(repo)
		if err != nil {
			return nil, queue.RefreshSpec{}, err
		}
		var runs []gh.CheckRun
		for page := 1; ; page++ {
			batch, response, fetchErr := s.rest.ListCheckRuns(
				ctx,
				budget.Sweep,
				owner,
				name,
				headSHA,
				gh.ListCheckRunsOptions{
					PerPage: s.config.PageSize,
					Page:    page,
				},
				"",
			)
			if fetchErr != nil {
				spec := queue.RefreshSpec{
					Kind: queue.KindRefreshChecks,
					Key:  key,
				}
				if isNotFound(fetchErr) {
					return tombstoneSnapshot(), spec, nil
				}
				return nil, spec, fmt.Errorf(
					"drift fetch checks %s: %w",
					key,
					fetchErr,
				)
			}
			runs = append(runs, batch...)
			if response.NextPage == 0 {
				break
			}
			page = response.NextPage - 1
		}
		sort.Slice(runs, func(i, j int) bool {
			return runs[i].ID < runs[j].ID
		})
		semanticRuns := make([]map[string]any, 0, len(runs))
		for _, run := range runs {
			semanticRuns = append(semanticRuns, map[string]any{
				"id":          run.ID,
				"node_id":     run.NodeID,
				"name":        run.Name,
				"status":      run.Status,
				"conclusion":  run.Conclusion,
				"details_url": run.DetailsURL,
				"app_slug":    run.AppSlug,
			})
		}
		return encodeSnapshot(map[string]any{
				"runs": semanticRuns,
			}), queue.RefreshSpec{
				Kind: queue.KindRefreshChecks,
				Key:  key,
			}, nil
	default:
		return nil, queue.RefreshSpec{}, fmt.Errorf(
			"unsupported drift entity kind %q",
			kind,
		)
	}
}

func semanticReviewThreads(node *gh.PullRequestNode) []map[string]any {
	threads := append(
		[]gh.ReviewThreadNode(nil),
		node.ReviewThreads.Nodes...,
	)
	sort.Slice(threads, func(i, j int) bool {
		return threads[i].ID < threads[j].ID
	})
	result := make([]map[string]any, 0, len(threads))
	for _, thread := range threads {
		comments := append(
			[]gh.ReviewCommentNode(nil),
			thread.Comments.Nodes...,
		)
		sort.Slice(comments, func(i, j int) bool {
			return comments[i].ID < comments[j].ID
		})
		semanticComments := make([]map[string]any, 0, len(comments))
		for _, comment := range comments {
			author := ""
			if comment.Author != nil {
				author = comment.Author.Login
			}
			semanticComments = append(semanticComments, map[string]any{
				"id":           comment.ID,
				"body":         comment.Body,
				"updated_at":   comment.UpdatedAt,
				"author_login": author,
			})
		}
		result = append(result, map[string]any{
			"id":          thread.ID,
			"is_resolved": thread.IsResolved,
			"is_outdated": thread.IsOutdated,
			"path":        thread.Path,
			"line":        thread.Line,
			"comments":    semanticComments,
		})
	}
	return result
}

func semanticDiff(cache, upstream []byte) (bool, []byte, error) {
	var cachedValue, upstreamValue any
	if err := json.Unmarshal(cache, &cachedValue); err != nil {
		return false, nil, fmt.Errorf("decode cache snapshot: %w", err)
	}
	if err := json.Unmarshal(upstream, &upstreamValue); err != nil {
		return false, nil, fmt.Errorf("decode upstream snapshot: %w", err)
	}
	cachedValue = normalizeSemantic(cachedValue)
	upstreamValue = normalizeSemantic(upstreamValue)
	if reflect.DeepEqual(cachedValue, upstreamValue) {
		return true, []byte(`{}`), nil
	}
	diff := diffValue(cachedValue, upstreamValue)
	encoded, err := json.Marshal(diff)
	if err != nil {
		return false, nil, fmt.Errorf("encode semantic diff: %w", err)
	}
	return false, encoded, nil
}

func normalizeSemantic(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		normalized := make(map[string]any, len(typed))
		for key, child := range typed {
			normalized[key] = normalizeSemantic(child)
		}
		return normalized
	case []any:
		normalized := make([]any, len(typed))
		allHaveID := len(typed) > 0
		for index, child := range typed {
			normalized[index] = normalizeSemantic(child)
			item, ok := normalized[index].(map[string]any)
			if !ok {
				allHaveID = false
				continue
			}
			if _, ok := item["id"].(string); !ok {
				if _, numeric := item["id"].(float64); !numeric {
					allHaveID = false
				}
			}
		}
		if allHaveID {
			sort.SliceStable(normalized, func(i, j int) bool {
				left := fmt.Sprint(normalized[i].(map[string]any)["id"])
				right := fmt.Sprint(normalized[j].(map[string]any)["id"])
				return left < right
			})
		}
		return normalized
	default:
		return value
	}
}

func diffValue(cached, upstream any) any {
	cachedMap, cachedOK := cached.(map[string]any)
	upstreamMap, upstreamOK := upstream.(map[string]any)
	if !cachedOK || !upstreamOK {
		return map[string]any{"cache": cached, "upstream": upstream}
	}
	keys := make(map[string]struct{}, len(cachedMap)+len(upstreamMap))
	for key := range cachedMap {
		keys[key] = struct{}{}
	}
	for key := range upstreamMap {
		keys[key] = struct{}{}
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	sort.Strings(names)
	diff := make(map[string]any)
	for _, key := range names {
		if reflect.DeepEqual(cachedMap[key], upstreamMap[key]) {
			continue
		}
		diff[key] = diffValue(cachedMap[key], upstreamMap[key])
	}
	return diff
}

func encodeSnapshot(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func tombstoneSnapshot() []byte {
	return []byte(`{"tombstoned":true}`)
}

func repoFromKey(key, prefix, suffix string) (string, error) {
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", fmt.Errorf("invalid entity key %q", key)
	}
	repo := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	if repo == "" {
		return "", fmt.Errorf("invalid entity key %q", key)
	}
	return repo, nil
}

func numberedKey(key, prefix string) (string, int, error) {
	repo, raw, err := stringKey(key, prefix)
	if err != nil {
		return "", 0, err
	}
	number, err := strconv.Atoi(raw)
	if err != nil || number <= 0 {
		return "", 0, fmt.Errorf("invalid numbered entity key %q", key)
	}
	return repo, number, nil
}

func stringKey(key, prefix string) (string, string, error) {
	if !strings.HasPrefix(key, prefix) {
		return "", "", fmt.Errorf("invalid entity key %q", key)
	}
	rest := strings.TrimPrefix(key, prefix)
	index := strings.LastIndex(rest, ":")
	if index <= 0 || index == len(rest)-1 {
		return "", "", fmt.Errorf("invalid entity key %q", key)
	}
	return rest[:index], rest[index+1:], nil
}

func splitRepo(fullName string) (string, string, error) {
	owner, repo, ok := strings.Cut(fullName, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", fmt.Errorf("invalid repository name %q", fullName)
	}
	return owner, repo, nil
}

func isNotFound(err error) bool {
	var httpErr *gh.HTTPError
	return errors.As(err, &httpErr) &&
		httpErr.StatusCode == http.StatusNotFound
}

func timestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}
