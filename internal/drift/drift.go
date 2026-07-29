// Package drift implements C-O3's sampled, full-fetch semantic validation.
package drift

import (
	"context"
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
	"github.com/acme/frontier/internal/queue"
	"github.com/acme/frontier/internal/store/dbgen"
)

const jobKindDetect = "drift_detect"

type Config struct {
	InstallationID int64
	Period         time.Duration
	SampleSize     int
	PageSize       int
	Now            func() time.Time
	Observer       Observer
}

type Observer interface {
	Divergence(context.Context, dbgen.DriftFinding)
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

type noopObserver struct{}

func (noopObserver) Divergence(context.Context, dbgen.DriftFinding) {}

type Options struct {
	Pool   *pgxpool.Pool
	REST   *gh.RESTClient
	Config Config
}

type Service struct {
	pool   *pgxpool.Pool
	rest   *gh.RESTClient
	config Config

	riverMu sync.RWMutex
	river   *river.Client[pgx.Tx]
}

func New(options Options) (*Service, error) {
	if options.Pool == nil || options.REST == nil {
		return nil, fmt.Errorf("drift detector requires Postgres and REST")
	}
	if options.Config.InstallationID <= 0 ||
		options.Config.Period <= 0 ||
		options.Config.SampleSize <= 0 ||
		options.Config.PageSize <= 0 ||
		options.Config.PageSize > 100 {
		return nil, fmt.Errorf("invalid drift detector configuration")
	}
	if options.Config.Now == nil {
		options.Config.Now = time.Now
	}
	if options.Config.Observer == nil {
		options.Config.Observer = noopObserver{}
	}
	return &Service{
		pool:   options.Pool,
		rest:   options.REST,
		config: options.Config,
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
						Queue:    queue.QueueSweep,
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

func (s *Service) Detect(
	ctx context.Context,
	args DetectArgs,
) ([]dbgen.DriftFinding, error) {
	if args.InstallationID != s.config.InstallationID {
		return nil, nil
	}
	if args.SampleSize <= 0 {
		return nil, fmt.Errorf("drift sample size must be positive")
	}
	rows, err := dbgen.New(s.pool).SampleCachedEntities(
		ctx,
		dbgen.SampleCachedEntitiesParams{
			InstallationID: args.InstallationID,
			SampleSize:     int32(args.SampleSize),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("sample cached entities: %w", err)
	}
	findings := make([]dbgen.DriftFinding, 0)
	for _, row := range rows {
		upstream, spec, err := s.fullFetch(
			ctx,
			row.EntityKind,
			row.EntityKey,
		)
		if err != nil {
			return nil, err
		}
		equal, diff, err := semanticDiff(row.CacheSnapshot, upstream)
		if err != nil {
			return nil, fmt.Errorf(
				"compare drift entity %s: %w",
				row.EntityKey,
				err,
			)
		}
		if equal {
			continue
		}
		finding, err := s.recordAndHeal(
			ctx,
			row,
			upstream,
			diff,
			spec,
		)
		if err != nil {
			return nil, err
		}
		findings = append(findings, finding)
		s.config.Observer.Divergence(ctx, finding)
	}
	return findings, nil
}

func (s *Service) recordAndHeal(
	ctx context.Context,
	sample dbgen.SampleCachedEntitiesRow,
	upstream []byte,
	diff []byte,
	spec queue.RefreshSpec,
) (dbgen.DriftFinding, error) {
	client := s.riverClient()
	if client == nil {
		return dbgen.DriftFinding{}, fmt.Errorf(
			"drift River client is not configured",
		)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return dbgen.DriftFinding{}, fmt.Errorf(
			"begin drift finding: %w",
			err,
		)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	now := s.config.Now().UTC()
	finding, err := dbgen.New(tx).InsertDriftFinding(
		ctx,
		dbgen.InsertDriftFindingParams{
			InstallationID:    s.config.InstallationID,
			EntityKind:        sample.EntityKind,
			EntityKey:         sample.EntityKey,
			DetectedAt:        timestamptz(now),
			CacheSnapshot:     sample.CacheSnapshot,
			UpstreamSnapshot:  upstream,
			Diff:              diff,
			RefreshEnqueuedAt: timestamptz(now),
		},
	)
	if err != nil {
		return dbgen.DriftFinding{}, fmt.Errorf(
			"insert drift finding: %w",
			err,
		)
	}
	if err := queue.InsertRefreshesTx(
		ctx,
		tx,
		client,
		[]queue.RefreshSpec{spec},
		queue.QueueSweep,
	); err != nil {
		return dbgen.DriftFinding{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return dbgen.DriftFinding{}, fmt.Errorf(
			"commit drift finding: %w",
			err,
		)
	}
	return finding, nil
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
		sort.Slice(rules, func(i, j int) bool {
			return rules[i].ID < rules[j].ID
		})
		semanticRules := make([]any, 0, len(rules))
		for _, rule := range rules {
			var semantic any
			if err := json.Unmarshal(rule.Raw, &semantic); err != nil {
				return nil, spec, fmt.Errorf(
					"decode upstream repository rule: %w",
					err,
				)
			}
			semanticRules = append(semanticRules, semantic)
		}
		return encodeSnapshot(map[string]any{
			"rules": semanticRules,
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

func semanticDiff(cache, upstream []byte) (bool, []byte, error) {
	var cachedValue, upstreamValue any
	if err := json.Unmarshal(cache, &cachedValue); err != nil {
		return false, nil, fmt.Errorf("decode cache snapshot: %w", err)
	}
	if err := json.Unmarshal(upstream, &upstreamValue); err != nil {
		return false, nil, fmt.Errorf("decode upstream snapshot: %w", err)
	}
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
