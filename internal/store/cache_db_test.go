package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

func TestWriteRaceBothOrdersNewerWinsConcurrently(t *testing.T) {
	pool, newPool := storeTestDatabase(t)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	for index, order := range [][]string{
		{"old", "new"},
		{"new", "old"},
	} {
		t.Run(strings.Join(order, "-then-"), func(t *testing.T) {
			repository := storeTestRepository(
				fmt.Sprintf("acme/write-race-%d", index),
				int64(2000+index),
				baseTime,
			)
			if _, err := NewEntityWriter(pool).ApplyRepository(
				context.Background(),
				repository,
				SyncSourceWebhook,
				"",
				baseTime,
			); err != nil {
				t.Fatalf("seed repository: %v", err)
			}

			pulls := map[string]PullRequestRecord{
				"old": storeTestPull(&repository, baseTime, "old-head"),
				"new": storeTestPull(
					&repository,
					baseTime.Add(time.Minute),
					"new-head",
				),
			}
			key := PullRequestEntityKey(
				repository.InstallationID,
				repository.GitHubID,
				pulls["old"].Number,
			)
			blocker, err := pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := dbgen.New(blocker).AcquireEntityAdvisoryLock(
				context.Background(),
				key,
			); err != nil {
				_ = blocker.Rollback(context.Background())
				t.Fatal(err)
			}

			type writeResult struct {
				name string
				err  error
			}
			results := make(chan writeResult, 2)
			startWrite := func(name string, ordinal int) {
				t.Helper()
				applicationName := fmt.Sprintf(
					"store-write-race-%d-%d-%s",
					index,
					ordinal,
					name,
				)
				writerPool := newPool(applicationName)
				go func() {
					_, err := NewEntityWriter(writerPool).ApplyPullRequest(
						context.Background(),
						pulls[name],
					)
					results <- writeResult{name: name, err: err}
				}()
				waitForAdvisoryWaiters(t, pool, "store-write-race-"+fmt.Sprint(index), ordinal)
			}

			startWrite(order[0], 1)
			startWrite(order[1], 2)
			if err := blocker.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result := <-results
				if result.err != nil {
					t.Fatalf("%s write: %v", result.name, result.err)
				}
			}

			row, err := dbgen.New(pool).GetPullRequestByKey(
				context.Background(),
				dbgen.GetPullRequestByKeyParams{
					RepoFullName: repository.FullName,
					PrNumber:     42,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if row.HeadSha != "new-head" ||
				!row.GhUpdatedAt.Valid ||
				!row.GhUpdatedAt.Time.Equal(pulls["new"].GitHubUpdatedAt) {
				t.Fatalf(
					"%s final row = %+v, newer write did not win",
					strings.Join(order, "-then-"),
					row,
				)
			}
		})
	}
}

func TestEqualTimestampDomainChangeAndTombstoneResurrection(t *testing.T) {
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	updatedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/equal-version", 2100, updatedAt)
	first := storeTestPull(&repository, updatedAt, "head-one")
	second := storeTestPull(&repository, updatedAt, "head-two")
	second.Title = "equal timestamp changed truth"
	second.SyncedAt = first.SyncedAt.Add(time.Second)

	if _, err := writer.ApplyPullRequest(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	result, err := writer.ApplyPullRequest(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DomainChanged || result.NewHeadSHA != "head-two" {
		t.Fatalf("equal timestamp change result = %+v", result)
	}

	key := PullRequestEntityKey(1, 2100, 42)
	observation, err := writer.BeginObservation(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	tombstonedAt := second.SyncedAt.Add(time.Second)
	if _, err := writer.TombstonePullRequestObserved(
		context.Background(),
		observation,
		repository,
		42,
		SyncSourceWebhook,
		tombstonedAt,
		nil,
	); err != nil {
		_ = observation.Close()
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}

	resurrected := second
	resurrected.SyncedAt = tombstonedAt.Add(time.Second)
	observation, err = writer.BeginObservation(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ApplyPullRequestObserved(
		context.Background(),
		observation,
		resurrected,
		nil,
	); err != nil {
		_ = observation.Close()
		t.Fatal(err)
	}
	if err := observation.Close(); err != nil {
		t.Fatal(err)
	}

	row, err := dbgen.New(pool).GetPullRequestByKey(
		context.Background(),
		dbgen.GetPullRequestByKeyParams{
			RepoFullName: repository.FullName,
			PrNumber:     42,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.TombstonedAt.Valid || row.HeadSha != "head-two" {
		t.Fatalf("resurrected row = %+v", row)
	}
}

func TestPullRequestResultSeparatesStackStateFromTitleChanges(t *testing.T) {
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	baseTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/stack-diff", 2150, baseTime)
	stackNumber := 142
	stackPosition := 2
	if _, err := writer.ApplyStack(
		context.Background(),
		StackRecord{
			Repository: repository,
			GitHubID:   7000,
			NodeID:     "stack-node-142",
			Number:     stackNumber,
			BaseRef:    "main",
			BaseSHA:    "base-one",
			Open:       true,
			Entries: []StackEntry{
				{
					Number:    41,
					State:     "open",
					UpdatedAt: baseTime,
					HeadRef:   "stack/one",
					HeadSHA:   "head-zero",
				},
				{
					Number:    42,
					State:     "open",
					UpdatedAt: baseTime,
					HeadRef:   "feature",
					HeadSHA:   "head-one",
				},
			},
			GitHubUpdatedAt: baseTime,
			SyncedAt:        baseTime,
			Source:          SyncSourceWebhook,
		},
	); err != nil {
		t.Fatal(err)
	}
	first := storeTestPull(&repository, baseTime, "head-one")
	first.StackNumber = &stackNumber
	first.StackPosition = &stackPosition
	first.StackSummary = &StackSummaryRecord{
		GitHubID: 7000,
		Number:   stackNumber,
		Size:     2,
		Position: stackPosition,
		BaseRef:  "main",
		BaseSHA:  "base-one",
	}
	first.MembershipKnown = true
	initial, err := writer.ApplyPullRequest(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !initial.StackStateChanged {
		t.Fatalf("initial stacked PR result = %+v", initial)
	}

	titleOnly := first
	titleOnly.Title = "new title"
	titleOnly.GitHubUpdatedAt = baseTime.Add(time.Minute)
	titleOnly.SyncedAt = first.SyncedAt.Add(time.Minute)
	titleResult, err := writer.ApplyPullRequest(context.Background(), titleOnly)
	if err != nil {
		t.Fatal(err)
	}
	if !titleResult.DomainChanged || titleResult.StackStateChanged {
		t.Fatalf("title-only PR result = %+v", titleResult)
	}

	newHead := titleOnly
	newHead.HeadSHA = "head-two"
	newHead.GitHubUpdatedAt = baseTime.Add(2 * time.Minute)
	newHead.SyncedAt = first.SyncedAt.Add(2 * time.Minute)
	headResult, err := writer.ApplyPullRequest(context.Background(), newHead)
	if err != nil {
		t.Fatal(err)
	}
	if !headResult.DomainChanged || !headResult.StackStateChanged {
		t.Fatalf("head-changing PR result = %+v", headResult)
	}

	summaryMismatch := newHead
	summaryMismatch.ReviewDecision = "APPROVED"
	summaryMismatch.GitHubUpdatedAt = baseTime.Add(3 * time.Minute)
	summaryMismatch.SyncedAt = first.SyncedAt.Add(3 * time.Minute)
	mismatchingSummary := *summaryMismatch.StackSummary
	mismatchingSummary.Size = 3
	summaryMismatch.StackSummary = &mismatchingSummary
	summaryResult, err := writer.ApplyPullRequest(
		context.Background(),
		summaryMismatch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !summaryResult.DomainChanged || !summaryResult.StackStateChanged {
		t.Fatalf("summary-mismatching PR result = %+v", summaryResult)
	}
}

func TestEntityKeyConstructorsMatchSQLGrammar(t *testing.T) {
	pool, _ := storeTestDatabase(t)
	writer := NewEntityWriter(pool)
	ctx := context.Background()
	at := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	repository := storeTestRepository("acme/key-grammar", 2200, at)
	pull := storeTestPull(&repository, at, "key-head")
	pull.Number = 42
	pull.GitHubID = 4200
	pull.NodeID = "pr-node-42"
	if _, err := writer.ApplyPullRequest(ctx, pull); err != nil {
		t.Fatal(err)
	}
	stack := StackRecord{
		Repository:      repository,
		GitHubID:        7000,
		NodeID:          "stack-node-7",
		Number:          7,
		BaseRef:         "main",
		BaseSHA:         "base",
		Open:            true,
		GitHubUpdatedAt: at,
		SyncedAt:        at.Add(time.Second),
		Source:          SyncSourceWebhook,
	}
	if _, err := writer.ApplyStack(ctx, stack); err != nil {
		t.Fatal(err)
	}

	checksKey := ChecksEntityKey(1, repository.GitHubID, pull.HeadSHA)
	checksObservation, err := writer.BeginObservation(ctx, checksKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ApplyChecksObserved(
		ctx,
		checksObservation,
		ChecksRecord{
			Repository: repository,
			HeadSHA:    pull.HeadSHA,
			Runs: []CheckRunRecord{{
				GitHubID:   9000,
				NodeID:     "check-node-9000",
				Name:       "unit",
				Status:     "completed",
				Conclusion: "success",
			}},
			SyncedAt: at.Add(2 * time.Second),
			Source:   SyncSourceWebhook,
		},
	); err != nil {
		_ = checksObservation.Close()
		t.Fatal(err)
	}
	if err := checksObservation.Close(); err != nil {
		t.Fatal(err)
	}

	rulesKey := RepoRulesEntityKey(1, repository.GitHubID)
	rulesObservation, err := writer.BeginObservation(ctx, rulesKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ApplyRepoRulesObserved(
		ctx,
		rulesObservation,
		RepoRulesRecord{
			Repository: repository,
			Rules: []RepoRuleRecord{{
				Key:  "main",
				Rule: []byte(`{"required":true}`),
			}},
			SyncedAt: at.Add(3 * time.Second),
			Source:   SyncSourceWebhook,
		},
	); err != nil {
		_ = rulesObservation.Close()
		t.Fatal(err)
	}
	if err := rulesObservation.Close(); err != nil {
		t.Fatal(err)
	}

	repoRow, err := dbgen.New(pool).GetRepoByGitHubID(ctx, repository.GitHubID)
	if err != nil {
		t.Fatal(err)
	}
	sqlScopes, err := dbgen.New(pool).ListRepositoryDerivationScopes(
		ctx,
		repoRow.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantScopes := []string{
		PullRequestEntityKey(1, repository.GitHubID, pull.Number),
		StackEntityKey(1, repository.GitHubID, stack.Number),
	}
	if !reflect.DeepEqual(sqlScopes, wantScopes) {
		t.Fatalf(
			"SQL derivation scopes = %v, Go constructors = %v",
			sqlScopes,
			wantScopes,
		)
	}

	rows, err := pool.Query(ctx, `
		SELECT entity_kind, lock_key
		FROM drift_entities
		WHERE installation_id = $1
		  AND entity_kind = ANY($2::text[])
		ORDER BY entity_kind
	`, repository.InstallationID, []string{
		"repository", "pull_request", "stack", "checks", "repo_rules",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sqlLockKeys := make(map[string]string)
	for rows.Next() {
		var kind, key string
		if err := rows.Scan(&kind, &key); err != nil {
			t.Fatal(err)
		}
		sqlLockKeys[kind] = key
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantLockKeys := map[string]string{
		"repository": RepositoryEntityKey(1, repository.GitHubID),
		"pull_request": PullRequestEntityKey(
			1,
			repository.GitHubID,
			pull.Number,
		),
		"stack":      StackEntityKey(1, repository.GitHubID, stack.Number),
		"checks":     checksKey,
		"repo_rules": rulesKey,
	}
	if !reflect.DeepEqual(sqlLockKeys, wantLockKeys) {
		t.Fatalf(
			"drift lock keys = %v, Go constructors = %v",
			sqlLockKeys,
			wantLockKeys,
		)
	}

	var discoveryKey string
	if err := pool.QueryRow(
		ctx,
		`SELECT ('repo-discovery:' || $1::bigint || ':' || $2::text)::text`,
		repository.InstallationID,
		repository.FullName,
	).Scan(&discoveryKey); err != nil {
		t.Fatal(err)
	}
	if want := RepositoryDiscoveryKey(
		repository.InstallationID,
		repository.FullName,
	); discoveryKey != want {
		t.Fatalf("SQL discovery key = %q, Go constructor = %q", discoveryKey, want)
	}
}

func waitForAdvisoryWaiters(
	t *testing.T,
	pool *pgxpool.Pool,
	applicationPrefix string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int
		err := pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE application_name LIKE $1
			  AND wait_event = 'advisory'
		`, applicationPrefix+"%").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"advisory waiters with prefix %q = %d, want at least %d",
				applicationPrefix,
				count,
				want,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func storeTestRepository(
	fullName string,
	id int64,
	updatedAt time.Time,
) RepositoryRecord {
	owner, name, _ := strings.Cut(fullName, "/")
	return RepositoryRecord{
		InstallationID:  1,
		OrgID:           1,
		GitHubID:        id,
		NodeID:          fmt.Sprintf("repo-node-%d", id),
		Owner:           owner,
		Name:            name,
		FullName:        fullName,
		DefaultBranch:   "main",
		DefaultHeadSHA:  "base",
		GitHubUpdatedAt: updatedAt,
	}
}

func storeTestPull(
	repository *RepositoryRecord,
	updatedAt time.Time,
	headSHA string,
) PullRequestRecord {
	return PullRequestRecord{
		Repository:      *repository,
		GitHubID:        4200,
		NodeID:          "pr-node-42",
		Number:          42,
		Title:           "race",
		State:           "open",
		HeadRef:         "feature",
		HeadSHA:         headSHA,
		BaseRef:         "main",
		BaseSHA:         "base",
		MembershipKnown: true,
		GitHubUpdatedAt: updatedAt,
		SyncedAt:        updatedAt.Add(time.Second),
		Source:          SyncSourceWebhook,
	}
}

func storeTestDatabase(
	t *testing.T,
) (*pgxpool.Pool, func(applicationName string) *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	schema := fmt.Sprintf("ghsync_store_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	newPool := func(applicationName string) *pgxpool.Pool {
		t.Helper()
		config, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.RuntimeParams["search_path"] = schema
		config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
		config.ConnConfig.RuntimeParams["application_name"] = applicationName
		pool, err := pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.Ping(context.Background()); err != nil {
			pool.Close()
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	pool := newPool("store-tests")
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		admin.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(
			dropCtx,
			"DROP SCHEMA "+identifier+" CASCADE",
		); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		admin.Close()
	})
	return pool, newPool
}
