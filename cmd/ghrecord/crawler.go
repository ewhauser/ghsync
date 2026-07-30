//nolint:gocritic // Recording snapshots are immutable values; copying them isolates resumable crawl state.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ewhauser/ghsync/internal/replay"
)

type crawlConfig struct {
	Owner            string
	Name             string
	Since            time.Time
	Until            time.Time
	OutputPath       string
	CursorPath       string
	SynthesizeStacks float64
	Seed             int64
	Client           *graphQLClient
}

type crawlResult struct {
	Events int
}

type crawlCursor struct {
	Version          int               `json:"version"`
	Owner            string            `json:"owner"`
	Name             string            `json:"name"`
	Since            time.Time         `json:"since"`
	Until            time.Time         `json:"until"`
	SynthesizeStacks float64           `json:"synthesize_stacks_percent"`
	Seed             int64             `json:"seed"`
	Phase            string            `json:"phase"`
	Repository       replay.Repository `json:"repository"`
	PullNumbers      []int             `json:"pull_numbers"`
	SearchAfter      *string           `json:"search_after,omitempty"`
	PullIndex        int               `json:"pull_index"`
	Pulls            []recordedPull    `json:"pulls"`
	CurrentPull      *recordedPull     `json:"current_pull,omitempty"`
	TimelineAfter    *string           `json:"timeline_after,omitempty"`
	TimelineComplete bool              `json:"timeline_complete,omitempty"`
	ThreadsAfter     *string           `json:"threads_after,omitempty"`
	ThreadsComplete  bool              `json:"threads_complete,omitempty"`
	HistoryAfter     *string           `json:"history_after,omitempty"`
	HistoryComplete  bool              `json:"history_complete,omitempty"`
	HistoryCheck     int               `json:"history_check_index,omitempty"`
	DefaultCommits   []recordedCommit  `json:"default_commits"`
}

type pageInfo struct {
	HasNextPage bool    `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

type recordedPull struct {
	ID             string         `json:"id"`
	DatabaseID     int64          `json:"databaseId"`
	Number         int            `json:"number"`
	Title          string         `json:"title"`
	State          string         `json:"state"`
	IsDraft        bool           `json:"isDraft"`
	Merged         bool           `json:"merged"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
	ClosedAt       *time.Time     `json:"closedAt"`
	MergedAt       *time.Time     `json:"mergedAt"`
	ReviewDecision string         `json:"reviewDecision"`
	Mergeable      string         `json:"mergeable"`
	HeadRefName    string         `json:"headRefName"`
	HeadRefOID     string         `json:"headRefOid"`
	HeadRepository *nameWithOwner `json:"headRepository"`
	BaseRefName    string         `json:"baseRefName"`
	BaseRefOID     string         `json:"baseRefOid"`
	BaseRepository *nameWithOwner `json:"baseRepository"`
	Author         graphQLActor   `json:"author"`
	Timeline       []timelineNode `json:"timeline"`
	Threads        []threadNode   `json:"threads"`
}

type graphQLActor struct {
	Login string `json:"login"`
}

type oidNode struct {
	OID     string     `json:"oid"`
	Parents parentPage `json:"parents"`
}

type parentPage struct {
	Nodes []oidNode `json:"nodes"`
}

type timelineNode struct {
	Typename        string          `json:"__typename"`
	ID              string          `json:"id"`
	DatabaseID      int64           `json:"databaseId"`
	Body            string          `json:"body"`
	State           string          `json:"state"`
	SubmittedAt     *time.Time      `json:"submittedAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	CreatedAt       time.Time       `json:"createdAt"`
	PreviousRefName string          `json:"previousRefName"`
	CurrentRefName  string          `json:"currentRefName"`
	Author          graphQLActor    `json:"author"`
	BeforeCommit    *oidNode        `json:"beforeCommit"`
	AfterCommit     *oidNode        `json:"afterCommit"`
	Commit          *recordedCommit `json:"commit"`
	Review          *recordedReview `json:"review"`
	PreviousReview  string          `json:"previousReviewState"`
	IsResolved      bool            `json:"isResolved"`
	IsOutdated      bool            `json:"isOutdated"`
	Path            string          `json:"path"`
	Line            *int            `json:"line"`
	Comments        commentPage     `json:"comments"`
}

type recordedReview struct {
	ID          string       `json:"id"`
	DatabaseID  int64        `json:"databaseId"`
	Body        string       `json:"body"`
	State       string       `json:"state"`
	SubmittedAt *time.Time   `json:"submittedAt"`
	UpdatedAt   time.Time    `json:"updatedAt"`
	Author      graphQLActor `json:"author"`
	Commit      *oidNode     `json:"commit"`
}

type recordedCommit struct {
	OID           string         `json:"oid"`
	CommittedDate time.Time      `json:"committedDate"`
	Parents       parentPage     `json:"parents"`
	CheckSuites   checkSuitePage `json:"checkSuites"`
}

type checkSuitePage struct {
	Nodes    []checkSuiteNode `json:"nodes"`
	PageInfo pageInfo         `json:"pageInfo"`
}

type checkSuiteNode struct {
	ID         string       `json:"id"`
	DatabaseID int64        `json:"databaseId"`
	Status     string       `json:"status"`
	Conclusion string       `json:"conclusion"`
	CreatedAt  time.Time    `json:"createdAt"`
	UpdatedAt  time.Time    `json:"updatedAt"`
	App        graphQLApp   `json:"app"`
	CheckRuns  checkRunPage `json:"checkRuns"`
}

type graphQLApp struct {
	Slug string `json:"slug"`
}

type checkRunPage struct {
	Nodes    []checkRunNode `json:"nodes"`
	PageInfo pageInfo       `json:"pageInfo"`
}

type checkRunNode struct {
	ID          string     `json:"id"`
	DatabaseID  int64      `json:"databaseId"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion"`
	DetailsURL  string     `json:"detailsUrl"`
	StartedAt   *time.Time `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt"`
}

type threadNode struct {
	ID         string      `json:"id"`
	IsResolved bool        `json:"isResolved"`
	IsOutdated bool        `json:"isOutdated"`
	Path       string      `json:"path"`
	Line       *int        `json:"line"`
	Comments   commentPage `json:"comments"`
}

type commentPage struct {
	Nodes    []commentNode `json:"nodes"`
	PageInfo pageInfo      `json:"pageInfo"`
}

type commentNode struct {
	ID                string       `json:"id"`
	DatabaseID        int64        `json:"databaseId"`
	PullRequestReview *databaseID  `json:"pullRequestReview"`
	Body              string       `json:"body"`
	Path              string       `json:"path"`
	Line              *int         `json:"line"`
	CreatedAt         time.Time    `json:"createdAt"`
	UpdatedAt         time.Time    `json:"updatedAt"`
	Author            graphQLActor `json:"author"`
}

type databaseID struct {
	DatabaseID int64 `json:"databaseId"`
}

type nameWithOwner struct {
	NameWithOwner string `json:"nameWithOwner"`
}

func crawl(ctx context.Context, config crawlConfig) (crawlResult, error) {
	if config.Client == nil {
		return crawlResult{}, fmt.Errorf("GraphQL client is required")
	}
	cursor, err := loadOrCreateCursor(config)
	if err != nil {
		return crawlResult{}, err
	}
	if cursor.Phase == "pulls" {
		if err := crawlPullNumbers(ctx, config, &cursor); err != nil {
			return crawlResult{}, err
		}
	}
	if cursor.Phase == "details" {
		if err := crawlPullDetails(ctx, config, &cursor); err != nil {
			return crawlResult{}, err
		}
	}
	if cursor.Phase == "history" {
		if err := crawlDefaultHistory(ctx, config, &cursor); err != nil {
			return crawlResult{}, err
		}
	}
	recording := buildRecording(cursor)
	if err := writeRecordingFile(config.OutputPath, recording); err != nil {
		return crawlResult{}, err
	}
	if err := os.Remove(config.CursorPath); err != nil && !os.IsNotExist(err) {
		return crawlResult{}, fmt.Errorf("remove completed crawl cursor: %w", err)
	}
	return crawlResult{Events: len(recording.Events)}, nil
}

func loadOrCreateCursor(config crawlConfig) (crawlCursor, error) {
	body, err := os.ReadFile(config.CursorPath)
	if err == nil {
		var cursor crawlCursor
		if err := json.Unmarshal(body, &cursor); err != nil {
			return crawlCursor{}, fmt.Errorf("decode crawl cursor: %w", err)
		}
		if err := cursorMatches(config, cursor); err != nil {
			return crawlCursor{}, err
		}
		return cursor, nil
	}
	if !os.IsNotExist(err) {
		return crawlCursor{}, fmt.Errorf("read crawl cursor: %w", err)
	}
	return crawlCursor{
		Version:          2,
		Owner:            config.Owner,
		Name:             config.Name,
		Since:            config.Since,
		Until:            config.Until,
		SynthesizeStacks: config.SynthesizeStacks,
		Seed:             config.Seed,
		Phase:            "pulls",
	}, nil
}

func cursorMatches(config crawlConfig, cursor crawlCursor) error {
	if cursor.Version != 2 ||
		cursor.Owner != config.Owner ||
		cursor.Name != config.Name ||
		!cursor.Since.Equal(config.Since) ||
		!cursor.Until.Equal(config.Until) ||
		cursor.SynthesizeStacks != config.SynthesizeStacks ||
		cursor.Seed != config.Seed {
		return fmt.Errorf(
			"crawl cursor does not match repository, window, or synthesis flags",
		)
	}
	switch cursor.Phase {
	case "pulls", "details", "history", "complete":
	default:
		return fmt.Errorf("crawl cursor has invalid phase %q", cursor.Phase)
	}
	if cursor.PullIndex < 0 ||
		cursor.PullIndex > len(cursor.PullNumbers) ||
		cursor.HistoryCheck < 0 ||
		cursor.HistoryCheck > len(cursor.DefaultCommits) {
		return fmt.Errorf("crawl cursor has invalid progress indexes")
	}
	if cursor.CurrentPull != nil {
		if cursor.Phase != "details" ||
			cursor.PullIndex >= len(cursor.PullNumbers) ||
			(cursor.CurrentPull.Number != 0 &&
				cursor.CurrentPull.Number !=
					cursor.PullNumbers[cursor.PullIndex]) {
			return fmt.Errorf("crawl cursor current pull does not match progress")
		}
	}
	return nil
}

func crawlPullNumbers(
	ctx context.Context,
	config crawlConfig,
	cursor *crawlCursor,
) error {
	for {
		var data struct {
			Repository struct {
				ID               string       `json:"id"`
				DatabaseID       int64        `json:"databaseId"`
				Name             string       `json:"name"`
				NameWithOwner    string       `json:"nameWithOwner"`
				UpdatedAt        time.Time    `json:"updatedAt"`
				Owner            graphQLActor `json:"owner"`
				DefaultBranchRef *struct {
					Name   string  `json:"name"`
					Target oidNode `json:"target"`
				} `json:"defaultBranchRef"`
			} `json:"repository"`
			Search struct {
				IssueCount int      `json:"issueCount"`
				PageInfo   pageInfo `json:"pageInfo"`
				Nodes      []struct {
					Number int `json:"number"`
				} `json:"nodes"`
			} `json:"search"`
		}
		searchQuery := fmt.Sprintf(
			"repo:%s/%s is:pr updated:%s..%s",
			config.Owner,
			config.Name,
			config.Since.Format("2006-01-02"),
			config.Until.Add(-time.Nanosecond).Format("2006-01-02"),
		)
		err := config.Client.call(
			ctx,
			listPullRequestsQuery,
			map[string]any{
				"owner": config.Owner,
				"name":  config.Name,
				"query": searchQuery,
				"after": cursor.SearchAfter,
			},
			&data,
		)
		if err != nil {
			return err
		}
		if data.Repository.ID == "" {
			return fmt.Errorf("repository %s/%s was not found", config.Owner, config.Name)
		}
		if data.Search.IssueCount > 1000 {
			return fmt.Errorf(
				"window matches %d pull requests; narrow it below GitHub search's 1000-result cap",
				data.Search.IssueCount,
			)
		}
		if cursor.Repository.ID == 0 {
			cursor.Repository = replay.Repository{
				ID:        data.Repository.DatabaseID,
				NodeID:    data.Repository.ID,
				Owner:     data.Repository.Owner.Login,
				Name:      data.Repository.Name,
				UpdatedAt: data.Repository.UpdatedAt,
			}
			if data.Repository.DefaultBranchRef != nil {
				cursor.Repository.DefaultBranch =
					data.Repository.DefaultBranchRef.Name
				cursor.Repository.DefaultBranchSHA =
					data.Repository.DefaultBranchRef.Target.OID
			}
		}
		seen := make(map[int]bool, len(cursor.PullNumbers))
		for _, number := range cursor.PullNumbers {
			seen[number] = true
		}
		for _, node := range data.Search.Nodes {
			if node.Number > 0 && !seen[node.Number] {
				cursor.PullNumbers = append(cursor.PullNumbers, node.Number)
				seen[node.Number] = true
			}
		}
		cursor.SearchAfter = data.Search.PageInfo.EndCursor
		if !data.Search.PageInfo.HasNextPage {
			sort.Ints(cursor.PullNumbers)
			cursor.Phase = "details"
		}
		if err := saveCursor(config.CursorPath, *cursor); err != nil {
			return err
		}
		if cursor.Phase != "pulls" {
			return nil
		}
		if cursor.SearchAfter == nil {
			return fmt.Errorf("pull search hasNextPage without endCursor")
		}
	}
}

func crawlPullDetails(
	ctx context.Context,
	config crawlConfig,
	cursor *crawlCursor,
) error {
	for cursor.PullIndex < len(cursor.PullNumbers) {
		if cursor.CurrentPull == nil {
			cursor.CurrentPull = &recordedPull{}
			cursor.TimelineAfter = nil
			cursor.TimelineComplete = false
			cursor.ThreadsAfter = nil
			cursor.ThreadsComplete = false
			if err := saveCursor(config.CursorPath, *cursor); err != nil {
				return err
			}
		}
		for !cursor.TimelineComplete {
			if err := fetchPullTimelinePage(
				ctx,
				config,
				cursor,
				cursor.PullNumbers[cursor.PullIndex],
			); err != nil {
				return err
			}
		}
		for !cursor.ThreadsComplete {
			if err := fetchPullThreadsPage(
				ctx,
				config,
				cursor,
				cursor.PullNumbers[cursor.PullIndex],
			); err != nil {
				return err
			}
		}
		checkpoint := func() error {
			return saveCursor(config.CursorPath, *cursor)
		}
		for index := range cursor.CurrentPull.Timeline {
			node := &cursor.CurrentPull.Timeline[index]
			switch node.Typename {
			case "PullRequestCommit":
				if node.Commit == nil {
					continue
				}
				if err := completeCommitChecks(
					ctx,
					config,
					node.Commit,
					checkpoint,
				); err != nil {
					return fmt.Errorf(
						"pull request %d commit %s: %w",
						cursor.CurrentPull.Number,
						node.Commit.OID,
						err,
					)
				}
			}
		}
		for index := range cursor.CurrentPull.Threads {
			thread := &cursor.CurrentPull.Threads[index]
			if err := completeThreadComments(
				ctx,
				config,
				thread.ID,
				&thread.Comments,
				checkpoint,
			); err != nil {
				return fmt.Errorf(
					"pull request %d review thread %s: %w",
					cursor.CurrentPull.Number,
					thread.ID,
					err,
				)
			}
		}
		cursor.CurrentPull.Threads = mergeTimelineThreads(
			cursor.CurrentPull.Threads,
			cursor.CurrentPull.Timeline,
		)
		cursor.Pulls = append(cursor.Pulls, *cursor.CurrentPull)
		cursor.PullIndex++
		cursor.CurrentPull = nil
		cursor.TimelineAfter = nil
		cursor.TimelineComplete = false
		cursor.ThreadsAfter = nil
		cursor.ThreadsComplete = false
		if err := saveCursor(config.CursorPath, *cursor); err != nil {
			return err
		}
	}
	cursor.Phase = "history"
	return saveCursor(config.CursorPath, *cursor)
}

func fetchPullThreadsPage(
	ctx context.Context,
	config crawlConfig,
	cursor *crawlCursor,
	number int,
) error {
	var data struct {
		Repository *struct {
			PullRequest *struct {
				ReviewThreads struct {
					PageInfo pageInfo     `json:"pageInfo"`
					Nodes    []threadNode `json:"nodes"`
				} `json:"reviewThreads"`
			} `json:"pullRequest"`
		} `json:"repository"`
	}
	err := config.Client.call(
		ctx,
		pullRequestThreadsQuery,
		map[string]any{
			"owner":  config.Owner,
			"name":   config.Name,
			"number": number,
			"after":  cursor.ThreadsAfter,
		},
		&data,
	)
	if err != nil {
		return err
	}
	if data.Repository == nil || data.Repository.PullRequest == nil {
		return fmt.Errorf("pull request %d disappeared", number)
	}
	page := data.Repository.PullRequest.ReviewThreads
	cursor.CurrentPull.Threads = append(
		cursor.CurrentPull.Threads,
		page.Nodes...,
	)
	cursor.ThreadsAfter = page.PageInfo.EndCursor
	cursor.ThreadsComplete = !page.PageInfo.HasNextPage
	if !cursor.ThreadsComplete && cursor.ThreadsAfter == nil {
		return fmt.Errorf(
			"pull request %d reviewThreads hasNextPage without endCursor",
			number,
		)
	}
	return saveCursor(config.CursorPath, *cursor)
}

func fetchPullTimelinePage(
	ctx context.Context,
	config crawlConfig,
	cursor *crawlCursor,
	number int,
) error {
	var data struct {
		Repository *struct {
			PullRequest *struct {
				recordedPull
				TimelineItems struct {
					PageInfo pageInfo       `json:"pageInfo"`
					Nodes    []timelineNode `json:"nodes"`
				} `json:"timelineItems"`
			} `json:"pullRequest"`
		} `json:"repository"`
	}
	err := config.Client.call(
		ctx,
		pullRequestTimelineQuery,
		map[string]any{
			"owner":  config.Owner,
			"name":   config.Name,
			"number": number,
			"after":  cursor.TimelineAfter,
		},
		&data,
	)
	if err != nil {
		return err
	}
	if data.Repository == nil || data.Repository.PullRequest == nil {
		return fmt.Errorf("pull request %d was not found", number)
	}
	page := data.Repository.PullRequest
	if cursor.CurrentPull.ID == "" {
		*cursor.CurrentPull = page.recordedPull
		cursor.CurrentPull.Timeline = nil
		cursor.CurrentPull.Threads = nil
	}
	cursor.CurrentPull.Timeline = append(
		cursor.CurrentPull.Timeline,
		page.TimelineItems.Nodes...,
	)
	cursor.TimelineAfter = page.TimelineItems.PageInfo.EndCursor
	cursor.TimelineComplete = !page.TimelineItems.PageInfo.HasNextPage
	if !cursor.TimelineComplete && cursor.TimelineAfter == nil {
		return fmt.Errorf(
			"pull request %d timeline hasNextPage without endCursor",
			number,
		)
	}
	return saveCursor(config.CursorPath, *cursor)
}

func completeThreadComments(
	ctx context.Context,
	config crawlConfig,
	threadID string,
	comments *commentPage,
	checkpoint func() error,
) error {
	after := comments.PageInfo.EndCursor
	for comments.PageInfo.HasNextPage {
		if after == nil {
			return fmt.Errorf(
				"review thread %s comments hasNextPage without endCursor",
				threadID,
			)
		}
		var data struct {
			Node *struct {
				Comments commentPage `json:"comments"`
			} `json:"node"`
		}
		err := config.Client.call(
			ctx,
			reviewThreadCommentsQuery,
			map[string]any{"id": threadID, "after": after},
			&data,
		)
		if err != nil {
			return err
		}
		if data.Node == nil {
			return fmt.Errorf("review thread %s disappeared", threadID)
		}
		comments.Nodes = append(
			comments.Nodes,
			data.Node.Comments.Nodes...,
		)
		comments.PageInfo = data.Node.Comments.PageInfo
		after = comments.PageInfo.EndCursor
		if err := checkpoint(); err != nil {
			return err
		}
	}
	return nil
}

func mergeTimelineThreads(
	threads []threadNode,
	timeline []timelineNode,
) []threadNode {
	result := append([]threadNode(nil), threads...)
	seen := make(map[string]bool)
	for _, thread := range result {
		seen[thread.ID] = true
	}
	for _, node := range timeline {
		if node.Typename != "PullRequestReviewThread" ||
			node.ID == "" ||
			seen[node.ID] {
			continue
		}
		seen[node.ID] = true
		result = append(result, threadNode{
			ID:         node.ID,
			IsResolved: node.IsResolved,
			IsOutdated: node.IsOutdated,
			Path:       node.Path,
			Line:       node.Line,
			Comments:   node.Comments,
		})
	}
	return result
}

func completeCommitChecks(
	ctx context.Context,
	config crawlConfig,
	commit *recordedCommit,
	checkpoint func() error,
) error {
	for index := range commit.CheckSuites.Nodes {
		if err := completeCheckRuns(
			ctx,
			config,
			&commit.CheckSuites.Nodes[index],
			checkpoint,
		); err != nil {
			return err
		}
	}
	after := commit.CheckSuites.PageInfo.EndCursor
	for commit.CheckSuites.PageInfo.HasNextPage {
		if after == nil {
			return fmt.Errorf(
				"commit %s checkSuites hasNextPage without endCursor",
				commit.OID,
			)
		}
		var data struct {
			Repository *struct {
				Object *struct {
					CheckSuites checkSuitePage `json:"checkSuites"`
				} `json:"object"`
			} `json:"repository"`
		}
		err := config.Client.call(
			ctx,
			commitCheckSuitesQuery,
			map[string]any{
				"owner": config.Owner,
				"name":  config.Name,
				"oid":   commit.OID,
				"after": after,
			},
			&data,
		)
		if err != nil {
			return err
		}
		if data.Repository == nil || data.Repository.Object == nil {
			return fmt.Errorf("commit %s disappeared", commit.OID)
		}
		page := data.Repository.Object.CheckSuites
		start := len(commit.CheckSuites.Nodes)
		commit.CheckSuites.Nodes = append(
			commit.CheckSuites.Nodes,
			page.Nodes...,
		)
		commit.CheckSuites.PageInfo = page.PageInfo
		after = page.PageInfo.EndCursor
		if err := checkpoint(); err != nil {
			return err
		}
		for index := start; index < len(commit.CheckSuites.Nodes); index++ {
			if err := completeCheckRuns(
				ctx,
				config,
				&commit.CheckSuites.Nodes[index],
				checkpoint,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func completeCheckRuns(
	ctx context.Context,
	config crawlConfig,
	suite *checkSuiteNode,
	checkpoint func() error,
) error {
	after := suite.CheckRuns.PageInfo.EndCursor
	for suite.CheckRuns.PageInfo.HasNextPage {
		if after == nil {
			return fmt.Errorf(
				"check suite %d checkRuns hasNextPage without endCursor",
				suite.DatabaseID,
			)
		}
		var data struct {
			Node *struct {
				CheckRuns checkRunPage `json:"checkRuns"`
			} `json:"node"`
		}
		err := config.Client.call(
			ctx,
			checkSuiteRunsQuery,
			map[string]any{"id": suite.ID, "after": after},
			&data,
		)
		if err != nil {
			return err
		}
		if data.Node == nil {
			return fmt.Errorf("check suite %d disappeared", suite.DatabaseID)
		}
		suite.CheckRuns.Nodes = append(
			suite.CheckRuns.Nodes,
			data.Node.CheckRuns.Nodes...,
		)
		suite.CheckRuns.PageInfo = data.Node.CheckRuns.PageInfo
		after = suite.CheckRuns.PageInfo.EndCursor
		if err := checkpoint(); err != nil {
			return err
		}
	}
	return nil
}

func crawlDefaultHistory(
	ctx context.Context,
	config crawlConfig,
	cursor *crawlCursor,
) error {
	for {
		checkpoint := func() error {
			return saveCursor(config.CursorPath, *cursor)
		}
		for cursor.HistoryCheck < len(cursor.DefaultCommits) {
			commit := &cursor.DefaultCommits[cursor.HistoryCheck]
			if err := completeCommitChecks(
				ctx,
				config,
				commit,
				checkpoint,
			); err != nil {
				return fmt.Errorf(
					"default branch commit %s: %w",
					commit.OID,
					err,
				)
			}
			cursor.HistoryCheck++
			if err := checkpoint(); err != nil {
				return err
			}
		}
		if cursor.HistoryComplete {
			cursor.Phase = "complete"
			return checkpoint()
		}
		var data struct {
			Repository *struct {
				DefaultBranchRef *struct {
					Name   string `json:"name"`
					Target struct {
						History struct {
							PageInfo pageInfo         `json:"pageInfo"`
							Nodes    []recordedCommit `json:"nodes"`
						} `json:"history"`
					} `json:"target"`
				} `json:"defaultBranchRef"`
			} `json:"repository"`
		}
		err := config.Client.call(
			ctx,
			defaultBranchHistoryQuery,
			map[string]any{
				"owner": config.Owner,
				"name":  config.Name,
				"since": config.Since.Format(time.RFC3339),
				"until": config.Until.Format(time.RFC3339),
				"after": cursor.HistoryAfter,
			},
			&data,
		)
		if err != nil {
			return err
		}
		if data.Repository == nil || data.Repository.DefaultBranchRef == nil {
			return fmt.Errorf("repository has no default branch history")
		}
		history := data.Repository.DefaultBranchRef.Target.History
		cursor.DefaultCommits = append(
			cursor.DefaultCommits,
			history.Nodes...,
		)
		cursor.HistoryAfter = history.PageInfo.EndCursor
		if !history.PageInfo.HasNextPage {
			cursor.HistoryComplete = true
		}
		if history.PageInfo.HasNextPage && cursor.HistoryAfter == nil {
			return fmt.Errorf("default history hasNextPage without endCursor")
		}
		if err := checkpoint(); err != nil {
			return err
		}
	}
}

func saveCursor(path string, cursor crawlCursor) error {
	body, err := json.MarshalIndent(cursor, "", "  ")
	if err != nil {
		return fmt.Errorf("encode crawl cursor: %w", err)
	}
	body = append(body, '\n')
	return writeAtomic(path, body, 0o600)
}

func writeRecordingFile(path string, recording replay.Recording) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create recording directory: %w", err)
	}
	temp, err := os.CreateTemp(directory, ".ghrecord-*.ndjson")
	if err != nil {
		return fmt.Errorf("create recording temporary file: %w", err)
	}
	tempPath := temp.Name()
	success := false
	defer func() {
		_ = temp.Close()
		if !success {
			_ = os.Remove(tempPath)
		}
	}()
	if err := replay.Write(temp, recording); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync recording: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close recording: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("install recording: %w", err)
	}
	success = true
	return nil
}

func writeAtomic(path string, body []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create cursor directory: %w", err)
	}
	temp, err := os.CreateTemp(directory, ".ghrecord-cursor-*")
	if err != nil {
		return fmt.Errorf("create crawl cursor temporary file: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()
	if err := temp.Chmod(mode); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set crawl cursor permissions: %w", err)
	}
	if _, err := temp.Write(body); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write crawl cursor: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync crawl cursor: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close crawl cursor: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("install crawl cursor: %w", err)
	}
	return nil
}

func buildRecording(cursor crawlCursor) replay.Recording {
	var events []replay.TimedEvent
	add := func(at time.Time, key string, event replay.Event) {
		if at.Before(cursor.Since) || !at.Before(cursor.Until) {
			return
		}
		events = append(events, replay.TimedEvent{
			At:        at.UTC(),
			StableKey: key,
			Event:     event,
		})
	}
	repository := cursor.Repository
	add(cursor.Since, "repository", replay.Event{
		Kind:       "repository",
		Repository: &repository,
	})

	finalPulls := make([]replay.PullRequest, 0, len(cursor.Pulls))
	for _, pull := range cursor.Pulls {
		finalPull := finalPullState(pull)
		finalPulls = append(finalPulls, finalPull)
		appendPullEvents(add, pull, finalPull)
	}
	finalPulls, syntheticMembers := replay.SynthesizeStackBases(
		repository,
		finalPulls,
		cursor.SynthesizeStacks,
		cursor.Seed,
	)
	finalByNumber := make(map[int]replay.PullRequest, len(finalPulls))
	for _, pull := range finalPulls {
		finalByNumber[pull.Number] = pull
	}
	for index := range events {
		pull := events[index].Event.PullRequest
		if pull == nil || !syntheticMembers[pull.Number] {
			continue
		}
		final := finalByNumber[pull.Number]
		pull.Head = final.Head
		pull.Base = final.Base
	}
	appendDefaultHistoryEvents(add, repository, cursor.DefaultCommits)
	stacks := replay.DeriveStacks(repository, finalPulls)
	for _, stack := range stacks {
		stack.Synthetic = stackHasSyntheticMember(stack, syntheticMembers)
		at := stackTimestamp(stack, finalByNumber, cursor.Since)
		if !at.Before(cursor.Until) {
			at = cursor.Until.Add(-time.Millisecond)
		}
		stackCopy := stack
		add(at, fmt.Sprintf("stack:%d", stack.ID), replay.Event{
			Kind:  "stack",
			Stack: &stackCopy,
		})
	}
	events = deduplicateTimedEvents(events)
	normalized, startedAt := replay.NormalizeEvents(events)
	return replay.Recording{
		Header: replay.Header{
			Type:             "recording",
			Version:          replay.RecordingVersion,
			Repository:       repository,
			Since:            cursor.Since,
			Until:            cursor.Until,
			StartedAt:        startedAt,
			SynthesizeStacks: cursor.SynthesizeStacks,
			Seed:             cursor.Seed,
		},
		Events: normalized,
	}
}

func appendPullEvents(
	add func(time.Time, string, replay.Event),
	raw recordedPull,
	final replay.PullRequest,
) {
	timeline := append([]timelineNode(nil), raw.Timeline...)
	sort.SliceStable(timeline, func(i, j int) bool {
		return timelineTime(timeline[i]).Before(timelineTime(timeline[j]))
	})
	current := final
	current.State = "open"
	current.Merged = false
	current.ClosedAt = nil
	current.MergedAt = nil
	current.UpdatedAt = raw.CreatedAt
	current.ReviewDecision = ""
	finalBase := final.Base
	if first := firstCommit(timeline); first != nil {
		current.Head.SHA = first.OID
	}
	for _, node := range timeline {
		if node.Typename == "BaseRefChangedEvent" &&
			node.PreviousRefName != "" {
			current.Base.Ref = node.PreviousRefName
			current.Base.SHA = ""
			break
		}
	}
	reviewStatesBeforeDismissal := make(map[int64]string)
	for _, node := range timeline {
		if node.Typename == "ReviewDismissedEvent" &&
			node.Review != nil {
			reviewStatesBeforeDismissal[node.Review.DatabaseID] =
				strings.ToLower(node.PreviousReview)
		}
	}
	opened := current
	opened.UpdatedAt = raw.CreatedAt
	add(raw.CreatedAt, fmt.Sprintf("pr:%d:opened", raw.Number), replay.Event{
		Kind:        "pull_request",
		Action:      "opened",
		PullRequest: &opened,
	})

	previousHead := strings.Repeat("0", 40)
	for index, node := range timeline {
		at := timelineTime(node)
		switch node.Typename {
		case "PullRequestCommit":
			if node.Commit == nil || node.Commit.OID == "" {
				continue
			}
			hadPreviousCommit := previousHead != strings.Repeat("0", 40)
			before := previousHead
			if parent := firstParentSHA(*node.Commit); parent != "" {
				before = parent
			}
			current.Head.SHA = node.Commit.OID
			current.UpdatedAt = at
			commit := replay.Commit{
				SHA:               node.Commit.OID,
				ParentSHA:         firstParentSHA(*node.Commit),
				Ref:               "refs/heads/" + current.Head.Ref,
				CommittedAt:       node.Commit.CommittedDate,
				PullRequestNumber: raw.Number,
			}
			add(
				at,
				fmt.Sprintf("pr:%d:commit:%s", raw.Number, node.Commit.OID),
				replay.Event{Kind: "commit", Commit: &commit},
			)
			push := replay.Push{
				Ref:      "refs/heads/" + current.Head.Ref,
				Before:   before,
				After:    current.Head.SHA,
				PushedAt: at,
			}
			add(at, fmt.Sprintf("pr:%d:commit:%s:push", raw.Number, node.Commit.OID), replay.Event{
				Kind: "push",
				Push: &push,
			})
			if hadPreviousCommit {
				state := current
				add(at, fmt.Sprintf("pr:%d:commit:%s:sync", raw.Number, node.Commit.OID), replay.Event{
					Kind:        "pull_request",
					Action:      "synchronize",
					PullRequest: &state,
				})
			}
			previousHead = node.Commit.OID
			appendCheckEvents(add, *node.Commit)
		case "PullRequestReview":
			if node.SubmittedAt == nil {
				continue
			}
			rawReview := recordedReview{
				ID:          node.ID,
				DatabaseID:  node.DatabaseID,
				Body:        node.Body,
				State:       node.State,
				SubmittedAt: node.SubmittedAt,
				UpdatedAt:   node.UpdatedAt,
				Author:      node.Author,
			}
			if node.Commit != nil {
				rawReview.Commit = &oidNode{OID: node.Commit.OID}
			}
			review := replayReview(rawReview)
			if previous := reviewStatesBeforeDismissal[node.DatabaseID]; previous != "" {
				review.State = previous
			}
			applyReviewDecision(&current, review.State)
			state := current
			add(*node.SubmittedAt, fmt.Sprintf("pr:%d:review:%d", raw.Number, node.DatabaseID), replay.Event{
				Kind:        "pull_request_review",
				Action:      "submitted",
				PullRequest: &state,
				Review:      &review,
			})
		case "ReviewDismissedEvent":
			if node.Review == nil {
				continue
			}
			review := replayReview(*node.Review)
			review.State = "dismissed"
			current.ReviewDecision = ""
			state := current
			add(at, fmt.Sprintf(
				"pr:%d:review:%d:dismissed",
				raw.Number,
				review.ID,
			), replay.Event{
				Kind:        "pull_request_review",
				Action:      "dismissed",
				PullRequest: &state,
				Review:      &review,
			})
		case "BaseRefChangedEvent":
			previous := current.Base
			current.Base.Ref = node.CurrentRefName
			current.Base.SHA = ""
			if current.Base.Ref == finalBase.Ref {
				current.Base.SHA = finalBase.SHA
			}
			current.UpdatedAt = at
			state := current
			add(at, fmt.Sprintf("pr:%d:base:%d", raw.Number, index), replay.Event{
				Kind:         "pull_request",
				Action:       "edited",
				PullRequest:  &state,
				PreviousBase: &previous,
			})
		case "HeadRefForcePushedEvent":
			if node.AfterCommit == nil {
				continue
			}
			before := previousHead
			if node.BeforeCommit != nil {
				before = node.BeforeCommit.OID
			}
			current.Head.SHA = node.AfterCommit.OID
			current.UpdatedAt = at
			commit := replay.Commit{
				SHA:               node.AfterCommit.OID,
				ParentSHA:         firstOIDParentSHA(*node.AfterCommit),
				Ref:               "refs/heads/" + current.Head.Ref,
				CommittedAt:       at,
				PullRequestNumber: raw.Number,
			}
			add(at, fmt.Sprintf("pr:%d:force:%d:commit", raw.Number, index), replay.Event{
				Kind:   "commit",
				Commit: &commit,
			})
			push := replay.Push{
				Ref:      "refs/heads/" + current.Head.Ref,
				Before:   before,
				After:    node.AfterCommit.OID,
				Forced:   true,
				PushedAt: at,
			}
			add(at, fmt.Sprintf("pr:%d:force:%d:push", raw.Number, index), replay.Event{
				Kind: "push",
				Push: &push,
			})
			state := current
			add(at, fmt.Sprintf("pr:%d:force:%d:sync", raw.Number, index), replay.Event{
				Kind:        "pull_request",
				Action:      "synchronize",
				PullRequest: &state,
			})
			previousHead = node.AfterCommit.OID
		case "ClosedEvent":
			current.State = "closed"
			current.ClosedAt = timePointer(at)
			current.UpdatedAt = at
			state := current
			add(at, fmt.Sprintf("pr:%d:closed:%d", raw.Number, index), replay.Event{
				Kind:        "pull_request",
				Action:      "closed",
				PullRequest: &state,
			})
		case "MergedEvent":
			current.State = "closed"
			current.Merged = true
			current.ClosedAt = timePointer(at)
			current.MergedAt = timePointer(at)
			current.UpdatedAt = at
			state := current
			add(at, fmt.Sprintf("pr:%d:merged:%d", raw.Number, index), replay.Event{
				Kind:        "pull_request",
				Action:      "closed",
				PullRequest: &state,
			})
		case "ReopenedEvent":
			current.State = "open"
			current.ClosedAt = nil
			current.UpdatedAt = at
			state := current
			add(at, fmt.Sprintf("pr:%d:reopened:%d", raw.Number, index), replay.Event{
				Kind:        "pull_request",
				Action:      "reopened",
				PullRequest: &state,
			})
		}
	}
	for _, thread := range raw.Threads {
		appendThreadEvents(add, raw, final, timeline, thread)
	}
}

func appendThreadEvents(
	add func(time.Time, string, replay.Event),
	rawPull recordedPull,
	finalPull replay.PullRequest,
	timeline []timelineNode,
	thread threadNode,
) {
	comments := make([]replay.ReviewComment, 0, len(thread.Comments.Nodes))
	for _, raw := range thread.Comments.Nodes {
		reviewID := int64(0)
		if raw.PullRequestReview != nil {
			reviewID = raw.PullRequestReview.DatabaseID
		}
		comment := replay.ReviewComment{
			ID:          raw.DatabaseID,
			NodeID:      raw.ID,
			ReviewID:    reviewID,
			Body:        truncateText(raw.Body),
			Path:        raw.Path,
			Line:        raw.Line,
			AuthorLogin: raw.Author.Login,
			CreatedAt:   raw.CreatedAt,
			UpdatedAt:   raw.UpdatedAt,
		}
		comments = append(comments, comment)
		commentCopy := comment
		pullCopy := pullStateAt(
			rawPull,
			finalPull,
			timeline,
			raw.CreatedAt,
		)
		add(raw.CreatedAt, fmt.Sprintf("pr:%d:comment:%d:created", finalPull.Number, raw.DatabaseID), replay.Event{
			Kind:        "review_comment",
			Action:      "created",
			PullRequest: &pullCopy,
			Comment:     &commentCopy,
		})
		if raw.UpdatedAt.After(raw.CreatedAt) {
			edited := comment
			pullCopy := pullStateAt(
				rawPull,
				finalPull,
				timeline,
				raw.UpdatedAt,
			)
			add(raw.UpdatedAt, fmt.Sprintf("pr:%d:comment:%d:edited", finalPull.Number, raw.DatabaseID), replay.Event{
				Kind:        "review_comment",
				Action:      "edited",
				PullRequest: &pullCopy,
				Comment:     &edited,
			})
		}
	}
	if len(comments) == 0 {
		return
	}
	at := comments[0].UpdatedAt
	for _, comment := range comments[1:] {
		if comment.UpdatedAt.After(at) {
			at = comment.UpdatedAt
		}
	}
	state := replay.ReviewThread{
		ID:         thread.ID,
		IsResolved: thread.IsResolved,
		IsOutdated: thread.IsOutdated,
		Path:       thread.Path,
		Line:       thread.Line,
		Comments:   comments,
	}
	action := "unresolved"
	if thread.IsResolved {
		action = "resolved"
	}
	pullCopy := pullStateAt(rawPull, finalPull, timeline, at)
	add(at, fmt.Sprintf("pr:%d:thread:%s", finalPull.Number, thread.ID), replay.Event{
		Kind:        "review_thread",
		Action:      action,
		PullRequest: &pullCopy,
		Thread:      &state,
	})
}

func pullStateAt(
	raw recordedPull,
	final replay.PullRequest,
	timeline []timelineNode,
	at time.Time,
) replay.PullRequest {
	current := final
	current.State = "open"
	current.Merged = false
	current.ClosedAt = nil
	current.MergedAt = nil
	current.UpdatedAt = raw.CreatedAt
	current.ReviewDecision = ""
	finalBase := final.Base
	if first := firstCommit(timeline); first != nil {
		current.Head.SHA = first.OID
	}
	for _, node := range timeline {
		if node.Typename == "BaseRefChangedEvent" &&
			node.PreviousRefName != "" {
			current.Base.Ref = node.PreviousRefName
			current.Base.SHA = ""
			break
		}
	}
	for _, node := range timeline {
		eventAt := timelineTime(node)
		if eventAt.After(at) {
			break
		}
		switch node.Typename {
		case "PullRequestCommit":
			if node.Commit != nil && node.Commit.OID != "" {
				current.Head.SHA = node.Commit.OID
				current.UpdatedAt = eventAt
			}
		case "BaseRefChangedEvent":
			current.Base.Ref = node.CurrentRefName
			current.Base.SHA = ""
			if current.Base.Ref == finalBase.Ref {
				current.Base.SHA = finalBase.SHA
			}
			current.UpdatedAt = eventAt
		case "PullRequestReview":
			reviewState := node.State
			if previous := reviewStateBeforeDismissal(
				timeline,
				node.DatabaseID,
			); previous != "" {
				reviewState = previous
			}
			applyReviewDecision(&current, reviewState)
		case "ReviewDismissedEvent":
			current.ReviewDecision = ""
		case "HeadRefForcePushedEvent":
			if node.AfterCommit != nil {
				current.Head.SHA = node.AfterCommit.OID
				current.UpdatedAt = eventAt
			}
		case "ClosedEvent":
			current.State = "closed"
			current.ClosedAt = timePointer(eventAt)
			current.UpdatedAt = eventAt
		case "MergedEvent":
			current.State = "closed"
			current.Merged = true
			current.ClosedAt = timePointer(eventAt)
			current.MergedAt = timePointer(eventAt)
			current.UpdatedAt = eventAt
		case "ReopenedEvent":
			current.State = "open"
			current.ClosedAt = nil
			current.UpdatedAt = eventAt
		}
	}
	if current.UpdatedAt.Before(raw.CreatedAt) {
		current.UpdatedAt = raw.CreatedAt
	}
	return current
}

func applyReviewDecision(pull *replay.PullRequest, state string) {
	switch strings.ToUpper(state) {
	case "APPROVED":
		pull.ReviewDecision = "APPROVED"
	case "CHANGES_REQUESTED":
		pull.ReviewDecision = "CHANGES_REQUESTED"
	}
}

func reviewStateBeforeDismissal(
	timeline []timelineNode,
	reviewID int64,
) string {
	for _, node := range timeline {
		if node.Typename == "ReviewDismissedEvent" &&
			node.Review != nil &&
			node.Review.DatabaseID == reviewID {
			return node.PreviousReview
		}
	}
	return ""
}

func appendDefaultHistoryEvents(
	add func(time.Time, string, replay.Event),
	repository replay.Repository,
	commits []recordedCommit,
) {
	ordered := append([]recordedCommit(nil), commits...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].CommittedDate.Before(ordered[j].CommittedDate)
	})
	before := strings.Repeat("0", 40)
	for _, commit := range ordered {
		if parent := firstParentSHA(commit); parent != "" {
			before = parent
		}
		recorded := replay.Commit{
			SHA:           commit.OID,
			ParentSHA:     firstParentSHA(commit),
			Ref:           "refs/heads/" + repository.DefaultBranch,
			CommittedAt:   commit.CommittedDate,
			DefaultBranch: true,
		}
		add(commit.CommittedDate, "default:"+commit.OID+":commit", replay.Event{
			Kind:   "commit",
			Commit: &recorded,
		})
		push := replay.Push{
			Ref:           "refs/heads/" + repository.DefaultBranch,
			Before:        before,
			After:         commit.OID,
			DefaultBranch: true,
			PushedAt:      commit.CommittedDate,
		}
		add(commit.CommittedDate, "default:"+commit.OID+":push", replay.Event{
			Kind: "push",
			Push: &push,
		})
		appendCheckEvents(add, commit)
		before = commit.OID
	}
}

func firstParentSHA(commit recordedCommit) string {
	if len(commit.Parents.Nodes) == 0 {
		return ""
	}
	return commit.Parents.Nodes[0].OID
}

func firstOIDParentSHA(commit oidNode) string {
	if len(commit.Parents.Nodes) == 0 {
		return ""
	}
	return commit.Parents.Nodes[0].OID
}

func replayReview(raw recordedReview) replay.Review {
	result := replay.Review{
		ID:          raw.DatabaseID,
		NodeID:      raw.ID,
		State:       strings.ToLower(raw.State),
		Body:        truncateText(raw.Body),
		AuthorLogin: raw.Author.Login,
		SubmittedAt: raw.UpdatedAt.UTC(),
	}
	if raw.SubmittedAt != nil {
		result.SubmittedAt = raw.SubmittedAt.UTC()
	}
	if raw.Commit != nil {
		result.CommitSHA = raw.Commit.OID
	}
	return result
}

func appendCheckEvents(
	add func(time.Time, string, replay.Event),
	commit recordedCommit,
) {
	for _, rawSuite := range commit.CheckSuites.Nodes {
		requested := replay.CheckSuite{
			ID:        rawSuite.DatabaseID,
			NodeID:    rawSuite.ID,
			HeadSHA:   commit.OID,
			Status:    "queued",
			AppSlug:   rawSuite.App.Slug,
			CreatedAt: rawSuite.CreatedAt,
			UpdatedAt: rawSuite.CreatedAt,
		}
		add(rawSuite.CreatedAt, fmt.Sprintf("suite:%d:requested", rawSuite.DatabaseID), replay.Event{
			Kind:       "check_suite",
			Action:     "requested",
			CheckSuite: &requested,
		})
		if strings.EqualFold(rawSuite.Status, "COMPLETED") {
			completed := requested
			completed.Status = "completed"
			completed.Conclusion = strings.ToLower(rawSuite.Conclusion)
			completed.UpdatedAt = rawSuite.UpdatedAt
			add(rawSuite.UpdatedAt, fmt.Sprintf("suite:%d:completed", rawSuite.DatabaseID), replay.Event{
				Kind:       "check_suite",
				Action:     "completed",
				CheckSuite: &completed,
			})
		}
		for _, rawRun := range rawSuite.CheckRuns.Nodes {
			startedAt := rawRun.StartedAt
			if startedAt == nil {
				startedAt = timePointer(rawSuite.CreatedAt)
			}
			created := replay.CheckRun{
				ID:         rawRun.DatabaseID,
				NodeID:     rawRun.ID,
				HeadSHA:    commit.OID,
				Name:       rawRun.Name,
				Status:     "queued",
				DetailsURL: rawRun.DetailsURL,
				AppSlug:    rawSuite.App.Slug,
				StartedAt:  startedAt,
			}
			add(*startedAt, fmt.Sprintf("run:%d:created", rawRun.DatabaseID), replay.Event{
				Kind:     "check_run",
				Action:   "created",
				CheckRun: &created,
			})
			if rawRun.CompletedAt != nil {
				completed := created
				completed.Status = "completed"
				completed.Conclusion = strings.ToLower(rawRun.Conclusion)
				completed.CompletedAt = rawRun.CompletedAt
				add(*rawRun.CompletedAt, fmt.Sprintf("run:%d:completed", rawRun.DatabaseID), replay.Event{
					Kind:     "check_run",
					Action:   "completed",
					CheckRun: &completed,
				})
			}
		}
	}
}

func finalPullState(raw recordedPull) replay.PullRequest {
	result := replay.PullRequest{
		ID:             raw.DatabaseID,
		NodeID:         raw.ID,
		Number:         raw.Number,
		Title:          truncateText(raw.Title),
		State:          strings.ToLower(raw.State),
		Draft:          raw.IsDraft,
		Merged:         raw.Merged,
		AuthorLogin:    raw.Author.Login,
		ReviewDecision: raw.ReviewDecision,
		MergeableState: raw.Mergeable,
		Head: replay.Branch{
			Ref: raw.HeadRefName,
			SHA: raw.HeadRefOID,
		},
		Base: replay.Branch{
			Ref: raw.BaseRefName,
			SHA: raw.BaseRefOID,
		},
		CreatedAt: raw.CreatedAt,
		UpdatedAt: raw.UpdatedAt,
		ClosedAt:  raw.ClosedAt,
		MergedAt:  raw.MergedAt,
	}
	if raw.HeadRepository != nil {
		result.Head.Repository = raw.HeadRepository.NameWithOwner
	}
	if raw.BaseRepository != nil {
		result.Base.Repository = raw.BaseRepository.NameWithOwner
	}
	return result
}

func timelineTime(node timelineNode) time.Time {
	if node.SubmittedAt != nil {
		return *node.SubmittedAt
	}
	if !node.CreatedAt.IsZero() {
		return node.CreatedAt
	}
	if node.Commit != nil {
		return node.Commit.CommittedDate
	}
	return node.UpdatedAt
}

func firstCommit(nodes []timelineNode) *recordedCommit {
	for index := range nodes {
		if nodes[index].Typename == "PullRequestCommit" &&
			nodes[index].Commit != nil {
			return nodes[index].Commit
		}
	}
	return nil
}

func deduplicateTimedEvents(events []replay.TimedEvent) []replay.TimedEvent {
	seen := make(map[string]bool, len(events))
	result := make([]replay.TimedEvent, 0, len(events))
	for _, event := range events {
		key := event.StableKey + ":" + event.At.UTC().Format(time.RFC3339Nano)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, event)
	}
	return result
}

func stackHasSyntheticMember(stack replay.Stack, members map[int]bool) bool {
	for _, number := range stack.PullRequests {
		if members[number] {
			return true
		}
	}
	return false
}

func stackTimestamp(
	stack replay.Stack,
	pulls map[int]replay.PullRequest,
	fallback time.Time,
) time.Time {
	at := fallback
	for _, number := range stack.PullRequests {
		if pull := pulls[number]; pull.UpdatedAt.After(at) {
			at = pull.UpdatedAt
		}
	}
	return at
}

func truncateText(value string) string {
	const maximumBytes = 512
	if len(value) <= maximumBytes {
		return value
	}
	value = value[:maximumBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func timePointer(value time.Time) *time.Time {
	copy := value.UTC()
	return &copy
}
