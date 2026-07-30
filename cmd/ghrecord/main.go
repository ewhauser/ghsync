package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const defaultGraphQLURL = "https://api.github.com/graphql"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ghrecord:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()
	return runContext(
		ctx,
		args,
		http.DefaultClient,
		defaultGraphQLURL,
		os.Stdout,
	)
}

func runContext(
	ctx context.Context,
	args []string,
	httpClient *http.Client,
	graphQLURL string,
	stdout io.Writer,
) error {
	flags := flag.NewFlagSet("ghrecord", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repository := flags.String("repo", "", "repository in owner/name form")
	sinceValue := flags.String("since", "", "inclusive date or RFC3339 timestamp")
	untilValue := flags.String(
		"until",
		"",
		"inclusive date or exclusive RFC3339 timestamp",
	)
	token := flags.String("token", "", "GitHub token with public-repository read access")
	output := flags.String("out", "", "recording NDJSON output path")
	cursor := flags.String(
		"cursor",
		"",
		"resume cursor path (default: <out>.cursor.json)",
	)
	synthesizeStacks := flags.Float64(
		"synthesize-stacks",
		0,
		"percentage of otherwise unstacked PRs to thread into stacks",
	)
	seed := flags.Int64("seed", 1, "deterministic stack-synthesis seed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("ghrecord does not accept positional arguments")
	}
	owner, name, err := parseRepository(*repository)
	if err != nil {
		return err
	}
	since, err := parseBoundary(*sinceValue, false)
	if err != nil {
		return fmt.Errorf("parse --since: %w", err)
	}
	until, err := parseBoundary(*untilValue, true)
	if err != nil {
		return fmt.Errorf("parse --until: %w", err)
	}
	if !until.After(since) {
		return fmt.Errorf("--until must be after --since")
	}
	if strings.TrimSpace(*token) == "" {
		return fmt.Errorf("--token is required")
	}
	if *output == "" {
		return fmt.Errorf("--out is required")
	}
	if *synthesizeStacks < 0 || *synthesizeStacks > 100 ||
		math.IsNaN(*synthesizeStacks) ||
		math.IsInf(*synthesizeStacks, 0) {
		return fmt.Errorf("--synthesize-stacks must be between 0 and 100")
	}
	cursorPath := *cursor
	if cursorPath == "" {
		cursorPath = *output + ".cursor.json"
	}
	same, err := samePath(*output, cursorPath)
	if err != nil {
		return err
	}
	if same {
		return fmt.Errorf("--cursor and --out must use different paths")
	}
	client, err := newGraphQLClient(
		graphQLURL,
		strings.TrimSpace(*token),
		httpClient,
	)
	if err != nil {
		return err
	}
	result, err := crawl(ctx, crawlConfig{
		Owner:            owner,
		Name:             name,
		Since:            since,
		Until:            until,
		OutputPath:       *output,
		CursorPath:       cursorPath,
		SynthesizeStacks: *synthesizeStacks,
		Seed:             *seed,
		Client:           client,
	})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		stdout,
		"recorded %d events from %s/%s (%s to %s)\n",
		result.Events,
		owner,
		name,
		since.Format(time.RFC3339),
		until.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("write recording summary: %w", err)
	}
	return nil
}

func parseRepository(value string) (string, string, error) {
	if strings.TrimSpace(value) != value {
		return "", "", fmt.Errorf("--repo must be in owner/name form")
	}
	owner, name, ok := strings.Cut(value, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("--repo must be in owner/name form")
	}
	return owner, name, nil
}

func samePath(left, right string) (bool, error) {
	leftAbsolute, err := filepath.Abs(left)
	if err != nil {
		return false, fmt.Errorf("resolve --out path: %w", err)
	}
	rightAbsolute, err := filepath.Abs(right)
	if err != nil {
		return false, fmt.Errorf("resolve --cursor path: %w", err)
	}
	return filepath.Clean(leftAbsolute) == filepath.Clean(rightAbsolute), nil
}

func parseBoundary(value string, endOfDate bool) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("value is required")
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return time.Time{}, fmt.Errorf("use YYYY-MM-DD or RFC3339")
	}
	if endOfDate {
		parsed = parsed.Add(24 * time.Hour)
	}
	return parsed.UTC(), nil
}
