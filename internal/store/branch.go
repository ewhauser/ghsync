package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/acme/frontier/internal/store/dbgen"
)

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
			continue
		}
		targets = append(
			targets,
			fmt.Sprintf("pr:%s:%d", repoFullName, pr.Number),
		)
	}
	sort.Strings(targets)
	return targets, nil
}
