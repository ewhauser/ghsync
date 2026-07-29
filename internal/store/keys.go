package store

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/acme/frontier/internal/outbox"
)

func RepositoryEntityKey(installationID, repositoryGitHubID int64) string {
	return outbox.RepositoryKey(installationID, repositoryGitHubID)
}

func RepositoryDiscoveryKey(installationID int64, fullName string) string {
	return fmt.Sprintf("repo-discovery:%d:%s", installationID, fullName)
}

func PullRequestEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return outbox.PullRequestKey(installationID, repositoryGitHubID, number)
}

func StackEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	number int,
) string {
	return outbox.StackKey(installationID, repositoryGitHubID, number)
}

func ChecksEntityKey(
	installationID int64,
	repositoryGitHubID int64,
	sha string,
) string {
	return outbox.ChecksKey(installationID, repositoryGitHubID, sha)
}

func RepoRulesEntityKey(
	installationID int64,
	repositoryGitHubID int64,
) string {
	return outbox.RepoRulesKey(installationID, repositoryGitHubID)
}

func derivationScope(
	repository RepositoryRecord,
	number int,
	stackNumber *int,
) string {
	if stackNumber != nil {
		return StackEntityKey(
			repository.InstallationID, repository.GitHubID, *stackNumber,
		)
	}
	return PullRequestEntityKey(
		repository.InstallationID, repository.GitHubID, number,
	)
}

func uniqueStrings(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func uniqueInts(values ...int) []int {
	seen := make(map[int]struct{}, len(values))
	result := make([]int, 0, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Ints(result)
	return result
}

func intPointer(value pgtype.Int4) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int32)
	return &result
}

func stackPointerFromPR(err error, value pgtype.Int4) *int {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return intPointer(value)
}

func nullableInt4(value *int) pgtype.Int4 {
	if value == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*value), Valid: true}
}

func nullableInt8(value int64) pgtype.Int8 {
	return pgtype.Int8{Int64: value, Valid: value > 0}
}

func timestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: !value.IsZero()}
}
