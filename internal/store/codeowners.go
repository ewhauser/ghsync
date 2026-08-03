package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// ResolveFileOwnerIdentities fills stable identities from facts already
// mirrored for this repository. It never performs a live GitHub lookup and
// leaves unknown users, teams, email owners, and malformed tokens explicit.
func (w *EntityWriter) ResolveFileOwnerIdentities(
	ctx context.Context,
	repoGitHubID int64,
	repositoryOwner string,
	owners []FileOwnerRecord,
) ([]FileOwnerRecord, error) {
	repo, err := dbgen.New(w.pool).GetRepoByGitHubID(ctx, repoGitHubID)
	if err != nil {
		return nil, fmt.Errorf("find CODEOWNERS repository: %w", err)
	}
	identities, err := dbgen.New(w.pool).ListCodeOwnerIdentities(ctx, repo.ID)
	if err != nil {
		return nil, fmt.Errorf("list known CODEOWNERS identities: %w", err)
	}
	type identity struct {
		githubID int64
		nodeID   string
		login    string
	}
	known := make(map[string]identity, len(identities))
	for _, item := range identities {
		known[item.OwnerType+"\x00"+strings.ToLower(item.OwnerLogin)] = identity{
			githubID: item.OwnerGhID,
			nodeID:   item.OwnerNodeID,
			login:    item.OwnerLogin,
		}
	}
	resolved := append([]FileOwnerRecord(nil), owners...)
	for index := range resolved {
		owner := &resolved[index]
		owner.ResolutionState = "unresolved"
		lookup := owner.OwnerName
		switch owner.OwnerType {
		case "user":
		case "team":
			parts := strings.SplitN(owner.OwnerName, "/", 2)
			if len(parts) != 2 ||
				!strings.EqualFold(parts[0], repositoryOwner) {
				continue
			}
			lookup = parts[1]
		default:
			continue
		}
		identity, ok := known[owner.OwnerType+"\x00"+strings.ToLower(lookup)]
		if !ok {
			continue
		}
		owner.ResolutionState = "resolved"
		owner.OwnerGitHubID = identity.githubID
		owner.OwnerNodeID = identity.nodeID
		owner.OwnerLogin = identity.login
	}
	return resolved, nil
}
