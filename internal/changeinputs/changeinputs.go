// Package changeinputs builds the bounded, source-derived ownership inputs
// attached to one exact pull-request base/head observation.
package changeinputs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/codeowners"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store"
)

// Hydrate preserves the direct authoritative path used by drift inspection.
// PR refreshes use HydrateFromMirror so ordinary sweeps avoid redundant
// CODEOWNERS reads.
func Hydrate(
	ctx context.Context,
	rest *gh.RESTClient,
	writer *store.EntityWriter,
	class budget.Class,
	repositoryID int64,
	repositoryOwner string,
	repositoryName string,
	number int,
	node *gh.PullRequestNode,
) (*store.PullRequestChangeSnapshotRecord, error) {
	return HydrateFromMirror(
		ctx,
		rest,
		NewSourceResolver(rest),
		writer,
		class,
		repositoryID,
		repositoryOwner,
		repositoryName,
		number,
		node,
		nil,
		true,
	)
}

// HydrateFromMirror reads changed-file rename supplements and resolves the
// effective CODEOWNERS source from matching mirror provenance before GitHub.
// GraphQL supplies the authoritative paths and connection completeness; REST
// is used only for facts GraphQL omits, invalidated ownership provenance, and
// the final base/head fence check.
func HydrateFromMirror(
	ctx context.Context,
	rest *gh.RESTClient,
	sources *SourceResolver,
	writer *store.EntityWriter,
	class budget.Class,
	repositoryID int64,
	repositoryOwner string,
	repositoryName string,
	number int,
	node *gh.PullRequestNode,
	mirrored *store.CodeownersSourceRecord,
	forceCodeowners bool,
) (*store.PullRequestChangeSnapshotRecord, error) {
	if rest == nil || sources == nil || writer == nil || node == nil {
		return nil, fmt.Errorf("change-input hydration requires clients and PR")
	}
	snapshot := &store.PullRequestChangeSnapshotRecord{
		BaseSHA:         node.BaseRefOID,
		HeadSHA:         node.HeadRefOID,
		FilesTotalCount: node.ChangedFiles,
		CodeownersRef:   node.BaseRefName,
		CodeownersSHA:   node.BaseRefOID,
	}
	if node.Files == nil {
		snapshot.FilesTruncated = true
	} else {
		snapshot.FilesTruncated = node.Files.Truncated
		if node.Files.TotalCount != node.ChangedFiles {
			snapshot.FilesTruncated = true
			snapshot.FilesTotalCount = max(
				node.Files.TotalCount,
				node.ChangedFiles,
			)
		}
		for _, file := range node.Files.Nodes {
			changeType := strings.ToLower(file.ChangeType)
			snapshot.Files = append(snapshot.Files, store.ChangedFileRecord{
				Path:       file.Path,
				ChangeType: changeType,
			})
		}
		if snapshot.FilesTotalCount < len(snapshot.Files) {
			snapshot.FilesTotalCount = len(snapshot.Files)
			snapshot.FilesTruncated = true
		}
	}

	needsRenames := false
	for index := range snapshot.Files {
		if snapshot.Files[index].ChangeType == "renamed" {
			needsRenames = true
			break
		}
	}
	if needsRenames {
		renames, truncated, err := rest.PullRequestFileRenames(
			ctx, class, repositoryOwner, repositoryName, number,
		)
		if err != nil {
			return nil, fmt.Errorf("fetch PR rename paths: %w", err)
		}
		snapshot.FilesTruncated = snapshot.FilesTruncated || truncated
		for index := range snapshot.Files {
			file := &snapshot.Files[index]
			if file.ChangeType != "renamed" {
				continue
			}
			file.PreviousPath = renames[file.Path]
			if file.PreviousPath == "" {
				// A rename without its source path is an incomplete snapshot,
				// even if the GraphQL connection itself was cursor-complete.
				snapshot.FilesTruncated = true
			}
		}
	}

	source := gh.CodeownersSource{State: gh.CodeownersUnavailable}
	if node.BaseRefOID != "" {
		var err error
		if forceCodeowners {
			source, err = rest.FindCodeowners(
				ctx,
				class,
				repositoryOwner,
				repositoryName,
				node.BaseRefOID,
				nil,
			)
		} else {
			source, err = sources.Resolve(
				ctx,
				class,
				repositoryID,
				repositoryOwner,
				repositoryName,
				node.BaseRefName,
				node.BaseRefOID,
				mirrored,
			)
		}
		if err != nil {
			return nil, fmt.Errorf("fetch effective CODEOWNERS: %w", err)
		}
	}
	snapshot.CodeownersPath = source.Path
	snapshot.CodeownersState = source.State
	snapshot.CodeownersSource = source.Content
	snapshot.CodeownersHash = sourceHash(&source)
	snapshot.CodeownersETag = source.ETag
	if source.State == gh.CodeownersPresent {
		rules := codeowners.Parse(source.Content)
		for _, file := range snapshot.Files {
			match, ok := codeowners.Resolve(rules, file.Path)
			if !ok {
				continue
			}
			seenTokens := make(map[string]struct{}, len(match.Owners))
			for _, owner := range match.Owners {
				if _, duplicate := seenTokens[owner.Token]; duplicate {
					continue
				}
				seenTokens[owner.Token] = struct{}{}
				snapshot.Owners = append(snapshot.Owners, store.FileOwnerRecord{
					Path:            file.Path,
					OwnerToken:      owner.Token,
					OwnerType:       string(owner.Type),
					OwnerName:       owner.Name,
					ResolutionState: "unresolved",
					SourcePattern:   match.Pattern,
					SourceLine:      match.Line,
				})
			}
		}
	}
	resolvedOwners, err := writer.ResolveFileOwnerIdentities(
		ctx, repositoryID, repositoryOwner, snapshot.Owners,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve CODEOWNERS identities: %w", err)
	}
	snapshot.Owners = resolveObservedIdentities(
		resolvedOwners, repositoryOwner, node,
	)

	// Deterministic ordering is part of both replace-set comparison and the
	// drift snapshot. Do not depend on GitHub, SQL, or map iteration order.
	sort.Slice(snapshot.Files, func(i, j int) bool {
		return snapshot.Files[i].Path < snapshot.Files[j].Path
	})
	sort.Slice(snapshot.Owners, func(i, j int) bool {
		if snapshot.Owners[i].Path == snapshot.Owners[j].Path {
			return snapshot.Owners[i].OwnerToken <
				snapshot.Owners[j].OwnerToken
		}
		return snapshot.Owners[i].Path < snapshot.Owners[j].Path
	})

	latest, _, err := rest.GetPull(
		ctx,
		class,
		repositoryOwner,
		repositoryName,
		number,
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("verify PR change-input fence: %w", err)
	}
	if latest.GetBase().GetSHA() != snapshot.BaseSHA ||
		latest.GetHead().GetSHA() != snapshot.HeadSHA {
		return nil, fmt.Errorf(
			"verify PR change-input fence: base/head changed during observation",
		)
	}
	return snapshot, nil
}

func resolveObservedIdentities(
	owners []store.FileOwnerRecord,
	repositoryOwner string,
	node *gh.PullRequestNode,
) []store.FileOwnerRecord {
	type identity struct {
		githubID int64
		nodeID   string
		login    string
	}
	known := make(map[string]identity)
	for _, request := range node.ReviewRequests.Nodes {
		reviewer := request.RequestedReviewer
		switch reviewer.Typename {
		case "User":
			if reviewer.ID != "" && reviewer.Login != "" {
				known["user\x00"+strings.ToLower(reviewer.Login)] = identity{
					githubID: reviewer.DatabaseID,
					nodeID:   reviewer.ID,
					login:    reviewer.Login,
				}
			}
		case "Team":
			if reviewer.ID != "" && reviewer.Slug != "" {
				known["team\x00"+strings.ToLower(reviewer.Slug)] = identity{
					githubID: reviewer.DatabaseID,
					nodeID:   reviewer.ID,
					login:    reviewer.Slug,
				}
			}
		}
	}
	for _, review := range node.Reviews.Nodes {
		if review.Author != nil && review.Author.Typename == "User" &&
			review.Author.ID != "" && review.Author.Login != "" {
			known["user\x00"+strings.ToLower(review.Author.Login)] = identity{
				nodeID: review.Author.ID, login: review.Author.Login,
			}
		}
	}
	for _, comment := range node.Comments.Nodes {
		if comment.Author != nil && comment.Author.Typename == "User" &&
			comment.Author.ID != "" && comment.Author.Login != "" {
			known["user\x00"+strings.ToLower(comment.Author.Login)] = identity{
				nodeID: comment.Author.ID, login: comment.Author.Login,
			}
		}
	}
	resolved := append([]store.FileOwnerRecord(nil), owners...)
	for index := range resolved {
		owner := &resolved[index]
		if owner.ResolutionState != "unresolved" {
			continue
		}
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
		identity, ok := known[owner.OwnerType+"\x00"+
			strings.ToLower(lookup)]
		if !ok {
			continue
		}
		owner.ResolutionState = "resolved"
		owner.OwnerGitHubID = identity.githubID
		owner.OwnerNodeID = identity.nodeID
		owner.OwnerLogin = identity.login
	}
	return resolved
}

func sourceHash(source *gh.CodeownersSource) string {
	digest := sha256.Sum256([]byte(
		source.State + "\x00" + source.Path + "\x00" + source.Content,
	))
	return hex.EncodeToString(digest[:])
}

// Semantic returns the null-safe, source-derived value embedded in drift
// snapshots. Source content itself is deliberately represented by its hash;
// consumers read the mirrored source from the public snapshot table.
func Semantic(snapshot *store.PullRequestChangeSnapshotRecord) map[string]any {
	files := make([]map[string]any, 0, len(snapshot.Files))
	for _, file := range snapshot.Files {
		var previous any
		if file.PreviousPath != "" {
			previous = file.PreviousPath
		}
		files = append(files, map[string]any{
			"path":          file.Path,
			"previous_path": previous,
			"change_type":   file.ChangeType,
		})
	}
	owners := make([]map[string]any, 0, len(snapshot.Owners))
	for index := range snapshot.Owners {
		owner := &snapshot.Owners[index]
		var githubID any
		if owner.OwnerGitHubID > 0 {
			githubID = owner.OwnerGitHubID
		}
		var nodeID, login any
		if owner.OwnerNodeID != "" {
			nodeID = owner.OwnerNodeID
		}
		if owner.OwnerLogin != "" {
			login = owner.OwnerLogin
		}
		owners = append(owners, map[string]any{
			"path":             owner.Path,
			"owner_token":      owner.OwnerToken,
			"owner_type":       owner.OwnerType,
			"owner_name":       owner.OwnerName,
			"resolution_state": owner.ResolutionState,
			"owner_gh_id":      githubID,
			"owner_node_id":    nodeID,
			"owner_login":      login,
			"source_pattern":   owner.SourcePattern,
			"source_line":      owner.SourceLine,
		})
	}
	var path any
	if snapshot.CodeownersPath != "" {
		path = snapshot.CodeownersPath
	}
	return map[string]any{
		"base_sha":          snapshot.BaseSHA,
		"head_sha":          snapshot.HeadSHA,
		"files_total_count": snapshot.FilesTotalCount,
		"files_truncated":   snapshot.FilesTruncated,
		"codeowners_ref":    snapshot.CodeownersRef,
		"codeowners_sha":    snapshot.CodeownersSHA,
		"codeowners_path":   path,
		"codeowners_state":  snapshot.CodeownersState,
		"codeowners_hash":   snapshot.CodeownersHash,
		"files":             files,
		"owners":            owners,
	}
}
