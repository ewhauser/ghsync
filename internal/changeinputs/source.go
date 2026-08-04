package changeinputs

import (
	"context"
	"fmt"
	"sync"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/gh"
	"github.com/ewhauser/ghsync/internal/store"
)

type sourceKey struct {
	repositoryID int64
	ref          string
}

type sourceEntry struct {
	sha      string
	source   gh.CodeownersSource
	fetching bool
	ready    chan struct{}
	lastUsed uint64
}

const maxSourceEntries = 1024

// SourceResolver reuses durable mirrored CODEOWNERS sources and collapses an
// invalidated repository/ref to one GitHub probe. Its bounded process-local
// map accelerates concurrent workers; durable snapshot provenance remains the
// source of reuse after eviction or restart. The mutex protects only in-memory
// state and is always released before external I/O (C-C6).
type SourceResolver struct {
	rest *gh.RESTClient

	mu      sync.Mutex
	entries map[sourceKey]*sourceEntry
	clock   uint64
}

// NewSourceResolver constructs a CODEOWNERS source resolver.
func NewSourceResolver(rest *gh.RESTClient) *SourceResolver {
	return &SourceResolver{
		rest:    rest,
		entries: make(map[sourceKey]*sourceEntry),
	}
}

// Resolve returns exact mirrored truth when its ref/SHA provenance matches.
// On invalidation, one caller refreshes the ref while peers wait without
// retaining a database transaction, advisory lock, or shared mutex.
func (r *SourceResolver) Resolve(
	ctx context.Context,
	class budget.Class,
	repositoryID int64,
	owner string,
	repo string,
	ref string,
	sha string,
	mirrored *store.CodeownersSourceRecord,
) (gh.CodeownersSource, error) {
	if r == nil || r.rest == nil {
		return gh.CodeownersSource{}, fmt.Errorf("CODEOWNERS resolver requires REST client")
	}
	if repositoryID <= 0 || owner == "" || repo == "" || sha == "" {
		return gh.CodeownersSource{}, fmt.Errorf("CODEOWNERS resolver requires repository and SHA")
	}
	keyRef := ref
	if keyRef == "" {
		// The #11 contract reads the immutable base SHA even when GitHub no
		// longer reports a branch name. Keep that case coalescible without
		// changing the persisted empty codeowners_ref provenance.
		keyRef = sha
	}
	key := sourceKey{repositoryID: repositoryID, ref: keyRef}
	for {
		r.mu.Lock()
		entry := r.entries[key]
		if entry == nil {
			r.evictLocked()
			entry = &sourceEntry{}
			r.entries[key] = entry
		}
		r.clock++
		entry.lastUsed = r.clock
		if entry.fetching {
			ready := entry.ready
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return gh.CodeownersSource{}, fmt.Errorf(
					"wait for CODEOWNERS source: %w",
					ctx.Err(),
				)
			case <-ready:
				continue
			}
		}

		if validSourceRecord(mirrored, ref, sha) {
			source := sourceFromRecord(mirrored)
			entry.sha = sha
			entry.source = source
			r.mu.Unlock()
			return source, nil
		}
		if entry.sha == sha {
			source := entry.source
			r.mu.Unlock()
			return source, nil
		}

		prior := entry.source
		if prior.State == "" && validPriorRecord(mirrored, ref) {
			prior = sourceFromRecord(mirrored)
		}
		entry.fetching = true
		entry.ready = make(chan struct{})
		r.mu.Unlock()

		var priorSource *gh.CodeownersSource
		if prior.State != "" {
			priorSource = &prior
		}
		source, err := r.rest.FindCodeowners(
			ctx,
			class,
			owner,
			repo,
			sha,
			priorSource,
		)

		r.mu.Lock()
		entry.fetching = false
		if err == nil {
			entry.sha = sha
			entry.source = source
		}
		close(entry.ready)
		entry.ready = nil
		r.mu.Unlock()
		if err != nil {
			return gh.CodeownersSource{}, fmt.Errorf("refresh CODEOWNERS source: %w", err)
		}
		return source, nil
	}
}

func (r *SourceResolver) evictLocked() {
	if len(r.entries) < maxSourceEntries {
		return
	}
	var (
		oldestKey sourceKey
		oldest    *sourceEntry
	)
	for key, entry := range r.entries {
		if entry.fetching ||
			(oldest != nil && entry.lastUsed >= oldest.lastUsed) {
			continue
		}
		oldestKey, oldest = key, entry
	}
	if oldest != nil {
		delete(r.entries, oldestKey)
	}
}

func sourceFromRecord(record *store.CodeownersSourceRecord) gh.CodeownersSource {
	if record == nil {
		return gh.CodeownersSource{}
	}
	return gh.CodeownersSource{
		Ref:     record.SHA,
		Path:    record.Path,
		Content: record.Content,
		State:   record.State,
		ETag:    record.ETag,
	}
}

func validSourceRecord(
	record *store.CodeownersSourceRecord,
	ref string,
	sha string,
) bool {
	return record != nil && record.Ref == ref && record.SHA == sha &&
		validPriorRecord(record, ref)
}

func validPriorRecord(record *store.CodeownersSourceRecord, ref string) bool {
	if record == nil || record.Ref != ref || record.SHA == "" {
		return false
	}
	source := sourceFromRecord(record)
	if record.Hash == "" || record.Hash != sourceHash(&source) {
		return false
	}
	switch record.State {
	case gh.CodeownersPresent:
		return codeownersPath(record.Path)
	case gh.CodeownersOversized:
		return codeownersPath(record.Path) && record.Content == ""
	case gh.CodeownersMissing:
		return record.Path == "" && record.Content == ""
	default:
		return false
	}
}

func codeownersPath(path string) bool {
	switch path {
	case ".github/CODEOWNERS", "CODEOWNERS", "docs/CODEOWNERS":
		return true
	default:
		return false
	}
}
