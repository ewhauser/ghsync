package fetch

import (
	"slices"
	"sync"
)

// repositoryLockSet serializes batches which observe the same repository in a
// process. Database advisory locks remain the cross-process correctness fence;
// this layer prevents local burst traffic from consuming database connections
// while it waits for that fence.
type repositoryLockSet struct {
	mu    sync.Mutex
	locks map[int64]*repositoryLock
}

type repositoryLock struct {
	mu   sync.Mutex
	refs int
}

func newRepositoryLockSet() *repositoryLockSet {
	return &repositoryLockSet{locks: make(map[int64]*repositoryLock)}
}

// lock returns an unlock function. Callers must supply sorted, distinct IDs so
// batches spanning several repositories acquire the locks in one stable order.
func (s *repositoryLockSet) lock(ids []int64) func() {
	locks := make([]*repositoryLock, 0, len(ids))
	s.mu.Lock()
	for _, id := range ids {
		lock := s.locks[id]
		if lock == nil {
			lock = &repositoryLock{}
			s.locks[id] = lock
		}
		lock.refs++
		locks = append(locks, lock)
	}
	s.mu.Unlock()

	for _, lock := range locks {
		lock.mu.Lock()
	}
	return func() {
		for index := len(locks) - 1; index >= 0; index-- {
			locks[index].mu.Unlock()
		}
		s.mu.Lock()
		for index, id := range ids {
			lock := locks[index]
			lock.refs--
			if lock.refs == 0 {
				delete(s.locks, id)
			}
		}
		s.mu.Unlock()
	}
}

func repositoryIDs(items []*pullBatchItem) []int64 {
	ids := make([]int64, 0, len(items))
	seen := make(map[int64]struct{}, len(items))
	for _, item := range items {
		if _, exists := seen[item.metadata.RepoGitHubID]; exists {
			continue
		}
		seen[item.metadata.RepoGitHubID] = struct{}{}
		ids = append(ids, item.metadata.RepoGitHubID)
	}
	slices.Sort(ids)
	return ids
}
