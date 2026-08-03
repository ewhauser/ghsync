package fetch

import (
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/store"
)

func TestRepositoryLockSetSerializesSharedRepositories(t *testing.T) {
	t.Parallel()
	locks := newRepositoryLockSet()
	unlockFirst := locks.lock([]int64{101})

	acquired := make(chan func(), 1)
	go func() { acquired <- locks.lock([]int64{101}) }()

	select {
	case unlock := <-acquired:
		unlock()
		t.Fatal("shared repository lock acquired while first batch held it")
	case <-time.After(50 * time.Millisecond):
	}

	unlockFirst()
	select {
	case unlock := <-acquired:
		unlock()
	case <-time.After(time.Second):
		t.Fatal("shared repository lock did not unblock")
	}
}

func TestRepositoryLockSetAllowsIndependentRepositories(t *testing.T) {
	t.Parallel()
	locks := newRepositoryLockSet()
	unlockFirst := locks.lock([]int64{101})
	defer unlockFirst()

	acquired := make(chan func(), 1)
	go func() { acquired <- locks.lock([]int64{202}) }()

	select {
	case unlock := <-acquired:
		unlock()
	case <-time.After(time.Second):
		t.Fatal("independent repository lock was blocked")
	}
}

func TestRepositoryIDsReturnsSortedDistinctIDs(t *testing.T) {
	t.Parallel()
	items := []*pullBatchItem{
		{metadata: store.FetchMetadata{RepoGitHubID: 3}},
		{metadata: store.FetchMetadata{RepoGitHubID: 1}},
		{metadata: store.FetchMetadata{RepoGitHubID: 3}},
	}
	got := repositoryIDs(items)
	want := []int64{1, 3}
	if len(got) != len(want) {
		t.Fatalf("repository IDs = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("repository IDs = %v, want %v", got, want)
		}
	}
}
