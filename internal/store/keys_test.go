package store

import "testing"

func TestNestedEntityLockKeysFollowGlobalOrder(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		first  string
		second string
	}{
		{
			name:   "repository discovery before repository write",
			first:  RepositoryDiscoveryKey(1, "acme/monolith"),
			second: RepositoryEntityKey(1, 1001),
		},
		{
			name:   "pull request before repository batch apply",
			first:  PullRequestEntityKey(1, 1001, 42),
			second: RepositoryEntityKey(1, 1001),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.first >= test.second {
				t.Fatalf(
					"nested lock order %q then %q is not lexically ascending",
					test.first,
					test.second,
				)
			}
		})
	}
}
