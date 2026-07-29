package fetch

import (
	"fmt"
	"strconv"
	"strings"
)

type entityKey struct {
	Kind   string
	Repo   string
	Value  string
	Number int
}

func parseEntityKey(raw, wantKind string) (entityKey, error) {
	prefix := wantKind + ":"
	if !strings.HasPrefix(raw, prefix) {
		return entityKey{}, fmt.Errorf("key %q is not a %s key", raw, wantKind)
	}
	remainder := strings.TrimPrefix(raw, prefix)
	repo, value, ok := strings.Cut(remainder, ":")
	if !ok || repo == "" || value == "" || !strings.Contains(repo, "/") {
		return entityKey{}, fmt.Errorf("invalid %s key %q", wantKind, raw)
	}
	key := entityKey{Kind: wantKind, Repo: repo, Value: value}
	if wantKind == "pr" || wantKind == "stack" {
		number, err := strconv.Atoi(value)
		if err != nil || number <= 0 {
			return entityKey{}, fmt.Errorf("invalid %s number in %q", wantKind, raw)
		}
		key.Number = number
	}
	return key, nil
}
