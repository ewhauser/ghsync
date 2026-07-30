package outbox

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestFenceKeyEncodesGhsyncName(t *testing.T) {
	t.Parallel()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(FenceKey))
	if got := strings.TrimLeft(string(encoded[:]), "\x00"); got != "ghsync" {
		t.Fatalf("writer fence key encodes %q, want ghsync", got)
	}
}
