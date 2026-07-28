package main

import "testing"

func TestValidateRoles(t *testing.T) {
	if err := validateRoles("all"); err != nil {
		t.Fatalf("all rejected: %v", err)
	}
	for _, roles := range []string{"", "bogus", "ingress", "all,event"} {
		if err := validateRoles(roles); err == nil {
			t.Fatalf("roles %q accepted", roles)
		}
	}
}
