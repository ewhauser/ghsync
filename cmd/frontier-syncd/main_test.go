package main

import "testing"

func TestValidateRoles(t *testing.T) {
	for _, roles := range []string{"all", "ingress", "dispatch", "ingress,dispatch"} {
		if err := validateRoles(roles); err != nil {
			t.Fatalf("%q rejected: %v", roles, err)
		}
	}
	for _, roles := range []string{"", "bogus", "all,event", "ingress,event"} {
		if err := validateRoles(roles); err == nil {
			t.Fatalf("roles %q accepted", roles)
		}
	}
}
