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

func TestParseRequeueOptions(t *testing.T) {
	tests := []struct {
		args []string
		want requeueOptions
	}{
		{
			args: []string{"--guid=delivery-1"},
			want: requeueOptions{guid: "delivery-1"},
		},
		{
			args: []string{"--all-parked"},
			want: requeueOptions{allParked: true},
		},
	}
	for _, test := range tests {
		got, err := parseRequeueOptions(test.args)
		if err != nil {
			t.Fatalf("%v rejected: %v", test.args, err)
		}
		if got != test.want {
			t.Fatalf("%v parsed as %+v, want %+v", test.args, got, test.want)
		}
	}
	for _, args := range [][]string{
		nil,
		{"--guid=delivery-1", "--all-parked"},
		{"--guid="},
		{"--all-parked", "extra"},
	} {
		if _, err := parseRequeueOptions(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}
