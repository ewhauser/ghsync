package testdb

import (
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestParseConfigSeparatesPoolMaxConnsFromPgtestdbURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		databaseURL  string
		wantMaxConns int32
		wantError    bool
	}{
		{
			name: "default",
			databaseURL: "postgres://test:test@localhost:5432/ghsync_test?" +
				"sslmode=disable",
			wantMaxConns: defaultTestPoolMaxConns,
		},
		{
			name: "explicit override",
			databaseURL: "postgres://test:test@localhost:5432/ghsync_test?" +
				"sslmode=disable&pool_max_conns=7",
			wantMaxConns: 7,
		},
		{
			name: "zero",
			databaseURL: "postgres://test:test@localhost:5432/ghsync_test?" +
				"pool_max_conns=0",
			wantError: true,
		},
		{
			name: "invalid",
			databaseURL: "postgres://test:test@localhost:5432/ghsync_test?" +
				"pool_max_conns=many",
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			conf, maxConns, err := parseConfig(test.databaseURL)
			if test.wantError {
				if err == nil {
					t.Fatal("parseConfig error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if maxConns != test.wantMaxConns {
				t.Fatalf(
					"pool max conns = %d, want %d",
					maxConns,
					test.wantMaxConns,
				)
			}
			options, err := url.ParseQuery(conf.Options)
			if err != nil {
				t.Fatal(err)
			}
			if options.Has("pool_max_conns") {
				t.Fatalf(
					"pgtestdb options include pool_max_conns: %q",
					conf.Options,
				)
			}
			if options.Get("sslmode") != "disable" {
				t.Fatalf("pgtestdb sslmode = %q, want disable", options.Get("sslmode"))
			}

			poolURL, err := withPoolMaxConns(conf.URL(), maxConns)
			if err != nil {
				t.Fatal(err)
			}
			poolConfig, err := pgxpool.ParseConfig(poolURL)
			if err != nil {
				t.Fatal(err)
			}
			if poolConfig.MaxConns != test.wantMaxConns {
				t.Fatalf(
					"runtime pool max conns = %d, want %d",
					poolConfig.MaxConns,
					test.wantMaxConns,
				)
			}
		})
	}
}
