# Storm and soak profiles

`cmd/soak` drives standalone fake GitHub, mutates authoritative fixture truth,
and emits signed deliveries to a running engine. It samples `/metrics`
throughout and fails on a budget-floor breach, open drift, parked delivery,
missing watermark progress, or C-Q2 p95/p99 violation.

## CI smoke

The independent `soak-smoke` CI job uses fresh Postgres:

```sh
go run ./cmd/soak \
  --profile=smoke \
  --duration=2m \
  --multiplier=10 \
  --engine-url=http://127.0.0.1:18080 \
  --fake-github-url=http://127.0.0.1:19797
```

Use `--events=internal/dispatch/testdata/s142_recorded.json` for recorded
traffic. `--recorded-rate` is its base events/second; `--multiplier=10` is the
M6 storm requirement.

## Release soak

Use a dedicated host and disposable database:

```sh
go run ./cmd/soak \
  --profile=48h \
  --duration=48h \
  --recorded-rate=1 \
  --multiplier=10 \
  --events=internal/dispatch/testdata/s142_recorded.json \
  --engine-url=http://127.0.0.1:18080 \
  --fake-github-url=http://127.0.0.1:19797
```

Retain metrics and daemon/fake logs. Attach the single-screen dashboard for
the full window and confirm drift stayed zero, no delivery parked, class
floors held, sweeps stayed below period, and the watermark advanced whenever
cache mutations committed.
