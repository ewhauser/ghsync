# Storm and soak profiles

`cmd/soak` is a strict verifier, not a traffic demo. It mutates standalone
fake-GitHub truth, emits signed deliveries, samples run-scoped metric deltas,
and exits nonzero unless all of the following are true:

- the exact configured count completed before the load deadline;
- achieved events/second is at least `recorded-rate * multiplier`;
- no starvation increment, parked delivery, or open drift appeared;
- the event queue, deliveries, refresh generations, and watermark all drain;
- a post-population drift pass inspects new samples and new durable watermark
  passes complete;
- run-scoped C-Q2 p95/p99 remain within 20s/60s;
- every mutated pull request in fake truth exactly matches the final cache.

The default smoke arithmetic is explicit: `120 seconds * 1 event/second * 10`
= **1,200 successful deliveries**. The 48-hour default is
`172,800 seconds * 1 * 10` = **1,728,000 successful deliveries**.

## CI smoke

CI provisions fresh Postgres, migrates it, starts fake GitHub and
`ghsyncd --roles=all`, runs `ghsyncd backfill`, and sets
`DRIFT_PERIOD=5s` so a durable pass must finish after population. The verifier
refuses to start traffic until that installation backfill and all of its real
cache-writer jobs have completed. The equivalent verifier command is:

```sh
go run ./cmd/soak \
  --profile=smoke \
  --duration=2m \
  --recorded-rate=1 \
  --multiplier=10 \
  --database-url="$DATABASE_URL" \
  --installation-id="$GITHUB_INSTALLATION_ID" \
  --engine-url=http://127.0.0.1:18080 \
  --fake-github-url=http://127.0.0.1:19797
```

Do not shorten the smoke by reducing its expected count. A shorter local
diagnostic may use `--profile=custom --duration=20s`; at the same rates it must
still emit exactly 200 events and pass every strict final assertion.

## Reproducible release soak

Use a dedicated host and a disposable Postgres database. Preserve the exact
artifact digest being evaluated.

1. Create an empty database and record its PostgreSQL version:

   ```sh
   createdb ghsync_soak_release
   export DATABASE_URL='postgres://ghsync@127.0.0.1:5432/ghsync_soak_release?sslmode=disable'
   psql "$DATABASE_URL" -Atc 'select version()' > postgres-version.txt
   ```

2. Build once, record checksums, and migrate with those bytes:

   ```sh
   go build -o ./ghsyncd ./cmd/ghsyncd
   go build -o ./fake-github ./cmd/fake-github
   go build -o ./ghsync-soak ./cmd/soak
   shasum -a 256 ./ghsyncd ./fake-github ./ghsync-soak \
     > artifact-sha256.txt
   ./ghsyncd migrate 2>&1 | tee migrate.log
   ```

3. Start the isolated fixture and engine. `serve --roles=all` is intentional
   here because this is one-host verification, not the production topology:

   ```sh
   export GITHUB_WEBHOOK_SECRET='release-soak-secret'
   export GITHUB_BASE_URL='http://127.0.0.1:19797'
   export GITHUB_TOKEN='release-soak-token'
   export GITHUB_INSTALLATION_ID='1'
   export GITHUB_ORG_ID='1'
   export HTTP_ADDR='127.0.0.1:18080'

   ./fake-github -addr=127.0.0.1:19797 \
     -webhook-secret="$GITHUB_WEBHOOK_SECRET" \
     > fake-github.log 2>&1 &
   echo $! > fake-github.pid

   ./ghsyncd serve --roles=all > ghsyncd.log 2>&1 &
   echo $! > ghsyncd.pid

   ./ghsyncd backfill 2>&1 | tee backfill-start.log
   ```

   The verifier below waits for the installation and repository backfill
   cursors, their refresh-generation children, and every cache-producing River
   queue to finish before it establishes the run baseline. A timeout is a
   failed run, never permission to begin against a partially populated cache.

4. Run the full profile and retain its stdout/stderr:

   ```sh
   ./ghsync-soak \
     --profile=48h \
     --duration=48h \
     --recorded-rate=1 \
     --multiplier=10 \
     --events=internal/dispatch/testdata/s142_recorded.json \
     --database-url="$DATABASE_URL" \
     --installation-id="$GITHUB_INSTALLATION_ID" \
     --engine-url=http://127.0.0.1:18080 \
     --fake-github-url=http://127.0.0.1:19797 \
     2>&1 | tee soak.log
   test "${PIPESTATUS[0]}" -eq 0
   ```

5. Capture final machine-readable evidence:

   ```sh
   curl -fsS http://127.0.0.1:18080/metrics > final-metrics.prom
   psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -c \
     "select name, encode(checksum,'hex'), applied_at from schema_migrations order by name" \
     > migration-ledger.txt
   psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -c \
     "select component, operation, success_count, sample_count, last_success_at, last_sample_at from operation_heartbeats order by 1,2" \
     > operation-heartbeats.txt
   psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -c \
     "select status, count(*) from webhook_deliveries group by status order by status" \
     > delivery-status.txt
   ```

## Sign-off evidence

Attach all of these to the release decision:

- artifact and PostgreSQL-version files;
- migration, daemon, fake, and soak logs;
- the final Prometheus exposition and dashboard export for the full window;
- migration ledger, operation heartbeats, and delivery-status query output;
- the soak success line showing expected count, emitted count, required rate,
  achieved rate, and sample count;
- the deployed-system webhook validation evidence referenced by
  `PHASE0-WEBHOOK-VALIDATION.md`.

Any nonzero soak exit, missing file, absent trust metric, alert interval, or
manual restart invalidates the run. Correct the cause and restart the full
profile from an empty disposable database.
