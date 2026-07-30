# Conformance and load testing

This replaces the deleted `ops/SOAK.md` (synthetic soak verifier) and
`ops/PHASE0-WEBHOOK-VALIDATION.md` (real-repo delivery recording). Two
tracks:

- **Track 1 — payload conformance:** every payload shape GitHub can send for
  the event families we consume parses, classifies, dispatches, and projects
  correctly.
- **Track 2 — recorded-repository load replay:** a real repository's history
  is crawled once, compiled into a truth-plus-webhook replay, and driven
  through fake GitHub at compressed time under strict end-to-end assertions.

## Design constraints

1. **Truth must move in lockstep with webhooks.** The engine's oracle is
   convergence: the cache must equal fake-GitHub truth and drift must find
   nothing. Replaying webhook payloads alone (from any dataset) is
   structurally useless — the fake's REST/GraphQL responses would contradict
   the stream and drift would correctly fail the run. Every traffic source
   below is therefore a sequence of *truth mutations* from which deliveries
   are derived, never bare payloads.
2. **Public datasets cannot cover checks.** GH Archive / the public Events
   API carry no `check_run`, `check_suite`, or review-thread events, and the
   checks path (C-Q2, `check_history`) is core. Payload-shape coverage comes
   from the octokit corpus (Track 1); realistic check sequences come from
   crawling a real repository's commits (Track 2).
3. **Reuse what exists:** the fakegithub fixture and its
   `ControlEmitPath`/`ControlTruthPath` control surface, the dispatch replay
   harness (`internal/dispatch/testdata/s142_*`), drift-as-oracle, and the
   thresholds in `ops/alerts.yaml`.

## Track 1: payload conformance

Consumed event families (from `config/dispatcher-rules.yaml`):
`pull_request`, `pull_request_review`, `pull_request_review_comment`,
`pull_request_review_thread`, `check_run`, `check_suite`, `push`.

### T1.1 Vendored corpus

- Source: [octokit/webhooks](https://github.com/octokit/webhooks)
  `payload-examples/api.github.com/<event>/` plus the JSON schemas (MIT,
  maintained by GitHub).
- `scripts/update-webhook-corpus.sh` downloads only the seven families at a
  pinned tag into `internal/conformance/corpus/<event>/*.json` and writes
  `internal/conformance/corpus/VERSION`. The corpus is committed; updating
  it is rerunning the script and reviewing the diff. Tests never touch the
  network.

### T1.2 Ingress conformance

For every corpus payload: sign with the test secret, POST through the real
ingress handler, assert acceptance and persistence; redeliver and assert GUID
dedupe. Malformed variants (truncated body, bad signature, wrong content
type) assert rejection without a stored delivery.

### T1.3 Dispatch conformance

Classify every corpus payload through both `DefaultRules()` and the shipped
`config/dispatcher-rules.yaml`. Expected outcomes (targets or deliberate
no-op) live in a golden file keyed by corpus filename, regenerated with
`-update`. Hard invariant: no corpus payload may park or panic — unknown
actions must classify to a rule or no-op explicitly.

### T1.4 Projection conformance

Feed the `pull_request`/`check_*` corpus payloads through the fetch/store
projection paths that consume webhook-shaped data. Asserts full-fidelity
payloads (all optional fields present, nulls, long strings, unexpected extra
fields) survive JSON decoding and sqlc row types. This is where "GitHub adds
a field / sends null where we assumed string" breaks loudly.

### T1.5 fake-github fidelity

Compile the octokit JSON schemas in a test and validate every payload
fake-github emits against them. Upgrade the fake's payload constructors until
green. After this, Track 2 traffic is production-shaped, so load runs also
exercise real parsing — today the fake emits minimal skeletons.

**Acceptance:** `go test ./internal/conformance/...` is corpus-complete,
offline, and in CI; the fake's emitted payloads schema-validate.

## Track 2: recorded-repository load replay

### T2.1 `cmd/ghrecord` — one-time crawler

- `ghrecord --repo owner/name --since 2026-01-01 --until 2026-07-01
  --token $GITHUB_TOKEN --out recording.ndjson`
- GraphQL crawl per PR: `timelineItems` (commits, reviews, review threads
  and their comments, base-ref changes, head force-pushes, close / merge /
  reopen), plus per-commit `checkSuites`/`checkRuns`. Branch pushes are
  reconstructed from PR head updates and default-branch history.
- **Stacks:** derived from base-ref chains (a PR whose base branch is another
  PR's head branch), which captures real stacked-PR usage; a
  `--synthesize-stacks=<percent>` knob threads additional PRs into stacks
  for repos that don't stack.
- Output is a **recording**: ordered logical events with relative
  timestamps and entity state (`{seq, at_ms, kind, ...}`) — not webhook
  payloads. Resumable via a cursor file so rate limits only pause the crawl.
- Committed artifact: one small recording (~2k events, a busy week of a
  mid-size repo) for CI. Large recordings are regenerated on demand, not
  committed.

### T2.2 Compiler — recording → replay steps

Deterministic transform of a recording into ordered steps, where each step
is a fixture-truth mutation plus its derived signed deliveries with payloads
built to the octokit schemas from recorded fields. Inter-arrival gaps are
preserved and compressed by `--speed`. `--copies=N` replays N entity-
renumbered copies in parallel ID spaces to scale width without inventing
event shapes, and `--loop` renumbers per lap for unbounded duration.

### T2.3 `cmd/loadgen` — the strict verifier (replaces `cmd/soak`)

Drives the fake's control surface with compiled steps. The fake applies each
truth mutation to its fixture itself — generalizing today's title-only
`applySoakMutation`/`SoakTruth` into full fixture mutations (rename the
`Soak*` identifiers to `Truth*` when this lands).

Keeps every strict assertion the soak had, and widens the final oracle:

- exact configured delivery count before the deadline; achieved rate ≥
  target;
- zero starvation increments, parked deliveries, or open drift findings;
- webhook deliveries, event queue, refresh generations, and watermark all
  drain;
- post-population drift pass and durable watermark passes complete;
- run-scoped C-Q2 p95/p99 within 20s/60s;
- **full-record convergence:** final fake fixture snapshot equals the cache
  for PRs, stacks, checks, and review threads — not just PR titles.

Chaos knobs, all flag-gated and off by default: duplicate deliveries,
bounded reordering, dropped deliveries (sweep must heal within its C-R1
bound), fake 500/429 bursts (budget backoff must engage, no starvation), and
engine SIGKILL/restart mid-run (resume must not break any assertion).

### T2.4 CI and release wiring

- CI job `load-smoke` (successor to the deleted `soak-smoke`): committed
  small recording, high `--speed`, full assertions, ~2–3 minute budget,
  fresh Postgres, `--roles=all`, backfill-gated start.
- Release load run: large recording (e.g., 30 days of a high-traffic repo,
  ideally one with real stacked PRs), multi-hour wall time via `--speed`,
  chaos knobs on, `--copies` sized to hold ≥10x the recorded rate. Evidence
  to attach: loadgen success line, final `/metrics` exposition, delivery
  status and heartbeat queries, alert-rule evaluation over the window —
  same spirit as the old soak sign-off list.

### T2.5 (optional, later) arrival shaping

Fit diurnal/burst arrival statistics from GH Archive (BigQuery) and apply
them as a gap-warping model over recordings. Out of scope until T2.1–T2.4
are green; replaying real recorded gaps is already representative.

## Milestones

| Milestone | Delivers | Done when |
|---|---|---|
| M-A | T1.1–T1.3 corpus, ingress + dispatch conformance | conformance tests corpus-complete in CI |
| M-B | T1.4–T1.5 projection conformance, schema-valid fake | fake payloads schema-validate; projection tests green |
| M-C | T2.1–T2.2 ghrecord, compiler, committed smoke recording | recording reproducibly compiled; replay smoke passes locally |
| M-D | T2.3–T2.4 loadgen, chaos knobs, CI `load-smoke`, release procedure | CI green with `load-smoke`; one full release-scale run passes |

Each milestone lands green in CI before the next starts. M-A/M-B are
independent of M-C/M-D and can proceed in parallel.
