# Frontier Sync Engine — Consolidated Opus 5 Code Review

2026-07-29 · Four Opus 5 reviewers, one per architectural area, each covering
four dimensions: quality (correctness/design), testing gaps, code quality,
missing documentation. Verbatim area reports: [review/](./review/). ~140
findings total; several verified empirically against the live test database
(reproduced deadlock, measured lock stalls, metric-exposition dumps).

## Verdict by dimension

| Dimension | Verdict |
|---|---|
| **Quality** | Core machinery is sound and unusually disciplined — complete advisory-lock coverage, uniformly correct 3-way CAS, single-transaction (write+dirty+outbox) discipline holds everywhere traced, the fence protocol is a genuine improvement over the xmin design. The defects cluster at boundaries the tests never cross: **multi-replica concurrency, time/retention edges, and cross-component lock ordering.** |
| **Testing gaps** | The recurring failure mode, named independently by three reviewers: **tests and alerts assert shape rather than behavior** — string-grepped alert thresholds, name-only metric assertions, a static-fixture order-independence test that proves convergence-to-a-constant, a soak starvation check that reads the wrong series. And **CI never runs `-race`** despite this being concurrency-heavy code that currently passes it. |
| **Code quality** | Good idiom overall; two oversized files (`cache.go` 2.0k lines, `fakegithub.go` 1.7k) with clear split seams; ~250 lines of removable duplication in entity-write prologues; helper triplication (`splitRepo` ×3); a public library (`streamclient`) whose API needs an error taxonomy and a snapshot lifecycle guard before external teams adopt it. |
| **Documentation** | The schema-tested CONTRACT.md is exemplary, but four consumer-critical facts are missing (grants, one-tailer rule, `occurred_at` vs `seq` ordering, transport). Several constraints were amended in code but never in SYNC_ENGINE.md (C-Q1's River-uniqueness protocol, C-P1's real index set). Two runbook defects are dangerous: a documented remediation that silently loses consumer data, and a first-symptom alert name the test suite explicitly forbids from existing. |

## Blocking findings (fix before the deployed-system verification)

1. **Reproduced multi-replica deadlock** — `BumpRefreshIntentGenerations` upserts a multi-key set with no deterministic ordering; two dispatchers with overlapping keys deadlock (reproduced: 60 keys, reversed order, 150 iterations). Compounded by: **any transient DB error in `DispatchBatch` terminates the whole process** — so the prescribed 2-replica topology converts a routine deadlock into serial process crashes. *(ingestion §1.1–1.2)*
2. **Permanent, un-failoverable budget-gate outage** — a failed `SaveBackoff` persist marks the gate unavailable forever but keeps renewing the lease, so no replacement can acquire it and nearly no metric fires. One transient DB error silently ends all GitHub fetching. *(budget §1.1)*
3. **Deriver/watermarker lock-order inversion** — the deriver takes dirty-row locks before the writer fence, closing a soft-deadlock cycle with the watermarker's pending-exclusive; **measured 1.0027s stall per occurrence** (= `deadlock_timeout`) at a 100ms watermark tick. One-line fix (fence first). *(stream §Q1)*
4. **`FrontierSweepConditionalHitRateLow` fires permanently by construction** — `clamp_min` applied to a per-second `rate()` on a ~60/hr request stream pins the denominator to 1; the alert is arithmetically incapable of not firing. A permanently-red page trains on-call to ignore the board. *(stream §Q4)*
5. **C-B4 is structurally unreachable for PRs and checks** — GraphQL gang writes blank stored PR ETags (`etag = EXCLUDED.etag` with empty value) and the checks refresh never sends a conditional at all; additionally a mid-pagination 404 tombstones every previously-live check run. No 304 test exists outside repo-rules — which is why this survived. *(cache §Q1–Q2, §T2)*
6. **No dispatch rules for `pull_request_review*` events** — review approvals/comments have no hint path and converge only via the ≤10-min sweep, silently blowing the 20s C-Q2 SLO on the most user-visible interaction. Silent because unmatched events are "successful no-ops" with no counter. *(ingestion §1.4)*
7. **Drift detector effectively disabled in production** — its quiescence gate treats any non-terminal job as busy, and C-R1 sweeps enqueue work every few minutes, so `Detect` almost never samples on a live installation; the bail path records no heartbeat. C-O3's "drift is measured" is currently aspirational. *(cache §Q4)*
8. **Retention destroys parked-delivery replayability** — the 90-day payload pruner has no status filter, so parked/pending deliveries lose `raw_body`; a later requeue re-parks them with a misleading JSON error. C-I5's "replayable" quietly expires. *(ingestion §1.3)*
9. **The resync runbook documents a data-loss command** — running `stream-tail --bootstrap` against a real consumer resets its durable cursor without the consumer re-snapshotting, silently discarding undelivered events; the "reference consumer" never demonstrates the correct resync loop it exists to demonstrate. *(stream §D2–D3)*

## High-value systemic fixes (one change, many findings)

- **Add `-race` to CI** for at least budget/gh/fakegithub/dispatch/stream — it currently passes and would have caught the fake-GitHub fixture data race the budget reviewer found. *(budget §T1–T2)*
- **Enforce the fence in the database** — a `BEFORE INSERT` trigger on `change_events` requiring the advisory fence converts the protocol's central obligation from code-review convention to mechanism; pair with `lock_timeout` + `idle_in_transaction_session_timeout` so the exclusive fence is a bounded barrier, not an unbounded one. *(stream §Q2–Q3)*
- **Make the C-I4 test mutate its fixture mid-replay** — the one change that turns the flagship property test from convergence-to-a-constant into a real order-independence proof exercising the CAS discard path. *(cache §T1)*
- **Concurrency tests matching the deployed topology**: two dispatchers over a shared delivery set; N goroutines racing lease acquisition at expiry; watermarker leader-kill failover; concurrent tailers. Every reproduced defect above lives on one of these untested paths. *(all areas)*
- **Semantic alert tests** — parse `expr` with the PromQL parser and evaluate the ratio/heartbeat rules against synthetic series; string-grep tests passed while three alert rules were broken. *(stream §T8)*

## What is genuinely strong (unanimous across reviewers)

Complete advisory-lock and fence coverage on every traced write path; the
three-way CAS (newer / equal-but-distinct / tombstone-resurrection) applied
uniformly; C-C3 single-transaction discipline including River follow-ups; the
recorded-replay and rebase-storm tests asserting exact rows and exact
`scheduled_at`; the fence tested against *real* writer transactions in both
commit and rollback directions; CONTRACT.md schema-tested in both directions
against live `pg_attribute`; the AST test that mechanically forces every env
var into the deployment doc — called out twice as "worth copying elsewhere."

## Suggested sequencing

1. Blocking items 1–4 (two are one-line-class; all four are outage-class).
2. `-race` in CI + the DB fence trigger + timeouts (systemic hardening).
3. Blocking 5–9 with their missing tests written first (T2/T1 pattern:
   the test that would have caught it lands with the fix).
4. Doc pass: amend C-Q1/C-P1 in SYNC_ENGINE.md, CONTRACT.md consumer facts,
   runbook corrections (D3 especially), config-knob tables.
5. Code-quality items (cache.go split only after its tests move in-package).

Full findings with file:line anchors and fix guidance:
[review/opus-review-ingestion.md](review/opus-review-ingestion.md) ·
[review/opus-review-budget.md](review/opus-review-budget.md) ·
[review/opus-review-cache.md](review/opus-review-cache.md) ·
[review/opus-review-stream-ops.md](review/opus-review-stream-ops.md)
