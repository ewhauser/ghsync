# Opus 5 Review — Stream / Derive / Ops / Public Surface

Scope: internal/stream, internal/outbox, internal/derive, pkg/streamclient,
internal/metrics, cmd/stream-tail, cmd/soak, db/CONTRACT.md, ops/, README,
docs consistency. Three claims verified empirically against live Postgres
16.14 (fence blocking semantics, the three-session soft-deadlock cycle, the
metrics-exposition series collision).

## 1. QUALITY (correctness / design)

**Q1 — [major] Deriver acquires the writer fence *after* taking `derivation_dirty` row locks, inverting the lock order every other outbox writer uses** (`derive.go:211` vs `:165-171`). Entity writers order: entity lock → fence (shared) → dirty-row lock. The deriver inverts: dirty-row locks → fence. That closes a cycle with the watermarker: deriver waits on fence behind watermarker's pending exclusive; watermarker waits on a writer's granted shared fence; writer waits on the deriver's granted dirty-row lock. Verified both halves live, then the full three-session cycle: Postgres classifies it a *soft* deadlock and resolves by queue reordering — after a full `deadlock_timeout`. **Measured deriver fence wait: 1.0027s** (deadlock_timeout=1s) — at a 100ms watermark tick, a derivation pass can eat a full second per occurrence. The comment at derive.go:320-322 shows the authors knew writers block on claimed rows — not that the writer holds the fence while blocking. **Fix:** acquire the fence immediately after `Begin`, before the claim (fence outermost in every outbox-writing tx); or claim the dirty set in a separate committed tx.

**Q2 — [major] The watermarker's exclusive fence is an unbounded global write barrier** (`watermark.go:127`, `store.go:11-47`). A pending exclusive blocks new shared requests (verified), so every tick — 10×/s at default — imposes a full write barrier lasting as long as the slowest in-flight writer. No `lock_timeout`, `statement_timeout`, or `idle_in_transaction_session_timeout` anywhere: one stuck writer freezes all entity writes and derivation, not just the watermark. The C-S2 amendment traded "any long tx stalls the watermark" for "any stuck writer stalls all writers" and neither doc acknowledges it. **Fix:** `SET LOCAL lock_timeout='1s'` around the fence acquire (timeout = retryable metered outcome); pool-wide idle-in-transaction timeout.

**Q3 — [major] The fence obligation is convention-only** (`outbox.go:87-94`, CONTRACT.md:225). Every current insert path complies, but `AcquireWriterFence` is a Go helper — any future insert (migration, psql, new package) silently voids C-S2. CONTRACT.md asserts "may not bypass it" with no mechanism. **Fix:** `BEFORE INSERT` trigger on `change_events` raising unless the backend holds the fence (verified key decomposition: classid=1181904750, objid=1953064306).

**Q4 — [major] `FrontierSweepConditionalHitRateLow` fires permanently by construction** (`alerts.yaml:4-18`). The ratio divides `rate()`s and `clamp_min(denominator, 1)` — sweeps issue ~0.017 req/s, so the denominator pins to 1 and the ratio is ~0.017, always <0.80. Fires 15 minutes after deploy, never clears, destroys the C-B4 signal. `alerts_test.go` greps for "0.80" and passes. **Fix:** `increase()`-based counts gated on a non-empty denominator.

**Q5 — [major] `FrontierResyncStorm` pages on any deployment with no registered consumer** (`alerts.yaml:247-255`): `or absent(...)`, severity page, no `for:` — a fresh install pages instantly; the sibling absence alert was deliberately a 5m warning. **Fix:** drop the absent() disjunct or downgrade with `for: 5m`.

**Q6 — [major] `frontier_c_b3_budget_remaining` fabricates a per-class dimension** (`collector.go:371-385`): the same installation-wide value observed 3× under class labels; DASHBOARD.md instructs graphing by class but the lines are identical by construction — C-B3's headroom invariant is unobservable. **Fix:** drop the label or compute real per-class headroom.

**Q7 — [minor]** `FrontierSweepPassMissing`'s 24h threshold covers all five sweep operations including stacks (bound 5min, cadence 3m45s) — dead for 23h59m without paging; the backstop staleness alert is silent for zero-row classes (`COALESCE(max(...),0)`). Split per operation with cadence-derived thresholds.

**Q8 — [minor] No C-P5 liveness signal:** no `('deriver', …)` heartbeat row; `FrontierDeriverBacklog` fires only >500 for 10m — a wedged deriver with 50 dirty scopes is invisible while every other trust loop has a heartbeat. Add the in-tx heartbeat + a PassMissing rule.

**Q9 — [minor]** Every 100ms step rewrites `stream_watermark` + `operation_heartbeats` even when nothing advanced — ~1.7M dead tuples/day on two single-row tables, no autovacuum guidance. Skip no-op updates; throttle the heartbeat; document autovacuum.

**Q10 — [minor]** The snapshot hands the pure deriver tombstoned rows and stack-owned PRs unfiltered (`snapshot.go:45-59`), though the scope-listing query defines both predicates; the obligation on the future deriver is undocumented. Filter to match, or document on `ScopeSnapshot.Data`.

**Q11 — [minor]** `ScopeResult.WorkItems` can never legally hold more than one element (validation requires identity == scope identity, rejects duplicates) — the plural API misleads about C-D2/C-D3 granularity. Make it singular or document "at most one".

**Q12 — [minor] The soak's C-B3 starvation assertion is vacuous** (verified by exposition dump): the family holds a zero-init unlabeled series and the real labeled one; `metricValue(..., nil)` reads `Metric[0]` — the unlabeled zero — so the check compares 0 to 0. SOAK.md advertises it as a pass condition; it cannot fail. **Fix:** sum all series when labels==nil.

**Q13 — [minor]** The soak's achieved-rate check is arithmetically redundant with the count assertion and only fires spuriously where the floor truncates. Drop or compare against `target/duration`.

**Q14 — [nit]** Concurrent first-touch of a new cursor: `ON CONFLICT DO NOTHING` under REPEATABLE READ then `SELECT … FOR UPDATE` can see no row → opaque terminal `ErrNoRows` from `Tail`. Treat as retryable or pre-create the cursor row in its own tx.

**Verified sound:** retention's horizon can only over-advance (conservative direction); `deliverPage`'s single REPEATABLE READ snapshot correctly covers horizon+page; Bootstrap correctly takes no fence; handler effects + cursor advance genuinely atomic.

## 2. TESTING GAPS

**T1 — [major] No fence-protocol regression guard** — the fence tests are genuinely good (real EntityWriter + real deriver via `WithSequenceAllocationHook`, commit and rollback) but nothing fails if a future writer omits the fence. Add the Q3 trigger, or an AST/grep test enumerating insert sites (the deployment-env-var test pattern).

**T2 — [major] The one test reproducing Q1's interleaving hides it:** the mid-pass dirty mark is a bare `pool.Exec` with no fence. Route it through the real fenced writer with a concurrent `Watermarker.Step` and assert the pass completes ~200ms.

**T3 — [major] No watermarker chaos coverage:** no leader-kill failover, no `pg_terminate_backend` while holding the exclusive fence (writers unblock, `safe_seq` never regresses), `ErrLeaseHeld` never asserted. DEPLOYMENT.md commits to "replicas 2; lease elects one" with no test behind it.

**T4 — [major] Listener churn is single-shot:** one kill covered; repeated termination driving backoff to max, persistent pool-exhaustion, and the `listener == nil` poll-only branch never execute.

**T5 — [minor]** Bootstrap/tail overlap untested (snapshot at ≥W may contain state from seq>W — documented, but no convergence test creates an event in the window).

**T6 — [minor]** Concurrent tailers on one (consumer, stream) → untyped 40001; untested, rule undocumented.

**T7 — [minor]** Metrics paths feeding page-severity alerts are unexecuted: name-only assertions; the consumer-cursor query, prunable-depth, sweep/drift label paths never run; inverting the CAS-ratio arguments would fail nothing.

**T8 — [minor]** `alerts_test.go` validates structure not semantics (cannot catch Q4/Q5/Q7). Parse exprs with the PromQL parser; table-drive the ratio/heartbeat rules against synthetic series.

**T9 — [minor] The soak has no change-stream consumer at all:** no streamclient runs; exactly-once, cursor durability, RESYNC-under-load — the properties the library exists for — are never exercised by the long-running oracle. A seq-keyed applied-set consumer with a final distinct-count assertion is cheap and strong.

## 3. CODE QUALITY

**C1 — [major]** `pkg/streamclient` error taxonomy is one type deep (`ErrResyncRequired` only); consumers cannot distinguish retryable from fatal; `Tail`'s error contract unstated. Add a retryable classification and document.

**C2 — [major]** `Snapshot` hands out a raw `pgx.Tx` with no lifecycle helper: forgetting Commit silently loses the cursor reset and leaks a pooled connection; the example shows only the happy path. Add `Commit`/idempotent `Close` or a callback-form Bootstrap.

**C3 — [major]** `Bootstrap` is unconditionally destructive and hides what it destroyed (reads prior cursor only to lock it, discards it). Expose `PriorSeq`; document that it discards every undelivered event below the watermark.

**C4 — [minor]** No server-side filtering in the public API (C-S5 promises stream/scope filters; consumers page-and-discard). Additive `Kinds`/`EntityKeyPrefix` config folded into the page WHERE.

**C5 — [minor]** The public package is bound to pgx v5 and DB co-location; intentional per plan §1 but unstated, while C-S1 advertises "bots, metrics, and other services".

**C6 — [minor]** `internal/metrics` imports `internal/stream` solely for a type name in one observer signature — the only structural observer seam broken; use plain values.

**C7 — [minor]** LISTEN-with-poll-fallback duplicated between derive.Run and streamclient.Tail with asymmetric robustness: only streamclient reconnects; a single terminated backend kills the deriver role. Extract or match robustness.

**C8/C9 — [nits]** `batchLabel` ignores its options and hardcodes 100 vs the 500 cap (no label distinguishes cap-saturated passes); `goto drain` in soak's 1,269-line main.

## 4. MISSING DOCUMENTATION

**D1 — [major] CONTRACT.md omits four consumer-critical facts:** (a) required grants (SELECT + INSERT/UPDATE on consumer_cursors + SELECT on watermark/horizons — no role/GRANT guidance anywhere); (b) the one-tailer-per-(consumer,stream) rule (enforced de facto by FOR UPDATE under RR; violation = opaque serialization error); (c) `occurred_at` is not ordered with `seq` (tx-start clock vs later allocation; retention prunes by occurred_at) — ordering must use seq alone, never stated; (d) direct Postgres is the only v1 transport.

**D2 — [major] The reference example consumer does not demonstrate the resync protocol** — stream-tail just exits 1 on error; no `errors.As`, no re-Bootstrap, no resume — while README/CONTRACT/plan all call it the reference, and the M5 exit criterion was "RESYNC_REQUIRED handling — with an example consumer binary". The single most subtle consumer obligation is the one thing the example omits.

**D3 — [major] `ops/runbooks/resync-storm.md:29-32` documents a data-loss command as the remediation:** running `stream-tail --bootstrap` against a real consumer resets its durable cursor to safe_seq without the consumer re-snapshotting — silently discarding every event between horizon and watermark. State that Bootstrap must be run *by the consumer* in the same tx as its projection replacement; the CLI flag is safe only under a throwaway consumer name.

**D4 — [minor]** Runbook shell blocks not executable as written: multiple long-running foreground `serve` invocations in single copy-paste blocks (webhook-outage lists five).

**D5 — [minor]** budget-exhaustion runbook's first symptom names `FrontierBudgetFloorBreached` — an alert alerts_test.go explicitly asserts must never exist.

**D6 — [minor]** watermark-stalled's lock query filters only `locktype='advisory'` — returns every entity lock with no way to find the fence. Use the verified classid/objid filter + `pg_blocking_pids()`.

**D7 — [minor]** README contradicts itself about the pruner (M4 section: "does not prune change_events"; M5 section and main.go: it does).

**D8 — [minor]** DASHBOARD.md's 304-ratio PromQL has an unbalanced parenthesis and reproduces Q4's broken rate-ratio.

**D9 — [minor]** The deployment config reference is verified one-way only (every env var appears; the reverse and the documented *defaults* are unchecked).

**D10 — [minor]** No Postgres tuning section: autovacuum for the two 10-writes/s single-row tables, lock/idle-in-tx timeouts to bound the fence, change_events growth sizing for the 7-day window.

**D11 — [nit]** IMPLEMENTATION_PLAN §2 shows the module rooted at `server/`; no such directory exists.

## Overall assessment

Unusually disciplined work. The change-stream core is the strongest part: the shared/exclusive fence is a genuinely better C-S2 mechanism than the xmin scheme, and unlike most "we fenced it" claims it is tested against the *real* writer and deriver transactions in both directions. streamclient gets the hard parts right (one-snapshot horizon+page, atomic handler+cursor, conservative horizon, real interleaving tests). CONTRACT.md is schema-tested both directions — rigor most contract docs never reach. The recurring failure mode is not sloppiness but *tests and alerts that assert shape rather than behavior*: threshold greps, name-only metric assertions, a soak check reading an arbitrary series — each passes while the thing it names is broken.

Three items should block: **Q4** (a permanently-red page trains on-call to ignore the board — worse than no alert), **Q1** (measured 1.00s soft-deadlock stall, one-line fix), **D3/D2** together (the documented remediation silently drops a live consumer's events, and the reference consumer never shows the loop that would make the correct fix obvious). **Q2/Q3** deserve a design decision rather than a patch: the fence made the watermark robust by making every writer depend on one unbounded lock with no timeout anywhere, held together by convention. A lock_timeout and a BEFORE INSERT trigger convert both from latent to loud. Everything else is refinement.
