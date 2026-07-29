# Opus 5 Review — Cache / Fetch / Reconciliation

Scope: internal/store, internal/fetch, internal/sweep, internal/drift,
db/migrations (all), db/queries (cache/sweep/drift). All in-scope suites run
green against the test database (store 0.3s, drift 2.0s, db 0.6s, fetch
20.7s, sweep 6.3s).

## 1. QUALITY (correctness / design)

**Q1 — major — `internal/fetch/convert.go:137-164` + `db/queries/cache.sql:179`.** `pullRecordFromNode` never sets `ETag`, and the upsert's `DO UPDATE` sets `etag = EXCLUDED.etag` unconditionally, so every accepted GraphQL-gang write blanks the PR's stored REST ETag; `refreshPRREST` then sends `If-None-Match: ""` forever. Same via `pullRecordsFromList` (backfill passes ""). C-B4's "304s must be the common case" is structurally unreachable for PRs. **Fix:** guard the etag assignment as `TouchPullRequestCheckedAt` already does; carry `response.ETag` into GraphQL-derived records.

**Q2 — major — `internal/fetch/handler.go:544-573`.** `RefreshChecks` passes "" as the conditional ETag on every page despite storing page-1's ETag — checks are never conditional; worse, a 404 on page N does `all = nil; break`, discarding pages 1..N-1, after which `ReplaceCheckRuns` tombstones every previously-live run for that SHA. **Fix:** send the stored ETag; return an error on mid-pagination 404.

**Q3 — major — `internal/fetch/backfill.go:287-363`.** The PR backfill phase ignores `args.Page`, re-paginates the whole open-PR set inside one job execution, repeats until `added == 0`, accumulating all PRs in an in-memory `seen` map. Crash mid-phase restarts from zero; memory unbounded — violating C-O5/C-R2 for the largest phase (the stacks phase does it correctly). **Fix:** one page per job via `backfill_cursors.page`; overlap-pass counter in the cursor row like `sweep_cursors.pass_new_count`.

**Q4 — major — `internal/drift/drift.go:242-288`.** The quiescence gate treats any non-terminal job in interactive|event|sweep|reconcile as busy, but C-R1 kickoffs enqueue refreshes for essentially every live entity every 3.75–7.5 min — so on a live installation `Detect` almost never samples; C-O3 is effectively disabled in production. The bail path skips `RecordSuccessN`, so the only symptom is an absent-heartbeat alarm. **Fix:** scope the gate to sampled entities (no outstanding generation for those keys) or gate on queue *age*.

**Q5 — major — `internal/fetch/coordinator.go:148-151, 289` + `internal/store/cache.go:1711-1724`.** The gang passes `batch.items[0].ctx` for all ≤25 entities, so `MarkCacheCommitted` stamps item 0's event state only: other webhooks report zero event→cache latency; item 0's is stamped up to 25×. C-Q2's SLO histogram measures ~1/25 of gang-served events. **Fix:** per-item ctx through `store.PullRequestApply`.

**Q6 — major — `internal/fetch/coordinator.go:224-232`.** Any transport failure — including a per-node review-thread completion error — fails all 25 entities and discards collected per-item results; the "one poisoned entity never discards healthy siblings" doc holds only for the write stage. **Fix:** isolate thread completion per node; preserve collected results on whole-call failure.

**Q7 — minor —** `ApplyChecksObserved` resolves dirty scopes via `ListPRScopesByHeadSHA(repo_full_name)` while everything else keys on `gh_id`; a stale/empty FullName silently yields zero scopes and `checks.changed` with nothing dirtied — a C-C3 half-effect failing open. **Fix:** key on `repos.gh_id`.

**Q8 — minor — redundant indexes on hot write paths:** `check_runs_repo_head_sha_idx` (0007:146) subsumed by the UNIQUE at :143; 0022's `change_events_prunable_idx` duplicates 0013's on the outbox insert path. Drop both forward.

**Q9 — minor —** the C-R1 staleness indexes (0010:72,76) lead with `repo_id` but every `ListStale*` filters on `installation_id` and sorts `last_checked_at` across repos — neither index serves filter or order. **Fix:** partial `(state, last_checked_at) WHERE tombstoned_at IS NULL`-style indexes or denormalize installation_id.

**Q10 — minor —** both retention batches order by prune key backed only by BRIN (cannot provide ordering) — each "bounded batch" top-N sorts the whole eligible range, contradicting the migration comments. **Fix:** btree on the prune key or moving-cutoff batches.

**Q11 — minor —** drift sampling `ORDER BY (source_id <= $after), source_id` over the `drift_entities` view fully materializes and sorts per kind, plus once more per sample via `GetCachedEntitySnapshot`. **Fix:** two indexed range queries.

**Q12 — minor —** `inspectSample` holds the entity's C-C1 session lock across the full network fetch (paginated checks behind the sweep budget class), blocking real refreshes for a read-only comparison. **Fix:** fetch first, lock, re-read, discard if `last_checked_at` moved.

**Q13 — minor —** `sweep_cursors.seen_keys` accumulates the full entity set rewritten as one JSONB array per page commit — O(pages × entities) write volume against C-P3's intent. **Fix:** child table + anti-join.

**Q14 — minor —** `classAndSource` maps interactive→`SyncSourceBackfill` and `SyncSourceManual` is never produced — C-C5's four-value enum is half real. Document or add an interactive source.

**Q15 — nit —** `stackFollowupSpecs` iterates a Go map, so follow-up enqueue order is nondeterministic — precisely what C-I4 tests should be sensitive to.

**Q16 — nit —** sweep cursors keyed by `repos.full_name` are never reaped on rename/tombstone → permanently-incomplete cursors reporting overrun forever. Key by `gh_id` or reap.

**Verified sound:** advisory-lock coverage complete (every write path locks + fences before mutating; `Touch*` correctly skip the fence); lock ordering consistent, no cycle found; pagination-skip defense genuinely conservative (skip → verification fetch; only confirmed 404 tombstones).

## 2. TESTING GAPS

**T1 — major —** `TestOrderIndependenceFinalCacheState` replays 3 events in 4 seeded shuffles against a **static** fixture: every permutation converges to the fixture by construction; the test cannot fail for ordering reasons and never exercises C-C2's stale-write discard — the mechanism C-I4 rests on. **Fix:** mutate the fixture between fetches, enumerate permutations of a longer stream, run deliveries concurrently.

**T2 — major — no ETag/304 coverage for PRs or checks** (all three hits are on the repo/rules path) — exactly why Q1/Q2 survive. **Fix:** C-B4 conformance test asserting stored etags non-empty and immediate re-refresh 304s.

**T3 — major — no partial-batch-failure test** (both existing tests inject *writer* errors; nothing scripts GraphQL transport/per-node failure — the path failing all 25).

**T4 — major — no mid-scan crash test for backfill** (phase transitions and concurrent mutation covered; interruption inside the unbounded rescan loop is not — Q3 invisible).

**T5 — minor —** `internal/store/cache_test.go` is 43 lines; all coverage of the 2,000-line writer lives in another package's DB-gated suite. Move write-race/tombstone/equal-timestamp tests in-package.

**T6 — minor —** 15 skip sites on `TEST_DATABASE_URL`; a DB-less `go test ./...` is a green run exercising almost nothing here. Gate on `-short`/build tag or assert in CI that DB suites ran.

**T7 — minor —** the write-race test is sequential (both orders, one goroutine). Make it genuinely concurrent.

**T8 — minor —** the cross-language key grammar is untested: nothing asserts SQL-side scope-key concatenation and the `drift_entities.lock_key` expressions agree with `outbox.StackKey`/`PullRequestKey` — silent divergence breaks C-C1 for drift and C-D2 for dirty marking. Extend the constructor test to compare against SQL-selected values.

**T9 — nit —** `waitIdle` polls `river_job` filtered by `args->>'key' LIKE '%:repo:%'` — keyless jobs invisible; can declare quiescence early; 20s bound is a future CI flake.

## 3. CODE QUALITY

**C1 — major —** `cache.go` (1,999 lines) mixes six concerns with clean seams: split into records/observation/per-entity-family files — pure file move.

**C2 — major —** five apply paths repeat the same 20-line prologue (begin → clock → repo → upsert → touch → markAndEmit → hook → commit) at :700, :1043, :1309, :1428 + three tombstone repeats. A `withEntityTx` helper removes ~250 lines and makes forgetting the fence structurally impossible.

**C3 — minor —** the `*Observed` suffix is a lie: all five route through `beginEntityTx` which silently falls back to a transaction-scoped lock when `observation == nil`. Require it (like the `Touch*` family) or drop the suffix.

**C4 — minor —** inconsistent error wrapping within one file (applyPullRequest wraps everything; TouchStack/TouchRepoRules return four bare errors).

**C5 — minor —** `sweep.go` (1,341 lines) carries scaffolding + state machine + enqueue helpers + ~100 lines of Observer fan-out boilerplate duplicated nearly verbatim in drift. Extract the fan-out; split the state machine.

**C6 — minor — helper triplication:** `splitRepo` ×3, `isNotFound` ×2, pgtype timestamp helper ×3 — exactly the helpers that must not diverge.

**C7 — minor —** `cache.sql` (725 lines) should split the C-C3 seam queries into `derivation.sql` so the atomicity contract is legible.

**C8/C9 — nits:** `_ = result` discard; per-sample `NewEntityWriter` construction in drift's hot loop.

## 4. MISSING DOCUMENTATION

**D1 — major — three entity-key grammars coexist; one is documented.** CONTRACT.md defines `{kind}:{installation_id}:{repo_gh_id}:{number}`; refresh/job keys use `{kind}:{owner}/{name}:{number}`; drift uses a third (`repo:{full_name}:metadata` etc. to satisfy `parseEntityKey`); `RepositoryDiscoveryKey` mints into the same advisory-lock namespace. Nothing states which is authoritative where or that the lock space is shared. **Fix:** a "private key grammars" section naming all three and their producers.

**D2 — major —** C-P4 gang parameters neither configurable nor documented: `BatchWindow`/`BackfillPageSize` default silently (5ms/100), K is hardcoded 25 though SYNC_ENGINE calls it a default; DEPLOYMENT.md documents every other tunable. Add and wire, or state they are fixed.

**D3 — minor —** `drift.Config.PageSize` silently aliased to `SWEEP_PAGE_SIZE` — an operator tuning sweeps also reshapes drift.

**D4 — minor —** heartbeat coverage partial and undocumented: repo_rules/closed_tracked sweeps, gap healing, retention record nothing, against 0021's "no series means green is impossible" rationale.

**D5 — minor —** migration comments contradict the shipped index set ("exactly one index" ×2 vs four today); C-P1 amended once, not again; 0022's predicate supersets 0005's (one droppable).

**D6 — minor —** the 30-day display window is hardcoded six times, undocumented as a constant, no knob, unstated coupling to `SWEEP_CLOSED_MAX_STALENESS`, and silently overrides C-R1's "closed ≤ 24h" reading.

**D7 — nit —** 0016's operator note says "before deploying 0016" from inside 0016 (off by one).

**D8 — nit —** undocumented exported symbols on the public seam: `SyncSource` + constants, the six normative key constructors, sweep kind constants, all `sweep.Config`/`drift.Config` fields — conspicuous because `EntityWriter`/`Observation`/`TransactionHook` are documented in the same file.

## Overall assessment

The core cache-integrity machinery is genuinely well built and, in the areas the design doc names load-bearing, correct: complete lock+fence coverage, the strongest CAS structure in the codebase (three-way newer/equal-but-distinct/resurrection applied uniformly), C-C3 holding everywhere traced including River follow-ups, correct and defensively-validated sweep staleness math, and a properly conservative disappearance path.

The weakness is at the edges tests don't reach, and the pattern is consistent: **the constraints with no test have no implementation.** C-B4 is the clearest case (Q1/Q2 + T2). C-O3 is disabled in practice (Q4); C-O5's resumability absent where it matters most (Q3); C-Q2's histogram measures 1/25 of gang events (Q5). The order-independence test is the emblematic gap — carefully constructed yet replaying against a fixture that never changes, validating convergence-to-a-constant rather than order-independence. Recommended sequence: fix Q1/Q2 with the T2 test that would have caught them; make the fixture mutate (T1); then split cache.go (C1/C2) only after its tests live in-package (T5).
