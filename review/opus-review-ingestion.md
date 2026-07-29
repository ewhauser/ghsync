# Opus 5 Review — Ingestion / Dispatch / Queue

Scope: internal/ingress, internal/dispatch, internal/queue, internal/config,
cmd/frontier-syncd, config/dispatcher-rules.yaml, ingestion-related db/.

## 1. QUALITY (correctness / design)

**CRITICAL — `db/queries/refresh_intent_generations.sql:4-17` (callers: `internal/dispatch/dispatcher.go:162`, `internal/queue/refresh.go:719`)**
`BumpRefreshIntentGenerations` upserts a multi-key set with no deterministic lock-acquisition order (`SELECT DISTINCT` with no `ORDER BY`; a HashAggregate is free to reorder), so two concurrent transactions that touch an overlapping key set in different orders deadlock. Reproduced against the review database: 60 keys, two sessions with reversed array order, 150 iterations → `ERROR: deadlock detected ... while inserting index tuple in relation "refresh_intent_generations"`. This is not theoretical — `ops/DEPLOYMENT.md:13` prescribes **2+ dispatch replicas**, and the real transaction holds these row locks across River's `InsertManyTx` and `SetWebhookDeliveryResults`, widening the window far beyond the probe. The same query is the fan-out path for fetch/sweep/drift (`internal/queue/refresh.go:719`), so dispatcher↔fetcher deadlocks are also reachable.
*Fix:* add `ORDER BY kind, refresh_key` to the `intents` CTE (and sort `deduped`/`intents` in Go before encoding, so the intent is explicit at both layers). C-P4 already establishes "locks taken in sorted key order" for entity advisory locks — the same rule was never applied to this table.

**MAJOR — `internal/dispatch/dispatcher.go:76-81` with `cmd/frontier-syncd/main.go:662-666, 703-707`**
Any error from `DispatchBatch` — including the transient Postgres deadlock above, a serialization failure, or a connection blip — propagates out of `Run`, into `serviceErrors`, and terminates the entire `frontier-syncd` process. No retry or backoff for transient failures, so a single 40007 in one replica takes down ingress, deriver, and watermarker in that process too.
*Fix:* classify retryable pgerrcodes (`40001`, `40P01`, connection errors) inside `Run`, log and back off (reuse `PollInterval` with jitter), reserve process exit for genuinely unrecoverable errors.

**MAJOR — `db/queries/sweep.sql:253-258`**
`PruneWebhookDeliveryPayloadBatch` nulls `raw_body` for every delivery older than the cutoff with **no status filter**, silently destroying the payloads of `parked` and `pending` deliveries. C-I5 states parked deliveries are "replayable"; after 90 days they are not, and nothing detects it: `requeue` reports success, the dispatcher claims the row, `Classify(event, nil)` fails on `json.Unmarshal` of a nil body, and the delivery re-parks with a misleading "unexpected end of JSON input".
*Fix:* add `AND candidate.status = 'processed'` to the batch predicate, and have `RequeueParkedWebhookDeliveries` exclude `payload_pruned_at IS NOT NULL`.

**MAJOR — `internal/dispatch/classify.go:249-257, 298-304, 350-357`**
A single malformed sub-field aborts classification of the *whole* delivery, discarding every intent it would have produced. A payload carrying `"stack": {}` (or any future preview shape where `number` moves/renames) makes `payloadStack` return non-nil, escalates to `TargetStack`, then hard-errors on `missing stack.number` — so the PR refresh and `resolve_stack_membership` intents are lost too, and the delivery parks after `MaxAttempts`. SYNC_ENGINE §2.1 names preview instability as a standing risk whose mitigation is "the sweep floor"; this code instead converts a preview schema drift into *every PR webhook parking*.
*Fix:* treat a failed stack escalation as `emit=false` (fall back to the unescalated `Target`), and make per-rule key-extraction errors skip that rule with a counted metric.

**MAJOR — `internal/dispatch/classify.go:56-78` and `config/dispatcher-rules.yaml`**
No rule for `pull_request_review`, `pull_request_review_comment`, or `pull_request_review_thread` — those strings appear nowhere in the repo. But `review_decision` and `review_threads` are first-class cached entities (`db/migrations/0007_mirror_cache.sql:75,102`), and reviews do not emit a `pull_request` event. A reviewer approving a PR has **no hint path at all** and converges only via the ≤10-min PR sweep (C-R1), blowing C-Q2's p95 ≤ 20s SLO for the most user-visible interaction in the product. Silent: unclassified events are "successful no-ops" (`classify.go:233`), no metric fires.
*Fix:* add `pull_request_review*` → `TargetPullRequest` rules to `DefaultRules()` and the YAML; emit a counter for events matching zero rules.

**MINOR — `internal/dispatch/dispatcher.go:127-139` with `db/queries/webhook_deliveries.sql:26-28`**
A classification failure writes the row back to `pending` in the same transaction and `Run` immediately re-loops (`count > 0`), so a poison delivery burns `MaxAttempts` back-to-back within milliseconds — C-I5's attempt budget consumed with zero backoff.
*Fix:* add `next_attempt_at` and exclude not-yet-due rows from `ClaimWebhookDeliveries`, or only `continue` when the batch made forward progress.

**MINOR — `cmd/frontier-syncd/main.go:436-446` and `749-759`**
`githubGate.Close` invoked twice on normal shutdown (explicit + deferred); harmless via `closeOnce` but the defer discards its error. *Fix:* drop the gate close from the defer.

**NIT — `cmd/frontier-syncd/main.go:310` vs `720-722`**
Start gate `worksJobs := roles[roleFetch] || roles[rolePruner]` vs shutdown gate additionally including `roleSweep`/`roleDrift`; can only diverge if `parseRoles` coupling is relaxed, then `Stop` is called on a never-started client. *Fix:* compute once, use in both places.

## 2. TESTING GAPS

DB-backed tests in scope skip without `TEST_DATABASE_URL`: `internal/dispatch/dispatcher_db_test.go` (4 tests, schema-isolated) and `internal/queue/queue_test.go` (3 tests, **not** schema-isolated). Credit: `TestFullRecordedReplayIngressToRiver` is a genuinely strong C-I4/C-I3 test; `TestRebaseStormEscalatesStackBranchesWithoutSlidingDebounce` is a real C-Q1/C-Q2 test with an exact `scheduled_at` assertion.

**MAJOR — no concurrency test anywhere in `internal/dispatch`.** Every DB test drives `DispatchBatch` sequentially from one goroutine; C-O2 claims N dispatchers safe and DEPLOYMENT.md deploys 2+ — which is why the CRITICAL deadlock survived. *Fix:* two `Dispatcher`s over a shared delivery set from separate goroutines; assert no error, every delivery `processed` exactly once, one River job per key.

**MAJOR — `Dispatcher.Run` entirely untested** (poll interval, idle, `ctx.Done`, error-propagation semantics that kill the process).

**MINOR — `internal/config/config_test.go:179-216`:** `clearConfigEnv` omits every M5 knob, so ambient env values leak into assertions; the two cross-field validations (`WatermarkRefresh < LeaseTTL/2`, `STREAM_RETENTION_AGE` 7-day floor) are untested.

**MINOR — `internal/queue/queue_test.go` runs against the shared public schema** while dispatch/fetch/sweep/drift isolate; `TestExplicitQueueSelectionDoesNotPollUnownedQueues` becomes flaky the moment another non-isolated package starts a River client. *Fix:* reuse the isolation pattern.

**MINOR — pruned-payload path untested** (`raw_body IS NULL` reaching the dispatcher) — why the retention MAJOR is invisible.

**NIT —** `internal/dispatch/dispatcher_test.go:12-29` covers only the debounce cap; the other five panic conditions in `New` unasserted.

## 3. CODE QUALITY

- **MINOR — dead code:** `drainDispatcher` (dispatcher_db_test.go:570) uncalled; `GetRefreshIntentGeneration` generated but never called (superseded by `GetRefreshIntentState`); `Handler.Mux()` used only by tests with `/healthz` duplicated in `serviceMux`.
- **MINOR —** `NoopArgs`/`noopWorker` registered on every production client; doc says "M2+ replaces it" — M6 complete. Move to test-only registrar.
- **MINOR — duplicated constant:** 15s C-Q2 cap defined as `dispatch.MaxDebounce` and `config.maxDispatchDebounce`.
- **MINOR — dead parameter:** `Observer.DispatchBatch(ctx, int, int)` parked count discarded by the only production implementation.
- **NIT:** `case TargetPullRequest: fallthrough` → combine cases; `New` panics while every peer constructor returns errors; `New` silently substitutes `DefaultClassifier()` for an empty rule set.

## 4. MISSING DOCUMENTATION

**MAJOR — `docs/SYNC_ENGINE.md:170-179` (C-Q1) contradicts the shipped implementation.** C-Q1 says uniqueness must exclude running; `NewRefreshInsertOptsForQueue` includes `JobStateRunning` (River requires it), satisfied instead by the durable `refresh_intent_generations` protocol. C-S2 got an amendment paragraph; C-Q1 got nothing — the rationale lives only in a migration comment. *Fix:* amend C-Q1 like C-S2; document the full generation protocol as a package comment.

**MINOR — two doc comments attached to the wrong symbol** (`queue.go:40-41` on `clientOptions` instead of `NewClient` — also stale "three queues"; `refresh.go:625-631` on `RefreshGeneration` instead of `InsertRefreshesTx`).

**MINOR — undocumented exported surface** in `internal/queue` (Kind consts, Args types/constructors, RefreshRequest/Spec/Generation, observers, insert-opts helpers) and `internal/config` (`Config`, `FromEnv`, `RequireFetchCredentials`).

**MINOR — the six River queues undocumented:** docs describe three priority classes; queue.go defines six (`reconcile`, `drift`, `pruner` are component queues); worker defaults and role→queue mapping in neither SYNC_ENGINE.md nor DEPLOYMENT.md.

**MINOR — `0001_webhook_deliveries.sql:1-3` misleading:** "exactly one index" is false (four now: PK, 0005 partial, 0011 BRIN, 0022 partial superset). C-P1 amended only for 0005. Evaluate dropping the 0005 index; amend C-P1.

**NIT —** `dispatcher-rules.yaml` push rule ships `stacked_target: stack` but real push payloads carry no stack object (synthetic-fixture only) — comment as pending Phase-0 or drop. `NewHandler` panics on bad sizes but silently accepts empty webhook secret.

## Overall assessment

Well-built and unusually disciplined about traceability; the ingress path really is minimal verify/insert/ack; the dispatcher's one-transaction batch delivers C-P2; the durable refresh-generation mechanism is a legitimately clever answer to River's uniqueness constraints. The recorded-replay and rebase-storm tests are among the better constraint tests seen — exact rows, exact `scheduled_at`, golden walled off from the classifier. The `TestDeploymentReferenceContainsEveryEnvironmentVariable` AST test is worth copying elsewhere.

The concentration of real defects is at the boundaries the tests never cross: concurrency and time. The `BumpRefreshIntentGenerations` deadlock is the headline — reachable in the prescribed 2+-replica topology, escalating to process termination because no loop distinguishes transient from fatal, unnoticed because every dispatch test is single-goroutine. The pruner silently voiding C-I5 replayability and the absent `pull_request_review` hints are both constraint-satisfied-on-tested-paths, violated-on-untested-paths. Fix lock ordering, add the two-dispatcher test, filter the pruner, add review-event rules; the rest is documentation drift from amending constraints in migration comments rather than SYNC_ENGINE.md.
