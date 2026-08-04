# ghsync Sync Engine — Go + Postgres Design

Draft v0.2. Decisions locked in this revision: **single org** (single-tenant
deployment; one installation), **github.com only** (public GitHub by
default; GHEC works identically — no GHES support), **90-day
retention** for raw webhook payloads and check history, **River**
(`riverqueue/river`) as the job queue, and **sqlc + pgx** as the data layer.

The sync engine keeps the local cache (the derivation engine's
input) correct against GitHub with bounded staleness and bounded API spend.
This document is organized around **constraints** — invariants the
implementation must satisfy — rather than library choices. Libraries are
replaceable (§8); the constraints are the contract, and every one of them
should be enforceable by a test (§9).

Stateful dependencies: **Postgres only**. No Redis, no Kafka, no external
queue. Every coordination primitive the engine needs (queues, locks, outbox,
budget state) is expressible in Postgres at this scale, and one stateful
system means one backup/restore/failover story.

---

## 1. Scope

In scope: webhook ingestion, authoritative refetch, cache writes, burst
coalescing, rate-budget management, reconciliation sweeps, derivation
triggering, and the versioned Postgres change feed that a future
`WatchWorkItems` API can consume.

Non-goals: the derivation engine's classification rules (separate pure
package), the git worker (dry-runs/rebases), mutation execution
(PreviewService), the gRPC/SSE API in `API_SPEC.md`, and the agent-runner
integration. Direct Postgres is the only v1 delivery transport.

## 2. The one architectural decision everything follows from

> **Webhooks are hints. Fetches are truth.**

Webhook payloads are used for two things only: deciding *what* to refresh and
*how urgently*. Entity state in the cache is only ever written from a
REST/GraphQL response. This single rule eliminates the entire class of
out-of-order-webhook bugs (GitHub explicitly does not guarantee delivery
order) at the cost of one conditional fetch per coalesced change — a cost
§C-B* shows we can afford. The narrow exception: fields the payload proves
monotonically (e.g. a delivery whose `pull_request.updated_at` is newer than
the cached row may mark the row stale) — but even then the payload never
becomes cache state itself.

### 2.1 Stack signal coverage (webhook gap analysis)

The gh-stack preview documents only two webhook surfaces for stacks: the
`stack` object (`id`, `number`, `size`, `position`, `base.ref`, `base.sha`)
embedded in every `pull_request` event for a stacked PR, and a `stacked`
action when a PR joins a stack. **No events are documented for stack
creation-as-object, rebase, retarget-after-merge, reorder, member removal, or
unstack.** The table below maps every stack state change the derivation
engine cares about to its best available signal; the dispatcher implements
the "hint" column, and the stacks sweep (C-R1, 5 min) is the guaranteed
floor for everything.

| Stack state change | Direct webhook? | Usable indirect hint (dispatcher rule) | Worst case without hint |
|---|---|---|---|
| PR added to stack | ✅ `pull_request.stacked` | — | — |
| Membership/order changed (`gh stack modify`, removal) | ❌ | **Stack-object diff**: every `pull_request` event carries the stack summary; if `size`/`position`/`id` disagree with cache (or `stack` went null), enqueue a stack refresh | Sweep (≤5 min) if no member PR emits any event |
| Bottom PR merges → upstack retargets | ❌ (no stack event) | `pull_request.closed` (merged) on a stacked PR → refresh the **whole stack**, not just the closed PR | Sweep |
| Server-side "Rebase stack" (force-pushes) | ❌ (undocumented) | Branch rewrites are real ref updates: expect `push` per branch and `pull_request.synchronize` per member — treat either on a stack branch as a whole-stack refresh. **Must be verified empirically in Phase 0**; undocumented whether server-generated rebases emit these | Sweep |
| Unstack / dissolve | ❌ | Next `pull_request` event on any ex-member arrives with `stack: null` → stack-object diff fires | Sweep |
| Trunk moves (dry-run staleness) | ✅ `push` on base ref | Standard event; enqueue dry-run re-evaluation for stacks based on that ref | — |
| Effective CODEOWNERS changes | ✅ `push` on default branch | Exact `.github/CODEOWNERS`, root `CODEOWNERS`, or `docs/CODEOWNERS` path change → one coalesced branch refresh, fanning out to the finite cached set of live open stacked and loose PRs based on that branch (no arbitrary count cutoff) | Reconciliation |
| Stack `open`/closed state | ❌ | Derivable from member PR states | Sweep |

Consequences baked into the design:

1. **The stack-object diff rule is the workhorse.** Because *every* PR event
   carries the stack summary, most membership drift is detected
   opportunistically within seconds — the sweep only covers changes that
   generate no PR events at all (pure reorder, unstack of an idle stack).
   When the complete tuple matches the cached stack ID, base, size, and member
   at that position, dispatch suppresses the eager stack fetch but retains the
   authoritative PR refresh. A resulting membership, position, head, state, or
   draft change still enqueues the stack transactionally. A JSON-null base SHA
   is valid upstream truth for historical and open stacks, but it can never
   prove a tuple match: unknown fails open to the eager stack fetch even when
   both the payload and cache use the empty-string unknown sentinel.
2. **Stacked-PR events escalate scope when needed.** For a PR in a stack,
   `closed`, `synchronize`, and base-edit events always retain an authoritative
   PR refresh. A tuple mismatch or a fetched membership, position, head, state,
   or draft change also refreshes the stack because those transitions affect
   stack-level truth.
3. **Preview instability is a named risk.** These webhook semantics are a
   private-preview surface and may change without notice. Mitigations: the
   sweep floor (correctness never depends on stack webhooks), the drift
   detector (C-O3) as the alarm, dispatcher rules as config so new event
   types/actions are a data change, and a standing ask to the GitHub preview
   team for first-class stack lifecycle events.

---

## 3. Constraints

### Ingestion (C-I)

- **C-I1 — Durable before acknowledged.** A webhook delivery is written to
  `webhook_deliveries` (raw body + headers) and committed before the handler
  returns 200. Processing is asynchronous. The HTTP handler does nothing else:
  verify, validate the delivery envelope, insert, ack. Target ack p99 < 250ms;
  GitHub times out at 10s and a timed-out delivery is a dropped delivery.
- **C-I2 — Verified or rejected.** HMAC (`X-Hub-Signature-256`) verified with
  constant-time compare before the body is parsed. Unverifiable deliveries are
  rejected 401 and never enqueued.
- **C-I3 — Duplicate-safe.** Dedupe on `X-GitHub-Delivery` GUID (unique
  index; insert `ON CONFLICT DO NOTHING`). Redeliveries and GitHub retries
  must be free.
- **C-I4 — Order-independent.** Processing any interleaving of deliveries
  converges to the same cache state. This falls out of §2 (payloads are
  hints), but it is a named constraint because it is the property tests'
  centerpiece (§9).
- **C-I5 — Poison-tolerant.** A delivery that fails processing N times is
  parked (dead-letter status) with its error, without blocking the partition
  it arrived on. Parked deliveries are visible in ops tooling and replayable.

### Cache integrity (C-C)

- **C-C1 — Serialized per entity.** At most one in-flight fetch+write per
  entity key (`repo`, `pr_number` / `stack_number`). Enforced by job-queue
  keying (§6), not by hope. Two workers may never interleave writes for the
  same PR.
- **C-C2 — Monotonic writes.** Every cache write carries the fetch's
  observed version (`updated_at`, `head_sha`, ETag). A write is applied only
  if it is not older than the stored version (compare-and-set in SQL). A slow
  response that lands after a newer one is discarded. This makes C-C1's
  guarantee robust even across worker crashes and retries.
- **C-C3 — Atomic write + invalidate + emit.** Entity upsert, dirty-marking
  of affected derivation scopes, and the outbox event insert happen in one
  Postgres transaction. There is no state where the cache changed but nothing
  will recompute, or where a recompute event exists for a write that rolled
  back.
- **C-C4 — Tombstones, not deletes.** Closed PRs, dissolved stacks, and
  uninstalled repos are marked, not removed. History (flake rates, review
  latency, "what changed") depends on it. Hard deletion is a scheduled
  retention job with its own policy, never a sync-path behavior.
- **C-C5 — Provenance on every row.** `synced_at`, `etag`, `sync_source`
  (webhook | reconcile | backfill | manual). Required for conditional
  requests (C-B4), staleness metrics (C-R1), and debugging.
- **C-C6 — No transaction or contended lock across external I/O.** A database
  transaction, contended advisory lock, or shared in-process lock must never
  overlap an HTTP/GraphQL/REST call or client network read/write. External
  fetches complete first; database-backed response data is fully materialized
  and the transaction committed before the first client write. Transaction
  callbacks are database/CPU-only. Otherwise a slow or cyclic network
  dependency can retain row/advisory locks, make peers hit `statement_timeout`,
  and exhaust the connection pool through retries.
  The one narrow exception is C-C1's per-entity observation: one dedicated
  pgx session (never an open transaction) holds that entity's session advisory
  lock across its authoritative GitHub fetch and write because serializing the
  complete fetch+write observation is the correctness boundary. It is keyed
  to one entity rather than shared across a repository batch. The session is
  returned to pgxpool only after a confirmed unlock; an unlock failure or
  cleanup timeout hijacks and closes the physical connection so PostgreSQL
  releases the session lock on backend teardown. Repository metadata found by
  a PR batch is different: its repository lock is acquired only after all
  fetch and hydration calls, held solely around `ApplyRepositoryObserved`, and
  released before any PR writer transaction. C-C2 makes two such narrowed
  windows converge when the older response arrives last. This separation and
  fail-closed cleanup are the durable lessons from
  [#18](https://github.com/ewhauser/ghsync/issues/18) and
  [#19](https://github.com/ewhauser/ghsync/issues/19).

The implementation audit is kept here so a new transaction or lock has an
explicit review checklist rather than relying on call-site folklore:

| Sites | Transaction / contended-lock window | External-I/O verdict |
|---|---|---|
| `fetch.Handler` entity refreshes and `ensureRepository` | A per-entity session observation spans that entity's fetch and write; no transaction is open during the fetch. Discovery's nested direct repository write is ordered `repo-discovery:...` then `repo:...`. | The documented C-C1 exception. Each lock protects one authoritative observation, never a repository batch. |
| `fetch.prCoordinator.execute` | Sorted `pr:...` observations span GraphQL and hydration. After every remote call, it briefly acquires `repo:...`, applies repository metadata, and releases it before PR writer transactions. | The only PR+repository site; ordering is `pr:...` then `repo:...`. There is no `repo:...` then `pr:...` site. |
| `drift.inspectSample` / `recordAndHeal` | The upstream read completes before the drift observation. The later finding transaction performs SQL and River `InsertTx` only. | No external call under the observation or transaction. |
| Entity-writer transactions and `TransactionHook` implementations | `withEntityTx` holds the entity/write fences around cache, dirty, outbox, and hook DB work. Fetch hooks close only over refresh specs and a River client, then call `queue.InsertRefreshesTx`. | River inserts are PostgreSQL work through the supplied transaction; no hook reaches the GitHub budget gate or another network client. |
| Dispatcher and deriver (`DispatchBatch`, `derive.runOnce`) | Claim/classify/insert and snapshot/derive/write respectively remain inside their transactions. Classification and derivation are CPU-only. | No network client is reachable from either transactional callback path. |
| Backfill and sweep cursor transactions | `advance*`, `persistPage`, `startOrResumeScope`, gap-window updates, pruning, and enqueue helpers run after list/redelivery calls return or before the next call begins. | REST/delivery reads and redeliveries are outside transactions; River `InsertTx` calls are DB-only. |
| Budget lease, refresh completion, retention, and watermarker transactions | Short row/advisory-lock windows contain only generated SQL, River DB operations, and bookkeeping. | No HTTP/GraphQL/client response I/O occurs in these windows. |
| Migration session lock and per-file transactions | The process-wide lock spans embedded River/application DDL against the same Postgres service. | Deliberate startup-only DB coordination; it performs no HTTP/GraphQL/client response I/O. |
| `streamclient.Bootstrap` and `deliverPage` | Cursor row locks and repeatable-read snapshots encompass caller projection/handler callbacks. | Their public contracts forbid network I/O; callers commit/close before external effects. Retry/backoff behavior is outside the transaction and unchanged. |
| `example-api` watch snapshot and replay | Rows/events are bounded and fully materialized, then the read transaction commits before SSE. Snapshot materialization is capped at two concurrent 64 MiB/100k-entity buffers. | Above-limit snapshots emit `snapshot_limit_exceeded`; there is no truncation, unbounded per-subscriber allocation, or client write under a transaction. |

### Budget (C-B)

- **C-B1 — One choke point.** Every GitHub call in the system goes through a
  single per-installation gate (one goroutine-owned budgeter per installation
  across its installation-token and App-JWT traffic, coordinated across
  processes via Postgres lease). No component holds its own HTTP client to
  GitHub. This is the constraint that makes every other budget rule
  enforceable.
- **C-B2 — Server headers are authoritative.** Remaining budget is tracked
  from `x-ratelimit-remaining` / `x-ratelimit-reset` response headers, never
  from local counting. On 403/429 secondary-limit responses, `Retry-After` is
  honored exactly, and the gate closes for the offending credential context,
  not the independent installation-token or App-JWT pool.
- **C-B3 — Priority classes with reserved headroom.** Three classes:
  `interactive` (a user is waiting: cold-start backfill of a just-opened
  view), `event` (webhook-driven refresh), `sweep` (reconciliation).
  Within each credential/resource budget, background classes may not spend
  below a floor (default: 20% of the hourly budget remains untouched by
  `sweep`, 10% by `event`), so interactive work and mutations always have
  room. Class starvation is a paging alert, not a silent behavior.
- **C-B4 — Conditional by default.** Reconciliation and any refetch of a
  possibly-unchanged entity sends `If-None-Match`; 304s are close to free and
  must be the common case for sweeps. A sweep that hits < 80% 304 rate on a
  quiet org indicates an ETag-handling bug. PR metadata, the first changed-file
  page, and the first check-runs page each reuse their own persisted validator.
  PR ownership hydration first
  reuses an exact ref/SHA CODEOWNERS source from the mirror. Provenance
  invalidation single-flights the repository/ref probe, reuses its content
  ETag, and remembers absent precedence paths only for the exact immutable
  commit. Drift validation remains an authoritative read and forces its
  healing refresh to bypass a source it has just shown to be divergent.
- **C-B5 — GraphQL is a separate budget with separate accounting.** Batched
  reads (the per-PR fan-out: reviews, ordinary issue comments, threads, check
  suites) go to GraphQL in
  `nodes(ids:)` batches; the budgeter tracks point cost from the response's
  `rateLimit` block. REST is for what GraphQL can't do (stacks endpoints,
  Actions logs) and for conditional cheapness.
- **C-B6 — Concurrency ceiling.** ≤ N concurrent requests per installation
  (default 40, well under GitHub's ~100 secondary limit), enforced at the
  same choke point.

### Coalescing (C-Q)

- **C-Q1 — Bursts collapse to one fetch.** Refresh intents are keyed
  (`repo`, `entity`, and where applicable `head_sha`). A rebase cascade that
  emits ~20 events across 3 branches in 10 seconds results in ≤ 1 fetch per
  affected entity. Implemented with River **unique jobs** plus a durable
  refresh generation for each `(kind, key)`.

  (Mechanism amended during M2 review: River 0.41 requires `running` in the
  supported public uniqueness mask. The shipped mask is
  `available/pending/retryable/running/scheduled`, not the originally
  specified running-excluded mask. The dispatcher and every other refresh
  producer bump `refresh_intent_generations.generation` in the same
  transaction that attempts the River insert. A worker records the generation
  it started, fetches authoritative truth, then locks and rechecks the durable
  generation in its completion transaction. If an intent arrived while it
  ran, the worker completes the current job and reinserts a follow-up pointer
  before committing. Thus a queued burst still collapses to one job, while an
  in-flight signal cannot be lost. The guarantee is unchanged; the durable
  generation protocol supplies it through River 0.41's supported mask.)
- **C-Q2 — Debounce is bounded.** The first intent fixes `ScheduledAt`;
  because coalesced duplicates never reschedule it, the delay is exactly one
  debounce window (default 5s) — the 15s hard cap holds by construction.
  Freshness SLO: p95 webhook-event → cache-updated ≤ 20s, p99 ≤ 60s.
- **C-Q3 — Check-run storms aggregate by SHA.** `check_run`/`check_suite`
  events for the same head SHA collapse into one checks refresh; individual
  check rows are written from the batched response, not per-event.

### Reconciliation (C-R)

- **C-R1 — Bounded staleness without webhooks.** Even with zero deliveries
  arriving, every entity class has a maximum staleness: open stacks ≤ 5 min
  (the gh-stack preview documents **no webhook events** for rebase / retarget
  / unstack — the sweep is the *only* signal for those transitions), open PRs
  ≤ 10 min, repo rules ≤ 1 h, closed-but-displayed entities ≤ 24 h. The
  sweeper's schedule derives from these numbers; they are config, not code.
  Large PR fan-outs assign River `scheduled_at` times across the cadence
  portion of that plan at enqueue time. Their hard completion deadlines remain
  unchanged, and every scheduled start retains the plan's full completion
  headroom.
- **C-R2 — Sweeps are incremental and resumable.** List-based (paginated,
  ETag'd, `sort=updated` where available) with a persisted cursor. A crashed
  sweep resumes, never restarts from zero, and a sweep that can't finish
  within its period raises a metric rather than piling onto the next period.
- **C-R3 — Disappearance is detected.** An entity present in cache but
  absent from its authoritative listing gets a verification fetch; 404 →
  tombstone (C-C4). Silent divergence between list and cache is the drift
  detector's job to catch (C-O3).
- **C-R4 — Gap healing uses the deliveries API.** On startup after downtime
  and on a schedule, compare GitHub's recorded deliveries
  (`/app/hook/deliveries`) against `webhook_deliveries`; request redelivery
  for gaps rather than falling back to full resync. A completed pass durably
  records both the root delivery's ID and the greatest `delivered_at` it saw.
  An incremental pass uses the ID only as an exact boundary identity: IDs are
  not compared or assumed monotonic across redeliveries. It stops after the
  page containing that identity, so the rest of that page is bounded overlap
  and no-new list cost is one request.

  Bounded overlap is not the C-R4 proof for a delivery that GitHub lists late
  behind the boundary. A durable deep pass is due every
  `GAP_DEEP_SCAN_PERIOD`; it ignores the incremental boundary and compares the
  fixed delivery-time lookback. The lookback is
  `GAP_COMPARISON_WINDOW + GAP_DEEP_SCAN_PERIOD`, so a delivery that first
  becomes visible no more than `GAP_COMPARISON_WINDOW` after its recorded
  `delivered_at` remains eligible at the next deep-pass start. Deep cadence and
  the next cutoff are anchored to the prior deep-pass start, so a long-running
  scan cannot silently consume this overlap. The comparison bound is one
  `GAP_DEEP_SCAN_PERIOD`, plus any active-pass overrun, queue/API time, and the
  target pass's completion. For a delivery on page `p`, capped jobs add at least
  `floor((p-1)/GAP_MAX_PAGES) * GAP_CONTINUATION_DELAY`. The configured
  lookback may not exceed GitHub.com's 72-hour recent-delivery retention.

  The deep scan stops only when an entire delivery-list page is older than its
  fixed cutoff, so a single out-of-order timestamp cannot terminate it. Lost
  state, an incompatible persisted window/page shape, an over-age pass, or an
  invalid opaque cursor restarts in deep mode from that bounded root. The
  cursor, pass watermark, scan mode, shape version, and deep completion are
  durable; the per-installation lease still fences advance and completion
  without holding a database lock across delivery-list or redelivery calls.

### Derivation & change feed (C-D)

- **C-D1 — Derivation is pure and elsewhere.** The engine consumes a cache
  snapshot and produces work items; it performs no I/O and lives in its own
  package with golden-fixture tests. The sync engine only decides *when* it
  runs and *what changed*.
- **C-D2 — Dirty granularity is (org, stack | loose PR).** A cache write
  marks exactly the derivation scopes it can affect. Recompute cost is
  O(affected stacks), never O(org). Viewer-specific classification (scopes,
  "your move") is computed per active viewer from the same stack-level
  derivation, so one busy stack doesn't fan out to per-user recomputes.
- **C-D3 — Stable work-item identity.** Work item IDs derive from domain
  identity (installation + repo + stack number, or repo + PR number for loose
  PRs), never from row IDs, so recomputes and process restarts don't churn
  the UI or the Watch stream.
- **C-D4 — Exactly-once-per-cursor change feed.** Derived changes are
  emitted through a transactional outbox with a per-org monotonic sequence.
  The v1 Postgres consumer contract is snapshot at sequence S, then deltas
  > S. A consumer that reconnects with its cursor misses nothing and
  duplicates nothing *per that cursor*; the outbox itself is at-least-once
  and consumers are keyed by sequence. A future `WatchWorkItems` API builds
  on this contract.
- **C-D5 — Snapshot-consistent reads.** Consumers read derived rows in a
  single Postgres transaction; a reader never observes a half-applied
  recompute. Future List/Get methods build on the same read model.

### Change stream for consumers (C-S)

Consumers of the synced data — our UI first, but potentially bots, metrics,
and other services — need streaming notification of change. The mechanism is
a generalization of the C-D4 outbox; CDC via logical replication was
considered and **rejected** (row-level events couple consumers to our schema,
replication slots hold WAL hostage when a consumer stalls, and Debezium-style
pipelines smuggle Kafka back into a design that deliberately excluded it).
The transactional outbox is the single integration seam: any future bridge
(outbound webhooks, a Kafka topic, an audit sink) is just another cursor
consumer.

- **C-S1 — Two tiers, one mechanism.** A single append-only
  `change_events(seq BIGSERIAL, stream, kind, entity_key, occurred_at,
  payload JSONB)` table carries two streams: `entities` (cache tier:
  pull_request / stack / checks changed — for service consumers) and
  `work_items` (derived tier: the deltas UIs want; supersedes the
  `work_item_events` sketch in §5). Rows are written in the same transaction
  as the state change (C-C3), so the stream can never disagree with the
  store. Total order within the table; consumer cursors are
  (consumer, stream) → seq.
- **C-S2 — Gap-free tailing.** `BIGSERIAL` allocates in *begin* order but
  rows appear in *commit* order — a tailer reading `seq > cursor` can skip an
  event whose smaller seq commits later. Tailers therefore only read below a
  **visibility watermark**. (Mechanism amended during M5 review: the original
  oldest-in-flight-snapshot/xmin fence lets ANY long transaction — including
  our own Bootstrap snapshots — stall the watermark. The implemented protocol
  is a shared/exclusive advisory fence: every outbox-writing transaction
  holds a shared advisory lock; the watermarker's momentary exclusive
  acquisition proves no writer is in flight, at which point
  max(committed seq) is safe. Read-only snapshots never take the fence.)

  (Mechanism hardened during the Wave A review: the exclusive fence is a
  bounded global write barrier. The watermarker applies a local
  `lock_timeout`; a busy or stuck writer produces a retryable, metered timeout
  instead of leaving a pending exclusive acquisition that freezes new
  writers. Every pooled connection also has an
  `idle_in_transaction_session_timeout`, and migration `0002` installs a
  database-enforced `BEFORE INSERT` trigger that rejects an unfenced
  `change_events` write. A stuck writer therefore times out rather than
  freezing the stream, and the trigger prevents a new writer from bypassing
  the fence; enforcement no longer depends on convention.)

  No consumer may ever observe a gap that later fills; this is the
  constraint that makes cursors trustworthy, and it gets its own test (§9).
- **C-S3 — Snapshot-then-stream bootstrap.** A consumer's contract is:
  snapshot at watermark W, then deltas with seq > W, resuming by cursor after
  disconnect — exactly-once per cursor (same contract C-D4 gives
  `WatchWorkItems`; C-S makes it uniform for every stream).
  `pkg/streamclient` implements this transaction over direct Postgres, the
  **only v1 transport**. The gRPC server-stream and SSE/`Last-Event-ID`
  mapping belong to the future API project in `API_SPEC.md`.
- **C-S4 — Explicit resync.** When a resuming cursor has fallen below
  retention (C-S7), `pkg/streamclient` returns typed
  `ErrResyncRequired`, and the consumer replaces its projection through
  Bootstrap before resuming. Slow consumers are never silently advanced and
  never block pruning. V1 has no API-server connection or fan-out buffer;
  bounded connection buffers remain an obligation of the future API project.
- **C-S5 — Exactly one tailer per consumer and stream.** A
  `(consumer, stream)` cursor has one tailer; sharing it across replicas
  intentionally produces `REPEATABLE READ` serialization failures. Elect a
  single tailer and fan out locally, or give independent consumers distinct
  names. `LISTEN/NOTIFY` is a wake-up hint and short-interval Postgres polling
  is the correctness path.
- **C-S6 — Envelope stability.** Events carry {seq, stream, kind, entity
  reference, occurred_at, versioned payload} — never internal row images.
  Evolution is additive-only; consumers must ignore unknown kinds and fields.
  The `entities`-stream payload is a *reference* (what changed, not the new
  state): v1 consumers fetch current state from the public Postgres read
  model in `db/CONTRACT.md`, which keeps the stream cheap and re-uses C-S3's
  consistency story.
- **C-S7 — Stream retention is independent of consumers.** Events prune at
  7 days (config). Pruning never waits on a lagging cursor — a consumer that
  falls behind retention gets `RESYNC_REQUIRED` (C-S4). This is the
  anti-replication-slot property: no consumer can hold storage hostage.

### Batching & write amplification (C-P)

The design's first line of defense is *semantic* batching: coalescing (C-Q1)
means transaction volume scales with **changed entities**, not with webhook
deliveries. These constraints cover the *mechanical* batching everywhere
else, so no pipeline stage does per-event work that could be per-batch work.

- **C-P1 — Ingress is per-delivery by nature, and that's fine — but it must
  stay minimal.** GitHub sends one HTTP request per delivery, and C-I1
  (durable-before-ack) forces a commit inside that request; there is nothing
  to batch against. The constraint is that this commit is *one single-row
  insert* into an append-only table whose write cost stays O(1) — no
  parsing beyond signature check, no classification, no joins.

  (Index policy amended during M2 and ingestion review: the shipped table has
  the `delivery_guid` primary key;
  `webhook_deliveries_pending_received_guid_idx`, a partial claim index over
  `(received_at, delivery_guid) WHERE status = 'pending'`;
  `webhook_deliveries_unfinished_received_idx`, a partial status index over
  `(status, received_at) WHERE status IN
  ('pending', 'processing', 'parked')`; and the compact
  `webhook_deliveries_received_at_brin_idx` used by retention. Claim-poll and
  retention cost are constraints alongside ingress-write cost, so the earlier
  "exactly one index" and pending-only descriptions are no longer the
  contract. Migrations are the source of truth for the exact index set.)

  Concurrent deliveries amortize WAL flushes via Postgres group commit.
  Budget: a single-row commit is ~1ms; even a 200 deliveries/sec redelivery
  burst is well inside a small instance's capacity. `synchronous_commit`
  stays **on** for this table — turning it off would silently void C-I1.
- **C-P2 — Dispatch is batched.** The dispatcher claims deliveries in
  batches (default 100), classifies them in memory, and commits one
  transaction per batch: River `InsertManyTx` for all refresh intents
  (unique no-ops included) + one `UPDATE ... WHERE id = ANY(...)` marking the
  batch processed. Per-delivery cost amortizes to a few statements per
  hundred deliveries.
- **C-P3 — Fetch results are written set-at-a-time.** One fetch produces one
  transaction, however many rows it touches: a checks refresh upserts all
  check runs for the SHA via a single `unnest`-based upsert (sqlc), review
  threads likewise, and changed files plus owners use two fenced JSONB
  replace sets. Never row-per-statement, never statement-per-event.
- **C-P4 — Due fetches gang into GraphQL batches.** The fetcher may claim up
  to K (default 25) due entity-refresh jobs at once and satisfy them with one
  `nodes(ids:)` GraphQL call, then apply results per entity (each under its
  C-C1 advisory lock; locks taken in sorted key order to make multi-entity
  batches deadlock-free). This is what makes reconnect-after-outage storms
  cheap: 500 stale PRs ≈ 20 GraphQL calls, not 500.
- **C-P5 — Derivation drains dirty sets, not dirty rows.** The deriver wakes
  (NOTIFY or interval), claims the *entire* current dirty set up to a cap,
  recomputes affected stacks in one engine pass, and writes work items +
  outbox deltas in one transaction per batch. A storm that dirties 50 stacks
  is one recompute cycle, not 50.
- **C-P6 — Feed consumers page.** Direct Postgres outbox tailing reads
  `WHERE seq > $cursor ORDER BY seq LIMIT N` batches; budget-state
  persistence is periodic snapshots from the in-memory budgeter, never
  per-request writes.

Worst-case sanity check: healing a 6-hour outage replays ~5,000 deliveries.
C-P1: 5,000 cheap inserts as fast as GitHub redelivers. C-P2: 50 dispatch
transactions. C-Q1: collapse to ~40 active entities. C-P4: ~2–3 GraphQL
batches. C-P5: a handful of derivation cycles. Total heavyweight
transactions: **dozens**, not thousands.

### Operations (C-O)

- **C-O1 — At-least-once jobs, idempotent handlers.** The job queue
  guarantees at-least-once execution with per-key serialization (C-C1),
  visibility timeout for crashed workers, capped retries with jitter, and a
  dead-letter state. Every handler is idempotent by construction (C-C2 makes
  refetch handlers naturally so).
- **C-O2 — Horizontal scale without coordination service.** Any number of
  identical processes may run; all mutual exclusion is Postgres
  (`FOR UPDATE SKIP LOCKED` for jobs, advisory locks for singleton roles like
  the sweeper scheduler and per-installation budgeter lease).
- **C-O3 — Drift is measured, not assumed away.** A sampling drift detector
  periodically full-fetches a random small set of entities, compares to
  cache, and emits a divergence metric. Divergence > 0 is a bug report with
  the diff attached, not a shrug.
- **C-O4 — First-class observables.** Per-installation: budget remaining by
  class and auth context, request and starvation rate by auth context, request
  attribution by endpoint family and HTTP outcome, and 304 ratio. Pipeline:
  event→cache
  latency histogram (C-Q2 SLO), queue depth by priority, oldest-unprocessed
  delivery age, staleness per entity class (C-R1), outbox lag, parked-job
  count. All of these have alert thresholds tied to the constraint they
  verify.
- **C-O5 — Backfill is resumable and budget-polite.** Installation
  onboarding (enumerate repos → open PRs → stacks → per-PR detail) runs as
  ordinary keyed jobs in the `interactive` class while a user waits on the
  empty state, degrading to `sweep` class for long tails. A crashed backfill
  resumes from its cursor.

---

## 4. Data flow

```
GitHub ──webhook──▶ ingress (verify, C-I1..3) ──▶ webhook_deliveries
                                                        │ (async)
                                                   dispatcher
                                     (classify → refresh intents, C-Q1 keys)
                                                        ▼
                                                    sync_jobs  ◀── sweeper (C-R1..3)
                                                        │            ▲
                                              fetcher pool ──────────┘ cursors
                                   (budget gate C-B1..6 → GitHub fetch)
                                                        ▼
                                    Tx: upsert entity (C-C2) + mark dirty
                                        + outbox `entity_changed` (C-C3)
                                                        ▼
                                                     deriver
                                  (pure engine over snapshot, C-D1..3)
                                                        ▼
                                    Tx: upsert work_items + outbox deltas (C-D4)
                                                        ▼
                                    Direct Postgres consumers: read model
                                    + streamclient snapshot/cursor tail
```

`LISTEN/NOTIFY` is a wake-up latency optimization only; correctness comes
from polling the outbox by sequence (a missed NOTIFY costs milliseconds, not
events).

## 5. Postgres schema (sketch)

```sql
-- Ingestion
webhook_deliveries(delivery_guid PRIMARY KEY, event, raw_body, headers,
                   received_at, status, attempts, last_error,
                   payload_pruned_at)

-- Job queue: River-owned tables (river_job, river_leader, ...) via River's
-- migrations. Our conventions on top:
--   priority queues: interactive | event | sweep    (C-B3 fetch classes)
--   component queues: reconcile | drift | pruner
--   job args:    {kind, key} where key = 'pr:acme/monolith:4812' | 'stack:...:142'
--   coalescing:  River UniqueOpts + durable refresh generations per C-Q1

-- Cache (mirrors; all rows carry provenance C-C5)
repos(...), repo_rules(...),
stacks(id, repo_id, number, base_ref, open, entries JSONB,
       gh_updated_at, etag, synced_at, sync_source, tombstoned_at)
pull_requests(id, repo_id, number, title, state, head_ref, head_sha,
              review_decision, gh_updated_at, etag, synced_at, ...)
pull_request_review_requests(repo_id, pr_number, reviewer_kind,
                             reviewer_gh_id, reviewer_node_id, ...)
pull_request_reviews(node_id PRIMARY KEY, repo_id, pr_number, author_kind,
                     state, submitted_at, commit_oid, gh_updated_at, ...)
pull_request_comments(node_id PRIMARY KEY, repo_id, pr_number, author_kind,
                      created_at, gh_updated_at, ...) -- no bodies
pull_request_change_snapshots(repo_id, pr_number, base_sha, head_sha,
                              files_total_count, files_truncated,
                              codeowners_ref, codeowners_sha,
                              codeowners_path, codeowners_state, ...)
pull_request_changed_files(repo_id, pr_number, path, previous_path,
                           change_type, base_sha, head_sha, ...)
pull_request_file_owners(repo_id, pr_number, path, owner_token, owner_type,
                         resolution_state, owner_node_id, ...)
review_threads(...), check_runs(...), check_history(...)

-- Derivation
derivation_dirty(scope_key, marked_at)             -- C-D2
work_items(id, org_id, identity_key UNIQUE, ...)   -- C-D3
change_events(seq BIGSERIAL, stream, kind, entity_key,   -- C-D4/C-S1 outbox
              occurred_at, payload JSONB)                -- streams: entities | work_items
stream_watermark(id, safe_seq, updated_at)               -- C-S2 visibility watermark
consumer_cursors(consumer, stream, seq)                  -- durable cursors for service consumers

-- Budget & coordination (single org ⇒ one installation row; kept keyed by
-- installation_id anyway so multi-org later is a data change, not a schema one)
installation_budgets(installation_id, class, remaining, reset_at, lease_owner, lease_until)
-- class: rest | app_jwt_rest | graphql
sweep_cursors(sweep_kind, installation_id, cursor, updated_at)
gap_heal_cursors(installation_id, cursor, high_watermark_at,
                 boundary_delivery_id, scan_mode,
                 last_deep_started_at, last_deep_completed_at, updated_at)

-- Retention (decided): webhook_deliveries.payload and check_history rows are
-- pruned at 90 days by a scheduled River periodic job; tombstoned entities
-- keep their skeleton rows (C-C4) — only bulky payload/history is pruned.
```

Write-if-newer (C-C2) is plain SQL, e.g.:

```sql
UPDATE pull_requests SET ..., gh_updated_at = $new, etag = $etag, synced_at = now()
WHERE id = $id AND (gh_updated_at IS NULL OR gh_updated_at <= $new);
```

## 6. Go process layout

One binary, role flags; all roles safe to run N× (C-O2). River supplies the
work-distribution primitives (queues, retries with backoff, scheduled and
periodic jobs, leader election, unique jobs); what remains ours is per-entity
write serialization — River's uniqueness dedupes *queued* jobs but two
workers can still run *different* jobs touching the same PR, so C-C1 is
enforced by a per-entity-key Postgres advisory lock taken inside the worker
transaction (C-C2's write-if-newer makes any remaining race harmless).
Retries: River's retry/backoff replaces hand-rolled attempts; C-I5's parking
maps to River's discarded state plus our error surfacing.

| Component | Responsibility | Constraints it owns |
|---|---|---|
| `ingress` | HTTP handler: verify, persist, ack | C-I1, C-I2, C-I3, C-P1 |
| `dispatcher` | Batch-claims deliveries → refresh intents; payload-as-hint classification | C-I4, C-I5, C-Q1, C-Q3, C-P2 |
| `fetcher` | River workers on the three fetch-priority queues; gangs due jobs into GraphQL batches; transactional set-writes | C-C1..C-C3, C-O1, C-P3, C-P4 |
| `budgeter` | Per-installation gate (leased singleton); classes, headers, concurrency | C-B1..C-B6 |
| `sweeper` | River **periodic jobs** (leader-elected by River) enqueue sweep work; cursors + disappearance checks + gap healing in ordinary workers | C-R1..C-R4 |
| `deriver` | Drain dirty scopes as a set → run pure engine → write derived + outbox per batch | C-D1..C-D4, C-P5 |
| Postgres consumer (external) | Snapshot-consistent reads and exactly one `streamclient` tailer per `(consumer, stream)` | C-D5, C-S3..C-S5 |
| `watermarker` | Leased singleton loop; advances the visibility watermark through the bounded writer fence | C-S2 |

Internal interfaces to keep the seams honest:

```go
type GitHubGate interface { // the only path to GitHub (C-B1)
    // Do holds the C-B6 admission slot through the response-body lifetime.
    // Reading to EOF or closing the body releases the slot. The
    // internal/budget package doc comments are the normative admission contract.
    Do(ctx, Class, *Request) (*Response, error)
}
type EntityWriter interface { // fetch result → tx (C-C2, C-C3)
    ApplyPR(ctx, FetchedPR) (Applied, error)
    ...
}
type Deriver interface { // pure (C-D1)
    Derive(Snapshot) []WorkItem
}
```

## 7. Budget math (sanity check against C-B)

Assume public GitHub's App-installation baseline (5,000 REST/hr + 5,000
GraphQL points/hr; GHEC reports 15,000 installation-token REST/hr, which the
budgeter adopts from rate headers), plus the App JWT's independent 5,000
REST/hr pool for delivery APIs and installation-token mint requests, one org,
60 engineers, ~40 active PRs, peak ~300 PR-affecting events/hr.

- Event path: 300 events/hr → coalescing (C-Q1) → ~120 fetch batches/hr ×
  (1 GraphQL batch + ~1 REST call) ≈ **~150 REST/hr + ~600 GraphQL points/hr**.
- Sweeps at C-R1 periods: stacks every 5 min (~2 ETag'd list pages), PRs
  every 10 min (~3 pages), rules hourly ≈ **~60 requests/hr, mostly 304s**.
- Backfill (one-time per install): ~40 PRs × batched ≈ minutes inside the
  `interactive` class.

Steady state ≈ **2–4% of budget** — an order of magnitude of headroom before
the floors in C-B3 even matter. The constraint set is not about surviving the
average; it's about the worst hour (org-wide rebase storm + reconnect after
an outage) degrading gracefully by priority instead of failing.

## 8. Libraries (decided)

| Need | Choice | Notes |
|---|---|---|
| Job queue | **`riverqueue/river`** (decided) | Three priority queues plus reconcile/drift/pruner component queues; supported-mask unique jobs + durable generations = C-Q1 coalescing; periodic jobs + leader election = sweeper scheduling; its own migrations sit alongside ours |
| Data layer | **`sqlc` + `jackc/pgx/v5`** (decided) | SQL-first: write-if-newer, dirty-marking, and outbox queries are named sqlc queries; transactions via pgx `Tx` passed to sqlc `WithTx` and to River's `InsertTx` so C-C3's single-transaction rule spans entity write + outbox + job insert |
| Migrations | `golang-migrate` or `tern` | Plain SQL files; run River's migrations in the same pipeline |
| GitHub REST | `google/go-github` | Plus thin wrapper for stacks endpoints (preview; not in the lib) |
| GitHub GraphQL | `shurcooL/githubv4` or hand-built queries | Point accounting needs raw `rateLimit` block either way |
| App auth | `bradleyfalzon/ghinstallation` | Installation token caching |
| Webhook parse | `google/go-github` webhooks + own HMAC | HMAC is 10 lines; don't outsource C-I2 |
| HTTP | stdlib `net/http` | Retries for *rate* concerns live in the budgeter; job-level retries are River's |
| Observability | OpenTelemetry + Prometheus | Metrics named after constraints (C-O4) |

Deliberately absent: Redis (Postgres covers locks/outbox; River covers the
queue), Kafka (outbox + LISTEN/NOTIFY covers the feed), any workflow engine,
any ORM (sqlc keeps the SQL visible — the SQL *is* the design).

River + sqlc interaction worth pinning down early: River's `rivertype` and
sqlc both want to own struct shapes. Keep them apart — River job args are
small JSON structs (kind + entity key only, never entity data), and sqlc
models never cross the job boundary. A job is a *pointer to work*, not a
payload (this also preserves C-I4: the fetch re-reads truth at run time).

## 9. Testing strategy

- **Order-independence property test (C-I4):** recorded webhook streams from
  a real repo, replayed in random permutations with random duplicates against
  a fake GitHub; final cache state must be identical across all runs.
- **Write-race test (C-C2):** two fetch results for the same PR applied in
  both orders → newer wins in both.
- **Coalescing test (C-Q1/Q2):** synthetic rebase storm (20 events / 3
  branches / 10s) → assert fetch count ≤ entities affected and freshness SLO.
- **Budget conformance (C-B):** fake GitHub returns declining rate headers +
  secondary-limit 403s; assert per-context class floors and backoff,
  Retry-After, and concurrent installation-token/App-JWT responses with
  conflicting headers.
- **Sweep/tombstone (C-R3):** entity vanishes from listing → tombstoned;
  never hard-deleted.
- **Watch resume (C-D4):** kill and reconnect a watcher mid-stream at every
  sequence position; assert no gap, no duplicate per cursor.
- **Outbox gap test (C-S2):** two writers where the transaction that
  allocated the *smaller* seq commits *after* a tailer read passes it;
  assert the tailer (reading below watermark) still delivers both, in order.
- **Resync test (C-S4/C-S7):** consumer paused past retention, then resumes
  → receives `RESYNC_REQUIRED`, re-snapshots, and converges with no
  duplicate application.
- **Drift detector as test oracle (C-O3):** long-running load replay against
  a scripted fake GitHub with divergence budget = 0 (see docs/TESTING.md).

## 10. Decisions & open questions

**Decided:**

1. **Single org.** Shared tables keep `installation_id`/`org_id` columns so
   multi-org later is a data change, not a schema migration — but no tenancy
   machinery is built now.
2. **Retention: 90 days** for `webhook_deliveries` payloads and
   `check_history`, pruned by a River periodic job (schema note in §5).
3. **No GHES.** github.com only (public GitHub or GHEC). The budgeter
   starts from public GitHub's baseline (5k REST/hr, 5k GraphQL points/hr
   per installation) and adopts the actual limits from observed rate
   headers; no per-host budget abstraction.
4. **River** for queuing; **sqlc + pgx** for the data layer (§8).

**Open:**

1. Do we sweep Actions job *logs* proactively for red required checks (warm
   Diagnosis pane) or fetch on first view (`interactive` class)? Default
   until decided: on-view, cached by (job id, attempt).
2. **Phase 0 validation (§2.1):** empirically confirm which events GitHub's
   server-side stack rebase actually emits (`push` / `pull_request.synchronize`
   per member, or nothing), and whether cascading retargets after a partial
   merge emit base-change events. The dispatcher rules ship either way; this
   determines whether rebase visibility is seconds (hints) or minutes (sweep),
   and whether the stacks sweep period can be relaxed from 5 min.
