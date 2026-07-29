# ghsync Sync Engine — Implementation Plan

v2.0 · 2026-07-28 · **Scope: the sync engine only.**

Builds exactly what [SYNC_ENGINE.md](./SYNC_ENGINE.md) specifies: webhook
ingestion, authoritative refetch, the cache, coalescing, budget management,
reconciliation, the derivation *seam*, and the change stream. **Explicitly
out of scope:** the gRPC API ([API_SPEC.md](./API_SPEC.md) stays as the
future contract, nothing from it is implemented), the derivation engine's
classification rules (separate effort; we ship the seam it plugs into), the
git worker, mutation execution, agent integration, and any SPA changes.

**Locked decisions carried in:** single org · GHEC only · stacks preview
enrolled · River queue · sqlc + pgx · Postgres as the only stateful dep ·
90-day retention.

---

## 1. The delivery interface is Postgres

With no API in scope, what consumers (UIs, services, the future API project)
get from this system is a **documented, versioned Postgres contract**:

1. **Read model:** the cache tables (repos, rules, stacks, pull_requests,
   review_threads, check_runs, check_history) with their provenance columns
   (C-C5) and tombstone semantics (C-C4). Consumers read them directly;
   snapshot-consistent reads are plain transactions.
2. **Change stream:** `change_events` + `stream_watermark` +
   `consumer_cursors` with the C-S guarantees (gap-free tailing below the
   watermark, snapshot-then-stream bootstrap, RESYNC_REQUIRED discipline,
   7-day retention).
3. **A small Go consumer library (`streamclient`)** that encapsulates the
   subtle parts — watermark-bounded tailing, cursor management, the resync
   protocol — so no consumer hand-rolls C-S2/C-S4 logic. This is a library,
   not a service; it ships with the engine and is the reference
   implementation of the stream contract.

Schema changes after v1 follow additive-only rules for these surfaces; the
schema doc (`db/CONTRACT.md`, written in M5) is part of the deliverable.

## 2. Repository layout

```
.                               # Go module root
├── cmd/
│   ├── ghsyncd/          # single binary, role flags (SYNC_ENGINE §6)
│   ├── fake-github/             # deterministic GitHub test double
│   ├── stream-tail/             # reference Postgres stream consumer
│   └── soak/                    # storm/soak verifier
├── internal/
│   ├── gh/                      # REST/GraphQL clients and stacks wrapper
│   ├── budget/                  # C-B admission and accounting gate
│   ├── ingress/ dispatch/ fetch/ queue/
│   ├── sweep/ drift/            # reconciliation and trust loops
│   ├── store/                   # sqlc output + entity writers (C-C)
│   ├── derive/ outbox/ stream/  # derivation seam and C-S machinery
│   └── metrics/                 # operational signals
├── pkg/streamclient/            # public consumer library (§1.3)
├── db/
│   ├── migrations/ queries/     # plain SQL (sqlc); River migrations alongside
│   └── CONTRACT.md              # the consumption contract (§1)
├── docker-compose.yml           # Postgres 16 + fake-github
└── Makefile                     # dev, test, lint, migrate
```

No proto/buf: there is no wire API in this project.

## 3. Milestones

Ordering rule: every milestone ends with its constraint tests green in CI.
Estimates assume 2 engineers.

### M0 — Foundations (≈1 wk)

Go module, sqlc + migration pipeline (ours + River's), River wired with the
three fetch-priority queues (interactive/event/sweep; later milestones add the
reconcile/drift/pruner component queues), config, docker-compose, CI (build,
vet, golangci-lint, tests). **Fake GitHub server skeleton** — REST + GraphQL
+ webhook emitter with scriptable scenarios; it is the test substrate for
every later milestone, so it starts first.

*Exit:* `make dev` boots an idle `ghsyncd`; CI green; fake GitHub can
serve a canned repo and emit a canned webhook.

### M1 — GitHub plumbing & budgeter (≈1.5 wk)

GitHub App registration (webhook URL, permissions). `gh/`: REST client with
the stacks-endpoints wrapper, GraphQL batcher with point accounting from the
`rateLimit` block, installation-token caching. The **budget gate** (C-B1–B6):
single choke point, header-authoritative accounting, priority-class floors,
Retry-After, concurrency ceiling, leased singleton.

*Exit:* budget conformance suite green against fake GitHub (declining
headers, secondary-limit 403s, class-floor starvation, global backoff).

### M2 — Ingestion, dispatch, coalescing (≈1.5 wk)

Ingress (C-I1–I5, C-P1: verify → single-row durable insert → ack, GUID
dedupe, poison parking). Dispatcher (C-P2 batch claims; hint rules including
§2.1 stack rules: stack-object diff, whole-stack escalation on member
events, `stacked` action). Coalescing via River unique jobs (C-Q1–Q3,
River 0.41's supported uniqueness mask, and the durable refresh-generation
follow-up protocol).

*Exit:* **replay harness** operational — recorded webhook streams from the
enrolled test repo, replayed in random permutations with duplicates,
asserting dispatch decisions (keys, priorities, job counts). Rebase-storm
fixture asserts ≤1 queued job per entity.

### M3 — Cache & fetchers (≈2 wk)

Mirror schema + entity writers: per-entity advisory locks (C-C1),
write-if-newer CAS (C-C2), single-transaction write + dirty-mark + outbox
event (C-C3), tombstones (C-C4), provenance (C-C5). Set-at-a-time writes and
GraphQL job-ganging with sorted lock order (C-P3/P4). Cold-start backfill as
resumable keyed jobs (C-O5). Dirty-marking lands here; the deriver loop that
drains it lands in M5.

*Exit:* backfill of the test org completes within the `interactive` class;
order-independence property test now asserts **identical final cache state
across permutations** (C-I4, the suite's centerpiece); write-race
both-orders test; storm test asserts fetch counts, not just job counts.

### M4 — Reconciliation, validation, drift (≈1.5 wk, overlaps M3)

Sweeper on River periodic jobs: C-R1 staleness schedule (stacks 5 min — the
only signal for rebase/retarget/unstack), resumable cursors (C-R2),
disappearance → verify → tombstone (C-R3), deliveries-API gap healing
(C-R4). Drift detector (C-O3). 90-day retention pruner.

**Phase-0 experiment:** on the enrolled repo, exercise server-side Rebase
Stack, partial-merge retarget, `gh stack modify`, unstack; record which
webhooks actually fire; tune dispatcher rules; append findings to
SYNC_ENGINE §2.1; confirm or relax the 5-min stacks sweep.

*Exit:* sweep-only mode (webhooks disabled) holds every C-R1 staleness bound
on the test org; drift detector divergence = 0 over 24h; gap-healing test
(drop deliveries, assert redelivery + convergence).

### M5 — Change stream, derivation seam, contract (≈1.5 wk)

Outbox generalization: `change_events` with `entities` stream live end-to-end
(C-S1), leader-elected watermarker (C-S2), retention/resync discipline
(C-S4/S7). `pkg/streamclient`: watermark-bounded tailing, cursors,
snapshot-then-stream bootstrap, RESYNC_REQUIRED handling — with an example
consumer binary. Deriver loop (C-D2 dirty-set draining, C-P5 batching)
behind the `Deriver` interface with a no-op implementation; the `work_items`
stream activates whenever the real engine (separate project) plugs in.
`db/CONTRACT.md` written and reviewed.

*Exit:* outbox gap test (smaller seq commits after tailer pass → still
delivered, in order); resync test (consumer paused past retention →
RESYNC_REQUIRED → re-snapshot converges, no duplicate application); example
consumer follows the live test org via `streamclient`.

### M6 — Hardening & operations (≈1 wk)

Dashboards per C-O4 — every metric named for the constraint it verifies
(budget by class, 304 ratio, event→cache latency vs C-Q2 SLO, queue depth,
oldest-unprocessed delivery, staleness by entity class, outbox/watermark lag,
parked jobs, drift). Alert thresholds. Runbooks: webhook outage recovery,
budget exhaustion, drift alarm, poison deliveries, resync storm. Storm/soak:
replay harness at 10× recorded rate for 48h, drift = 0, SLOs held.
Deployment: single binary, role flags, rolling restart safety (C-O2).

*Exit:* soak passes; runbooks reviewed; on-call can answer "is the cache
trustworthy right now?" from one dashboard.

**Total: ~9 engineer-weeks critical path; ~5–6 calendar weeks with 2
engineers** (M4 overlaps M3; M5's streamclient can start once M3's outbox
writes exist).

## 4. Constraint → test → milestone traceability

| Constraint group | Test (SYNC_ENGINE §9) | Lands in |
|---|---|---|
| C-B (budget) | conformance vs fake GitHub | M1 |
| C-I (ingestion) | order-independence: decisions | M2 |
| C-Q / C-P | storm: job counts (M2) → fetch/tx counts (M3) | M2/M3 |
| C-C (cache) | write-race both-orders; C-I4 final-state permutations | M3 |
| C-R (reconcile) | sweep-only staleness; tombstone; gap healing | M4 |
| C-O3 (drift) | 24h soak divergence = 0 | M4, M6 |
| C-S (stream) | outbox gap; resync; streamclient example | M5 |
| C-D seam | dirty-drain batching with no-op deriver | M5 |
| C-O4 (ops) | dashboards + 10× storm soak | M6 |

## 5. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Stacks preview API changes | rework in `gh/` wrapper + dispatcher rules | one wrapper; rules as config; drift detector as alarm; M4 findings documented |
| Server-side rebase emits no events (M4 experiment) | rebase visibility = sweep latency | sweep floor already designed; raise with GitHub preview team |
| Consumers misuse the Postgres contract (ad-hoc queries, cursor hand-rolling) | correctness bugs blamed on the engine | `streamclient` as the blessed path; CONTRACT.md marks exactly which tables/columns are public; everything else schema-private |
| Derivation seam shaped wrong for the real engine | rework at plug-in time | seam mirrors the `Deriver` interface already specced in SYNC_ENGINE §6; validate early against PLAN.md §3.3 rules with a throwaway partial impl if needed |
| Fake-GitHub fidelity gaps | tests pass, prod surprises | replay harness uses *recorded* real traffic as its base; M4 runs against the real enrolled repo, not the fake |

## 6. Definition of done (v1)

A deployed `ghsyncd` that: mirrors the enrolled org with staleness
inside C-R1 bounds and drift = 0; survives webhook outages via gap healing +
sweeps; spends < 10% of the GHEC budget in steady state with class floors
enforced; exposes the documented Postgres read model and an `entities`
change stream consumable via `streamclient`; and has the dashboards/runbooks
to operate it. The API, derivation rules, git worker, and SPA cutover are
separate follow-on projects consuming this contract.
