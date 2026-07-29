# Opus 5 Review — Budget Gate / GitHub Clients / Fake GitHub

Scope: internal/budget, internal/gh, internal/fakegithub,
db/queries/installation_budgets.sql and related migrations.

## 1. QUALITY (correctness / design)

**MAJOR — `internal/budget/gate.go:594-598`: a failed backoff persist kills the gate but keeps renewing the lease, so the installation never fails over.**
When `SaveBackoff` fails (including transient pgx errors), `g.loseLease(...)` sets `unavailable = ErrLeaseLost` permanently, but — unlike the other three `loseLease` call sites — does **not** call `runtime.cancel()`. `maintainLease` keeps renewing, `lease_until` stays fresh, every other process gets `ErrLeaseHeld` at startup and exits, and this process refuses 100% of GitHub calls with no recovery path. Silent, permanent, un-failoverable loss of all fetch/sweep/drift; nearly invisible (rejected admissions never fire `onRequest`), noticed only by slow indirect alerts.
*Fix:* treat transport-level `SaveBackoff` failure as retryable (keep in-memory closure, re-persist on next snapshot tick), reserve `loseLease` for `ok == false`; in either case call `g.lease.cancel()` alongside `loseLease` so the lease lapses and a replacement can acquire.

**MAJOR — `internal/budget/gate.go:322-325`: the C-B6 concurrency ceiling has no class priority; a `sweep` burst can starve `interactive` indefinitely.**
C-B3 headroom is enforced only against the rate budget; the concurrency check is class-blind and waiters wake via broadcast `close(g.changed)` with no ordering. A reconnect storm of `sweep` fetches (the C-P4 "500 stale PRs" scenario) can hold all 40 slots while a cold-start `interactive` backfill makes no progress. No test.
*Fix:* reserve a share of `MaxConcurrent` per class (mirror `floorFor`), or per-class wait queues drained interactive → event → sweep.

**MINOR — `gate.go:157-160`: nested `Auth` requests bypass the global secondary-limit closure and unavailable/lease checks.** Production token renewal always takes this path, so C-B2's "gate closes for the whole installation" has a permanent hole for the token endpoint. *Fix:* keep slot reuse but consult `unavailable`/`backoffUntil` under lock; document the exemption.

**MINOR — `gate.go:23-24`: C-B3 floors are compile-time constants; `RESTLimit`/`GraphQLLimit`/`MaxConcurrent`/`SecondaryLimitFallback` exist as Options but main.go never wires them to config.** *Fix:* add floors to Options; plumb all five through config + DEPLOYMENT.md.

**MINOR — `internal/gh/rest.go:444-449`: `RESTResponse.ETag` can be empty on a 304; `internal/sweep/sweep.go:705` stores it back, turning every later sweep page into a full 200** (the exact C-B4 regression the >80% alert is meant to catch). `fetch/handler.go:147` gets it right, showing the contract is ambiguous. *Fix:* in `getJSON`, fall back to the outgoing `If-None-Match` on ETag-less 304s.

**MINOR — `installation_budgets.sql:80` vs `:100-105`: the periodic snapshot overwrites `backoff_until` unconditionally while `SaveBackoff` guards with a max** — safe only via an accidental in-memory monotonic invariant; `Close` makes this the last write before handoff. *Fix:* same CASE guard in the snapshot query.

**MINOR — `lease.go:316-322`: `renewRetryDelay` returns 0 once confirmed expiry passes** → zero-delay renew loop hammering Postgres until the watchdog wins the select race. *Fix:* floor the retry, return early when remaining ≤ 0.

**NIT —** `gated.HTTP.Body.Close()` without nil-Body guard at rest.go:439, graphql.go:432, token.go:213, deliveries.go:102 (panics for bodyless fakes). `decodeHTTPError` puts up to 1 MiB of body into `HTTPError.Message`.

## 2. TESTING GAPS

**MAJOR — `.github/workflows/ci.yml:36`: CI runs `go test ./...` with no `-race`, anywhere** — in the most concurrency-sensitive area of the codebase. Everything passes under `-race` locally today, making adding it free.

**MAJOR — fake fixture data race:** `fakegithub.go:669-678` (also 766-809, 811-889, 891-919) takes a shallow `Fixture` copy under the mutex then reads slice *elements* outside it, while `applySoakMutation` (552-587) mutates those elements in place under the mutex — a genuine race between in-flight reads and control-plane mutations, i.e., the soak's steady state. Invisible because soak-smoke also runs without `-race`. `WithRequestHook` hands out `*Fixture` under the same lock inviting the same pattern. *Fix:* deep-copy served slices in `checkRepo` or copy-on-write mutation.

**MAJOR — the installation-token endpoint short-circuits the mux, bypassing `beginRequest`/`endRequest` and `nextRate`** (fakegithub.go:419-422), so the fake cannot observe token exchanges in `MaxConcurrent()` and never budget-charges them. `TestConcurrencyCeiling` proves nothing about auth requests vs C-B6; the nested-admission design is untestable through the fake. *Fix:* route `/app/installations/*` through concurrency accounting; add a ceiling test forcing renewal mid-burst.

**MAJOR — `manualClock` lost-wakeup window can hang tests forever:** `waitForChange` computes delay then re-reads now inside `NewTimer`; an `Advance` between the two yields a deadline past the intended one with nothing else waking the waiter. `TestSecondaryLimitClosesGateGloballyForRetryAfter` has no timeout guard, so failure mode is a 10-minute panic. Same non-atomic pattern at lease.go:275, 288-290. *Fix:* deadline-based `NewTimerAt(time.Time)`; add a select guard to that test.

**MAJOR — neither `/app/hook/deliveries` nor `/graphql` validates `Authorization` at all**; wiring `DeliveriesClient` with the wrong `TokenProvider` would pass every test; `gh.AppTokens` has 0% coverage in-package. *Fix:* fake requires valid App JWT on deliveries endpoints, `fake-installation-*` bearer elsewhere.

**MINOR —** the only Postgres lease test is single-threaded (the `FOR UPDATE`-CTE pattern exists solely to survive concurrent steal — never exercised); no partial-row rollback test. C-B5 point accounting effectively untested (fake hardcodes cost 1; floors tested REST-only; GraphQL floor path never taken). 429 secondary limit and HTTP-date Retry-After never exercised end-to-end. `total_count` computed after pagination (trap). Busy-wait test helpers burn a core.

## 3. CODE QUALITY

`nextPage`/`nextCursor` duplicate a 15-line Link parser, both called per response. `fakegithub.go` is 1,657 lines/one 24-field struct — split into server/rest/graphql/ratelimit/webhooks. `rateBudget`/`rateSnapshot` field-identical. `nextRate` and `tryAdmit` return six unnamed values each — want result structs. `manualClock` copy-pasted between packages (move to `internal/clocktest`); fake↔gh type mirroring is defensible, the clock is not. `DeliveriesClient` embeds full `RESTClient` by value to reach `getJSON` — make it a method on the shared client type. `s.authorizations` grows unbounded (soak leak). `health` ignores `r`.

## 4. MISSING DOCUMENTATION

**The most important gap: `Gate.Do`'s admission protocol is documented nowhere.** The C-B6 slot releases only at body EOF/Close, so a caller forgetting `Body.Close()` burns a slot *permanently* — 40 leaks deadlock the installation with no timeout and no metric. Also undocumented: `Do` can return non-nil `*Response` with non-nil error (caller must still close); redirects disabled by design; `Auth` may send from inside an existing admission. Belongs on the `Do` doc comment and SYNC_ENGINE §6.

**`LeaseStore` boolean contract undefined:** `false` must mean *proven* loss of ownership; transport failures must be errors — a future implementer returning `false` on transport failure gets the gate permanently killed (see MAJOR above).

**Undocumented exported symbols:** request constructors, `ErrClosed`/`ErrLeaseLost`, `NewPostgresLeaseStore` + methods; the entire `RESTClient` surface, GraphQL types, token types, deliveries client. Highest-value: `GetRepository`/`GetStack`/`GetPull` return `(nil, response, nil)` on 304 — an undocumented nil-deref trap differing from list methods.

**Fake-GitHub scripting API has no reference** (options, panics, rate-step consumption semantics, 304-refund suppression, token-endpoint exemptions, newest-first ordering, in-package-only webhook client). A `doc.go` would pay for itself immediately.

**`ops/DEPLOYMENT.md` missing every budget knob:** 40 ceiling, 20%/10% floors, 60s fallback, 15000/5000 limits, and the 30-second lease TTL/10s renew — the number that bounds the stop/start handoff the topology section is built around.

## Overall assessment

A well-built area. The gate's core mechanics are unusually careful: authoritative headers with correct same-window merge, slot tied to body lifetime, admission-time fail-closed lease check, the nested-admission escape solving a real deadlock, `Close` renewing through the drain. The lease SQL uses the right pattern; the fake models things most fakes get wrong. Two-thirds of findings are polish.

The two that matter: the lease bug converts one transient DB error into a permanent, un-failoverable, near-silent outage (one call site forgot the `runtime.cancel()` its three siblings perform); the class-blind ceiling undercuts C-B3 under exactly the load C-P4 anticipates. The conformance suite is meaningful but its blind spots cluster: token endpoint exempt from both mechanisms C-B1/C-B6 are about, no Authorization checks, GraphQL cost always 1. Adding `-race` to CI is the cheapest single improvement and would immediately surface the fixture race.
