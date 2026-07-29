# ghsync — Stacked PR Work Dashboard: Build Plan

A full-stack React app implementing the Claude Design prototype in
`Stacked PR Dashboard.dc.html`, built on top of GitHub's Stacked PRs feature
(`gh-stack`, currently private preview — docs: https://github.github.com/gh-stack/).

---

## 1. What the prototype is

A triage dashboard ("ghsync") that answers one question: **what is my single
highest-leverage move across all my stacks and loose PRs right now?**

### Surfaces

- **Header**: org switcher, scope tabs (**Authored / Review requested /
  Participating**, with counts), lens toggle (**Board / Focus**), avatar.
- **Board lens**: 4 columns —
  1. **Your move** — root blockers only you can clear (failing required check,
     changes-requested review, review requested of you)
  2. **Maintenance** — stack shape: rebases & conflicts
  3. **Ready to land** — everything green, no auto-merge, waiting on the human
  4. **Waiting on others** — reviewers, CI, other teams; with a separate
     **"AI code review"** sub-group for PRs gated on a required AI review
- **Card anatomy**: stack glyph (one colored segment per PR, bottom-up; merged =
  violet, blocker = red glow, attention = amber, ready = green, draft = dashed,
  waiting = gray; dependency-blocked layers rendered at 50% opacity), name,
  `repo · S-nnn · n/m merged` meta line, root-cause highlight line, optional
  **blast radius** line ("A fix rewrites 3 upstack branches · 14 checks re-run"),
  action buttons, live **agent chip** (spinner + link to run) when an agent is
  working the item.
- **Focus lens**: left rail ranked **by leverage** (each item: status dot, name,
  chip like "Unblocks 3" / "1-click · unblocks 3" / "Lands 1 now" / "Local work"
  / "Not your move", meta line); detail pane with crumb, headline, subhead,
  badges, **Diagnosis** table (key/value rows with badges like Required /
  CODEOWNERS / Blocking), action buttons, "Meanwhile, in parallel" and
  "If you fix this / Blast radius" lists, and a **stack ladder** card
  (top → trunk, FRONTIER tag on the lowest unmerged PR, per-PR state chip,
  "blocked by ↓" dep chips, merged count / "n merged hidden", trunk row).
- **Action modals** (every mutating action shows a preview first):
  - **Merge preview** — what lands now (largest contiguous ready prefix),
    what happens after (retarget, auto-rebase, next root blocker). Atomic.
  - **Rebase preview** — force-pushed branches, checks re-run, approvals that
    may be dismissed.
  - **Nudge / Assign reviewer** — checked-first dedupe note, side effects,
    done-when; optional Slack ping checkbox.
  - **Agent run preview** — intent, **Context** checklist (attachments the
    agent gets), editable **Prompt** textarea, optional **picker** with a code
    preview pane (select failing jobs, or select review comments to address),
    optional extra toggle, "Start agent run" confirm → agent chip appears on
    the item.
- **Toasts** for every confirm; footer states the product principles: *only the
  root cause is loud · dependency-blocked layers stay muted · clean rebase ≠
  conflict · branch-rewriting actions state their blast radius · no auto-merge:
  ready still needs you.*

### Visual system

Dark theme (#000 bg, #0a0a0a cards, 1px #1f1f1f borders, 10px radii), Geist +
Geist Mono, color tokens: red `#f76e6e`, amber `#f5a623`, green `#3fcf8e`,
gray `#8f8f8f`, violet `#9b7bff` (violet = merged + all AI/agent affordances).
Prototype props worth keeping as user settings: `defaultLens`,
`showMergedLayers`, `showAgentActions`.

### Scenario coverage in the mock data (our functional spec)

1. **S-142** — stack with failing required check on the ghsync PR; upstack
   layers dependency-blocked; flake-vs-code-failure judgment; delegate-fix agent
   with failing-job picker (required vs optional/flaky jobs).
2. **S-138** — all-ready stack that just needs a clean rebase (dry-run found no
   conflicts) — "1-click" maintenance; approval-dismissal warning derived from
   repo rules.
3. **S-131** — ready ghsync PR (merge largest ready prefix = 1) + CODEOWNER
   gate above it; nudge (dedupe-aware), direct-assign with suggestions, and an
   "AI: pick best reviewer" agent action.
4. **#4788** — loose ready PR (same vocabulary, no stack semantics).
5. **#4802 / #4761** — PRs human-approved but waiting on a required **AI code
   review** gate (the "AI code review" board sub-group).
6. **#4790** — loose PR with changes-requested; agent addresses selected review
   comments only, never resolves a thread without a change or an answer;
   questions are surfaced for the human, not guessed.
7. **S-129** — rebase **conflict** (web rebase disabled): local resolution via
   copyable CLI steps, or delegate to an agent that resolves in a checkout and
   posts the diff for sign-off.
8. **S-135** — review-requested scope: review layer 2 in dependency order;
   layer above still changing ("hold your pass"); agent **pre-review** is
   explicitly advisory and never satisfies a required approval.

---

## 2. What GitHub's APIs give us vs. what we must build

### 2.1 Available from GitHub today

**Stacks (gh-stack, private preview — repo must be enrolled):**

| Capability | API |
|---|---|
| Stack topology | REST `GET /repos/{o}/{r}/stacks` and `GET .../stacks/{stack_number}` → `id`, `number`, `base.ref`, `open`, `pull_requests[]` (bottom→top: `number`, `state`, `draft`, `merged_at`, `head.ref`, `head.sha`) |
| Stack on a PR | `stack` object on every REST PR payload (`id`, `number`, `size`, `position` (1 = bottom), `base.ref`, `base.sha`); GraphQL `PullRequest.stack` / `PullRequest.stackEntry` (read-only) |
| Create / extend / dissolve stacks | REST `POST .../stacks` (2–100 PRs, bottom→top), `POST .../stacks/{n}/add`, `POST .../stacks/{n}/unstack` |
| Freshness | `pull_request` webhooks carry the `stack` object on all lifecycle events; new `stacked` action when a PR joins a stack |
| Merge semantics | GitHub-native: branch protection evaluates against the **stack base**; merging a PR atomically lands it + everything below; partial merges retarget/auto-rebase the remainder; closing a middle PR blocks upstack; squash uses `git rebase --onto` to avoid artificial conflicts; **no auto-merge, no rule bypass** (both "coming soon"); merge queue evaluates PRs individually |
| Server-side cascading rebase | **UI-only** "Rebase Stack" button (unsigned commits); the CLI does it locally (`gh stack rebase/sync`, `push` = `--force-with-lease --atomic`). **No documented REST/GraphQL endpoint.** |
| Actions context | `github.event.pull_request.stack` available in workflows |

**Standard GitHub APIs (GA) covering the rest of the read model:**

- Scope tabs → Search: `is:open is:pr author:@me` / `review-requested:@me` /
  `involves:@me` (org-filtered).
- Reviews & threads → GraphQL `reviewDecision`, `reviews`, `reviewThreads`
  (`isResolved`, `path`, `line`, comment bodies) — powers "changes requested",
  "2 unresolved threads", and the comment picker.
- Checks → Checks API (check runs/suites per SHA, `required` via branch
  protection / rulesets), Statuses API; Actions API for workflow runs, **jobs,
  per-job logs, and annotations** — powers the Diagnosis table and failing-job
  picker.
- Review requests → `POST /pulls/{n}/requested_reviewers` (re-request = nudge;
  direct-assign an individual), `timeline` for "requested 2 days ago".
- CODEOWNERS → fetch/parse `CODEOWNERS` + GraphQL `suggestedReviewers`; repo
  rules via branch protection / rulesets APIs (`dismiss_stale_reviews`,
  required checks, required signatures → "web rebase allowed" style judgments).
- Merge → standard `PUT /pulls/{n}/merge` (stack semantics are server-side).
- Blame (reviewer suggestion) → GraphQL `Blame` on touched paths.

### 2.2 Gaps — APIs/services we must supply ourselves

1. **Work-item derivation engine** (the product): classify each stack/loose PR
   into board columns; find the **ghsync** (lowest unmerged PR) and the **root
   blocker**; compute the **largest contiguous ready prefix**; mute
   dependency-blocked layers; generate highlight lines, "meanwhile in parallel",
   and cascade text. Pure derivation over GitHub data — but nothing on GitHub
   computes it.
2. **Leverage ranking** ("Unblocks 3", rail order): scoring over the derived
   graph (PRs unblocked, one-click-ness, staleness, review latency).
3. **Dry-run rebase / conflict prediction** ("Dry-run: replays cleanly", "clean
   rebase ≠ conflict"): no GitHub API. Build a **git worker** that fetches the
   repo (bare/partial clone) and replays each layer with `git merge-tree`
   (git ≥ 2.38, in-memory, no checkout) bottom-up to classify *clean* vs
   *conflict* and name the conflicting files/commits.
4. **Rebase execution**: since there's no REST endpoint for the server-side
   "Rebase Stack", the worker performs the cascading rebase itself and
   force-pushes `--force-with-lease --atomic` (exactly what `gh stack push`
   does). Optionally shell out to the `gh stack` CLI inside the worker to stay
   behaviorally identical. Conflicted rebases are never auto-resolved — they
   route to the "local resolution / delegate to agent" path per the prototype.
5. **Blast-radius computation**: branches force-pushed, checks that will re-run
   (count check runs on affected SHAs), approvals that may be dismissed
   (approved reviews × repo `dismiss_stale_reviews`). Derived, but only by us.
6. **Failure diagnosis & flake judgment**: parse Actions job logs/annotations
   into "first failure at file:line"; keep per-check history to say "same
   assertion failed on 2 runs of this SHA — retrying won't help" vs "11% flaky
   — a retry may do". Requires our own **check-run history store**.
7. **Review-latency tracking** ("requested 2 days ago — no response"): timeline
   data + our own aggregates per reviewer/team (also feeds "4 reviews already
   in queue" load signal for reviewer suggestions).
8. **Reviewer suggestion**: CODEOWNERS ∩ blame overlap ∩ current load ∩
   timezone — our service (optionally agent-assisted).
9. **Agent runs**: the entire delegate/pre-review/pick-reviewer surface —
   run creation with locked scope (branches/PRs), context attachment (logs,
   diffs, threads), editable prompt, live status chips, run viewer. Built on
   the **Claude Agent SDK** with sandboxed checkouts; nothing on GitHub.
10. **AI code review gate**: the prototype treats "ai-code-review" as a
    required check. If the org uses one (e.g., a required GitHub App check),
    we just *display* it — detecting the sub-group means recognizing
    configured AI-check names (per-org setting).
11. **Slack integration** (optional pings/DMs on nudge/assign): Slack app +
    our endpoints.
12. **Persistence & freshness**: webhook ingestion + periodic reconciliation
    into our own store; GitHub has no "give me all my work, classified" query.

### 2.3 Preview-status risks

- gh-stack requires **per-repo waitlist enrollment**; API shapes may change.
  → Isolate all stack calls behind one adapter; ship a **degraded mode** that
  infers pseudo-stacks from branch-target chains (the same detection the
  GitHub UI banner uses) so the app works on non-enrolled repos.
- Webhooks don't (yet) fire for rebase/retarget/unstack operations —
  reconciliation polling covers the gap.
- No auto-merge / rule bypass exists yet — conveniently, the prototype's
  philosophy ("no auto-merge: ready still needs you") matches.

---

## 3. Architecture

```
┌────────────────────────────────────────────────────────────┐
│ React app (Vite + TS)                                      │
│ TanStack Router/Query · Tailwind · SSE live updates        │
└───────────────▲────────────────────────────────────────────┘
                │ REST + SSE (our API)
┌───────────────┴────────────────────────────────────────────┐
│ API server (Node + TS, Fastify)                            │
│  auth (GitHub App user OAuth) · REST · SSE fan-out         │
├────────────────────────────────────────────────────────────┤
│ Derivation engine (pure TS pkg): classification, ghsync, │
│ ready-prefix, leverage rank, blast radius — fully unit-    │
│ testable against fixture snapshots                         │
├──────────────┬──────────────────┬──────────────────────────┤
│ Sync workers │ Git worker       │ Agent runner             │
│ (BullMQ)     │ (ephemeral bare  │ (Claude Agent SDK,       │
│ webhooks +   │ clones, merge-   │ sandboxed checkout,      │
│ reconcile    │ tree dry-runs,   │ scope-locked, streams    │
│ + log parse  │ cascading rebase │ status)                  │
│              │ + atomic push)   │                          │
├──────────────┴──────────────────┴──────────────────────────┤
│ Postgres (canonical cache + derived + history)  ·  Redis   │
└───────────────▲────────────────────────────────────────────┘
                │ REST/GraphQL (installation + user tokens), webhooks
        ┌───────┴───────┐
        │    GitHub     │
        └───────────────┘
```

**Key decisions**

- **GitHub App** (not OAuth app): installation tokens for repo data/webhooks;
  user-to-server OAuth so merges/reviews/nudges are attributed to the actual
  user and pass CODEOWNER rules correctly. Fine-grained permissions: PRs (rw),
  checks (r), actions (r), contents (rw — rebase pushes), members (r).
- **Server-derived UI state**: the client renders what the API says; all
  classification/ranking/preview math lives server-side in the derivation
  engine so board and focus lenses can't disagree.
- **Previews are first-class**: every mutating endpoint has a sibling
  `POST …/preview` that returns the modal payload (sections, blast radius,
  confirm label). Confirm calls the mutating endpoint with the preview's id —
  guaranteeing "the thing you saw is the thing that runs" (and letting us
  invalidate previews when SHAs move underneath).
- **SSE over WebSockets** (one-directional updates; simpler infra).
- **Monorepo** (pnpm + Turborepo): `apps/web`, `apps/api`, `apps/workers`,
  `packages/engine`, `packages/github` (typed adapter incl. stacks endpoints +
  degraded-mode pseudo-stacks), `packages/ui`, `packages/shared` (zod DTOs).

### 3.1 Data model (Postgres, sketch)

- `orgs`, `repos` (+ rules snapshot: dismiss_stale_reviews, required checks,
  signatures), `users`
- `stacks` (gh id/number, repo, base_ref, open) · `stack_entries` (position,
  pr_id)
- `pull_requests` (number, title, author, draft, state, head/base ref+sha,
  review_decision, mergeable, updated_at)
- `review_threads` (resolved, path, line, author, excerpt) ·
  `review_requests` (reviewer|team, requested_at, re_requested_at)
- `check_runs` (pr sha, name, required, status, conclusion, started/completed,
  job ids) · `check_history` (name × repo aggregates → flake rate)
- `job_failures` (parsed: file, line, message, excerpt) — powers Diagnosis +
  picker code panes
- `dry_runs` (stack, target sha, result clean|conflict, conflict files, ts)
- `work_items` (derived, materialized: scope flags, column, root_cause kind,
  rank, chip, highlight, blast summary, ready_prefix) — rebuilt by the engine
  on any input change; history kept for "what changed" toasts
- `agent_runs` (work_item, kind, scope lock: repo+branches+PRs, prompt,
  context selections, status, log ref, result summary)
- `previews` (id, kind, payload, computed_at, input shas — invalidated on
  drift), `action_log` (who did what via the dashboard)
- `settings` (per user: default lens, show merged layers, show agent actions;
  per org: AI-check names, Slack workspace)

### 3.2 Our API surface (v1)

```
GET  /api/me · GET /api/orgs
GET  /api/work?scope=authored|review|participating   → board + rail (ranked)
GET  /api/work/:id                                   → focus payload
       (headline, badges, diagnosis rows, ladder, parallel/cascade, actions)
GET  /api/stream?scope=…                             → SSE deltas

POST /api/work/:id/actions/:action/preview           → modal payload
POST /api/work/:id/actions/:action                   → { previewId, options }
       actions: merge | rebase | nudge | assign
POST /api/agent-runs                                 → { workItemId, kind,
       prompt, context[], selections[] (jobs|comments), options }
GET  /api/agent-runs/:id · GET /api/agent-runs/:id/stream

GET  /api/prs/:id/failing-jobs                       → picker items + excerpts
GET  /api/prs/:id/review-comments?unresolved=1       → picker items + code
GET  /api/prs/:id/reviewer-suggestions               → owners × blame × load

POST /webhooks/github                                 (HMAC-verified)
```

### 3.3 Derivation engine rules (from the prototype)

- **ghsync** = lowest unmerged PR in the stack.
- **Root blocker** = first gate on the ghsync, priority: required check
  failing → changes requested → your review requested → conflict/non-linear →
  CODEOWNER/review missing → CI running/queued → AI gate. Everything above the
  ghsync with an unmet dependency is **dependency-blocked** (muted, never a
  card of its own; "blocked by ↓").
- **Columns**: root blocker actionable *by the viewer* → **Your move**; shape
  problems (non-linear, conflict) with otherwise-ready layers → **Maintenance**;
  ready prefix ≥ 1 → **Ready to land**; else **Waiting on others**
  (sub-group AI when the sole gate is a configured AI check). One effort can
  emit cards in multiple columns (S-131 does).
- **Drafts** are excluded from merge math; ladder shows them dashed.
- **Ranking**: unblock count desc, then one-click-ness, then ready-lands,
  then staleness; "Not your move" items sink.
- **Chips**: `Unblocks N` (dep-blocked PRs behind the root blocker),
  `1-click · unblocks N` (clean dry-run), `Lands N now` (ready prefix),
  `Local work` (conflict), `Not your move` (automated gate running).

---

## 4. Delivery phases

### Phase 0 — Foundations (≈1 wk)
Monorepo scaffold; GitHub App (dev org enrolled in stacks preview); OAuth
sign-in; webhook receiver + HMAC; Postgres/Redis; CI; typed GitHub adapter with
stacks endpoints + recorded fixtures.

### Phase 1 — Read-only dashboard (≈2–3 wks)  ← first usable value
Sync pipeline (search by scope → PRs → stacks → reviews/threads → checks →
rules; webhook-driven + reconcile). Derivation engine v1 with fixture tests
reproducing all 8 prototype scenarios. React shell: header/scope tabs/lens
toggle, Board lens (columns, cards, glyphs, blast lines), Focus lens (rail,
diagnosis, ladder), pixel-matched to the prototype (dark theme, Geist,
tokens as CSS vars). All actions deep-link to GitHub. SSE live updates.
Settings: defaultLens, showMergedLayers.

### Phase 2 — Safe actions with previews (≈2 wks)
Preview/confirm framework (drift invalidation). **Merge** (ready-prefix
preview → `PUT /merge` as the user, atomic, post-merge cascade text).
**Nudge** (dedupe-aware re-request) and **Assign reviewer** (suggestions:
CODEOWNERS ∩ blame ∩ load). Failing-job Diagnosis: Actions log fetch + parser
(first-failure extraction), flake history baseline. Action log + toasts.

### Phase 3 — Maintenance: dry-run + rebase (≈2–3 wks)
Git worker (ephemeral bare clones, `git merge-tree` replay bottom-up) →
`dry_runs`; "clean rebase ≠ conflict" split on the board. Rebase execution:
cascading rebase + `--force-with-lease --atomic` push (or `gh stack` CLI in
the worker), with blast-radius preview (branches, checks re-run, approvals at
risk from repo rules). Conflict path: copyable CLI steps card. Guardrails:
lease on pre-preview SHAs, single in-flight op per stack, action log.

### Phase 4 — Agent integration (≈3 wks)
**Decision: the user has an existing agent runner; for this build it is mocked.**
We build the full UI/API surface against an `AgentRunner` interface
(`start/cancel/status`) whose mock implementation scripts state transitions
and posts them to `POST /webhooks/agent-runner` — the same ingress the real
runner will use, so swapping it in later is a config change. Run kinds: **fix failing check** (job picker), **address review comments**
(comment picker; never resolve without change/answer; questions skipped for
the human), **resolve rebase conflict** (posts diff for sign-off), **rebase
shepherd**, **pre-review** (advisory; never submits a GitHub review),
**pick reviewer**. Agent-run preview modal (context checklist, editable
prompt, pickers with code panes); live chips via SSE; run viewer page.
Settings: showAgentActions.

### Phase 5 — Polish & breadth (ongoing)
AI-check gate recognition (per-org config) + "AI code review" sub-group;
Slack app (nudge/DM options); review-latency + reviewer-load aggregates;
ranking tuning; degraded pseudo-stack mode for non-enrolled repos; multi-org;
keyboard nav; empty/loading states; telemetry.

---

## 5. Stack summary

| Layer | Choice |
|---|---|
| Frontend | React 19, Vite, TypeScript, TanStack Router + Query, Tailwind (dark tokens from prototype), Geist/Geist Mono |
| API | Node 22 + Fastify, zod DTOs shared with the client, SSE |
| Jobs | BullMQ + Redis |
| DB | Postgres (Drizzle) |
| GitHub | GitHub App (installation + user-to-server tokens), REST + GraphQL, webhooks; stacks REST adapter with degraded mode |
| Git ops | Ephemeral worker containers, `git merge-tree` dry-runs, `--force-with-lease --atomic` pushes, optional `gh stack` CLI |
| Agents | Claude Agent SDK, sandboxed checkouts, scope-locked credentials |
| Testing | Engine: fixture snapshots of all 8 prototype scenarios; API: recorded GitHub cassettes; E2E: Playwright against a seeded enrolled test repo |

## 6. Decisions & open questions

**Decided:**

1. The target org **is enrolled** in the stacks private preview — the real
   Stacks API is available from day one (degraded pseudo-stack mode becomes a
   nice-to-have, not a requirement).
2. Agent runner: **mocked** for this build behind the `AgentRunner` interface;
   the user's existing runner plugs in later via `/webhooks/agent-runner`.
3. Credentials: the browser **never** holds GitHub tokens. All GitHub calls go
   through our API, which injects the right token per call (user-to-server
   token for attributed mutations, installation token for reads and git
   pushes). Tokens are encrypted at rest server-side; the client holds only an
   httpOnly session cookie. If raw passthrough is ever needed, it will be a
   narrow allowlisted `/api/github-proxy/...`, not token exposure.

**Open:**

1. Which AI code-review product should the "AI review gate" recognize
   (check-run name), and is it a required check in the target repos?
2. Slack integration in scope for v1, or defer (prototype treats it as an
   optional checkbox)?
3. Single-tenant (one org) or multi-tenant from the start? (Auth/token and
   webhook routing complexity differs.)
