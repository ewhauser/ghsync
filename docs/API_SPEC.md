# Frontier Backend — gRPC API Specification (AIP style)

Draft v0.1. Defines the gRPC surface for the Frontier stacked-PR dashboard
backend, following Google's API Improvement Proposals (AIPs). The SPA consumes
these via HTTP/JSON transcoding (AIP-127); the current MSW mock endpoints map
1:1 onto these methods (mapping table at the end).

Package: `frontier.v1`

---

## 1. Design notes

- **Almost everything is derived and read-only.** Stacks, pull requests, and
  work items are projections of GitHub state computed by the derivation
  engine. They are output-only resources: no Create/Update/Delete, just
  Get/List/Watch (AIP-121 exception for computed resources; fields annotated
  `OUTPUT_ONLY` per AIP-203).
- **Every mutation is preview-then-confirm.** A `Preview` is a first-class
  resource created against a work item action; executing it is a custom method
  (AIP-136) that returns a long-running operation (AIP-151). Executing a stale
  preview fails with `FAILED_PRECONDITION` (AIP-193) carrying a
  `PreconditionFailure` detail naming the drifted SHA.
- **Work items are per-viewer.** Classification ("your move") depends on who is
  looking, so `WorkItem` lives under `users/{user}` and requests normally use
  the authenticated alias `users/me` (AIP-159-style alias, resolved
  server-side).
- **GitHub mirrors keep GitHub's numbering.** Stack and PR resource IDs are the
  repo-scoped numbers GitHub assigns, so names are stable and deep-linkable.
- Standard fields follow AIP-142 (`create_time`, `update_time`, `expire_time`
  as `google.protobuf.Timestamp`), pagination follows AIP-158, filtering
  AIP-160, resource names AIP-122.

## 2. Resource hierarchy

```
users/{user}                                   # alias: users/me (existing UserService)
users/{user}/workItems/{work_item}             # derived, per-viewer
users/{user}/workItems/{work_item}/previews/{preview}
orgs/{org}                                     # GitHub App installation
orgs/{org}/repos/{repo}
orgs/{org}/repos/{repo}/stacks/{stack}         # {stack} = GitHub stack number
orgs/{org}/repos/{repo}/stacks/{stack}/dryRuns/{dry_run}
orgs/{org}/repos/{repo}/pullRequests/{pull_request}   # {pull_request} = PR number
```

**Out of scope — existing services.** User identity/settings and agent runs are
served by APIs that already exist; this spec does not define them. Frontier
references them by resource name (see §2.1) rather than owning them.

### 2.1 Integration with the existing agent-run and user APIs

- `WorkItem.agent_run` holds the resource name of a run in the existing
  agent-runner API; the derivation engine subscribes to that API's run state to
  light up chips and to gate one-run-per-stack concurrency.
- The **run plan** the agent dialog renders (intent, context attachments,
  default prompt, job/comment pickers) is Frontier-derived data, so it stays in
  this spec as `WorkItemService.GenerateAgentRunPlan`. The client generates a
  plan here, then creates the run through the existing agent API, passing the
  plan's scope lock and attachments through whatever fields that API defines
  (open question #4).

---

## 3. WorkItemService

The core read surface: the derivation engine's output.

```proto
service WorkItemService {
  // AIP-132. Filter (AIP-160): scope = "AUTHORED"; ordering is always
  // leverage rank (order_by unsupported; the ranking IS the product).
  rpc ListWorkItems(ListWorkItemsRequest) returns (ListWorkItemsResponse) {
    option (google.api.http) = { get: "/v1/{parent=users/*}/workItems" };
  }

  rpc GetWorkItem(GetWorkItemRequest) returns (WorkItem) {
    option (google.api.http) = { get: "/v1/{name=users/*/workItems/*}" };
  }

  // Change stream for live board updates (replaces the SPA's SSE endpoint).
  // Server-streaming watch; first responses replay current state, then deltas.
  rpc WatchWorkItems(WatchWorkItemsRequest) returns (stream WorkItemChange) {
    option (google.api.http) = { get: "/v1/{parent=users/*}/workItems:watch" };
  }

  // AIP-136, side-effect free. Computes the plan for a delegated-agent action:
  // intent, context attachments, default prompt, scope lock, and the
  // job/comment picker. The run itself is then created through the EXISTING
  // agent-runner API (see §2.1); this method only derives what to send it.
  rpc GenerateAgentRunPlan(GenerateAgentRunPlanRequest) returns (AgentRunPlan) {
    option (google.api.http) = {
      post: "/v1/{work_item=users/*/workItems/*}:generateAgentRunPlan" body: "*" };
  }
}

message GenerateAgentRunPlanRequest {
  string work_item = 1 [(google.api.field_behavior) = REQUIRED];
  ActionKind kind = 2 [(google.api.field_behavior) = REQUIRED];
      // DELEGATE_FIX | PRE_REVIEW | PICK_REVIEWER
}

// Mirrors today's AgentPreviewDto: intent, context entries, default_prompt,
// optional picker (unit JOBS | COMMENTS, items with code excerpts and
// selected_by_default), optional toggle, plus the computed scope_lock
// (repos/branches the run may push) — all OUTPUT_ONLY.
message AgentRunPlan { /* … */ }

message WorkItem {
  option (google.api.resource) = {
    type: "frontier.example.com/WorkItem"
    pattern: "users/{user}/workItems/{work_item}"
  };

  string name = 1 [(google.api.field_behavior) = IDENTIFIER];
  string display_name = 2 [(google.api.field_behavior) = OUTPUT_ONLY];

  // Reference to the mirrored stack, or empty for a loose PR (AIP-122 §refs).
  string stack = 3 [(google.api.resource_reference) = {
    type: "frontier.example.com/Stack" }, (google.api.field_behavior) = OUTPUT_ONLY];
  string repo = 4 [(google.api.resource_reference) = {
    type: "frontier.example.com/Repo" }, (google.api.field_behavior) = OUTPUT_ONLY];

  repeated WorkScope scopes = 5 [(google.api.field_behavior) = OUTPUT_ONLY];
  int32 rank = 6 [(google.api.field_behavior) = OUTPUT_ONLY];
  Leverage leverage = 7 [(google.api.field_behavior) = OUTPUT_ONLY];
  Severity severity = 8 [(google.api.field_behavior) = OUTPUT_ONLY];
  string rail_meta = 9 [(google.api.field_behavior) = OUTPUT_ONLY];

  repeated StackEntry entries = 10 [(google.api.field_behavior) = OUTPUT_ONLY];
  repeated Card cards = 11 [(google.api.field_behavior) = OUTPUT_ONLY];
  Focus focus = 12 [(google.api.field_behavior) = OUTPUT_ONLY];

  // Present while an agent run is active on this item. Resource name in the
  // EXISTING agent-runner API (not defined in this spec).
  string agent_run = 13 [(google.api.field_behavior) = OUTPUT_ONLY];

  google.protobuf.Timestamp update_time = 14
      [(google.api.field_behavior) = OUTPUT_ONLY];
}

enum WorkScope { WORK_SCOPE_UNSPECIFIED = 0; AUTHORED = 1; REVIEW_REQUESTED = 2; PARTICIPATING = 3; }
enum Severity  { SEVERITY_UNSPECIFIED = 0; NEUTRAL = 1; INFO = 2; SUCCESS = 3; WARNING = 4; DANGER = 5; }
enum BoardColumn { BOARD_COLUMN_UNSPECIFIED = 0; YOUR_MOVE = 1; MAINTENANCE = 2; READY_TO_LAND = 3; WAITING_ON_OTHERS = 4; }

message Leverage {
  LeverageKind kind = 1;
  int32 count = 2;  // set for UNBLOCKS / ONE_CLICK_UNBLOCKS / LANDS_NOW / LANDS_AFTER_FIX
}

message Card {
  string card_id = 1;
  BoardColumn column = 2;
  CardGroup group = 3;            // PEOPLE | AI_REVIEW
  Severity severity = 4;
  string highlight = 5;           // engine prose
  string layer_note = 6;
  string blast_radius = 7;
  repeated Action actions = 8;
}

message Action {
  ActionKind kind = 1;            // MERGE, REBASE, NUDGE, ASSIGN, DELEGATE_FIX,
                                  // PRE_REVIEW, PICK_REVIEWER, START_REVIEW,
                                  // VIEW_LOGS, OPEN_THREADS, VIEW_AI_RUN,
                                  // OPEN_ON_GITHUB, COPY_CLI_STEPS
  Emphasis emphasis = 2;
  int32 target_number = 3;        // PR number for labels like "Merge #5118"
  string uri = 4;                 // deep link for navigation kinds
  string copy_text = 5;           // clipboard payload for COPY_CLI_STEPS
}

message ListWorkItemsRequest {
  string parent = 1 [(google.api.field_behavior) = REQUIRED];  // users/me
  int32 page_size = 2;
  string page_token = 3;
  string filter = 4;              // e.g. scope = "AUTHORED"
}

message ListWorkItemsResponse {
  repeated WorkItem work_items = 1;
  string next_page_token = 2;
  int32 total_size = 3;           // AIP-132; tab counts come from filtered
                                  // List calls with page_size = 0
}

message WorkItemChange {
  enum ChangeType { CHANGE_TYPE_UNSPECIFIED = 0; UPSERTED = 1; DELETED = 2; }
  ChangeType change_type = 1;
  WorkItem work_item = 2;         // name only when DELETED
}
```

`Focus` carries the detail pane: `headline`, `subheadline`, `badges`,
`diagnostics` (label/value/badge rows), `actions` — all output-only engine
prose, mirroring today's `WorkItemFocusDto`.

---

## 4. PreviewService

The preview-then-confirm contract for `MERGE`, `REBASE`, `NUDGE`, `ASSIGN`.

```proto
service PreviewService {
  // AIP-133. Computes the preview against current GitHub state and pins the
  // input SHAs. Previews expire (expire_time) and are single-use.
  rpc CreatePreview(CreatePreviewRequest) returns (Preview) {
    option (google.api.http) = {
      post: "/v1/{parent=users/*/workItems/*}/previews" body: "preview" };
  }

  rpc GetPreview(GetPreviewRequest) returns (Preview) {
    option (google.api.http) = { get: "/v1/{name=users/*/workItems/*/previews/*}" };
  }

  // AIP-136 + AIP-151. Executes exactly what the preview showed. Fails with
  // FAILED_PRECONDITION (PreconditionFailure detail) if any pinned SHA moved,
  // the preview expired, or was already executed. The operation's metadata is
  // ExecutePreviewMetadata; response is ExecutePreviewResponse.
  rpc ExecutePreview(ExecutePreviewRequest) returns (google.longrunning.Operation) {
    option (google.api.http) = {
      post: "/v1/{name=users/*/workItems/*/previews/*}:execute" body: "*" };
    option (google.longrunning.operation_info) = {
      response_type: "ExecutePreviewResponse"
      metadata_type: "ExecutePreviewMetadata"
    };
  }
}

message Preview {
  option (google.api.resource) = {
    type: "frontier.example.com/Preview"
    pattern: "users/{user}/workItems/{work_item}/previews/{preview}"
  };

  string name = 1 [(google.api.field_behavior) = IDENTIFIER];
  ActionKind action = 2 [(google.api.field_behavior) = REQUIRED | IMMUTABLE];

  string title = 3  [(google.api.field_behavior) = OUTPUT_ONLY];
  string intent = 4 [(google.api.field_behavior) = OUTPUT_ONLY];
  repeated PreviewSection sections = 5 [(google.api.field_behavior) = OUTPUT_ONLY];
  PreviewOption option = 6 [(google.api.field_behavior) = OUTPUT_ONLY];  // e.g. Slack ping

  // SHAs of every branch the action reads or rewrites; the execute-time
  // precondition (conceptually an AIP-154 etag over external state).
  repeated BranchPin pins = 7 [(google.api.field_behavior) = OUTPUT_ONLY];

  google.protobuf.Timestamp create_time = 8 [(google.api.field_behavior) = OUTPUT_ONLY];
  google.protobuf.Timestamp expire_time = 9 [(google.api.field_behavior) = OUTPUT_ONLY];
}

message ExecutePreviewRequest {
  string name = 1 [(google.api.field_behavior) = REQUIRED];
  bool option_selected = 2;       // the preview's single optional toggle
}
```

Merge/nudge/assign complete fast but still return LROs for a uniform client
contract; rebase genuinely runs long (git worker: cascading rebase +
`--force-with-lease --atomic` push).

---

## 5. StackService & DryRunService

Read mirrors of gh-stack state, plus conflict prediction.

```proto
service StackService {
  rpc GetStack(GetStackRequest) returns (Stack);      // AIP-131
  rpc ListStacks(ListStacksRequest) returns (ListStacksResponse);  // AIP-132; filter: open = true
}

service DryRunService {
  // AIP-133 + AIP-151. Runs `git merge-tree` replay bottom-up in the git
  // worker. Normally triggered by the sync pipeline; exposed for manual re-runs.
  rpc CreateDryRun(CreateDryRunRequest) returns (google.longrunning.Operation);
  rpc GetDryRun(GetDryRunRequest) returns (DryRun);
  rpc ListDryRuns(ListDryRunsRequest) returns (ListDryRunsResponse);  // newest first
}

message Stack {
  option (google.api.resource) = {
    type: "frontier.example.com/Stack"
    pattern: "orgs/{org}/repos/{repo}/stacks/{stack}"
  };
  string name = 1 [(google.api.field_behavior) = IDENTIFIER];
  string base_ref = 2 [(google.api.field_behavior) = OUTPUT_ONLY];
  bool open = 3 [(google.api.field_behavior) = OUTPUT_ONLY];
  // Bottom → top, mirroring GitHub's Stacks API entry order.
  repeated string pull_requests = 4 [(google.api.resource_reference) = {
    type: "frontier.example.com/PullRequest" }, (google.api.field_behavior) = OUTPUT_ONLY];
  google.protobuf.Timestamp update_time = 5 [(google.api.field_behavior) = OUTPUT_ONLY];
}

message DryRun {
  option (google.api.resource) = {
    type: "frontier.example.com/DryRun"
    pattern: "orgs/{org}/repos/{repo}/stacks/{stack}/dryRuns/{dry_run}"
  };
  string name = 1 [(google.api.field_behavior) = IDENTIFIER];
  string target_sha = 2 [(google.api.field_behavior) = OUTPUT_ONLY];
  Result result = 3 [(google.api.field_behavior) = OUTPUT_ONLY];  // CLEAN | CONFLICT
  repeated ConflictFile conflicts = 4 [(google.api.field_behavior) = OUTPUT_ONLY];
  google.protobuf.Timestamp create_time = 5 [(google.api.field_behavior) = OUTPUT_ONLY];
}
```

---

## 6. PullRequestService

Mirrored PR state plus the review-request side effects that don't need the
preview flow internals (they still go through PreviewService when triggered
from a work item — these are the underlying operations).

```proto
service PullRequestService {
  rpc GetPullRequest(GetPullRequestRequest) returns (PullRequest);
  rpc ListPullRequests(ListPullRequestsRequest) returns (ListPullRequestsResponse);

  // AIP-136 custom methods (side effects on GitHub, attributed to the caller).
  // Dedupe-aware: bumps an existing open request instead of duplicating.
  rpc RequestReview(RequestReviewRequest) returns (RequestReviewResponse) {
    option (google.api.http) = {
      post: "/v1/{name=orgs/*/repos/*/pullRequests/*}:requestReview" body: "*" };
  }

  // Computed, read-only: CODEOWNERS ∩ blame overlap ∩ review load ∩ timezone.
  rpc SuggestReviewers(SuggestReviewersRequest) returns (SuggestReviewersResponse) {
    option (google.api.http) = {
      get: "/v1/{name=orgs/*/repos/*/pullRequests/*}:suggestReviewers" };
  }

  // Parsed failure diagnostics for the agent-run job picker and Diagnosis pane.
  rpc ListJobFailures(ListJobFailuresRequest) returns (ListJobFailuresResponse);

  // Unresolved review threads for the agent-run comment picker.
  rpc ListReviewThreads(ListReviewThreadsRequest) returns (ListReviewThreadsResponse);
}

message PullRequest {
  option (google.api.resource) = {
    type: "frontier.example.com/PullRequest"
    pattern: "orgs/{org}/repos/{repo}/pullRequests/{pull_request}"
  };
  string name = 1 [(google.api.field_behavior) = IDENTIFIER];
  string title = 2 [(google.api.field_behavior) = OUTPUT_ONLY];
  PrState state = 3 [(google.api.field_behavior) = OUTPUT_ONLY];   // engine health state
  string state_note = 4 [(google.api.field_behavior) = OUTPUT_ONLY];
  bool dependency_blocked = 5 [(google.api.field_behavior) = OUTPUT_ONLY];
  string head_ref = 6 [(google.api.field_behavior) = OUTPUT_ONLY];
  string head_sha = 7 [(google.api.field_behavior) = OUTPUT_ONLY];
  string stack = 8 [(google.api.resource_reference) = {
    type: "frontier.example.com/Stack" }, (google.api.field_behavior) = OUTPUT_ONLY];
  int32 stack_position = 9 [(google.api.field_behavior) = OUTPUT_ONLY];  // 1 = bottom
  google.protobuf.Timestamp update_time = 10 [(google.api.field_behavior) = OUTPUT_ONLY];
}

message JobFailure {
  string job_id = 1;
  string title = 2;               // "integration-suite · shard 3"
  string check_name = 3;
  bool required = 4;
  string file = 5;
  int32 line = 6;
  repeated string excerpt = 7;    // log lines for the code pane
  FlakeJudgment flake = 8;        // DETERMINISTIC | LIKELY_FLAKE + rate
}
```


## 7. Long-running operations & errors

- One `google.longrunning.Operations` service (AIP-151) serves all LROs
  (`ExecutePreview`, `CreateDryRun`).
- Error model per AIP-193, notable codes:
  - `FAILED_PRECONDITION` — stale/expired/consumed preview, stack no longer
    linear, PR closed underneath. Details carry `PreconditionFailure` with the
    drifted branch/SHA so the client can re-preview.
  - `PERMISSION_DENIED` — caller's GitHub identity lacks the underlying GitHub
    permission (merge, review request); we never escalate via the app token
    for user-attributed actions.
  - `RESOURCE_EXHAUSTED` — GitHub rate-limit backpressure, with `RetryInfo`.
  - `NOT_FOUND` — unknown work item (also returned when the derivation engine
    has since dissolved it, e.g. everything merged).
- Idempotency: `CreatePreview` and `CreateDryRun` accept a `request_id`
  (AIP-155).

## 8. Internal (not in the public surface)

- Webhook ingestion (GitHub → sync pipeline) and the reconciliation poller.
- Git worker RPCs (dry-run replay, rebase execution) — private service the
  public API delegates to.
- A subscription on the existing agent-runner API's run state (the seam the
  current MSW mock stands in for) so the derivation engine can surface chips
  and re-rank when runs finish.

## 9. Mapping from the current mock REST surface

| SPA endpoint (MSW today) | gRPC method |
|---|---|
| `GET /api/me` | existing UserService (`GetUser(users/me)`) |
| `GET /api/work?scope=` | `WorkItemService.ListWorkItems(parent=users/me, filter=scope=…)` |
| `GET /api/work/:id` | `WorkItemService.GetWorkItem` |
| (SSE, planned) | `WorkItemService.WatchWorkItems` |
| `POST /api/work/:id/actions/:kind/preview` | `PreviewService.CreatePreview` |
| `POST /api/work/:id/actions/:kind` | `PreviewService.ExecutePreview` (LRO) |
| `POST /api/work/:id/agent-preview` | `WorkItemService.GenerateAgentRunPlan` |
| `POST /api/agent-runs` | existing agent-runner API (create run from plan) |
| agent chip live state | existing agent-runner API (run state watch) |
| (planned) failing-jobs / review-comments pickers | `PullRequestService.ListJobFailures` / `ListReviewThreads` |
| (planned) reviewer suggestions | `PullRequestService.SuggestReviewers` |
| (planned) manual dry-run | `DryRunService.CreateDryRun` (LRO) |
| (planned) settings | existing UserService settings |

Open questions for v0.2:
1. Should `WorkItem` embed `Focus` and `cards` (current design, one round trip)
   or split detail into a view enum (AIP-157 `read_mask`/view)? Embedding is
   fine at today's payload sizes; revisit if List gets heavy.
2. Whether `RequestReview`/`SuggestReviewers` should be exposed publicly at all
   or only reached through `PreviewService` — currently both, since pickers
   need the read methods regardless.
3. LRO vs. plain response for fast actions (merge/nudge): spec'd as LRO for
   uniformity; downgrade nudge/assign to synchronous if the extra hop annoys.
4. How `AgentRunPlan` maps onto the existing agent-runner API's create-run
   request: does that API accept a scope lock and context attachments directly,
   or does Frontier need to pass the plan by reference for the runner to fetch?
