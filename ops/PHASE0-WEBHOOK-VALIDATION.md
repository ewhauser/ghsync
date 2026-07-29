# Phase-0 stacked-PR webhook validation

This protocol is the real-enrolled-repository experiment required by M4. It
cannot be completed against `internal/fakegithub`: the purpose is to measure
GitHub private-preview behavior that is not documented.

## Safety and setup

Export the non-secret coordinates for the test installation:

```bash
export GITHUB_APP_ID=123456
export GITHUB_INSTALLATION_ID=789012
export GITHUB_PRIVATE_KEY_PATH=/absolute/path/to/test-app.private-key.pem
export OWNER_REPO=acme/frontier-phase0
export CAPTURE_DIR=/absolute/path/to/frontier-phase0-20260728
mkdir -p "$CAPTURE_DIR"
```

Create a short-lived App JWT locally. Never paste the JWT, installation token,
or private key into the experiment artifacts:

```bash
export APP_JWT="$(
  ruby -ropenssl -rjson -rbase64 -e '
    key = OpenSSL::PKey::RSA.new(File.read(ARGV.fetch(1)))
    now = Time.now.to_i
    encode = ->(value) {
      Base64.urlsafe_encode64(
        JSON.generate(value),
        padding: false
      )
    }
    body = [
      encode.call({ alg: "RS256", typ: "JWT" }),
      encode.call({
        iat: now - 60,
        exp: now + 540,
        iss: ARGV.fetch(0)
      })
    ].join(".")
    puts "#{body}.#{Base64.urlsafe_encode64(
      key.sign(OpenSSL::Digest.new("SHA256"), body),
      padding: false
    )}"
  ' "$GITHUB_APP_ID" "$GITHUB_PRIVATE_KEY_PATH"
)"
```

Mint an installation token for authoritative resource fetches:

```bash
export INSTALLATION_TOKEN="$(
  curl --fail-with-body --silent --show-error \
    --request POST \
    --header "Accept: application/vnd.github+json" \
    --header "Authorization: Bearer $APP_JWT" \
    --header "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/app/installations/$GITHUB_INSTALLATION_ID/access_tokens" |
    jq --exit-status --raw-output .token
)"
```

1. Use the enrolled non-production repository designated for Frontier tests.
   Do not use a repository with unrelated active contributors or protected
   production branches.
2. Record:
   - repository `OWNER/REPO`;
   - GitHub App ID and installation ID (never tokens or private-key material);
   - UTC start time;
   - `gh --version`;
   - the current gh-stack preview/API version, if the CLI exposes it;
   - the commit SHA at the bottom and top of the test stack.
3. Create a five-PR test stack from disposable branches. Put a unique marker in
   every branch and PR title: `frontier-phase0-YYYYMMDD-HHMM`.
4. Confirm Frontier ingress is enabled and healthy. Start a capture that records
   only delivery skeletons and routing fields:
   `delivery GUID`, `delivered_at`, `event`, `action`, repository ID/name, PR
   number, `ref`, `before`, `after`, and the embedded stack tuple
   `(id, number, size, position, base.ref, base.sha)`. Do not copy PR bodies,
   review bodies, tokens, signatures, or raw payloads into the experiment log.
5. Record the newest delivery ID immediately before each action. Use
   `GET /app/hook/deliveries?per_page=100` with an App JWT so each observation
   can be bounded to deliveries newer than that ID.
6. Allow at least two minutes with no other repository activity before each
   action. If unrelated activity occurs during an observation window, mark the
   run contaminated and repeat it.

### Capture every delivery page

Capture GitHub's complete App delivery window as JSON Lines. Pagination follows
GitHub's opaque `Link` cursor and terminates only when no `rel="next"` link is
returned; it never infers a page number or stops because a page appears old:

```bash
next_url='https://api.github.com/app/hook/deliveries?per_page=100'
: >"$CAPTURE_DIR/github-deliveries.jsonl"
while [ -n "$next_url" ]; do
  headers_file="$(mktemp)"
  body_file="$(mktemp)"
  curl --fail-with-body --silent --show-error \
    --dump-header "$headers_file" \
    --output "$body_file" \
    --header "Accept: application/vnd.github+json" \
    --header "Authorization: Bearer $APP_JWT" \
    --header "X-GitHub-Api-Version: 2022-11-28" \
    "$next_url"
  jq --compact-output '.[]' "$body_file" \
    >>"$CAPTURE_DIR/github-deliveries.jsonl"
  next_url="$(
    awk 'BEGIN { IGNORECASE=1 }
      /^link:/ {
        count = split($0, links, ",")
        for (i = 1; i <= count; i++) {
          if (links[i] ~ /rel="next"/ &&
              match(links[i], /<[^>]+>/)) {
            print substr(links[i], RSTART + 1, RLENGTH - 2)
          }
        }
      }' "$headers_file"
  )"
  rm -f "$headers_file" "$body_file"
done
```

Transform delivery metadata and the ingress capture into
`$CAPTURE_DIR/phase0-records.jsonl`. Every line must validate against
[`phase0-delivery-record.schema.json`](phase0-delivery-record.schema.json).
The schema intentionally excludes payload bodies, credentials, signatures,
and author identity. A capture is incomplete if the GitHub list hit an
operator-imposed time/page limit; record the failure and resume from the
returned opaque next cursor instead of publishing a partial result.

Validate the artifact before analysis:

```bash
record_file="$(mktemp)"
validation_failed=0
while IFS= read -r record; do
  printf '%s\n' "$record" >"$record_file"
  npx --yes ajv-cli@5.0.0 validate \
    --spec=draft2019 \
    --strict=true \
    --validate-formats=false \
    -s ops/phase0-delivery-record.schema.json \
    -d "$record_file" ||
    validation_failed=1
done <"$CAPTURE_DIR/phase0-records.jsonl"
rm -f "$record_file"
test "$validation_failed" -eq 0
```

For every action below, retain observations for 120 seconds after the GitHub
operation reports success. Record both arrival time at GitHub
(`delivered_at`) and arrival time at Frontier ingress so event-to-ingress delay
is distinguishable from a missing event.

### Authoritative post-action fetches

Fetch the PR collection, each affected PR, and its stack resource after every
experiment. Keep the response ETag and fetched-at time alongside the sanitized
projection used in the result table:

```bash
owner="${OWNER_REPO%%/*}"
repo="${OWNER_REPO#*/}"
curl --fail-with-body --silent --show-error \
  --header "Accept: application/vnd.github+json" \
  --header "Authorization: Bearer $INSTALLATION_TOKEN" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/$OWNER_REPO/pulls?state=all&per_page=100" |
  jq '[.[] | {
    number, state, merged_at, updated_at,
    base: {ref: .base.ref, sha: .base.sha},
    head: {ref: .head.ref, sha: .head.sha}
  }]' >"$CAPTURE_DIR/authoritative-pulls.json"

export PR_NUMBER=123
curl --fail-with-body --silent --show-error \
  --header "Accept: application/vnd.github+json" \
  --header "Authorization: Bearer $INSTALLATION_TOKEN" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/$OWNER_REPO/pulls/$PR_NUMBER" |
  jq '{
    number, state, merged_at, updated_at,
    base: {ref: .base.ref, sha: .base.sha},
    head: {ref: .head.ref, sha: .head.sha}
  }' >"$CAPTURE_DIR/pr-$PR_NUMBER.json"

export STACK_NUMBER=123
curl --fail-with-body --silent --show-error \
  --header "Accept: application/vnd.github+json" \
  --header "Authorization: Bearer $INSTALLATION_TOKEN" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/$OWNER_REPO/stacks/$STACK_NUMBER" |
  jq '{
    id, number, state, updated_at,
    pull_requests: [
      .pull_requests[] | {
        number, position,
        base: {ref: .base.ref, sha: .base.sha},
        head: {ref: .head.ref, sha: .head.sha}
      }
    ]
  }' >"$CAPTURE_DIR/stack-$STACK_NUMBER.json"
```

If the installed private-preview version uses a different stack URL or media
type, replace only those two values and record them in the run metadata. A
404/410 is data and must be retained for dissolve tests.

## Experiment A: server-side Rebase Stack

1. Record every member's `head.sha`, `base.ref`, `base.sha`, stack position,
   and the trunk SHA.
2. Advance trunk with one harmless commit outside the stack.
3. Invoke GitHub's server-side **Rebase stack** action in the web UI. Record the
   exact click time in UTC and the time the operation completes.
4. Collect all deliveries newer than the pre-action delivery ID for 120
   seconds.
5. Record, per stack member:
   - whether a `push` event arrived for its branch and its `before`/`after`;
   - whether `pull_request.synchronize` arrived;
   - whether any other `pull_request` action arrived;
   - whether the embedded stack tuple reflects the new base/position;
   - ordering and duplicate deliveries.
6. Fetch the authoritative stack and PR resources and record the final SHAs.
   The observation passes only when the captured event set can be reconciled
   against those final resources; absence of events is a valid measured result.

## Experiment B: partial merge and cascading retarget

1. Start with all stack layers open and record every PR's base/head SHA.
2. Merge the bottom PR through GitHub's normal merge UI. Record click and
   completion times in UTC.
3. Collect deliveries for 120 seconds.
4. Record:
   - the bottom PR's `pull_request.closed` delivery and `merged` value;
   - every up-stack `pull_request` event and action;
   - any `push` event for the trunk or stack branches;
   - whether each up-stack payload's base ref/SHA and stack tuple already show
     the retarget;
   - whether a member receives no direct delivery despite an authoritative
     base change.
5. Fetch the authoritative stack and every remaining PR. Record the final base
   chain and compare it to the delivery observations.

## Experiment C: `gh stack modify`

1. Record the initial stack order and stack tuple for every member.
2. Run the exact installed CLI's documented command to reorder one middle
   layer and remove a different layer. Paste the command line into the
   experiment log, but redact tokens and local paths.
3. Collect deliveries for 120 seconds.
4. Record every event/action, the affected PR number, and old/new stack tuple.
   Explicitly record members whose authoritative position changed but which
   emitted no delivery.
5. Fetch the authoritative stack and every affected PR; record final order and
   membership.

## Experiment D: unstack/dissolve

1. Record the remaining stack ID, number, size, order, and member SHAs.
2. Use the installed CLI's documented unstack/dissolve operation. Paste the
   exact command into the log with secrets redacted.
3. Collect deliveries for 120 seconds.
4. For every former member, record whether a `pull_request` event arrived and
   whether its embedded `stack` value was absent/null. Record any stack-specific
   event even if Frontier does not yet recognize it.
5. Fetch every former PR and the former stack resource. Record whether the
   stack endpoint returns 200, 404, or a closed representation.

## Result table

Create one row per action/member combination:

| Run | UTC action time | Delivery GUID | Event | Action | PR/ref | Stack before | Stack in payload | Authoritative after | Delay | Duplicate? | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|

Also record a concise result for each hypothesis:

- server rebase emits `push` per rewritten branch: yes/no/inconsistent;
- server rebase emits `pull_request.synchronize` per member:
  yes/no/inconsistent;
- partial merge emits up-stack base-edit events: yes/no/inconsistent;
- reorder/removal emits a PR event for every affected member:
  yes/no/inconsistent;
- unstack emits a PR event with `stack: null`: yes/no/inconsistent;
- any previously unknown event/action observed: exact event/action pair.

## Feed findings back into dispatcher data

1. Copy [`config/dispatcher-rules.yaml`](../config/dispatcher-rules.yaml) to the
   deployment-owned configuration location and set `DISPATCH_RULES_FILE` to
   that path.
2. Change only rules supported by the recorded evidence:
   - add an observed event/action pair when it identifies a refresh target;
   - treat observations as additive evidence only: an unseen action is not
     evidence that GitHub never emits it;
   - narrow `action: "*"` only after the complete API action domain for that
     event is documented, every excluded action has an authoritative semantic
     analysis and a golden negative replay, and the narrowing has explicit
     design approval;
   - use `stacked_target: stack` when a PR/ref signal proves stack-wide state
     may have changed;
   - retain the `resolve_stack_membership` rule for PR events even when stack
     payload behavior appears reliable.
3. Run `go test ./internal/dispatch` and the recorded replay test with the new
   table. Unknown events must remain successful no-ops and malformed configured
   events must still park normally.
4. Deploy the rule-file change independently of code. Repeat the four
   experiments and verify that every observed signal enqueues the intended
   pointer-only refresh.
5. Do not relax the five-minute stack bound from one clean run. A proposed
   relaxation requires repeated runs on different days with identical complete
   signal coverage, plus an explicit design review; until then the sweep is the
   correctness floor.
