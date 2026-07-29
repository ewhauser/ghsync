# Cache trust dashboard

The C-O4 dashboard is one screen with one question in its title: **Is the
cache trustworthy right now?** Use a five-minute default range. Red means no;
yellow means correct now but losing safety margin.

## Row 1: trust verdict

Use four large stat panels:

1. **Freshness:** C-Q2 p95/p99 from
   `frontier_c_q2_event_to_cache_latency_seconds` beside 20s/60s limits, plus
   `frontier_c_q2_oldest_unprocessed_delivery_age_seconds`.
2. **Bounded staleness:** table
   `frontier_c_r1_cache_staleness_seconds` /
   `frontier_c_r1_staleness_bound_seconds` by `entity_class`.
3. **Correctness:** open
   `frontier_c_o3_drift_findings{state="open"}` and
   `frontier_c_i5_parked_deliveries`. Either above zero is red.
4. **Stream safety:** `frontier_c_s2_watermark_lag_sequences`,
   `frontier_c_s2_watermark_age_seconds`, and maximum consumer cursor lag.

This row alone answers the on-call question. Link each stat to its runbook.

## Row 2: freshness path

- Event-to-cache p50/p95/p99 with fixed 20s and 60s lines.
- Queue depth by queue, oldest delivery age, and dispatch batch-size heatmap.
- Fetch rate by kind, CAS reject ratio, tombstones, and sweep 304 ratio with a
  fixed 0.80 line.

## Row 3: reconciliation and budget

- Budget remaining and floor by class/resource; request and starvation rates.
- Gate-closed seconds.
- Sweep duration and period by kind, gap-heal requests, drift state, and
  pruner deletes.

## Row 4: stream and derivation

- Watermark sequence lag/age, outbox depth, and advance rate.
- Consumer cursor lag/age by consumer/stream and resync rate.
- Deriver pass duration and dirty backlog.

Keep active alerts down the right edge. Do not split the view by daemon role:
`frontier_c_o4_role_enabled` identifies the scraped process, while every
role's `/metrics` reads the same authoritative Postgres state. The verdict
therefore survives partial rollouts and role outages.
