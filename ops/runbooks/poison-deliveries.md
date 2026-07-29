# Poison deliveries

## Symptoms

- `FrontierPoisonDeliveryParked`.
- `frontier_c_i5_parked_deliveries` is nonzero.
- Dispatch logs repeat a classification error through
  `DISPATCH_MAX_ATTEMPTS`.

## Diagnosis

```sql
SELECT delivery_guid, event, received_at, attempts, last_error,
       payload_pruned_at
FROM webhook_deliveries
WHERE status = 'parked'
ORDER BY received_at;

SELECT event, last_error, count(*)
FROM webhook_deliveries
WHERE status = 'parked'
GROUP BY event, last_error ORDER BY count(*) DESC;
```

If payload is pruned, use GitHub's delivery view; never reconstruct it from
`last_error`.

## Remediation

Fix `DISPATCH_RULES_FILE`, then roll the dispatch role through the deployment
platform. If starting it manually, keep this foreground process running in a
separate shell/deployment action:

```sh
# In a separate shell/deployment action:
frontier-syncd serve --roles=dispatch
```

Once the fixed dispatch role is ready, test one GUID:

```sh
frontier-syncd requeue --guid=DELIVERY_GUID
```

After it processes cleanly, replay at most 100 exact GUIDs:

```sh
frontier-syncd requeue --guids=guid-1,guid-2
```

Alternatively, replay the next bounded batch sharing one diagnosed signature:

```sh
frontier-syncd requeue \
  --event=pull_request \
  --error-contains='unsupported action reopened'
```

## Escalation

Escalate if the delivery parks again, its payload is unavailable, multiple
event families are affected, or parking threatens the C-Q2 age SLO.
