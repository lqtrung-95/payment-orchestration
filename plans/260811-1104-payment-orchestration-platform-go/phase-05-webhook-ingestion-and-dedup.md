# Phase 05 — Webhook Ingestion, Dedup, Out-of-Order Handling

**Priority:** P0 · **Status:** Not started · **Week:** 8

**Shippable checkpoint.** After this phase the project is complete and demoable. Everything later is upside.

## Key insights

- Webhooks are untrusted, unordered, duplicated, and may arrive **before** the API response that created the transaction. All four at once, in production, routinely.
- Dedup is not "have I seen this event ID." It is dedup *plus* state-machine guards, because a duplicate of a **stale** event is different from a duplicate of a current one.
- Ingestion must be fast and dumb: verify, persist raw, ack, process async. A PSP that gets a slow 200 will retry and amplify your load exactly when you are already struggling.

## Requirements

**Functional**
- HMAC signature verification with timestamp tolerance (replay window).
- Raw event persisted before any processing; ack immediately.
- Dedup by provider event ID.
- Out-of-order tolerance: stale events rejected by the state machine, not by ordering assumptions.
- Webhook-before-API-response race handled without creating orphan state.

**Non-functional**
- p99 ingestion ack under 50ms.
- Replay of the raw event log reproduces identical final state.

## Architecture

```
POST /webhooks/{provider}
  → verify signature + timestamp window
  → INSERT raw event (unique index on provider + event_id)
  → 200 OK                                    [fast path ends here]
  → publish to Kafka: webhook.received
       ↓ consumer
     resolve transaction → guard transition → apply → ledger posting
```

**Tables**
```
webhook_events_raw (id, provider, provider_event_id UNIQUE, payload,
                    signature_valid, received_at, processed_at, status)
```
The unique index is the dedup enforcement point — Redis is a fast-path optimisation, never the source of truth. Explaining *why* the DB constraint is authoritative and the cache is advisory is a strong interview beat.

**Out-of-order strategy:** every event carries the PSP's own timestamp/sequence. The transaction stores `last_applied_event_at`. An event older than what has been applied is recorded as `superseded` — never applied, never silently dropped. Combined with the Phase 02 transition matrix, a `captured → authorized` event is rejected structurally rather than by ad-hoc checks.

**Webhook-before-response race:** if no transaction matches, park the event in `pending_correlation` with a short TTL and retry correlation. Do **not** create a transaction from a webhook — that is how phantom payments appear.

## Related code files

**Create**
- `internal/webhook/` — handler, verifier, dedup, correlator
- `internal/webhook/providers/` — per-provider signature schemes and payload mapping
- `internal/worker/webhook_processor.go`
- `migrations/` — raw event table

## Implementation steps

1. Per-provider signature verification (Stripe scheme for the real one; HMAC-SHA256 for simulators). Constant-time compare.
2. Timestamp tolerance window (5 min) to bound replay attacks; reject outside, and log.
3. Raw persist + unique index; treat unique-violation as "already seen" and return 200 — a duplicate must never look like an error to the PSP.
4. Fast ack, then publish to Kafka for async processing.
5. Correlation: provider event → transaction, via PSP reference stored at authorize time.
6. `pending_correlation` parking with TTL + retry; alert if TTL expires.
7. Guarded application: transition matrix + `last_applied_event_at` staleness check.
8. Replay tool: reprocess the raw log from scratch into a clean DB, assert identical final state.
9. Tests against every Phase 03 webhook fault: duplicates, reordering, webhook-before-response, floods.

## Todo

- [ ] Per-provider signature verification, constant-time
- [ ] Timestamp replay window
- [ ] Raw persist + unique-index dedup
- [ ] Fast ack → async Kafka processing
- [ ] Transaction correlation
- [ ] `pending_correlation` parking + TTL
- [ ] Staleness guard + transition enforcement
- [ ] Raw-log replay tool
- [ ] Fault-mode test suite

## Success criteria

- Same webhook delivered 100× → exactly one state transition, one ledger effect, 200 every time.
- Events delivered in fully reversed order → correct final state.
- Webhook arriving before the API response → correctly correlated, no orphan transaction.
- Replaying the raw log into an empty DB reproduces byte-identical final state.

## Risks

| Risk | Mitigation |
|------|-----------|
| Signature schemes differ per provider | Isolate per provider; never a shared "generic" verifier |
| `pending_correlation` grows unbounded | TTL + alert + DLQ on expiry |
| Slow processing causes PSP retry storms | Ack before processing — the whole point of the fast path |

## Security considerations

- Unverified signature → reject with 401 and log; never process, never park.
- Webhook endpoints are public: rate limit per provider IP range, cap body size.
- Raw payloads may contain PII — encrypt at rest, define a retention window.

## Next steps

**Stop and assess here.** Ship, write the README, record the demo. Only start Phase 06 once 01–05 are genuinely complete.
