# Phase 05 — Webhook Ingestion, Dedup, Out-of-Order Handling

**Priority:** P0 · **Status:** Complete · **Week:** 8 · **Verified on 2026-08-12**

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

- [x] Per-provider signature verification, constant-time
- [x] Timestamp replay window
- [x] Raw persist + unique-index dedup
- [x] Fast ack → async Kafka processing
- [x] Transaction correlation
- [x] Correlation parking + escalation — via the retry ladder, not a separate table
- [x] Staleness guard + transition enforcement
- [x] Raw-log replay tool (`webhookctl replay`)
- [x] Fault-mode test suite

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

## Verified on 2026-08-12

**Chaos run** — 30 payments against the async provider with every webhook fault live
(duplicate 0.25, out-of-order 0.15, before-response 0.10, plus the ambiguous-outcome faults):

| Measure | Result |
|---|---|
| Payments resolved | 30 / 30 authorized |
| Provider charges | 30 distinct references — no double charges |
| Webhook requests served | 65, **all 200** |
| Duplicates absorbed | 24, all answered 200 |
| Events stored | 41 unique |
| Outcomes | 28 applied · 7 superseded · 6 ignored |
| Transactions with a duplicate `authorized` audit row | **0** |
| Worst ingest latency | 6.4ms (target: p99 under 50ms) |
| `webhookctl replay` | 41 events, 0 would change state |

**Tests** — full suite green under `-race`, lint clean. Webhook coverage:
signature/tamper/wrong-secret/replay-window/missing-header rejection; 100×
redelivery producing one stored event and one transition; unverified payload
persisting nothing; stored bytes still verifying against the delivered signature;
stale supersession; fully reversed delivery reaching the same state; illegal
transition rejected; confirmation of the current state writing no audit row;
reprocessing being a no-op; whole-log replay changing nothing. End-to-end over
real Kafka and a real HTTP endpoint: async resolution by callback,
duplicate+out-of-order together producing one transition, webhook-before-response
correlating without inventing a transaction, and a deferred retry not stalling
live work.

## Bugs found

**Sequence 0 was treated as "no sequence".** Ingest substituted a timestamp when
the parsed sequence was zero, which rewrote the *oldest* event in a batch into the
newest and silently defeated the staleness guard. Caught because the out-of-order
fault emits `head - 1` = 0. Fixed by making the adapter's value authoritative;
deriving a sequence is now the adapter's job, since only it knows whether its
provider has one. Regression test pins it.

**Confirmations were recorded as transitions.** An event agreeing with the current
state passed `CanTransitionTo` (same-state moves are legal) and was written to the
audit trail, so a payment resolved by the recovery path and then confirmed by its
callback showed two `authorized` rows. Found in the first live run — 15 rows for
12 payments. Now `ignored`.

**Deferred retries stalled the entire consumer.** The delay tiers slept in the
handler. Records from all assigned partitions are processed by one goroutine, so
one message on the 5-minute tier held **20 live payments at `created`** — found
by inspecting consumer-group lag during the demo, not by a test. A 30-minute wait
would additionally exceed the rebalance timeout and evict the consumer. Fixed by
pausing and rewinding the partition instead of sleeping. The regression test was
confirmed non-vacuous by restoring the old behaviour and watching it fail.

## Deviations from the plan

- **`pending_correlation` table dropped.** The retry ladder already provides the
  waiting, escalation, and DLQ that the table's TTL sweeper would have
  duplicated. One mechanism instead of two.
- **`last_applied_event_at` → `last_applied_event_seq`.** A sequence survives
  clock skew between provider hosts; a timestamp does not.
- **Replay narrowed** from "reproduces identical state in an empty database" to
  "changes nothing against current state". The original is unachievable without
  event-sourcing the whole aggregate — the log holds callbacks, not the API calls
  and provider responses that create and move transactions. See ADR 0008.

## Deferred

- **Capture and refund events are parsed but not applied.** Neither operation has
  an HTTP surface, and applying a capture from a callback would post to the ledger
  from a path with no amount reconciliation behind it.
- **`webhook_events_raw` grows without bound** and may hold PII. Needs retention
  and encryption at rest.
- **No rate limiting** on the public endpoint. Body size is capped; request rate
  is not.
- **Still no authentication** on the merchant API. `X-Merchant-Id` remains an
  unauthenticated header.
- **Consumer lag is not exported**, so a paused partition is invisible without
  querying Kafka by hand — which is exactly how the stall above was found.

## Next steps

**Stop and assess here.** Ship, write the README, record the demo. Only start Phase 06 once 01–05 are genuinely complete.
