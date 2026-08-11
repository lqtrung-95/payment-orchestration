# Phase 04 — Transactional Outbox, Kafka, Retry, DLQ

**Priority:** P0 · **Status:** Not started · **Weeks:** 6–7

The async backbone. This is the phase that reads as "distributed systems" on a resume — the JD names *distributed, cache, message* mechanisms explicitly.

## Key insights

- Kafka, not a Redis job queue. BullMQ-style queues read as web development; Kafka reads as infrastructure and is what ByteDance actually runs at scale.
- **Partition by `merchant_id`** (the shard key from Phase 02) so per-merchant event ordering is guaranteed. Global ordering is neither achievable nor needed — being able to explain that trade-off is the point.
- At-least-once delivery + idempotent consumers. Exactly-once delivery does not exist; exactly-once *effect* does. Say it that way in interviews.
- The outbox write and the ledger write must be in the same DB transaction, or you get lost or phantom events. This is why Phase 01's `WithTx` mattered.

## Requirements

**Functional**
- Outbox table written atomically with domain state; publisher relays to Kafka.
- Retry with exponential backoff + full jitter, capped attempts, then DLQ.
- DLQ inspection and replay tooling.
- Circuit breaker per PSP.

**Non-functional**
- No event loss under publisher crash, broker unavailability, or consumer crash mid-processing.
- Consumer lag observable and alertable.

## Architecture

```
payment tx (DB)
  ├── domain rows        ┐ same transaction
  └── outbox row         ┘
        ↓ publisher (poll + FOR UPDATE SKIP LOCKED)
      Kafka topic  [partition = hash(merchant_id)]
        ↓ consumer group (idempotent handler)
      PSP call → state transition → ledger posting
        ↓ on failure
      retry topic (delayed) → … → DLQ topic
```

**Topics:** `payment.authorize`, `payment.capture`, `payment.refund`, `payment.events`, `payment.retry.{5s,30s,5m,30m}`, `payment.dlq`

**Retry ladder:** tiered delay topics rather than in-memory sleeps — a consumer restart must not lose scheduled retries.

**Retry policy is error-class-aware, not uniform:**

| Error class | Policy |
|-------------|--------|
| `NetworkError`, `Timeout` | Retry — but `GetStatus` first, never blind |
| `RateLimited` | Retry with longer backoff, honour `Retry-After` |
| `Declined`, `InsufficientFunds` | **Never retry** — terminal, retrying is user-hostile and can trigger fraud flags |
| `SuspectedFraud` | Never retry, alert |
| `InvalidInstrument` | Never retry, mark instrument for re-verification (Phase 06) |
| `Unknown` | Retry once via `GetStatus` only |

Blindly retrying everything is the most common naive answer — the differentiated one is this table.

**Circuit breaker:** per-PSP, closed → open → half-open. Open triggers failover routing (Phase 08) rather than a hard failure.

## Related code files

**Create**
- `internal/outbox/` — writer, publisher, claim loop
- `internal/messaging/kafka/` — producer, consumer group, topic config
- `internal/worker/` — handlers per topic
- `internal/resilience/` — backoff, jitter, circuit breaker
- `cmd/worker/` — worker entrypoint
- `cmd/dlqctl/` — DLQ inspect + replay CLI

## Implementation steps

1. Outbox table (`id, aggregate_id, topic, payload, status, attempts, available_at, created_at`) + writer helper that enlists in the caller's transaction.
2. Publisher loop with `FOR UPDATE SKIP LOCKED` for safe multi-instance operation; mark published only after broker ack.
3. Kafka topic provisioning in Compose; partition count sized to demonstrate ordering guarantees.
4. Consumer group with manual offset commit **after** successful handling.
5. Idempotent handler wrapper: processed-event table keyed by event ID, unique index as the enforcement point.
6. Backoff with full jitter; document why full jitter over exponential-only (thundering herd).
7. Retry ladder via delay topics; DLQ after cap.
8. `dlqctl` — list, inspect, replay single or bulk.
9. Circuit breaker per PSP, metrics on state transitions.
10. Crash tests: kill publisher mid-batch, kill consumer mid-handle, kill broker — assert zero loss, zero duplicate effect.

## Todo

- [ ] Outbox table + transactional writer
- [ ] Publisher with `SKIP LOCKED` claim loop
- [ ] Kafka topics + partitioning by merchant
- [ ] Consumer group, manual offset commit
- [ ] Idempotent handler wrapper + processed-event table
- [ ] Backoff with full jitter
- [ ] Tiered retry topics + DLQ
- [ ] `dlqctl` inspect/replay
- [ ] Per-PSP circuit breaker
- [ ] Crash test suite (publisher / consumer / broker)

## Success criteria

- Kill any component mid-flight: zero lost events, zero duplicated effects, ledger still balances.
- Declined payments never retry — provable by test.
- DLQ replay drives a stuck transaction to a correct terminal state.

## Risks

| Risk | Mitigation |
|------|-----------|
| Kafka in Compose is fiddly | KRaft single-broker mode; pin the image |
| Outbox polling adds latency | Poll at 100ms; document the CDC/Debezium alternative in an ADR instead of building it |
| Retry storms under PSP outage | Circuit breaker + jitter + per-PSP concurrency cap |

## Security considerations

- Outbox payloads carry payment data — no PAN, token references only. Consider column encryption for payloads at rest.
- DLQ replay is a privileged operation: audit every replay with actor and reason.

## Next steps

Phase 05 — webhook ingestion. The inbound counterpart, and where the duplicate/out-of-order faults from Phase 03 get answered.
