# Phase 08 — Sharding + Distributed Transactions (TCC)

**Priority:** P1 — the system-design interview phase · **Status:** Partially complete · **Week:** 11

The JD asks for "large-scale distributed system design" and "in-depth understanding of distributed, cache, message... mechanisms." This phase is what lets you answer the sharding and consistency questions from experience rather than from a book.

## Key insights

- TCC (Try-Confirm-Cancel) is the dominant distributed-transaction pattern in Asian payment systems and is the vocabulary ByteDance interviews use. Knowing TCC vs Saga vs 2PC — and *when each fails* — is a differentiator.
- The shard key was decided in Phase 02 (`merchant_id`). This phase implements it. The interview question is never "did you shard" — it is "what breaks when a transaction spans shards," which is exactly what TCC answers.
- Hot keys are real: one merchant's balance read 10k/s destroys a naive design. The JD names *cache* explicitly.

## Requirements

**Functional**
- N logical shards with a routing layer; physical mapping configurable.
- Cross-shard money movement via TCC with reservation records.
- Account-level distributed locking to serialize concurrent writes.
- Hot-key caching for balance reads with correctness guarantees.

**Non-functional**
- Rebalancing a logical shard requires no application code change.
- No lost or double-spent funds under concurrent cross-shard transfers.

## Architecture

**Sharding**
```
logical shards: 64 (fixed)         → physical DBs: 2–4 (configurable mapping)
shard_id = hash(merchant_id) % 64
```
Fixed logical count with a variable physical mapping is the standard escape hatch — resharding becomes a mapping change, not a rehash of every row. Document this; it is a common interview question.

Routing layer resolves shard from the aggregate key before any query. Cross-shard queries are forbidden by construction — the repository API cannot express them.

**TCC for cross-shard transfers**
```
Try     — reserve funds on source shard (reservation row, balance not yet moved)
Confirm — commit both sides, release reservation
Cancel  — release reservation, no movement
```
```
tcc_transactions (id, state, initiated_at, timeout_at, participants)
tcc_reservations (id, tcc_id, shard_id, account_id, amount_minor,
                  state, created_at, expires_at)
```
A coordinator drives the state machine; a timeout sweeper cancels stranded `Try` phases. Reservations are idempotent — `Confirm` twice is a no-op, which is why the phase depends on Phase 04's idempotent-consumer work.

**Why TCC over the alternatives** (write this as an ADR — likely to be asked directly):
- **2PC** — blocking; a coordinator crash holds locks indefinitely. Unacceptable for payments.
- **Saga** — non-blocking but requires semantic compensation, and compensation for a *captured* payment is a refund, which is not a true inverse (fees, FX, and user perception all differ).
- **TCC** — reservation makes the resource unavailable without committing, so compensation is exact. Costs an explicit `Try` API on every participant.

**Distributed locking:** Postgres advisory locks keyed by account ID for same-shard serialization; Redis-based lease for cross-shard coordination with fencing tokens. Note in the ADR that a Redis lock alone is not safe without fencing.

**Hot keys:** balance snapshot table updated on write, cached in Redis with explicit invalidation. Reads served from cache; the authoritative balance is always re-derivable from postings. Cache is an optimization, never a source of truth — the same principle as the Phase 05 dedup index.

## Related code files

**Create**
- `internal/platform/sharding/` — resolver, router, shard-aware repository
- `internal/tcc/` — coordinator, participant interface, reservation store, timeout sweeper
- `internal/platform/lock/` — advisory locks, Redis lease with fencing
- `internal/domain/ledger/snapshot.go` — balance snapshots + cache

**Modify:** all repositories from Phases 02, 06, 07 become shard-aware.

## Implementation steps

1. Shard resolver + configurable logical→physical mapping; multi-pool connection management.
2. Make repositories shard-aware; make cross-shard queries impossible at the API level.
3. Migration runner that applies to every physical shard.
4. TCC coordinator + participant interface; reservation store.
5. Try/Confirm/Cancel for cross-shard transfer, all idempotent.
6. Timeout sweeper for stranded reservations.
7. Advisory locks for same-shard serialization.
8. Redis lease with fencing tokens for cross-shard coordination.
9. Balance snapshots + Redis cache with invalidation on write.
10. Concurrency tests: N concurrent cross-shard transfers, assert conservation of money.
11. ADRs: shard key rationale, TCC vs Saga vs 2PC, cache-as-optimization.

## Todo

- [x] Shard resolver + physical mapping
- [x] Shard-aware repositories (payment write path; back-office stays on shard 0)
- [x] Multi-shard migration runner
- [x] TCC coordinator + participants
- [x] Idempotent Try/Confirm/Cancel
- [x] Reservation timeout sweeper
- [x] Advisory locks (same-shard)
- [ ] Redis lease + fencing (cross-shard) — not needed: every participant's work is single-shard by construction
- [ ] Balance snapshots + cache invalidation — cut, see ADR 0010
- [x] Conservation-of-money concurrency tests
- [x] ADR 0010

## Success criteria

- 1,000 concurrent cross-shard transfers → total money in the system unchanged, to the cent.
- Killing the coordinator mid-TCC → sweeper cancels reservations, no funds stranded.
- Remapping logical shards to a different physical count requires config only.
- Cached balance always reconcilable to the derived balance from postings.

## Risks

| Risk | Mitigation |
|------|-----------|
| Sharding retrofit breaks Phases 02–07 | Repository interfaces were shard-key-aware from Phase 02 — this is why that decision was made early |
| TCC coordinator becomes a single point of failure | Coordinator state lives in the DB; any instance can resume. Document it. |
| Week is too short for all of this | Priority order: sharding → TCC → locking → cache. Cut from the tail. |

## Security considerations

- Shard routing must never be driven by client input — always derived server-side from the authenticated merchant.
- Reservation expiry is a money-safety control; make its sweeper failure loudly alertable.

## Next steps

Phase 09 — observability and the load/chaos numbers that turn all of this into resume evidence.
