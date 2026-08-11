# Phase 02 — Ledger, Transaction State Machine, Idempotency

**Priority:** P0 — the spine · **Status:** Complete · **Weeks:** 3–4

The three things that cannot be retrofitted. Everything else in the project is additive; get these wrong and you rebuild.

## Key insights

- A money project without double-entry reads as fake to anyone who has worked in payments. Balances are **derived** from immutable postings, never updated in place.
- Money is `int64` minor units + ISO-4217 currency code. Never float, never decimal-in-string. This is a two-minute decision that signals domain seriousness for the life of the repo.
- The **sharding key is decided here** even though sharding is implemented in Phase 08. Retrofitting a shard key across a ledger schema is a rewrite.
- Idempotency is not "check if key exists." It is: key + request fingerprint + stored response + in-flight lock, with a defined answer for every race.

## Requirements

**Functional**
- Double-entry ledger: accounts, journal entries, postings. Every entry balances.
- Transaction state machine with explicit, enforced transitions.
- Idempotent `POST /payments` — same key + same body returns the original response; same key + different body returns 409.
- Full audit trail: every state change is an append-only row, never an update.

**Non-functional**
- Ledger invariant `sum(debits) == sum(credits)` provable by query at any instant.
- Concurrent writes to the same account are serialized without deadlock.

## Architecture

**Ledger**
```
accounts        (id, owner_type, owner_id, currency, type, shard_key)
journal_entries (id, idempotency_key, transaction_id, created_at, description)
postings        (id, entry_id, account_id, direction, amount_minor, currency)
```
Balance = aggregate over `postings`. Materialized snapshot table added in Phase 08 for hot accounts.

**Transaction state machine**
```
created → authorizing → authorized → capturing → captured → settled
                ↓            ↓           ↓          ↓
             failed      cancelled    failed    refunded / partially_refunded
                                                     ↓
                                                  expired
```
Transitions declared in one table in code; a DB check constraint plus a version column enforces them. Illegal transition = error, never a silent no-op. Stale webhook arrivals rely on this (Phase 05).

**Idempotency**
```
idempotency_keys (key, request_fingerprint, state, response_body,
                  response_status, transaction_id, locked_at, expires_at)
```
States: `in_flight` → `completed` | `failed`. Concurrent request on an `in_flight` key returns 409 with `Retry-After`, never a second charge.

**Sharding key decision:** shard by `merchant_id` (hash → logical shard). Rationale and the rejected alternatives (`user_id`, `transaction_id`) go in an ADR now — the "why" is a likely interview question.

## Related code files

**Create**
- `internal/domain/money/` — `Money` type, arithmetic, currency guards
- `internal/domain/ledger/` — account, entry, posting, invariant checks
- `internal/domain/transaction/` — aggregate + state machine
- `internal/service/payment/` — orchestration entrypoint
- `internal/middleware/idempotency/`
- `migrations/` — ledger, transaction, idempotency schemas

## Implementation steps

1. `Money` type: minor units, currency-safe add/sub/split, allocation without rounding loss, explicit `Mul`/`Div` semantics. Property-test that allocation never creates or destroys cents.
2. Ledger schema + repository. Postings insert-only. Enforce balance-per-entry in a DB trigger *and* in code.
3. Balance query + an invariant checker callable from tests and from the health endpoint.
4. Transaction aggregate + transition table. Version column for optimistic locking.
5. State change audit table, append-only.
6. Idempotency middleware: fingerprint = hash of method + path + canonicalized body. Handle all four races — concurrent same key, retry after success, retry after failure, key reuse with different body.
7. `POST /payments` end-to-end: idempotency → transaction created → ledger entry (pending) → response. No PSP call yet.
8. Concurrency tests: N goroutines, same idempotency key, assert exactly one transaction and one journal entry.
9. Write ADRs: money representation, sharding key, idempotency scope + TTL.

## Todo

- [x] `Money` type + property tests
- [x] Ledger schema, repo, balance-per-entry enforcement
- [x] Invariant checker
- [x] Transaction aggregate + enforced state machine
- [x] Append-only state audit
- [x] Idempotency middleware covering all four races
- [x] `POST /payments` skeleton flow
- [x] Concurrency test suite
- [x] ADRs: money, ledger, shard key, idempotency, transition matrix

## Verified on 2026-08-11

Migrations `000002`–`000004`; ADRs [0001](../../docs/adr/0001-money-as-integer-minor-units.md)–[0005](../../docs/adr/0005-transition-matrix-enforced-in-two-places.md).

**Money** — 20,000-case property test: allocation never creates or destroys a
minor unit, for any amount and any ratios. Split parts differ by at most one
unit. Overflow detected on add, subtract, negate, multiply, and inside
allocation. Decimal amounts rejected at the JSON boundary.

**Ledger** — unbalanced entry rejected at COMMIT by the deferred constraint
trigger; entry with no postings rejected; cross-currency posting rejected by the
composite foreign key; `UPDATE`/`DELETE` on postings rejected. Balances derived
and oriented by account type. Invariant holds across a 25-entry three-way fee
split.

**Transactions** — Go and SQL transition matrices proven identical by test.
Illegal transitions rejected in both. `captured <= amount` and
`refunded <= captured` enforced as CHECK constraints. 20 concurrent writers:
version advances exactly once per winner, losers get `ErrVersionConflict`.
Audit trail append-only.

**Idempotency** — all four races covered. Stale claim taken over *and* the
displaced owner fenced out by claim token. Keys scoped per merchant. 100
concurrent claims → exactly one winner.

**End to end over HTTP** — 50 concurrent `POST /v1/payments` with one key → 1
transaction, 1 key row (42×201, 8×409 in-flight). Replay byte-identical.
Reformatted JSON still replays. Key reuse with a different body → 409. Another
merchant reading the payment → 404. Ledger balanced.

## Deferred to a later phase

- **No authentication.** `X-Merchant-Id` is an unauthenticated header — any
  caller can claim to be any merchant. Must be replaced before this is exposed.
- **No expiry reaper** for idempotency records; the table grows unbounded.
- Capture and refund exist on the aggregate but have no HTTP surface, since
  both need a provider.

## Success criteria

- 1,000 concurrent requests with one idempotency key → exactly one transaction, exactly one journal entry, identical response body to all callers.
- Every illegal state transition rejected with a typed error; test covers the full transition matrix.
- Invariant checker returns balanced after every test suite run.

## Risks

| Risk | Mitigation |
|------|-----------|
| Over-engineering the ledger into a general accounting system | Scope to what payments needs: authorize, capture, refund, settle, fee. Nothing else. |
| Shard key chosen wrong | Write the ADR with rejected alternatives; merchant-scoped queries dominate this workload |
| Idempotency TTL ambiguity | Pick 24h, document it, mirror common PSP behaviour |

## Security considerations

- Idempotency keys are client-supplied — treat as untrusted, length-cap, scope per merchant so keys cannot collide or be probed across tenants.
- Audit rows must capture actor and source IP.
- No PII in the ledger; reference by ID only.

## Next steps

Phase 03 — the fault-injecting PSP simulator. It is what makes every correctness claim in this phase actually testable.
