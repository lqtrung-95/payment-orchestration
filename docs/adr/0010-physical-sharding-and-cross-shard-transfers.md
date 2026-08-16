# ADR 0010 — Physical sharding and cross-shard transfers

**Status:** Accepted · **Date:** 2026-08-16

## Context

ADR 0003 chose `merchant_id` as the shard key and stamped a derived key on every
row from the first migration. Nothing routed on it. Every table lived in one
database, and the key was a column that cost nothing and proved nothing.

Making it real forces two questions that a single database never asks: which
database does a given query go to, and what happens to an operation that needs
two of them at once.

## Decision

### Sixty-four logical shards, mapped onto a power-of-two number of databases

The key stays `s00`–`s63`, derived from the merchant with FNV-1a and stored on
the row. The number of physical databases is deployment configuration.

The physical count is constrained to a power of two that divides 64. That is not
tidiness — it is what makes growth tractable. Doubling the count splits every
existing range exactly in half, so the migration is "copy logical shards 32–63
to the new database": a contiguous bulk move, verifiable by counting rows per
shard key, that leaves the other half untouched. An arbitrary count produces
uneven ranges whose boundaries shift for shards that were never meant to move.

Routing reads the **stored** key rather than re-deriving it from the merchant.
Re-deriving would mean a change in the hash function silently sends reads to a
different database than the one holding the rows.

### The router cannot express a cross-shard transaction

There is no method that opens a transaction spanning two shards, because
Postgres cannot commit across databases. Such a method could only be two
transactions wearing one name, and the first time one committed and the other
did not, the caller would have been told nothing had gone wrong.

### Reads route by merchant, and there is no lookup by id alone

`Authorize` takes the merchant explicitly; `Get` takes it as it always did.
Fetching a payment from its id alone would mean querying all 64 shards, which
makes every read as expensive as the largest deployment and stops the system
being sharded in any useful sense.

This is the cost sharding actually imposes, and it is paid at the API boundary
rather than hidden behind a fan-out.

### What is merchant-partitioned, and what is not

| Data | Where | Why |
|---|---|---|
| Payments, audit trail, ledger, idempotency claims, outbox | Merchant's shard | The outbox decides it: the row is written in the same transaction as the domain change, so it can only live where that change lives. One relay per shard follows. |
| Fee schedules, FX rates | Every shard | Read inside shard transactions. Replicated reference data, not routed lookups. |
| Webhook log, settlement files, reconciliation breaks, transfer coordinator state | Shard 0 | No merchant to partition on. A webhook arrives before any payment has been resolved from it; a transfer spans two merchants and belongs to neither shard. |
| Consumer dedup index | Shard 0 | An event id carries no merchant. One unique constraint arbitrating "already handled" for every message is worth the concentration. |

Concentrating the unpartitioned tables makes shard 0 hotter than its peers. That
is a real and accepted cost; the alternative is cross-shard consistency for data
that has no partition key.

### Cross-shard money movement uses try-confirm-cancel

A transfer between merchants on different databases has no single transaction
available to it. The alternatives, and why each was rejected:

**Two independent commits.** Whichever crashes second leaves money debited and
never credited, or credited and never debited, with nothing recording that a
decision was outstanding.

**Two-phase commit.** Blocking. A coordinator that dies during the prepared
phase leaves participants holding locks until a human intervenes. On a payment
ledger that is an outage, not a delay.

**Saga.** Non-blocking, but compensation is semantic rather than exact. The
compensation for a completed transfer is a reverse transfer, and the two are not
inverses: the funds were spendable in between, which is precisely the window in
which the account can be overdrawn.

**TCC.** The reservation makes funds unavailable without moving them, so
cancelling is exact — nothing was posted, and there is nothing to undo. The cost
is an explicit Try on every participant and a sweeper for holds nobody resolved.

### The commit point is durable before any confirm runs

`trying → confirming` is written and committed before either side posts. Before
it, the transfer may be cancelled freely. After it, every participant has already
agreed and the transfer is owed completion: a failing confirm is retried, never
converted into a cancel.

This single rule is what makes crash recovery decidable. The sweeper reads the
state and knows, without asking anyone, whether to release the holds or to keep
confirming.

### Each shard posts a balanced entry against a suspense account

```
source shard:       Dr merchant payable    Cr transfer suspense
destination shard:  Dr transfer suspense   Cr merchant payable
```

Each entry balances within its own database, which it must — the balance trigger
is per-entry and the databases share nothing. Across shards the two suspense
legs are equal and opposite, so **the system-wide suspense position is zero
whenever no transfer is in flight**, and a non-zero total is exactly the signal
that one half completed and the other did not.

Between the two confirms it is legitimately non-zero. That window is the honest
representation of a distributed transfer.

### Availability subtracts holds, under an advisory lock

Available balance is the derived payable balance minus every outstanding
reservation. The read and the insert are serialised by a transaction-scoped
advisory lock on the merchant and currency.

Without the lock, two transfers of 600 against a balance of 1,000 both read the
same available figure before either inserts, and both pass a check that was
correct when it ran. Removing the lock and re-running the concurrency test
overdraws the account to −200, which is how this was confirmed rather than
assumed.

## Consequences

- Sharding is opt-in by configuration. Unset means one database holding all 64
  logical shards, byte-identical in behaviour to before routing existed.
- Migrations run on every shard and report all failures together. A shard left a
  version behind is a shard whose merchants fail on the first query touching the
  new column.
- The invariant checker sums across shards. One that read only shard 0 would
  report zero while another database was unbalanced.
- Transfers have a CLI entry point (`transferctl`) rather than an HTTP one. The
  service has no authentication yet, and an unauthenticated endpoint that moves
  money between merchants is worse than no endpoint.
- The sweeper is a money-safety control, not a background job: a reservation is
  unspendable funds, and its failure is logged at error level on every pass.

## What this does not do

- **No resharding tooling.** The mapping supports growth; nothing here copies
  rows between databases or coordinates the cutover. The property claimed is
  that the move is contiguous and verifiable, not that it is automated.
- **No hot-key cache.** Balances are still derived from postings on every read.
  A cached balance with explicit invalidation was in the plan and is not built;
  the derived read is correct and the caching is an optimisation with its own
  invalidation bugs.
- **No Redis lease with fencing tokens.** Same-shard serialisation uses Postgres
  advisory locks, which is sufficient because every participant's work is
  single-shard by construction. A cross-shard lock would need fencing, and the
  protocol is designed so that nothing needs one.
- **Transfers are between merchant payable accounts only.** Payouts to bank
  accounts and marketplace splits would use the same coordinator; neither is
  built.
