# ADR 0003 — Shard key is merchant_id

**Status:** Accepted · **Date:** 2026-08-11

## Context

The data set will eventually need to span more than one database. The shard key
determines which queries stay local and which become scatter-gather, and it
cannot be changed afterwards without rewriting every row while the service
stays online.

Physical routing is not built yet. The key is decided and stored now precisely
because backfilling it later is the expensive path.

## Decision

`shard_key = "s" + (fnv1a64(merchant_id) mod 64)`, stored on every transaction,
ledger account, and journal entry.

The logical shard count is fixed at **64** and mapped onto a variable number of
physical databases. Growing capacity is then a mapping change, not a rehash.

FNV-1a is used because it is in the standard library and its output is fixed
forever. A hash whose results could change between Go releases would silently
relocate existing merchants.

The key is stored rather than recomputed on read, so that changing the
derivation later cannot silently move existing rows.

## Alternatives rejected

- **user_id.** Splits a single merchant's ledger across every shard, making
  balance, settlement, and reconciliation — the dominant queries — scatter-gather.
- **transaction_id.** Best possible distribution, worst possible locality: every
  merchant-level aggregate touches every shard.
- **Variable logical shard count (hash mod N physical).** Adding a database
  rehashes and relocates most rows. The fixed-logical indirection exists solely
  to avoid this.
- **Deriving the key on read instead of storing it.** Couples row placement to
  the current code version.

## Consequences

- Merchant-scoped reads stay within one shard.
- A very large merchant cannot be split across shards. If that becomes real, the
  answer is a per-merchant override in the mapping layer, not a different key.
- Cross-shard money movement needs a distributed transaction protocol, decided
  separately.
- Shard routing must always derive from the authenticated merchant server-side,
  never from client input.
