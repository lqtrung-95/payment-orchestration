# ADR 0002 — Double-entry ledger with derived balances

**Status:** Accepted · **Date:** 2026-08-11

## Context

The system must be able to answer, at any moment, how much is owed to each
merchant and how much each provider owes us — and must be able to prove those
figures against a settlement file.

## Decision

A double-entry ledger: immutable `postings` grouped into `journal_entries`.
Amounts are always positive; `direction` (debit or credit) carries the sign.

**Balances are never stored.** They are derived by aggregating postings and
oriented by account type, so a positive figure always reads as "more of what
this account is for".

Four rules are enforced by the database, not by convention:

1. Entries must balance per currency, checked by a `DEFERRABLE INITIALLY
   DEFERRED` constraint trigger that fires at COMMIT — an entry is legitimately
   unbalanced between its first and last posting.
2. Postings and journal entries reject `UPDATE` and `DELETE`.
3. A posting may only touch an account holding the same currency, enforced by a
   composite foreign key on `(account_id, currency)`.
4. An entry with no postings is rejected.

**The ledger records money that moved, not intent.** Creating or authorising a
payment posts nothing; postings begin at capture.

## Alternatives rejected

- **Stored balance column, updated on write.** A second source of truth that
  drifts from the entries it summarises. Once it has drifted there is no way to
  tell which of the two is wrong. A cached balance is added later as an
  optimisation, but the derived figure stays authoritative.
- **Signed amounts without a direction column.** Makes "a negative credit"
  expressible, and every downstream report then has to decide what it means.
- **Enforcing balance only in application code.** Migrations, admin sessions,
  and repair scripts all bypass application code. The domain layer checks too,
  but only so the error names the offending postings.
- **Posting at authorisation.** An authorisation is a hold at the issuer, not a
  transfer. Recording it would inflate every balance by the value of holds that
  may never be captured, and reconciliation against settlement would never match.

## Consequences

- Balance reads cost an aggregate. Hot accounts need a cached snapshot, which is
  an optimisation over a still-authoritative derivation.
- Corrections are made by writing a reversing entry, never by editing history.
- The invariant `sum(debits) = sum(credits)` per currency is checkable by a
  single query, and is exported as a continuous gauge rather than left to tests.
- Cleaning test data requires `TRUNCATE`; `DELETE` is refused by design.
