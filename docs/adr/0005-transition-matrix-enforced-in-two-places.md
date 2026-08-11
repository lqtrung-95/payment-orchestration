# ADR 0005 — Transition matrix enforced in two places

**Status:** Accepted · **Date:** 2026-08-11

## Context

A payment transaction moves through a lifecycle. An illegal move — settling
money that was never captured, un-refunding a refund — is a money bug, not a
validation nicety.

## Decision

The matrix is declared **twice**, and a test proves the copies identical.

1. **In Go**, as `allowedTransitions` on the aggregate. Rejecting an illegal
   move here returns a typed error at the call site.
2. **In Postgres**, as rows in `transaction_state_transitions`, checked by a
   trigger on `UPDATE OF state`. This catches writes that bypass the aggregate
   entirely — migrations, admin sessions, repair scripts.

`TestTransitionMatrixMatchesDatabase` reads both and compares. Two encodings of
one rule drift apart unless something compares them, and a drift means the
application believes a transition is legal that the database will reject at
runtime, or the reverse.

The matrix is held as *data* rather than as a `CHECK` expression so it can be
queried and compared.

Same-state updates are permitted and are not transitions: a second partial
refund changes amounts, not state. A state with no outgoing row is terminal —
`failed`, `cancelled`, `expired`, `refunded`.

Two amount invariants are `CHECK` constraints, since they hold regardless of
state: `captured <= amount` and `refunded <= captured`.

Concurrency is handled by a `version` column. Writers update `WHERE version =`
the value they read, so of two concurrent captures exactly one succeeds and the
loser is told, rather than silently overwriting.

## Alternatives rejected

- **Application-only enforcement.** Anything that writes SQL directly bypasses it.
- **Database-only enforcement.** The error arrives as a constraint violation
  with no domain context, and the aggregate cannot refuse an invalid operation
  before doing work.
- **A `CHECK` expression listing legal pairs.** Not queryable, so the Go copy
  cannot be compared against it, and editing it requires a table rewrite.
- **Last-write-wins instead of a version column.** Two concurrent captures would
  both appear to succeed, with one silently lost.

## Consequences

- Adding a transition means editing two places; the test fails loudly otherwise.
- Callers must handle `ErrVersionConflict` by re-reading and re-deciding, not by
  retrying the same write — its preconditions no longer hold.
- The audit trail (`transaction_state_changes`) is append-only and written in
  the same transaction as the change, so it cannot disagree with the row it
  describes.
