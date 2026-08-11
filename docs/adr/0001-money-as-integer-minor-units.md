# ADR 0001 — Money as integer minor units

**Status:** Accepted · **Date:** 2026-08-11

## Context

Every monetary value in the system needs one representation. The choice is
load-bearing: it cannot be changed later without touching the ledger, the API
contract, and every stored row.

## Decision

`money.Money` is an `int64` count of minor units paired with an ISO-4217
currency. There is no constructor accepting a float, and none will be added.

Arithmetic is currency-checked and overflow-checked; both failures return an
error. Proportional amounts (fees, splits) go through `Allocate`, which
distributes the truncation remainder one minor unit at a time so the parts
always sum back to the original exactly.

The API accepts and returns amounts as integers. A decimal such as `125.50` is
rejected at the boundary rather than truncated.

## Alternatives rejected

- **float64.** Cannot represent 0.10 exactly. Errors accumulate silently and
  surface much later as reconciliation breaks that look like provider faults.
- **Decimal string, parsed on use.** Correct in principle, but it pushes parsing
  and rounding decisions to every call site, and one call site will get it wrong.
- **`shopspring/decimal` or similar.** Arbitrary precision solves a problem this
  domain does not have — currencies have a fixed minor unit — at the cost of
  heap allocation on every operation and an ambiguous rounding policy.
- **Storing a currency exponent alongside every amount.** Adds a field that must
  be kept consistent, to answer a question the currency code already answers.

## Consequences

- Amounts are exact, comparable, and safe to sum in SQL.
- Currencies with non-standard exponents (JPY, VND, KWD) work without special
  cases; the exponent is consulted only for display.
- Callers must handle an error from arithmetic. This is deliberate friction: a
  currency mismatch is a bug, and silently coercing it would hide the bug.
- Amounts above roughly 9.2 × 10^18 minor units are unrepresentable. For any
  real currency this is far beyond plausible transaction values.
