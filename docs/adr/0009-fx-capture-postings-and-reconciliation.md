# ADR 0009 — FX, capture postings, and settlement reconciliation

**Status:** Accepted · **Date:** 2026-08-15

## Context

Reconciliation diffs what a provider says against what we recorded. That
requires three things this system did not have: money actually recorded in the
ledger, a way to express rates, and a taxonomy for disagreement.

The first was a genuine gap rather than an oversight in planning. The ledger was
built in phase 02 and **nothing ever wrote to it** — `RecordEntry` was called
only by its own tests, and `journal_entries` was empty. Reconciling against it
would have classified every settlement row as missing internally, and the
eight-category taxonomy would have been decoration.

## Decision

### Rates are fixed-point integers, quoted in major units

Scaled by 1e9. Same reasoning as money in ADR 0001: 1.1 has no exact binary
representation, and a conversion wrong by one ten-millionth becomes a break
nobody can explain.

Major units, not minor, because minor units differ per currency — a rate
expressed in them would silently mean something different for USD than for JPY.

**Conversion consults the currency exponent, and it is the only thing in the
system that does.** Within a currency, minor units are minor units and the
exponent is display-only. Across currencies it is arithmetic: 100.00 USD to JPY
is two decimal places becoming none, so ignoring it is wrong by a factor of a
hundred rather than by a rounding error. The comment in `money/currency.go`
saying arithmetic never consults the table was true when written; it is now
corrected rather than left to mislead.

### Rounding is half to even, in one shared place

Half-up rounds every exact tie in the same direction, so error accumulates one
way and eventually surfaces as systematic drift against a provider's own
figures. Half-even splits the ties. A test asserts zero drift across a thousand
ties, which half-up fails.

It lives in `money.DivRoundHalfEven` because both FX conversion and fee
calculation need it. Two copies of a rounding rule drift apart, and the symptom
is a fee that disagrees with a conversion by one minor unit in a way nobody can
reproduce.

### Capture posts three legs

    Dr  psp clearing         gross
    Cr  merchant payable     gross − fee
    Cr  platform fee revenue fee

The fee comes out of what the merchant is owed, never out of what the customer
pays. The customer authorised one amount and that is the amount taken; netting a
fee against the charge would make authorisation and capture disagree, which is a
reconciliation break by construction.

Authorisation still posts nothing. A hold at the issuer is a reservation, not a
transfer, and recording it would inflate every balance by the value of holds
that may never be captured.

### Validation happens before the provider call

Ordering here costs real money if it is wrong, and it was wrong. The aggregate
refuses an over-capture, but that refusal ran *after* the provider call: the
provider took 80.00 against a 50.00 authorisation and we then declined to record
it. Funds gone, ledger empty, no trace.

Everything checkable — currency, sign, remaining capturable, state — is now
checked before the request reaches the provider. The regression test asserts the
provider's charge count, because asserting only "an error was returned" and "the
ledger is empty" passes in both the broken and the fixed version.

### Reconciliation reads the ledger, not the transaction table

`payment_transactions.captured_minor` says what the service intended to record.
The postings say what was actually accounted for. Reconciling against the column
would compare the provider to our own intent and quietly agree with itself.

### Matching is exact on the provider reference only

The plan called for a fuzzy fallback on amount and date. It is deliberately not
built. Pairing two records because they share an amount and a day produces a
confident wrong answer, which is worse than an unmatched pair a human looks at —
because nobody ever looks at the confident one again. The plan's own risk table
already said never to auto-resolve a fuzzy match; not making the match at all is
the same argument taken to its conclusion.

### The taxonomy is the deliverable, and its order is load-bearing

Eight categories, considered most-specific first. A duplicate settlement also
presents as an amount mismatch; so does FX drift. Testing the specific
explanations before the general one is what stops every break landing in the
vaguest bucket that fits. Reordering the classifier silently degrades it into a
single "amounts differ" category.

`fx_drift` is decided by reproducing the provider's own arithmetic, not by size:
does applying the rate it says it used produce the figure it sent? A difference
of the right size for the wrong reason is still unexplained, and treating size
alone as the criterion is how a real error gets closed as drift.

`timing_cutoff` is the one category defined by *when* the capture happened
rather than by what the row says. A capture outside the file's window is not
missing — it settles in the next file — and reporting it as lost trains
operators to ignore the category.

### The settlement amount is its own column

`gross_minor` is what the customer was charged; `settled_minor` is what lands in
our account, in `settlement_currency`. They were briefly the same column, which
meant one field held either a EUR figure or a USD one depending on the value of
a different field. The arithmetic happened to work because both sides made the
same assumption — the worst kind of correct. The three FX fields are now
required together, enforced by a CHECK.

### Only explained categories may auto-resolve

`fx_drift` and `timing_cutoff`, and both because the difference is *explained*
rather than merely small. Everything else needs a decision: an amount mismatch
might be the provider's error or ours, and guessing on an operator's behalf is
how a real discrepancy gets closed as noise.

### Re-running is safe

Breaks carry a natural identity — file, category, and match key — so a repeat
run recognises the same disagreement rather than raising it again, and any
decision already recorded against it survives. Files are identified by content
hash, because providers re-send them, rename them, and occasionally send
yesterday's data under today's name.

Resolving demands an actor and a reason, enforced by a CHECK constraint as well
as in Go. A break closed with no name and no reason is indistinguishable from
one quietly deleted. A decided break is never reopened; the correct move is a
new break referencing the same records, so the history of what was decided when
survives.

## Consequences

- Capture has an HTTP surface, closing a standing gap. Refund still does not.
- Fee schedules are effective-dated rows, so a superseded schedule is a real and
  reproducible `fee_mismatch` rather than a hypothetical one.
- `settlement_files`, `settlement_rows`, and `recon_breaks` grow without bound
  and hold financial data; they need retention and encryption at rest.
- Auto-resolution writes an adjustment entry for `fx_drift` and none for
  `timing_cutoff`, which is the distinction that matters: one is money that
  moved, the other is a payment arriving in the next file. Resolving any other
  category by hand still records a decision without an entry.
- The settlement entry — moving the balance out of the charge-currency clearing
  account and into the bank — is not built, so that account still shows a
  converted payment outstanding after it has settled.
- The 100k-rows-in-60s target is unmeasured. Reconciliation currently loads a
  file and its ledger window entirely into memory, which is fine at demo scale
  and is the first thing to revisit under load.
