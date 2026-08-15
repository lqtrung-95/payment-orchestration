# Phase 07 — FX Conversion + Settlement Reconciliation

**Priority:** P1 — the moat · **Status:** Complete · **Week:** 10 · **Verified on 2026-08-15**

Reconciliation is the rarest item on the entire feature list. Thousands of candidates have "integrated Stripe." Almost none have diffed a settlement file against an internal ledger and classified the breaks. ByteDance names *reconciliations* and *foreign exchange conversion* directly in their team description.

## Key insights

- FX makes reconciliation genuinely hard, which is exactly why they belong in the same phase. Authorization FX ≠ settlement FX, and that gap is itself a break category no toy project models.
- Real reconciliation is not "do the totals match." It is classify → explain → propose resolution → track to closure. The classification taxonomy is the deliverable.
- Seeded breaks are how you prove detection. Generate settlement files with known, deliberate defects and assert 100% detection.

## Requirements

**Functional**
- Multi-currency ledger with per-currency accounts.
- FX rate provider with rate locking at authorization + TTL.
- Settlement file ingestion (CSV/JSON, per provider format).
- Matching engine producing classified breaks.
- Break resolution workflow with audit trail.

**Non-functional**
- Reconcile 100k settlement rows in under 60s.
- Reconciliation is idempotent — re-running the same file changes nothing.

## Architecture

**FX**
```
fx_rates       (id, base, quote, rate, source, valid_from, valid_to)
fx_rate_locks  (id, transaction_id, base, quote, rate, locked_at, expires_at)
```
Rate locked at authorization with a TTL. At settlement, the provider's actual rate is compared against the locked rate — any delta posts to an **FX gain/loss account** in the ledger. That posting is what keeps double-entry balanced across currencies, and is the detail that shows real accounting understanding.

**Reconciliation pipeline**
```
settlement file → parse (per-provider adapter) → normalize
  → match against ledger (provider ref, then amount+date fuzzy)
  → classify unmatched → persist breaks → report
```

**Break taxonomy**

| Category | Meaning | Typical cause |
|----------|---------|---------------|
| `missing_at_psp` | In our ledger, not in settlement | Not yet settled, or lost at PSP |
| `missing_internally` | In settlement, not in our ledger | Dropped webhook, ingestion gap |
| `amount_mismatch` | Both present, amounts differ | Partial capture, adjustment |
| `fx_drift` | Amounts differ, explained by rate delta | Auth rate ≠ settlement rate |
| `fee_mismatch` | Net differs by fee amount | Fee schedule drift |
| `timing_cutoff` | Present in the adjacent settlement window | Cutoff boundary, timezone |
| `duplicate_settlement` | Same transaction settled twice | PSP-side error |
| `currency_mismatch` | Currency codes differ | Routing or config error |

Eight categories. `fx_drift` and `timing_cutoff` are the ones that signal you have actually thought about this.

**Resolution workflow:** each break gets `open → investigating → resolved | written_off`, with an adjustment journal entry where resolution moves money. Auto-resolve rules for `fx_drift` within tolerance and `timing_cutoff`; everything else requires explicit action.

## Related code files

**Create**
- `internal/domain/fx/` — rate provider, lock, conversion
- `internal/recon/` — parser adapters, normalizer, matcher, classifier
- `internal/recon/breaks/` — taxonomy, resolution workflow
- `internal/worker/recon_job.go`
- `cmd/reconctl/` — run reconciliation, inspect breaks
- `testdata/settlements/` — generator with seeded, known defects

## Implementation steps

1. Multi-currency accounts; enforce that a single journal entry never mixes currencies without an explicit FX leg.
2. FX rate provider (simulated, deterministic) + historical rate storage.
3. Rate lock at authorization with TTL; expiry forces re-quote.
4. FX gain/loss account + posting logic on settlement delta.
5. Per-provider settlement file parsers → normalized internal shape.
6. Matching: exact on provider reference first, then fuzzy on amount + date window.
7. Classifier implementing all eight categories, in priority order.
8. Break persistence + resolution workflow + adjustment entries.
9. Settlement generator with seeded defects — one per category, count-controlled.
10. `reconctl` + a summary report (matched, break count by category, total exposure).
11. Auto-resolve rules for `fx_drift` within tolerance and `timing_cutoff`.

## Todo

- [x] Multi-currency ledger accounts — created on demand, per currency
- [x] FX rate provider + historical storage
- [x] Rate lock with TTL, append-only, expiry enforced inside Convert
- [ ] FX gain/loss posting — **not built**, see Deferred
- [x] Per-provider settlement parsers
- [x] Matching engine — exact only; fuzzy deliberately not built, see Deviations
- [x] Eight-category classifier
- [x] Break resolution workflow — adjustments modelled, not written
- [x] Seeded settlement generator
- [x] `reconctl` + summary report with exposure
- [x] Auto-resolve rules — `fx_drift` and `timing_cutoff` only

## Success criteria

- 100% of seeded breaks detected and correctly classified, across all eight categories.
- Ledger stays balanced after every FX posting and every adjustment entry.
- Re-running the same settlement file produces zero new breaks (idempotent).
- 100k rows reconciled in under 60s.

## Risks

| Risk | Mitigation |
|------|-----------|
| Fuzzy matching produces false positives | Tight amount+date window; never auto-resolve a fuzzy match |
| FX rounding creates phantom breaks | Banker's rounding, documented tolerance threshold |
| Scope creep into a full accounting system | Reconciliation + adjustments only. No P&L, no tax, no reporting suite. |

## Security considerations

- Settlement files contain financial data — encrypt at rest, restrict access, log every read.
- Adjustment entries are money movement: require actor attribution and reason on every one.
- Auto-resolve tolerance is a config value with an audit trail; changing it is a privileged operation.

## Verified on 2026-08-15

- **All eight categories detected and correctly classified** from a generated
  file with one defect of each kind planted deliberately, asserted by count. A
  clean payment in the same file reconciles silently, so the test also proves
  the classifier does not manufacture breaks.
- **FX drift is distinguished from an unexplained mismatch**: the same size of
  difference lands in `fx_drift` when the provider's stated rate reproduces its
  figure, and would land in `amount_mismatch` otherwise.
- **Re-running a reconciliation raises zero new breaks**, and re-ingesting
  identical bytes under a different filename recognises the original file.
- **Capture posts a balanced three-legged entry**; debits equal credits, and
  payable plus fee equals clearing, so no minor unit is created or destroyed.
- **Rounding is unbiased** across a thousand exact ties, which half-up fails.
- Full suite green under `-race`, lint clean.

## Bugs found

**Capture charged the customer before validating.** The amount was checked after
the provider call, so capturing 80.00 against a 50.00 authorisation took the
funds and then refused to record them — money gone, ledger empty. Found by
probing the provider's charge count; the obvious assertions (an error is
returned, the ledger is empty) pass in both the broken and fixed versions.
Validation now runs before the request reaches the provider.

**The ledger had never been written to.** `RecordEntry` was called only by its
own tests. This phase's premise — diffing settlement against the ledger — was
unimplementable until capture existed, which the plan did not account for.

## Deviations from the plan

- **Fuzzy matching not built.** Pairing records that merely share an amount and
  a date produces a confident wrong answer, which is worse than an unmatched
  pair a human reviews. The plan's risk table already said never to auto-resolve
  a fuzzy match; declining to make the match at all is the same argument
  followed through.
- **Capture and fee schedules added.** Not in this phase's scope, but the phase
  is meaningless without them.
- **`timing_cutoff` is decided by capture time, not by the row.** A payment
  outside the file's window is absent from it by definition; a late row would
  simply match and agree.

## Deferred

- **FX gain/loss posting is not written.** The account purpose exists and the
  drift is detected and reported, but no adjustment entry is raised, so rate
  movement is visible rather than accounted. This is the largest remaining gap
  in the phase and the honest limit on the "keeps a cross-currency ledger
  balanced" claim.
- **Adjustment entries generally.** `recon_breaks.adjustment_entry_id` is
  modelled and never populated: resolving a break records a decision without
  moving money.
- **100k rows in under 60s is unmeasured.** Reconciliation loads a file and its
  ledger window into memory, which suits demo scale and is the first thing to
  revisit under load.
- **Refund has no HTTP surface**, and refund settlement rows are not classified.
- **Settlement files are unencrypted** and retained indefinitely.

## Next steps

Phase 08 — sharding and distributed transactions.
