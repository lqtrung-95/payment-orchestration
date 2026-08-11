# Phase 07 — FX Conversion + Settlement Reconciliation

**Priority:** P1 — the moat · **Status:** Not started · **Week:** 10

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

- [ ] Multi-currency ledger accounts
- [ ] FX rate provider + historical storage
- [ ] Rate lock with TTL
- [ ] FX gain/loss posting
- [ ] Per-provider settlement parsers
- [ ] Matching engine (exact → fuzzy)
- [ ] Eight-category classifier
- [ ] Break resolution workflow + adjustments
- [ ] Seeded settlement generator
- [ ] `reconctl` + summary report
- [ ] Auto-resolve rules

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

## Next steps

Phase 08 — sharding and distributed transactions.
