# Phase 06 — Payment Instrument Binding + Lifecycle

**Priority:** P1 — direct bullseye on the primary target role · **Status:** Not started · **Week:** 9

The Payment Network (A118879B) team's stated mandate: *"supporting binding payment instruments via diverse payment channels and managing the instruments lifecycle."* This phase lets you discuss their exact daily work using their exact vocabulary.

## Key insights

- This is the highest-leverage optional phase for the primary role. If time runs short after Phase 05, do **this** before FX or sharding.
- You never store a PAN. Ever. The vault stores a provider token plus non-sensitive metadata (brand, last4, expiry, fingerprint). Being able to explain PCI scope reduction is the point.
- The instrument fingerprint is what enables dedup across providers — the same card bound twice via different channels should resolve to one logical instrument.
- Instruments have their own lifecycle, independent of transactions. Conflating the two is the naive design.

## Requirements

**Functional**
- Bind instruments across channels: card, e-wallet, bank account.
- Instrument state machine with verification flows.
- Re-binding and token refresh.
- Cross-provider dedup by fingerprint.
- Soft removal preserving transaction history.

**Non-functional**
- No sensitive authentication data at rest.
- Instrument reads are hot — cached with explicit invalidation on state change.

## Architecture

**Instrument state machine**
```
pending_verification → active → suspended → active
         ↓               ↓  ↓        ↓
       failed         expired│     removed
                            └→ requires_reverification → active
```

**Tables**
```
payment_instruments (id, user_id, channel, provider, provider_token,
                     fingerprint, brand, last4, expiry_month, expiry_year,
                     state, verified_at, created_at, version)
instrument_events   (id, instrument_id, event_type, actor, metadata, created_at)  -- append only
instrument_bindings (id, instrument_id, provider, provider_token, state)          -- same card, N providers
```

**Channels and their differing verification paths**

| Channel | Binding | Verification |
|---------|---------|--------------|
| Card | Provider tokenization | Zero-amount auth, or 3DS via `psp-redirect-sim` |
| E-wallet | OAuth-style redirect + consent | Provider callback |
| Bank account | Account + routing details | Micro-deposit simulation (async, multi-day → simulated) |

Three genuinely different flows, which is the point — it forces a real abstraction rather than a card-shaped one.

**Lifecycle automation**
- Expiry sweeper: instruments expiring within 30 days flagged; expired → `expired`.
- Network token refresh simulation — provider notifies of an updated token, binding updated in place, transactions unaffected.
- `InvalidInstrument` from Phase 04's retry table transitions the instrument to `requires_reverification`. This is the cross-phase link worth calling out in interviews.

## Related code files

**Create**
- `internal/domain/instrument/` — aggregate, state machine, fingerprinting
- `internal/service/instrument/` — binding, verification, lifecycle
- `internal/vault/` — token storage, encryption, metadata
- `internal/worker/instrument_sweeper.go`
- `migrations/` — instrument tables

## Implementation steps

1. Instrument aggregate + enforced state machine (same pattern as Phase 02; reuse the transition-table approach).
2. Vault: encrypted provider tokens (envelope encryption, key from config), plaintext non-sensitive metadata only.
3. Fingerprint strategy — provider-supplied where available, else a derived hash; document collision handling.
4. Card binding: tokenize → zero-auth verify → `active`.
5. Wallet binding: redirect + callback correlation (reuses the Phase 05 correlation pattern).
6. Bank binding: micro-deposit issue → verify amounts → `active`, with attempt limits.
7. Cross-provider dedup: same fingerprint → one logical instrument, multiple bindings.
8. Expiry sweeper + token refresh handler.
9. Wire `InvalidInstrument` decline → `requires_reverification`.
10. Redis caching of active instruments per user, invalidated on any state change.

## Todo

- [ ] Instrument aggregate + state machine
- [ ] Encrypted vault + metadata split
- [ ] Fingerprint + cross-provider dedup
- [ ] Card binding + zero-auth verification
- [ ] Wallet binding via redirect/callback
- [ ] Bank binding via micro-deposit
- [ ] Expiry sweeper + token refresh
- [ ] Decline-driven re-verification wiring
- [ ] Instrument cache + invalidation
- [ ] Append-only instrument event log

## Success criteria

- Same card bound via two providers → one logical instrument, two bindings.
- Full lifecycle demoable: bind → verify → use → decline → re-verify → expire → remove.
- Removed instrument retains transaction history integrity (no cascade delete, ever).
- No sensitive authentication data present anywhere in the schema — provable by inspection.

## Risks

| Risk | Mitigation |
|------|-----------|
| Scope creep into a full PCI vault | Simulate provider tokenization; the abstraction is the deliverable, not a real vault |
| Fingerprint collisions | Document the strategy and its limits in an ADR; collisions require manual review, not auto-merge |
| Three verification flows overrun the week | Card first, then bank, wallet last — cut wallet if needed |

## Security considerations

- Envelope encryption for tokens; key rotation path documented even if not implemented.
- Instrument access strictly scoped to the owning user — write the authorization test before the feature.
- Every state change lands in `instrument_events` with actor attribution.
- Micro-deposit verification needs attempt limits — otherwise it is a brute-force oracle.

## Next steps

Phase 07 — FX and reconciliation.
