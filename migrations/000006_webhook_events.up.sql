-- Raw webhook event log and the ordering guard that makes replay safe.

CREATE TYPE webhook_event_status AS ENUM (
    -- Persisted and acknowledged, not yet processed.
    'received',
    -- Applied to a transaction.
    'applied',
    -- Older than what has already been applied. Never applied, never dropped.
    'superseded',
    -- The transition it implied is absent from the state machine.
    'rejected',
    -- Understood, but implying no state change.
    'ignored',
    -- No transaction could be correlated within the parking window.
    'unmatched'
);

CREATE TABLE webhook_events_raw (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    provider          TEXT NOT NULL,
    provider_event_id TEXT NOT NULL,

    -- The exact bytes received, not JSONB.
    --
    -- The signature was computed over these bytes. JSONB reorders keys and
    -- rewrites whitespace, so a stored JSONB payload can no longer be verified
    -- against the signature that arrived with it — which would make the raw log
    -- useless as evidence, and that is its entire purpose.
    payload           BYTEA NOT NULL,
    signature         TEXT NOT NULL,

    -- The provider's own ordering token, normalised by the per-provider mapping.
    -- Providers that expose no sequence get one derived from the event time.
    sequence          BIGINT NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL,

    -- The provider's reference for the charge; how the event finds its
    -- transaction. Recorded even when correlation fails, so an unmatched event
    -- can still be investigated.
    reference         TEXT NOT NULL,
    transaction_id    UUID REFERENCES payment_transactions (id),

    status            webhook_event_status NOT NULL DEFAULT 'received',
    note              TEXT,

    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at      TIMESTAMPTZ,

    -- Deduplication, enforced here rather than in a cache.
    --
    -- Redis in front of this is a latency optimisation and may be wrong in both
    -- directions after a failover; a unique index cannot be. A provider that
    -- retries a delivery it believes failed must hit this constraint and be told
    -- 200, not have the event processed a second time.
    CONSTRAINT webhook_events_raw_provider_event_unique
        UNIQUE (provider, provider_event_id)
);

-- Correlation lookup, and the replay tool's ordering.
CREATE INDEX webhook_events_raw_reference_idx ON webhook_events_raw (reference, sequence);
CREATE INDEX webhook_events_raw_transaction_idx ON webhook_events_raw (transaction_id)
    WHERE transaction_id IS NOT NULL;

-- Finds events still awaiting correlation, for the unmatched-event alert.
CREATE INDEX webhook_events_raw_unresolved_idx ON webhook_events_raw (received_at)
    WHERE status IN ('received', 'unmatched');

-- Ordering guard -------------------------------------------------------------
--
-- Webhooks arrive out of order routinely. The guard is a high-water mark rather
-- than an assumption about arrival order: an event whose sequence is at or below
-- what has already been applied is stale by its own account, whenever it turns
-- up. Combined with the transition matrix, a late `authorizing` event after a
-- `captured` one is refused twice over.
ALTER TABLE payment_transactions
    ADD COLUMN last_applied_event_seq BIGINT NOT NULL DEFAULT 0;

-- Correlation is by provider reference alone. The reference is globally unique
-- and is the only identifier a webhook carries, so the composite index on
-- (psp, psp_reference) cannot serve the lookup.
DROP INDEX IF EXISTS payment_transactions_psp_reference_idx;
CREATE INDEX payment_transactions_psp_reference_idx
    ON payment_transactions (psp_reference)
    WHERE psp_reference IS NOT NULL;
