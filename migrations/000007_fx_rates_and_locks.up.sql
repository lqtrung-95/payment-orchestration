-- Foreign exchange rates and the locks taken against them.

-- Rates are fixed-point integers, never floating point.
--
-- Same reasoning as money itself (see the money package): a rate of 1.1 has no
-- exact binary representation, and a conversion that is off by one ten-millionth
-- becomes a reconciliation break that nobody can explain. The scale is 1e9, so
-- a EUR/USD rate of 1.085 is stored as 1085000000.
--
-- The convention is "one major unit of base buys rate/1e9 major units of quote".
-- Major, not minor: minor units differ per currency, and a rate expressed in
-- them would silently change meaning between USD and JPY.
CREATE TABLE fx_rates (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    base       currency_code NOT NULL,
    quote      currency_code NOT NULL,
    rate_nano  BIGINT NOT NULL CHECK (rate_nano > 0),

    source     TEXT NOT NULL,

    -- Rates are kept historically rather than overwritten. Reconciliation asks
    -- "what was the rate when this authorised", months later, and a table that
    -- only holds the current rate cannot answer it.
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to   TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fx_rates_distinct_pair CHECK (base <> quote),
    CONSTRAINT fx_rates_valid_window CHECK (valid_to IS NULL OR valid_to > valid_from)
);

-- Point-in-time lookup: the newest rate for a pair whose window contains an
-- instant.
CREATE INDEX fx_rates_lookup_idx ON fx_rates (base, quote, valid_from DESC);

-- At most one open-ended rate per pair per source. Two current rates for the
-- same pair means every conversion is a coin flip over which one it used.
CREATE UNIQUE INDEX fx_rates_current_idx ON fx_rates (base, quote, source)
    WHERE valid_to IS NULL;

-- Rate locks -----------------------------------------------------------------
--
-- The rate quoted to a customer at authorisation, held for a bounded window.
-- Settlement compares the provider's actual rate against this one, and the
-- difference posts to the FX gain/loss account — which is what keeps a
-- cross-currency ledger balanced rather than quietly absorbing the movement.
CREATE TABLE fx_rate_locks (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL REFERENCES payment_transactions (id),

    base           currency_code NOT NULL,
    quote          currency_code NOT NULL,
    rate_nano      BIGINT NOT NULL CHECK (rate_nano > 0),
    source         TEXT NOT NULL,

    locked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,

    -- One lock per transaction. A second lock would mean two rates were promised
    -- for one payment, and nothing downstream could say which was honoured.
    CONSTRAINT fx_rate_locks_one_per_transaction UNIQUE (transaction_id),
    CONSTRAINT fx_rate_locks_distinct_pair CHECK (base <> quote),
    CONSTRAINT fx_rate_locks_expiry_after_lock CHECK (expires_at > locked_at)
);

CREATE INDEX fx_rate_locks_expiry_idx ON fx_rate_locks (expires_at);

-- A lock that can be edited after the fact is not a lock. Re-quoting creates a
-- new transaction's lock; it never rewrites the promise already made.
CREATE TRIGGER fx_rate_locks_append_only
    BEFORE UPDATE OR DELETE ON fx_rate_locks
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();
