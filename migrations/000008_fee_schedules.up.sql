-- Merchant fee schedules.
--
-- Effective-dated rather than mutable. A fee is a term of a commercial
-- agreement: reconstructing what a merchant was charged last quarter requires
-- knowing the schedule that applied then, and a table that only holds the
-- current rate cannot answer it. It is also the honest cause of a
-- `fee_mismatch` break — the provider applied a schedule we had already
-- superseded.

CREATE TABLE fee_schedules (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The platform-wide default is stored as the literal '*'. A merchant with
    -- no negotiated rate falls back to it rather than to a constant compiled
    -- into the service, so the effective rate is always answerable from data.
    merchant_id    TEXT NOT NULL,
    currency       currency_code NOT NULL,

    -- Basis points: 290 is 2.90%. Integer, because a percentage stored as a
    -- float reintroduces exactly the representation problem that keeping money
    -- in minor units avoids.
    basis_points   INT NOT NULL CHECK (basis_points >= 0 AND basis_points <= 10000),
    fixed_minor    BIGINT NOT NULL DEFAULT 0 CHECK (fixed_minor >= 0),

    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fee_schedules_window CHECK (effective_to IS NULL OR effective_to > effective_from),
    CONSTRAINT fee_schedules_unique_start UNIQUE (merchant_id, currency, effective_from)
);

-- Point-in-time lookup: the newest schedule whose window contains an instant.
CREATE INDEX fee_schedules_lookup_idx
    ON fee_schedules (merchant_id, currency, effective_from DESC);

-- At most one open-ended schedule per merchant and currency. Two would make
-- every fee calculation a coin flip over which agreement applied.
CREATE UNIQUE INDEX fee_schedules_current_idx
    ON fee_schedules (merchant_id, currency)
    WHERE effective_to IS NULL;

-- A conservative platform default so a merchant with no negotiated rate still
-- produces a defined fee rather than an error at capture time.
INSERT INTO fee_schedules (merchant_id, currency, basis_points, fixed_minor, effective_from)
VALUES
    ('*', 'USD', 290, 30, '2020-01-01T00:00:00Z'),
    ('*', 'EUR', 290, 25, '2020-01-01T00:00:00Z'),
    ('*', 'GBP', 290, 20, '2020-01-01T00:00:00Z'),
    ('*', 'SGD', 320, 50, '2020-01-01T00:00:00Z'),
    ('*', 'JPY', 360, 0,  '2020-01-01T00:00:00Z');
