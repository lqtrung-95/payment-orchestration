-- Settlement file ingestion and reconciliation.
--
-- Reconciliation is not "do the totals match". It is: match what the provider
-- says against what we recorded, classify every disagreement, and track each
-- one to a decision. The taxonomy is the deliverable — a single "mismatch"
-- bucket tells an operator nothing about what to do next.

CREATE TABLE settlement_files (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider       TEXT NOT NULL,
    filename       TEXT NOT NULL,

    -- Ingestion is idempotent on content, not on filename. Providers re-send
    -- files, rename them, and occasionally send yesterday's file again with
    -- today's name; hashing the bytes is the only identity that survives all of
    -- that.
    content_sha256 BYTEA NOT NULL,

    -- The window the file claims to cover. Used to tell a genuinely missing
    -- payment from one that simply settles in the next file.
    period_start   TIMESTAMPTZ NOT NULL,
    period_end     TIMESTAMPTZ NOT NULL,

    row_count      INT NOT NULL CHECK (row_count >= 0),
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT settlement_files_content_unique UNIQUE (provider, content_sha256),
    CONSTRAINT settlement_files_period CHECK (period_end > period_start)
);

CREATE TABLE settlement_rows (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id             UUID NOT NULL REFERENCES settlement_files (id) ON DELETE CASCADE,
    line_number         INT NOT NULL,

    -- The provider's own reference for the charge, and the only identifier that
    -- links its records to ours.
    provider_reference  TEXT NOT NULL,

    gross_minor         BIGINT NOT NULL,
    fee_minor           BIGINT NOT NULL DEFAULT 0,
    net_minor           BIGINT NOT NULL,
    currency            currency_code NOT NULL,

    -- Set when the provider settled in a different currency than it charged.
    -- The rate it actually used is what a locked rate gets compared against.
    settlement_currency currency_code,
    settlement_rate_nano BIGINT CHECK (settlement_rate_nano IS NULL OR settlement_rate_nano > 0),

    settled_at          TIMESTAMPTZ NOT NULL,

    -- The original line, kept verbatim. A parser bug is only diagnosable if the
    -- input survived the parse.
    raw                 TEXT NOT NULL,

    CONSTRAINT settlement_rows_line_unique UNIQUE (file_id, line_number)
);

CREATE INDEX settlement_rows_reference_idx ON settlement_rows (provider_reference);
CREATE INDEX settlement_rows_file_idx ON settlement_rows (file_id);

-- Reconciliation runs ---------------------------------------------------------

CREATE TABLE recon_runs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id       UUID NOT NULL REFERENCES settlement_files (id) ON DELETE CASCADE,

    -- Who asked for this run. Reconciliation proposes money movement, so the
    -- request is attributable like any other privileged action.
    actor         TEXT NOT NULL,

    matched_count INT NOT NULL DEFAULT 0,
    break_count   INT NOT NULL DEFAULT 0,

    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

CREATE INDEX recon_runs_file_idx ON recon_runs (file_id, started_at DESC);

CREATE TYPE recon_break_category AS ENUM (
    'missing_at_psp',       -- in our ledger, absent from settlement
    'missing_internally',   -- in settlement, absent from our ledger
    'amount_mismatch',      -- both present, amounts differ, unexplained
    'fx_drift',             -- amounts differ, explained by the rate moving
    'fee_mismatch',         -- net differs by the fee, schedule drift
    'timing_cutoff',        -- settles in an adjacent window, not missing
    'duplicate_settlement', -- the provider settled one charge twice
    'currency_mismatch'     -- currencies disagree outright
);

CREATE TYPE recon_break_status AS ENUM ('open', 'investigating', 'resolved', 'written_off');

CREATE TABLE recon_breaks (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              UUID NOT NULL REFERENCES recon_runs (id) ON DELETE CASCADE,
    file_id             UUID NOT NULL REFERENCES settlement_files (id) ON DELETE CASCADE,

    category            recon_break_category NOT NULL,

    -- The natural identity of the disagreement: a provider reference, or a
    -- transaction id when there is no settlement row to point at. Combined with
    -- the file and category it makes re-running a reconciliation idempotent —
    -- the same break is recognised rather than raised a second time.
    match_key           TEXT NOT NULL,

    transaction_id      UUID REFERENCES payment_transactions (id),
    settlement_row_id   BIGINT REFERENCES settlement_rows (id) ON DELETE CASCADE,

    -- What we expected, what arrived, and the gap. Nullable because a missing
    -- record has no counterpart to compare against.
    expected_minor      BIGINT,
    actual_minor        BIGINT,
    delta_minor         BIGINT,
    currency            currency_code,

    detail              TEXT NOT NULL,

    status              recon_break_status NOT NULL DEFAULT 'open',
    resolution_note     TEXT,
    resolved_by         TEXT,
    resolved_at         TIMESTAMPTZ,

    -- Set when resolving the break moved money. An adjustment without an entry
    -- is an opinion; with one it is accounting.
    adjustment_entry_id UUID REFERENCES journal_entries (id),

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT recon_breaks_identity UNIQUE (file_id, category, match_key),

    -- A resolved break has to say who decided and why. Reconciliation decisions
    -- are the audit trail for money that did not go where it should have.
    CONSTRAINT recon_breaks_resolution_attributed CHECK (
        status IN ('open', 'investigating')
        OR (resolved_by IS NOT NULL AND resolution_note IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX recon_breaks_run_idx ON recon_breaks (run_id);
CREATE INDEX recon_breaks_open_idx ON recon_breaks (category, created_at)
    WHERE status IN ('open', 'investigating');
CREATE INDEX recon_breaks_transaction_idx ON recon_breaks (transaction_id)
    WHERE transaction_id IS NOT NULL;
