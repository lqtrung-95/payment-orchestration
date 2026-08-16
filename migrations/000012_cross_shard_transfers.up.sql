-- Cross-shard transfers, coordinated with try-confirm-cancel.
--
-- Two merchants on different physical databases cannot be moved between in one
-- transaction: Postgres does not commit across databases. The alternative to a
-- protocol is two independent commits, which fail halfway and leave money in
-- neither place or in both.
--
-- Both tables are created on every shard because migrations are, but they are
-- used from different places. tcc_transfers is coordinator state and is only
-- written on shard 0 — the transfer spans two merchants and belongs to neither
-- shard. tcc_reservations is participant state and is written on the
-- participant's own shard, because reserving funds has to check a balance and
-- record the hold in one transaction, and that balance lives there.

CREATE TYPE tcc_state AS ENUM (
    'trying',      -- reservations being taken; may still be cancelled
    'confirming',  -- every participant reserved; the transfer WILL complete
    'confirmed',
    'cancelling',
    'cancelled'
);

CREATE TYPE tcc_reservation_state AS ENUM ('reserved', 'confirmed', 'cancelled');

CREATE TYPE tcc_role AS ENUM ('source', 'destination');

CREATE TABLE tcc_transfers (
    id                    UUID PRIMARY KEY,
    state                 tcc_state NOT NULL DEFAULT 'trying',

    source_merchant       TEXT NOT NULL,
    source_shard_key      TEXT NOT NULL,
    destination_merchant  TEXT NOT NULL,
    destination_shard_key TEXT NOT NULL,

    amount_minor          BIGINT NOT NULL CHECK (amount_minor > 0),
    currency              currency_code NOT NULL,

    -- Retrying a transfer request must not move the money twice. The caller's
    -- key is the identity of the intent, and the unique index is what decides
    -- between two concurrent submissions of it.
    idempotency_key       TEXT NOT NULL,

    -- After this instant a transfer still in `trying` is cancelled by the
    -- sweeper. A reservation held forever by a coordinator that died is funds
    -- the merchant cannot spend and no one is going to release.
    timeout_at            TIMESTAMPTZ NOT NULL,

    attempts              INT NOT NULL DEFAULT 0,
    last_error            TEXT,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at           TIMESTAMPTZ,

    CONSTRAINT tcc_transfers_idempotency_key UNIQUE (idempotency_key),

    -- A transfer to oneself has no participants to coordinate and would take
    -- the same advisory lock twice.
    CONSTRAINT tcc_transfers_distinct_parties
        CHECK (source_merchant <> destination_merchant)
);

-- The sweeper's query: unresolved transfers, oldest deadline first.
CREATE INDEX tcc_transfers_unresolved_idx ON tcc_transfers (timeout_at)
    WHERE state IN ('trying', 'confirming', 'cancelling');

CREATE TRIGGER tcc_transfers_set_updated_at
    BEFORE UPDATE ON tcc_transfers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE tcc_reservations (
    id           UUID PRIMARY KEY,
    transfer_id  UUID NOT NULL,
    role         tcc_role NOT NULL,

    merchant_id  TEXT NOT NULL,
    shard_key    TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency     currency_code NOT NULL,

    state        tcc_reservation_state NOT NULL DEFAULT 'reserved',

    -- The journal entry written when this side confirmed. NULL while reserved,
    -- which is the point of the protocol: the hold exists and the money has not
    -- moved. It also makes confirmation self-evidencing — a reservation marked
    -- confirmed with no entry is a bug this column exposes.
    entry_id     UUID REFERENCES journal_entries (id),

    expires_at   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ,

    -- Try is idempotent by construction: a second attempt for the same side of
    -- the same transfer collides here rather than taking a second hold.
    CONSTRAINT tcc_reservations_one_per_role UNIQUE (transfer_id, role),

    CONSTRAINT tcc_reservations_entry_matches_state
        CHECK ((state = 'confirmed') = (entry_id IS NOT NULL))
);

-- Serves the available-balance query, which subtracts outstanding holds from
-- the derived balance. Partial because only reserved rows reduce what is
-- spendable; resolved ones are already reflected in the postings.
CREATE INDEX tcc_reservations_outstanding_idx
    ON tcc_reservations (merchant_id, currency)
    WHERE state = 'reserved';

CREATE INDEX tcc_reservations_transfer_idx ON tcc_reservations (transfer_id);

CREATE TRIGGER tcc_reservations_set_updated_at
    BEFORE UPDATE ON tcc_reservations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
