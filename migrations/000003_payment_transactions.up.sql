-- Payment transaction aggregate and its state machine.

CREATE TYPE transaction_state AS ENUM (
    'created',
    'authorizing',
    'authorized',
    'capturing',
    'captured',
    'settled',
    'failed',
    'cancelled',
    'refunded',
    'partially_refunded',
    'expired'
);

CREATE TABLE payment_transactions (
    id              UUID PRIMARY KEY,
    merchant_id     TEXT NOT NULL,

    -- Derived from merchant_id. Stored rather than recomputed so that changing
    -- the hash function later cannot silently relocate existing rows.
    shard_key       TEXT NOT NULL,

    idempotency_key TEXT NOT NULL,
    state           transaction_state NOT NULL DEFAULT 'created',

    amount_minor    BIGINT NOT NULL CHECK (amount_minor > 0),
    currency        currency_code NOT NULL,
    captured_minor  BIGINT NOT NULL DEFAULT 0 CHECK (captured_minor >= 0),
    refunded_minor  BIGINT NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0),

    psp             TEXT,
    psp_reference   TEXT,

    -- Optimistic locking. Writers update WHERE version = the value they read,
    -- so two concurrent capture attempts cannot both succeed.
    version         INT NOT NULL DEFAULT 1,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Domain invariants that hold regardless of which code path writes the row.
    -- Capturing more than was authorised, or refunding more than was captured,
    -- is a money-creating bug; the database refuses it outright.
    CONSTRAINT payment_transactions_capture_within_authorisation
        CHECK (captured_minor <= amount_minor),
    CONSTRAINT payment_transactions_refund_within_capture
        CHECK (refunded_minor <= captured_minor)
);

CREATE INDEX payment_transactions_merchant_created_idx
    ON payment_transactions (merchant_id, created_at DESC);
CREATE INDEX payment_transactions_shard_key_idx ON payment_transactions (shard_key);
CREATE INDEX payment_transactions_state_idx ON payment_transactions (state)
    WHERE state IN ('authorizing', 'capturing');
CREATE INDEX payment_transactions_psp_reference_idx
    ON payment_transactions (psp, psp_reference)
    WHERE psp_reference IS NOT NULL;

CREATE TRIGGER payment_transactions_set_updated_at
    BEFORE UPDATE ON payment_transactions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Transition matrix -----------------------------------------------------------
--
-- Held as data rather than as a CHECK expression so the legal moves can be
-- queried, and so the application's copy can be asserted equal to this one in a
-- test. Two encodings of the same rule drift apart unless something compares
-- them.

CREATE TABLE transaction_state_transitions (
    from_state transaction_state NOT NULL,
    to_state   transaction_state NOT NULL,
    PRIMARY KEY (from_state, to_state)
);

INSERT INTO transaction_state_transitions (from_state, to_state) VALUES
    -- Nothing has been sent to a provider yet, so the payment can still be
    -- abandoned outright.
    ('created', 'authorizing'),
    ('created', 'cancelled'),
    ('created', 'failed'),
    ('created', 'expired'),

    ('authorizing', 'authorized'),
    ('authorizing', 'failed'),
    -- A redirect-based authorisation the customer never completed.
    ('authorizing', 'expired'),

    ('authorized', 'capturing'),
    ('authorized', 'cancelled'),
    ('authorized', 'failed'),
    -- Authorisation holds lapse if never captured.
    ('authorized', 'expired'),

    ('capturing', 'captured'),
    ('capturing', 'failed'),
    -- A capture that failed for a retryable reason returns to authorized so the
    -- attempt can be repeated without re-authorising the customer.
    ('capturing', 'authorized'),

    ('captured', 'settled'),
    ('captured', 'partially_refunded'),
    ('captured', 'refunded'),

    ('settled', 'partially_refunded'),
    ('settled', 'refunded'),

    ('partially_refunded', 'refunded');

-- Reaching a state with no outgoing row is terminal: failed, cancelled,
-- expired, refunded.

CREATE OR REPLACE FUNCTION assert_valid_transaction_transition()
RETURNS TRIGGER AS $$
BEGIN
    -- Same-state updates change amounts, not state — a second partial refund,
    -- for example — and are not transitions.
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM transaction_state_transitions
        WHERE from_state = OLD.state AND to_state = NEW.state
    ) THEN
        RAISE EXCEPTION 'illegal transaction state transition: % -> % (transaction %)',
            OLD.state, NEW.state, NEW.id
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER payment_transactions_valid_transition
    BEFORE UPDATE OF state ON payment_transactions
    FOR EACH ROW EXECUTE FUNCTION assert_valid_transaction_transition();

-- Audit trail -----------------------------------------------------------------

CREATE TABLE transaction_state_changes (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES payment_transactions (id),

    -- Null on the row recording creation, which has no prior state.
    from_state     transaction_state,
    to_state       transaction_state NOT NULL,

    reason         TEXT,
    -- Who caused this: an API caller, a webhook from a named provider, or a
    -- background job. Attribution is what makes the trail usable in an incident.
    actor          TEXT NOT NULL,
    source_ip      INET,
    metadata       JSONB,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX transaction_state_changes_transaction_idx
    ON transaction_state_changes (transaction_id, created_at);

CREATE TRIGGER transaction_state_changes_append_only
    BEFORE UPDATE OR DELETE ON transaction_state_changes
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();
