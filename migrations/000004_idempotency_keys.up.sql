-- Idempotency key storage.
--
-- The unique constraint below is the authority. Redis may later short-circuit
-- the common case, but a cache cannot arbitrate a race between two concurrent
-- requests carrying the same key — only a constraint inside the transaction can.

CREATE TYPE idempotency_state AS ENUM ('in_flight', 'completed', 'failed');

CREATE TABLE idempotency_keys (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Keys are scoped per merchant. A global key namespace lets one merchant
    -- collide with another's key by accident, and lets them probe for it on
    -- purpose: a bare 409 would reveal that some other tenant used that value.
    merchant_id         TEXT NOT NULL,
    key                 TEXT NOT NULL,

    -- Digest of the canonicalised request. Replaying a key with a different
    -- body is a client bug, and answering it with the first request's response
    -- would silently drop the second payment.
    request_fingerprint BYTEA NOT NULL,

    state               idempotency_state NOT NULL DEFAULT 'in_flight',

    -- Fencing token, reissued on every claim. Completing requires presenting
    -- the current token, so an owner whose lapsed claim was taken over cannot
    -- write its stale result over the new owner's work. Without this, a process
    -- that stalled past the lock TTL and then woke up would overwrite the
    -- outcome of the request that legitimately replaced it.
    claim_token         UUID NOT NULL DEFAULT gen_random_uuid(),

    -- Captured so a retry can be answered byte-for-byte as the original was.
    --
    -- Stored as BYTEA, not JSONB. JSONB normalises on write — it reorders object
    -- keys and rewrites whitespace — so a replayed response would differ from
    -- the original byte for byte. That breaks any client verifying a signature
    -- over the response, and makes "identical on retry" untrue in exactly the
    -- way that is hardest to notice. Querying into stored responses is not a
    -- requirement; reproducing them exactly is.
    response_status     INT,
    response_body       BYTEA,
    transaction_id      UUID REFERENCES payment_transactions (id),

    -- When the in-flight claim was taken. A row stuck in_flight past this plus
    -- the request timeout belongs to a process that died mid-request.
    locked_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT idempotency_keys_scoped_key UNIQUE (merchant_id, key),

    -- A completed row without a stored response cannot answer a retry, which
    -- would leave the caller unable to ever learn the outcome.
    CONSTRAINT idempotency_keys_completed_has_response
        CHECK (state <> 'completed' OR response_status IS NOT NULL)
);

CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys (expires_at);

-- Finds claims abandoned by a crashed process.
CREATE INDEX idempotency_keys_in_flight_idx ON idempotency_keys (locked_at)
    WHERE state = 'in_flight';

CREATE TRIGGER idempotency_keys_set_updated_at
    BEFORE UPDATE ON idempotency_keys
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
