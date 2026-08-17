-- Per-merchant API keys.
--
-- This table lives on shard 0 rather than on the merchant's own shard, for the
-- same reason the webhook log and the consumer dedup index do: authentication
-- happens before the merchant is known. A key presented on an incoming request
-- carries no routable identity until it has been looked up, so there is nowhere
-- else it could be.

CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id TEXT NOT NULL,

    -- The public half of the key, stored in clear and indexed. It is what turns
    -- verification into a single-row lookup: the secret is hashed and therefore
    -- unsearchable, so without a plaintext handle every request would have to
    -- scan the table hashing as it went.
    key_prefix  TEXT NOT NULL,

    -- SHA-256 of the whole key.
    --
    -- Not bcrypt or argon2, deliberately. Those exist to make brute force
    -- expensive against secrets humans chose, which are short and guessable.
    -- This secret is 32 bytes from crypto/rand — there is no dictionary to
    -- attack and no rainbow table to build, so a slow hash would buy nothing
    -- and would put tens of milliseconds on every authenticated request.
    key_hash    BYTEA NOT NULL,

    -- Free text, so an operator can tell "ci" from "production" when deciding
    -- what is safe to revoke.
    name        TEXT NOT NULL DEFAULT '',

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Answers "is anything still using this key" before revoking it. Updated
    -- lazily rather than on every request; see the store for why.
    last_used_at TIMESTAMPTZ,

    -- Revoked rather than deleted. A key that authenticated a payment last week
    -- is part of that payment's history, and deleting the row destroys the only
    -- record of which credential acted.
    revoked_at  TIMESTAMPTZ,

    CONSTRAINT api_keys_prefix_unique UNIQUE (key_prefix)
);

CREATE INDEX api_keys_merchant_idx ON api_keys (merchant_id) WHERE revoked_at IS NULL;
