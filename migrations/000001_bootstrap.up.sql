-- Bootstrap schema objects shared by every later migration.

-- Reusable trigger function keeping updated_at honest. Application code can be
-- bypassed by migrations, admin sessions, and repair scripts; a trigger cannot,
-- which matters when updated_at is used to reason about incident timelines.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Monetary amounts are stored as BIGINT minor units paired with an ISO-4217
-- code, never as floating point. This domain documents and enforces the
-- currency half of that pair at the schema level.
CREATE DOMAIN currency_code AS CHAR(3)
    CHECK (VALUE ~ '^[A-Z]{3}$');
