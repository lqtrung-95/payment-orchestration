-- Double-entry ledger.
--
-- Balances are never stored. They are derived by aggregating postings, which
-- are insert-only. A stored balance is a second source of truth that drifts
-- from the entries it summarises, and once it has drifted there is no way to
-- tell which one is wrong.

CREATE TYPE account_type AS ENUM ('asset', 'liability', 'revenue', 'expense', 'equity');

CREATE TYPE posting_direction AS ENUM ('debit', 'credit');

CREATE TABLE ledger_accounts (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- An account is identified naturally by whose it is and what it represents.
    owner_type   TEXT NOT NULL,   -- merchant | platform | psp
    owner_id     TEXT NOT NULL,
    purpose      TEXT NOT NULL,   -- payable | clearing | fee_revenue | fx_gain_loss
    account_type account_type NOT NULL,
    currency     currency_code NOT NULL,

    -- Shard key is recorded from the outset even though routing arrives in a
    -- later phase. Backfilling a shard key across a populated ledger means
    -- rewriting every row while keeping the service online.
    shard_key    TEXT NOT NULL,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One account per owner, purpose, and currency. A merchant transacting in
    -- two currencies holds two payable accounts, never one with mixed units.
    CONSTRAINT ledger_accounts_natural_key UNIQUE (owner_type, owner_id, purpose, currency),

    -- Referenced by the composite foreign key on postings below, which is what
    -- makes a currency mismatch structurally impossible.
    CONSTRAINT ledger_accounts_id_currency UNIQUE (id, currency)
);

CREATE INDEX ledger_accounts_shard_key_idx ON ledger_accounts (shard_key);

CREATE TRIGGER ledger_accounts_set_updated_at
    BEFORE UPDATE ON ledger_accounts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE journal_entries (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Nullable: adjustments raised by reconciliation belong to no single
    -- payment transaction.
    transaction_id UUID,
    shard_key      TEXT NOT NULL,
    description    TEXT NOT NULL,

    -- occurred_at is when the money moved economically; created_at is when this
    -- system learned of it. Reconciliation needs both, because a settlement file
    -- routinely reports movement that happened before we were told about it.
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX journal_entries_transaction_id_idx ON journal_entries (transaction_id)
    WHERE transaction_id IS NOT NULL;
CREATE INDEX journal_entries_shard_key_occurred_at_idx ON journal_entries (shard_key, occurred_at);

CREATE TABLE postings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entry_id     UUID NOT NULL REFERENCES journal_entries (id),
    account_id   UUID NOT NULL,

    -- Amounts are always positive; direction carries the sign. Allowing signed
    -- amounts would make "a negative credit" expressible, and every report then
    -- needs to decide what that means.
    direction    posting_direction NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency     currency_code NOT NULL,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Composite reference: a posting can only touch an account that holds the
    -- same currency. This is enforced by the schema rather than by convention.
    CONSTRAINT postings_account_currency_fk
        FOREIGN KEY (account_id, currency) REFERENCES ledger_accounts (id, currency)
);

CREATE INDEX postings_entry_id_idx ON postings (entry_id);
CREATE INDEX postings_account_id_idx ON postings (account_id);

-- Append-only enforcement -----------------------------------------------------
--
-- Application code can be bypassed by a migration, an admin session, or a
-- well-meaning repair script. History that can be edited is not an audit trail.

CREATE OR REPLACE FUNCTION reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not permitted', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER postings_append_only
    BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER journal_entries_append_only
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

-- Balance enforcement ---------------------------------------------------------
--
-- Checked by a DEFERRABLE constraint trigger so it runs at COMMIT, once all of
-- an entry's postings exist. A non-deferred check would fire after the first
-- posting, when the entry is legitimately still unbalanced.

CREATE OR REPLACE FUNCTION assert_journal_entry_balances()
RETURNS TRIGGER AS $$
DECLARE
    imbalance RECORD;
    posting_count INT;
BEGIN
    SELECT count(*) INTO posting_count FROM postings WHERE entry_id = NEW.id;

    IF posting_count = 0 THEN
        RAISE EXCEPTION 'journal entry % has no postings', NEW.id
            USING ERRCODE = 'check_violation';
    END IF;

    -- Grouped by currency: an entry may legitimately span currencies, but each
    -- currency must balance on its own. An FX conversion balances because it
    -- carries an explicit gain/loss leg, not because two currencies net out.
    FOR imbalance IN
        SELECT currency,
               sum(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) AS delta
        FROM postings
        WHERE entry_id = NEW.id
        GROUP BY currency
        HAVING sum(CASE WHEN direction = 'debit' THEN amount_minor ELSE -amount_minor END) <> 0
    LOOP
        RAISE EXCEPTION 'journal entry % does not balance in %: debits minus credits = %',
            NEW.id, imbalance.currency, imbalance.delta
            USING ERRCODE = 'check_violation';
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER journal_entries_must_balance
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_journal_entry_balances();
