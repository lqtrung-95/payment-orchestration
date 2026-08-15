-- What the provider will actually pay, in the currency it will pay it in.
--
-- Previously the settlement amount was carried in gross_minor, which is
-- denominated in the *charge* currency. For a converted payment those are two
-- different units, so one column was silently holding either — a EUR figure or
-- a USD one depending on a different column's value. Reconciliation arithmetic
-- happened to work because both sides made the same assumption, which is the
-- worst kind of correct.
--
-- gross_minor and currency now always describe what the customer was charged.
-- settled_minor and settlement_currency describe what lands in our account.
ALTER TABLE settlement_rows
    ADD COLUMN settled_minor BIGINT;

-- The three FX columns are meaningful only together: an amount with no currency
-- cannot be compared to anything, and a rate with neither cannot be applied.
ALTER TABLE settlement_rows
    ADD CONSTRAINT settlement_rows_fx_columns_together CHECK (
        (settlement_currency IS NULL AND settlement_rate_nano IS NULL AND settled_minor IS NULL)
        OR
        (settlement_currency IS NOT NULL AND settlement_rate_nano IS NOT NULL AND settled_minor IS NOT NULL)
    );
