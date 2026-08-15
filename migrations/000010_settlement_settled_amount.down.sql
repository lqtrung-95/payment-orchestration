ALTER TABLE settlement_rows DROP CONSTRAINT IF EXISTS settlement_rows_fx_columns_together;
ALTER TABLE settlement_rows DROP COLUMN IF EXISTS settled_minor;
