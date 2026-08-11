DROP INDEX IF EXISTS payment_transactions_psp_reference_idx;
CREATE INDEX payment_transactions_psp_reference_idx
    ON payment_transactions (psp, psp_reference)
    WHERE psp_reference IS NOT NULL;

ALTER TABLE payment_transactions DROP COLUMN IF EXISTS last_applied_event_seq;

DROP TABLE IF EXISTS webhook_events_raw;
DROP TYPE IF EXISTS webhook_event_status;
