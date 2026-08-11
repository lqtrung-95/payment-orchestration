DROP TABLE IF EXISTS postings;
DROP TABLE IF EXISTS journal_entries;
DROP TABLE IF EXISTS ledger_accounts;

DROP FUNCTION IF EXISTS assert_journal_entry_balances();
DROP FUNCTION IF EXISTS reject_mutation();

DROP TYPE IF EXISTS posting_direction;
DROP TYPE IF EXISTS account_type;
