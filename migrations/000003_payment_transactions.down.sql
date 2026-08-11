DROP TABLE IF EXISTS transaction_state_changes;
DROP TABLE IF EXISTS payment_transactions;
DROP TABLE IF EXISTS transaction_state_transitions;

DROP FUNCTION IF EXISTS assert_valid_transaction_transition();

DROP TYPE IF EXISTS transaction_state;
