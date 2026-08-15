package payment_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
)

// authorizedPayment creates and authorises a payment, returning its id.
func (h *harness) authorizedPayment(t *testing.T, key string, amountMinor int64) uuid.UUID {
	t.Helper()

	tx := h.create(t, key, amountMinor)
	if tx.State != domain.StateAuthorized {
		t.Fatalf("setup: state after authorize = %s, want authorized", tx.State)
	}
	return tx.ID
}

// capture drives a payment all the way to captured and returns it.
func (h *harness) capture(t *testing.T, id uuid.UUID, amount money.Money) *domain.Transaction {
	t.Helper()

	tx, err := h.service.Capture(context.Background(), payment.CaptureInput{
		TransactionID: id,
		MerchantID:    "m_test",
		Amount:        amount,
		Actor:         "test",
	})
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}
	return tx
}

// ledgerTotals sums every posting by direction, across all accounts.
func (h *harness) ledgerTotals(t *testing.T) (debits, credits int64) {
	t.Helper()

	err := h.db.Pool().QueryRow(context.Background(), `
		SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'debit'), 0),
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'credit'), 0)
		FROM postings`).Scan(&debits, &credits)
	if err != nil {
		t.Fatalf("sum postings: %v", err)
	}
	return debits, credits
}

// balanceFor returns the derived balance of an account by owner and purpose.
func (h *harness) balanceFor(t *testing.T, ownerType, ownerID, purpose string) int64 {
	t.Helper()

	var net int64
	err := h.db.Pool().QueryRow(context.Background(), `
		SELECT COALESCE(SUM(CASE WHEN p.direction = 'debit' THEN p.amount_minor
		                         ELSE -p.amount_minor END), 0)
		FROM ledger_accounts a
		LEFT JOIN postings p ON p.account_id = a.id
		WHERE a.owner_type = $1 AND a.owner_id = $2 AND a.purpose = $3`,
		ownerType, ownerID, purpose).Scan(&net)
	if err != nil {
		t.Fatalf("balance for %s/%s/%s: %v", ownerType, ownerID, purpose, err)
	}
	return net
}

// The point of this whole phase: capture is the first thing that posts to the
// ledger, and it must split the money three ways without inventing or losing a
// single minor unit.
func TestCapturePostsABalancedThreeLeggedEntry(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	id := h.authorizedPayment(t, "capture-basic", 100_00)
	tx := h.capture(t, id, money.MustNew(100_00, "USD"))

	if tx.State != domain.StateCaptured {
		t.Fatalf("state = %s, want captured", tx.State)
	}
	if tx.Captured.Amount() != 100_00 {
		t.Errorf("captured = %d, want 10000", tx.Captured.Amount())
	}

	debits, credits := h.ledgerTotals(t)
	if debits != credits {
		t.Errorf("ledger does not balance: debits %d, credits %d", debits, credits)
	}
	if debits != 100_00 {
		t.Errorf("debits = %d, want 10000 — the full captured amount", debits)
	}

	// Default USD schedule is 290bps + 30. On 10000 that is 290 + 30 = 320.
	const wantFee = 320
	feeBalance := -h.balanceFor(t, "platform", "platform", "fee_revenue") // revenue is credit-normal
	if feeBalance != wantFee {
		t.Errorf("fee revenue = %d, want %d", feeBalance, wantFee)
	}

	payable := -h.balanceFor(t, "merchant", "m_test", "payable") // liability is credit-normal
	if payable != 100_00-wantFee {
		t.Errorf("merchant payable = %d, want %d", payable, 100_00-wantFee)
	}

	clearing := h.balanceFor(t, "psp", "psp-test", "clearing") // asset is debit-normal
	if clearing != 100_00 {
		t.Errorf("psp clearing = %d, want 10000", clearing)
	}

	// The fee comes out of the merchant's share, never out of the customer's
	// charge: the customer authorised 100.00 and the provider owes us 100.00.
	if payable+feeBalance != clearing {
		t.Errorf("payable %d + fee %d != clearing %d — money was created or destroyed",
			payable, feeBalance, clearing)
	}
}

// A merchant shipping half an order captures half. The ledger must reflect what
// actually moved, not what was authorised.
func TestPartialCapturePostsOnlyWhatMoved(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	id := h.authorizedPayment(t, "capture-partial", 100_00)
	tx := h.capture(t, id, money.MustNew(40_00, "USD"))

	if tx.Captured.Amount() != 40_00 {
		t.Errorf("captured = %d, want 4000", tx.Captured.Amount())
	}
	if tx.Amount.Amount() != 100_00 {
		t.Errorf("authorised amount changed to %d; capture must not rewrite it", tx.Amount.Amount())
	}

	debits, credits := h.ledgerTotals(t)
	if debits != credits {
		t.Errorf("ledger does not balance: debits %d, credits %d", debits, credits)
	}
	if debits != 40_00 {
		t.Errorf("posted %d, want 4000 — only the captured amount moves", debits)
	}
}

// Capturing more than was authorised is money creation, and the request must
// never reach the provider.
//
// Asserting the provider's charge count is the whole point. Rejecting the
// capture *after* calling the provider also produces an error and an empty
// ledger, so a test that only checked those two things would pass while the
// customer was charged 80.00 with nothing recorded anywhere. That is exactly
// what this code did before the check was moved ahead of the provider call.
func TestCaptureCannotExceedTheAuthorisedAmount(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	id := h.authorizedPayment(t, "capture-over", 50_00)
	chargesBefore := h.store.Count()

	_, err := h.service.Capture(context.Background(), payment.CaptureInput{
		TransactionID: id,
		MerchantID:    "m_test",
		Amount:        money.MustNew(80_00, "USD"),
		Actor:         "test",
	})
	if !errors.Is(err, domain.ErrExceedsAuthorized) {
		t.Fatalf("Capture of more than was authorised = %v, want ErrExceedsAuthorized", err)
	}

	if got := h.store.Count(); got != chargesBefore {
		t.Errorf("provider charges went from %d to %d — the customer was charged for a capture we then refused",
			chargesBefore, got)
	}
	debits, _ := h.ledgerTotals(t)
	if debits != 0 {
		t.Errorf("a rejected capture posted %d to the ledger, want 0", debits)
	}
}

// The same ordering rule for a capture in the wrong currency.
func TestCaptureInAnotherCurrencyNeverReachesTheProvider(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	id := h.authorizedPayment(t, "capture-currency", 50_00)
	chargesBefore := h.store.Count()

	_, err := h.service.Capture(context.Background(), payment.CaptureInput{
		TransactionID: id,
		MerchantID:    "m_test",
		Amount:        money.MustNew(50_00, "EUR"),
		Actor:         "test",
	})
	if !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Fatalf("Capture in the wrong currency = %v, want ErrCurrencyMismatch", err)
	}
	if got := h.store.Count(); got != chargesBefore {
		t.Errorf("provider charges went from %d to %d on a currency mismatch", chargesBefore, got)
	}
}

// A payment belonging to someone else is not capturable, and is reported as
// missing rather than forbidden so that probing learns nothing.
func TestCaptureRefusesAnotherMerchantsPayment(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	id := h.authorizedPayment(t, "capture-tenant", 25_00)
	_, err := h.service.Capture(context.Background(), payment.CaptureInput{
		TransactionID: id,
		MerchantID:    "m_someone_else",
		Amount:        money.MustNew(25_00, "USD"),
		Actor:         "test",
	})
	if err == nil {
		t.Fatal("captured another merchant's payment")
	}

	debits, _ := h.ledgerTotals(t)
	if debits != 0 {
		t.Errorf("a cross-tenant capture posted %d to the ledger, want 0", debits)
	}
}
