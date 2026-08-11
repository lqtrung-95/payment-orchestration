package transaction

import (
	"errors"
	"testing"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

func newAuthorized(t *testing.T, amount int64) *Transaction {
	t.Helper()

	tx, err := New("m_1", "key-1", money.MustNew(amount, "USD"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	for _, s := range []State{StateAuthorizing, StateAuthorized, StateCapturing} {
		if err := tx.TransitionTo(s); err != nil {
			t.Fatalf("TransitionTo(%s) returned error: %v", s, err)
		}
	}
	return tx
}

func TestNewValidatesInput(t *testing.T) {
	tests := []struct {
		name     string
		merchant string
		key      string
		amount   money.Money
		wantErr  error
	}{
		{"missing merchant", "", "k", money.MustNew(100, "USD"), ErrMerchantRequired},
		{"missing key", "m_1", "", money.MustNew(100, "USD"), ErrIdempotencyKeyReqd},
		{"zero amount", "m_1", "k", money.MustNew(0, "USD"), ErrAmountNotPositive},
		{"negative amount", "m_1", "k", money.MustNew(-1, "USD"), ErrAmountNotPositive},
		{"invalid currency", "m_1", "k", money.Money{}, money.ErrInvalidCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.merchant, tt.key, tt.amount); !errors.Is(err, tt.wantErr) {
				t.Errorf("New error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewDerivesShardKey(t *testing.T) {
	a, err := New("m_1", "k1", money.MustNew(100, "USD"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	b, err := New("m_1", "k2", money.MustNew(200, "USD"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	// The same merchant must always land on the same shard, or its ledger would
	// be split across shards and every balance would need a scatter-gather.
	if a.ShardKey != b.ShardKey {
		t.Errorf("same merchant produced shard keys %q and %q", a.ShardKey, b.ShardKey)
	}
	if a.ShardKey == "" {
		t.Error("shard key is empty")
	}
}

func TestCaptureRejectsMoreThanAuthorized(t *testing.T) {
	tx := newAuthorized(t, 5000)

	if err := tx.Capture(money.MustNew(5001, "USD")); !errors.Is(err, ErrExceedsAuthorized) {
		t.Fatalf("over-capture error = %v, want ErrExceedsAuthorized", err)
	}
	// The rejected attempt must leave the aggregate untouched.
	if !tx.Captured.IsZero() {
		t.Errorf("captured = %s after a rejected capture, want zero", tx.Captured)
	}
	if tx.State != StateCapturing {
		t.Errorf("state = %s after a rejected capture, want capturing", tx.State)
	}
}

func TestPartialCaptureAccumulates(t *testing.T) {
	tx := newAuthorized(t, 5000)

	if err := tx.Capture(money.MustNew(2000, "USD")); err != nil {
		t.Fatalf("first capture returned error: %v", err)
	}
	if tx.State != StateCaptured {
		t.Errorf("state = %s, want captured", tx.State)
	}

	remaining, err := tx.RemainingCapturable()
	if err != nil {
		t.Fatalf("RemainingCapturable returned error: %v", err)
	}
	if want := money.MustNew(3000, "USD"); !remaining.Equal(want) {
		t.Errorf("remaining capturable = %s, want %s", remaining, want)
	}

	if err := tx.Capture(money.MustNew(3000, "USD")); err != nil {
		t.Fatalf("second capture returned error: %v", err)
	}
	if want := money.MustNew(5000, "USD"); !tx.Captured.Equal(want) {
		t.Errorf("captured = %s, want %s", tx.Captured, want)
	}

	// The authorised total is now exhausted.
	if err := tx.Capture(money.MustNew(1, "USD")); !errors.Is(err, ErrExceedsAuthorized) {
		t.Errorf("capture past the authorised total error = %v, want ErrExceedsAuthorized", err)
	}
}

func TestRefundTransitionsByRemainingBalance(t *testing.T) {
	tx := newAuthorized(t, 5000)
	if err := tx.Capture(money.MustNew(5000, "USD")); err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}

	if err := tx.Refund(money.MustNew(2000, "USD")); err != nil {
		t.Fatalf("partial refund returned error: %v", err)
	}
	if tx.State != StatePartiallyRefunded {
		t.Errorf("state = %s after partial refund, want partially_refunded", tx.State)
	}

	if err := tx.Refund(money.MustNew(3000, "USD")); err != nil {
		t.Fatalf("final refund returned error: %v", err)
	}
	if tx.State != StateRefunded {
		t.Errorf("state = %s after full refund, want refunded", tx.State)
	}

	// Refunded is terminal, so nothing further is possible.
	if err := tx.Refund(money.MustNew(1, "USD")); err == nil {
		t.Error("refund past the captured total succeeded, want error")
	}
}

func TestRefundRejectsMoreThanCaptured(t *testing.T) {
	tx := newAuthorized(t, 5000)
	if err := tx.Capture(money.MustNew(2000, "USD")); err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}

	// Only the captured amount can be returned, never the full authorisation.
	if err := tx.Refund(money.MustNew(2001, "USD")); !errors.Is(err, ErrExceedsCaptured) {
		t.Errorf("over-refund error = %v, want ErrExceedsCaptured", err)
	}
	if !tx.Refunded.IsZero() {
		t.Errorf("refunded = %s after a rejected refund, want zero", tx.Refunded)
	}
}

func TestTransitionToRejectsIllegalEdge(t *testing.T) {
	tx, err := New("m_1", "key-1", money.MustNew(100, "USD"))
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	if err := tx.TransitionTo(StateSettled); !errors.Is(err, ErrIllegalTransition) {
		t.Errorf("created -> settled error = %v, want ErrIllegalTransition", err)
	}
	if tx.State != StateCreated {
		t.Errorf("state = %s after a rejected transition, want created", tx.State)
	}
	if err := tx.TransitionTo(State("nonsense")); err == nil {
		t.Error("transition to an unknown state succeeded, want error")
	}
}
