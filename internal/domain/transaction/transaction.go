// Package transaction holds the payment transaction aggregate and the state
// machine that governs it.
//
// The aggregate owns its invariants. Every mutation goes through a method that
// checks the transition and the amounts, so a caller cannot assemble an
// impossible transaction by assigning fields. The database enforces the same
// rules independently — belt and braces, because the cost of one bad write here
// is money that does not exist.
package transaction

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
)

var (
	ErrAmountNotPositive  = errors.New("amount must be positive")
	ErrExceedsAuthorized  = errors.New("capture exceeds authorized amount")
	ErrExceedsCaptured    = errors.New("refund exceeds captured amount")
	ErrMerchantRequired   = errors.New("merchant id is required")
	ErrIdempotencyKeyReqd = errors.New("idempotency key is required")
)

type Transaction struct {
	ID             uuid.UUID
	MerchantID     string
	ShardKey       string
	IdempotencyKey string
	State          State

	// Amount is the authorized total. Captured and Refunded track how much of
	// it has actually moved, which is what refunds and reconciliation reason
	// about — the authorized figure alone says nothing about money movement.
	Amount   money.Money
	Captured money.Money
	Refunded money.Money

	PSP          string
	PSPReference string

	// Version backs optimistic locking. A writer updates WHERE version equals
	// what it read, so of two concurrent captures exactly one succeeds.
	Version int

	// LastAppliedEventSeq is the high-water mark of provider events applied to
	// this transaction. Webhooks arrive out of order routinely, so an event is
	// judged stale by its own sequence rather than by when it turned up.
	LastAppliedEventSeq int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

// New builds a transaction in the created state.
func New(merchantID, idempotencyKey string, amount money.Money) (*Transaction, error) {
	if merchantID == "" {
		return nil, ErrMerchantRequired
	}
	if idempotencyKey == "" {
		return nil, ErrIdempotencyKeyReqd
	}
	if !amount.IsValid() {
		return nil, fmt.Errorf("amount: %w", money.ErrInvalidCurrency)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: got %s", ErrAmountNotPositive, amount)
	}

	zero, err := money.Zero(amount.Currency())
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	return &Transaction{
		ID:             uuid.New(),
		MerchantID:     merchantID,
		ShardKey:       sharding.KeyForMerchant(merchantID),
		IdempotencyKey: idempotencyKey,
		State:          StateCreated,
		Amount:         amount,
		Captured:       zero,
		Refunded:       zero,
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// TransitionTo moves the transaction to target, rejecting any edge absent from
// the matrix. Rejection is an error rather than a silent no-op: a caller that
// believed a payment was captured must not carry on as though it were.
func (t *Transaction) TransitionTo(target State) error {
	if err := target.Validate(); err != nil {
		return err
	}
	if !t.State.CanTransitionTo(target) {
		return fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, t.State, target)
	}
	t.State = target
	t.UpdatedAt = time.Now().UTC()
	return nil
}

// Capture records a captured amount and moves to the matching state.
//
// Partial captures are normal: a merchant shipping half an order captures half
// the authorisation. The state lands on captured once anything has been taken;
// how much is carried by Captured, not by the state.
func (t *Transaction) Capture(amount money.Money) error {
	if !amount.IsPositive() {
		return fmt.Errorf("%w: got %s", ErrAmountNotPositive, amount)
	}

	newCaptured, err := t.Captured.Add(amount)
	if err != nil {
		return err
	}
	cmp, err := newCaptured.Cmp(t.Amount)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return fmt.Errorf("%w: %s captured of %s authorized", ErrExceedsAuthorized, newCaptured, t.Amount)
	}

	if err := t.TransitionTo(StateCaptured); err != nil {
		return err
	}
	t.Captured = newCaptured
	return nil
}

// Refund records a refunded amount, landing on refunded when the full captured
// amount has been returned and partially_refunded otherwise.
func (t *Transaction) Refund(amount money.Money) error {
	if !amount.IsPositive() {
		return fmt.Errorf("%w: got %s", ErrAmountNotPositive, amount)
	}

	newRefunded, err := t.Refunded.Add(amount)
	if err != nil {
		return err
	}
	cmp, err := newRefunded.Cmp(t.Captured)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return fmt.Errorf("%w: %s refunded of %s captured", ErrExceedsCaptured, newRefunded, t.Captured)
	}

	target := StatePartiallyRefunded
	if cmp == 0 {
		target = StateRefunded
	}
	if err := t.TransitionTo(target); err != nil {
		return err
	}
	t.Refunded = newRefunded
	return nil
}

// RemainingCapturable is the authorized amount not yet captured.
func (t *Transaction) RemainingCapturable() (money.Money, error) {
	return t.Amount.Sub(t.Captured)
}

// RemainingRefundable is the captured amount not yet refunded.
func (t *Transaction) RemainingRefundable() (money.Money, error) {
	return t.Captured.Sub(t.Refunded)
}
