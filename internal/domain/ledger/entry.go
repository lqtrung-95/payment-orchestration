package ledger

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Direction is which side of the ledger a posting falls on.
type Direction string

const (
	Debit  Direction = "debit"
	Credit Direction = "credit"
)

// Posting is one leg of an entry. The amount is always positive; the direction
// carries the sign. Allowing a signed amount would make "a negative credit"
// expressible, and every downstream report would then have to decide what that
// means.
type Posting struct {
	AccountID uuid.UUID
	Direction Direction
	Amount    money.Money
}

// Entry is a balanced set of postings recorded as a single atomic fact.
type Entry struct {
	ID uuid.UUID

	// Nil for adjustments raised by reconciliation, which belong to no single
	// payment transaction.
	TransactionID *uuid.UUID

	ShardKey    string
	Description string

	// OccurredAt is when the money moved economically; CreatedAt is when this
	// system learned of it. A settlement file routinely reports movement that
	// happened before we were told about it, and reconciliation needs both.
	OccurredAt time.Time
	CreatedAt  time.Time

	Postings []Posting
}

// NewEntry builds an entry and verifies it balances before it reaches the
// database.
//
// The same rule is enforced by a deferred constraint trigger in Postgres. This
// check exists so the failure surfaces at the call site with the offending
// postings in hand, rather than at COMMIT where the transaction has already
// done its other work.
func NewEntry(transactionID *uuid.UUID, shardKey, description string, occurredAt time.Time, postings ...Posting) (*Entry, error) {
	if len(postings) == 0 {
		return nil, ErrNoPostings
	}

	for i, p := range postings {
		if !p.Amount.IsValid() {
			return nil, fmt.Errorf("posting %d: %w", i, money.ErrInvalidCurrency)
		}
		if !p.Amount.IsPositive() {
			return nil, fmt.Errorf("posting %d: %w, got %s", i, ErrAmountNotPositive, p.Amount)
		}
		if p.Direction != Debit && p.Direction != Credit {
			return nil, fmt.Errorf("posting %d: unknown direction %q", i, string(p.Direction))
		}
	}

	if err := assertBalanced(postings); err != nil {
		return nil, err
	}

	return &Entry{
		ID:            uuid.New(),
		TransactionID: transactionID,
		ShardKey:      shardKey,
		Description:   description,
		OccurredAt:    occurredAt.UTC(),
		CreatedAt:     time.Now().UTC(),
		Postings:      postings,
	}, nil
}

// assertBalanced checks debits equal credits within every currency present.
//
// Balance is per currency, not across the entry as a whole. An entry may span
// currencies, but each one must net to zero on its own: an FX conversion
// balances because it carries an explicit gain or loss leg, not because two
// unrelated currencies happen to offset numerically.
func assertBalanced(postings []Posting) error {
	net := make(map[money.Currency]int64, 2)

	for _, p := range postings {
		delta := p.Amount.Amount()
		if p.Direction == Credit {
			delta = -delta
		}

		cur := p.Amount.Currency()
		sum := net[cur] + delta
		// Sign-based overflow check, matching money.Add.
		if (delta > 0 && sum < net[cur]) || (delta < 0 && sum > net[cur]) {
			return fmt.Errorf("%w: %s totals overflow int64", money.ErrOverflow, cur)
		}
		net[cur] = sum
	}

	for cur, delta := range net {
		if delta != 0 {
			return fmt.Errorf("%w: %s debits minus credits = %d", ErrUnbalancedEntry, cur, delta)
		}
	}
	return nil
}
