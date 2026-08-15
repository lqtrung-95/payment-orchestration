package fx

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

var (
	// ErrLockExpired means the promised rate is no longer honourable and the
	// operation must re-quote rather than proceed.
	ErrLockExpired = errors.New("fx rate lock has expired")

	ErrLockTTLNotPositive = errors.New("fx rate lock TTL must be positive")
)

// Lock is a rate promised to a customer at authorisation, held for a bounded
// window.
//
// The window exists because a quote is a commitment: between authorising and
// settling, the market moves, and someone absorbs the difference. Holding the
// rate makes that explicit and bounded — the customer sees one number, and the
// movement lands in the FX gain/loss account where it can be reported, rather
// than being quietly absorbed into whatever the settlement file happened to say.
type Lock struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	Rate          Rate
	LockedAt      time.Time
	ExpiresAt     time.Time
}

// NewLock takes a lock on a rate for the given TTL.
func NewLock(transactionID uuid.UUID, rate Rate, at time.Time, ttl time.Duration) (Lock, error) {
	if transactionID == uuid.Nil {
		return Lock{}, errors.New("fx rate lock requires a transaction id")
	}
	if rate.Nano <= 0 {
		return Lock{}, fmt.Errorf("%w: got %d", ErrInvalidRate, rate.Nano)
	}
	if ttl <= 0 {
		return Lock{}, fmt.Errorf("%w: got %s", ErrLockTTLNotPositive, ttl)
	}

	locked := at.UTC()
	return Lock{
		ID:            uuid.New(),
		TransactionID: transactionID,
		Rate:          rate,
		LockedAt:      locked,
		ExpiresAt:     locked.Add(ttl),
	}, nil
}

// IsExpired reports whether the promise has lapsed.
//
// Expiry is inclusive of the boundary: a lock is dead at its expiry instant, not
// one nanosecond after. Honouring a rate at exactly its deadline is the kind of
// off-by-one that only shows up as a customer dispute.
func (l Lock) IsExpired(now time.Time) bool { return !now.Before(l.ExpiresAt) }

// Convert applies the locked rate, refusing once the lock has lapsed.
//
// Expiry is checked here rather than left to the caller: a lock whose expiry is
// only enforced by whoever remembers to look is not enforced at all, and the
// failure mode — honouring a stale rate — is silent money loss.
func (l Lock) Convert(amount money.Money, now time.Time) (money.Money, error) {
	if l.IsExpired(now) {
		return money.Money{}, fmt.Errorf("%w: locked %s, expired %s",
			ErrLockExpired, l.LockedAt.Format(time.RFC3339), l.ExpiresAt.Format(time.RFC3339))
	}
	return l.Rate.Convert(amount)
}
