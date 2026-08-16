// Package tcc moves money between merchants that live on different databases.
//
// Postgres cannot commit across databases, so a transfer whose two sides sit on
// different shards has no single transaction available to it. The alternatives
// were weighed and rejected:
//
//   - Two independent commits. Whichever crashes second leaves money debited
//     and never credited, or credited and never debited, with nothing recording
//     that a decision was pending.
//
//   - Two-phase commit. Blocking: a coordinator that dies during the prepared
//     phase leaves every participant holding locks until a human intervenes.
//     On a payment ledger that is an outage, not a delay.
//
//   - Saga. Non-blocking, but compensation is semantic rather than exact. The
//     compensation for a completed transfer is a reverse transfer, and the two
//     are not inverses once fees, FX, or an intervening balance check are
//     involved — the account was spendable in between, which is precisely the
//     window that lets it be overdrawn.
//
//   - Try-confirm-cancel. The reservation makes funds unavailable without
//     moving them, so cancelling is exact: nothing was posted, and there is
//     nothing to undo. The cost is an explicit Try step on every participant
//     and a sweeper to release holds nobody resolved.
//
// The commit point is the transition from trying to confirming. Before it, the
// transfer may be cancelled freely. After it, every participant has already
// agreed and the transfer will complete — a confirm that fails is retried, never
// abandoned.
package tcc

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// State is the coordinator's view of a transfer.
type State string

const (
	StateTrying     State = "trying"
	StateConfirming State = "confirming"
	StateConfirmed  State = "confirmed"
	StateCancelling State = "cancelling"
	StateCancelled  State = "cancelled"
)

// PastCommitPoint reports whether every participant has reserved. A transfer
// past this point is owed completion: cancelling it would release a hold the
// other side has already been promised.
func (s State) PastCommitPoint() bool { return s == StateConfirming || s == StateConfirmed }

// Role distinguishes the side that gives up funds from the side that receives
// them. Only the source is capacity-checked; the destination cannot refuse.
type Role string

const (
	RoleSource      Role = "source"
	RoleDestination Role = "destination"
)

// ReservationState is a participant's view of its own side.
type ReservationState string

const (
	ReservationReserved  ReservationState = "reserved"
	ReservationConfirmed ReservationState = "confirmed"
	ReservationCancelled ReservationState = "cancelled"
)

// Transfer is the coordinator record. It is the only durable account of a
// transfer in flight, which is what lets any instance resume one whose
// coordinator died.
type Transfer struct {
	ID    uuid.UUID
	State State

	SourceMerchant string
	SourceShardKey string
	DestMerchant   string
	DestShardKey   string
	Amount         money.Money
	IdempotencyKey string

	TimeoutAt time.Time
	Attempts  int
	LastError string

	CreatedAt  time.Time
	ResolvedAt *time.Time
}

// CrossShard reports whether the two sides live on different databases.
//
// Same-shard transfers still go through the protocol. Two code paths for one
// operation means the rare one is the untested one, and the rare one here is
// the one that moves money between databases.
func (t *Transfer) CrossShard() bool { return t.SourceShardKey != t.DestShardKey }

// Reservation is a participant's hold. Its existence reduces what the merchant
// can spend without any posting having been written.
type Reservation struct {
	ID         uuid.UUID
	TransferID uuid.UUID
	Role       Role
	MerchantID string
	ShardKey   string
	Amount     money.Money
	State      ReservationState
	EntryID    *uuid.UUID
	ExpiresAt  time.Time
}

var (
	// ErrInsufficientFunds means the source cannot cover the transfer once
	// outstanding holds are subtracted from its derived balance.
	ErrInsufficientFunds = errors.New("insufficient available balance")

	// ErrTransferNotFound is returned for an unknown transfer id.
	ErrTransferNotFound = errors.New("transfer not found")

	// ErrAlreadyResolved means a confirm or cancel arrived for a transfer that
	// has already finished. Reported rather than swallowed: a late cancel for a
	// confirmed transfer is a caller bug, and answering it with silence would
	// let the caller believe the money came back.
	ErrAlreadyResolved = errors.New("transfer already resolved")

	// ErrSameMerchant rejects a transfer with one party. It has nothing to
	// coordinate and would take the same lock twice.
	ErrSameMerchant = errors.New("source and destination are the same merchant")
)
