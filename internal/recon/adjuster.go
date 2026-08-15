package recon

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// Adjuster posts the ledger entries that resolving a break implies.
//
// Detecting a difference and recording a decision about it are not the same as
// accounting for it. Until an entry exists, the books still say we are owed
// what we originally booked, and the discrepancy lives only in a report —
// which is exactly the state reconciliation is supposed to end.
type Adjuster struct {
	ledger LedgerWriter
}

// LedgerWriter is the narrow slice of the ledger repository this needs.
type LedgerWriter interface {
	EnsureAccount(ctx context.Context, q postgres.Querier, acct ledgerdomain.Account) (ledgerdomain.Account, error)
	RecordEntry(ctx context.Context, q postgres.Querier, entry *ledgerdomain.Entry) error
}

func NewAdjuster(ledger LedgerWriter) *Adjuster { return &Adjuster{ledger: ledger} }

// PostFXDrift records rate movement between authorisation and settlement.
//
// The entry is single-currency — the settlement currency — and has two legs:
//
//	gain: Dr psp clearing   Cr fx gain/loss
//	loss: Dr fx gain/loss   Cr psp clearing
//
// The provider's clearing balance is the counterparty because the provider is
// who owes us more or less than we booked. Posting it against the merchant's
// payable instead would push our own rate risk onto the merchant, who was
// promised the locked rate and is entitled to it.
//
// A separate clearing account exists per currency, by the ledger's natural key,
// so this creates one in the settlement currency the first time a converted
// payment settles. That is correct — a clearing balance is per currency — but
// it is only half the picture: the entry that moves the original balance out of
// clearing and into the bank is not built, so the charge-currency clearing
// account still shows the payment outstanding. See the phase notes.
func (a *Adjuster) PostFXDrift(
	ctx context.Context,
	tx pgx.Tx,
	b Break,
	transactionID uuid.UUID,
	shardKey, providerName string,
) (uuid.UUID, error) {
	if b.Delta == nil || b.Delta.IsZero() {
		return uuid.Nil, fmt.Errorf("fx drift break %s has no delta to post", b.MatchKey)
	}

	currency := b.Delta.Currency()
	clearing, err := a.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
		Owner:    ledgerdomain.Owner{Type: "psp", ID: providerName},
		Purpose:  ledgerdomain.PurposeClearing,
		Type:     ledgerdomain.AccountTypeAsset,
		Currency: currency, ShardKey: shardKey,
	})
	if err != nil {
		return uuid.Nil, err
	}

	gainLoss, err := a.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
		Owner:    ledgerdomain.Owner{Type: "platform", ID: "platform"},
		Purpose:  ledgerdomain.PurposeFXGainLoss,
		Type:     ledgerdomain.AccountTypeRevenue,
		Currency: currency, ShardKey: shardKey,
	})
	if err != nil {
		return uuid.Nil, err
	}

	// Postings carry a positive amount and a direction, so the sign of the
	// drift selects the directions rather than the magnitude.
	amount := *b.Delta
	debit, credit := clearing.ID, gainLoss.ID
	if !amount.IsPositive() {
		if amount, err = amount.Neg(); err != nil {
			return uuid.Nil, err
		}
		debit, credit = gainLoss.ID, clearing.ID
	}

	entry, err := ledgerdomain.NewEntry(&transactionID, shardKey,
		fmt.Sprintf("fx drift on settlement: %s", b.Detail), time.Now().UTC(),
		ledgerdomain.Posting{AccountID: debit, Direction: ledgerdomain.Debit, Amount: amount},
		ledgerdomain.Posting{AccountID: credit, Direction: ledgerdomain.Credit, Amount: amount},
	)
	if err != nil {
		return uuid.Nil, err
	}
	if err := a.ledger.RecordEntry(ctx, tx, entry); err != nil {
		return uuid.Nil, err
	}

	return entry.ID, nil
}
