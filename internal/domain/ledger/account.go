// Package ledger holds the double-entry accounting model.
//
// Balances are always derived by aggregating postings and are never stored as a
// column. A stored balance is a second source of truth that drifts from the
// entries it summarises, and once it has drifted there is no way to tell which
// of the two is wrong.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

var (
	ErrUnbalancedEntry    = errors.New("journal entry does not balance")
	ErrNoPostings         = errors.New("journal entry has no postings")
	ErrAmountNotPositive  = errors.New("posting amount must be positive")
	ErrUnknownAccountType = errors.New("unknown account type")
)

// AccountType determines which direction increases an account's balance.
type AccountType string

const (
	AccountTypeAsset     AccountType = "asset"
	AccountTypeLiability AccountType = "liability"
	AccountTypeRevenue   AccountType = "revenue"
	AccountTypeExpense   AccountType = "expense"
	AccountTypeEquity    AccountType = "equity"
)

func (a AccountType) Validate() error {
	switch a {
	case AccountTypeAsset, AccountTypeLiability, AccountTypeRevenue, AccountTypeExpense, AccountTypeEquity:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnknownAccountType, string(a))
	}
}

// DebitIncreases reports whether a debit raises this account's balance.
//
// Assets and expenses are debit-normal; liabilities, revenue, and equity are
// credit-normal. Signing balances by this rule is what makes a merchant payable
// of 100 read as "we owe 100" rather than as negative money.
func (a AccountType) DebitIncreases() bool {
	return a == AccountTypeAsset || a == AccountTypeExpense
}

// Owner identifies whose account this is.
type Owner struct {
	Type string // merchant | platform | psp
	ID   string
}

// Purpose is what an account represents within its owner.
type Purpose string

const (
	PurposeClearing   Purpose = "clearing"     // asset: funds a provider owes us
	PurposePayable    Purpose = "payable"      // liability: funds we owe a merchant
	PurposeFeeRevenue Purpose = "fee_revenue"  // revenue: our fee
	PurposeFXGainLoss Purpose = "fx_gain_loss" // revenue: rate movement between auth and settlement
	PurposeSettlement Purpose = "settlement"   // asset: funds received into our bank

	// PurposeTransferSuspense is the holding account for money in flight
	// between shards. A cross-shard transfer writes a balanced entry on each
	// database — the source pays into suspense, the destination pays out of it
	// — because each entry must balance within the database that holds it.
	//
	// The suspense balances therefore net to zero across all shards once no
	// transfer is mid-flight, and a non-zero total is exactly the signal that
	// one side of a transfer completed and the other did not.
	PurposeTransferSuspense Purpose = "transfer_suspense" // liability: money in flight between shards
)

type Account struct {
	ID        uuid.UUID
	Owner     Owner
	Purpose   Purpose
	Type      AccountType
	Currency  money.Currency
	ShardKey  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Balance is the signed net position of an account, oriented so that a positive
// value always means "more of what this account is for".
type Balance struct {
	AccountID uuid.UUID
	Amount    money.Money

	// Debits and credits are retained alongside the net figure because a
	// reconciliation break is often visible in the gross flows when the net
	// happens to match.
	Debits  money.Money
	Credits money.Money
}
