// Package recon matches a provider's settlement file against this system's own
// ledger and classifies every disagreement.
package recon

import (
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Row is one settled charge, in this system's vocabulary rather than any
// provider's.
type Row struct {
	ID         int64
	FileID     uuid.UUID
	LineNumber int

	// ProviderReference is the only identifier linking the provider's records
	// to ours.
	ProviderReference string

	// Gross is what the customer was charged; Fee is what the provider kept;
	// Net is what it says it is paying us. Net is carried rather than derived,
	// because a provider whose own three figures disagree is itself a finding.
	Gross money.Money
	Fee   money.Money
	Net   money.Money

	// SettlementCurrency, SettlementRateNano and Settled describe a converted
	// payment: what we will actually be paid, in the currency we will be paid
	// in, and the rate used to get there. Gross stays in the charge currency —
	// carrying both in one field is how a EUR figure and a USD one end up
	// indistinguishable.
	SettlementCurrency money.Currency
	SettlementRateNano int64
	Settled            money.Money

	SettledAt time.Time

	// Raw is the original line. A parser bug is only diagnosable if the input
	// survived the parse.
	Raw string
}

// HasFX reports whether the provider converted between charging and settling.
func (r Row) HasFX() bool {
	return r.SettlementCurrency != "" &&
		r.SettlementCurrency != r.Gross.Currency() &&
		r.SettlementRateNano > 0 &&
		r.Settled.IsValid()
}

// File is a settlement file and the window it claims to cover.
type File struct {
	ID       uuid.UUID
	Provider string
	Filename string

	// ContentSHA256 is the ingestion identity. Providers re-send files, rename
	// them, and occasionally send yesterday's data under today's name; hashing
	// the bytes is the only identity that survives all three.
	ContentSHA256 []byte

	// PeriodStart and PeriodEnd bound what the file claims to cover, which is
	// what distinguishes a genuinely missing payment from one that simply
	// settles in the next file.
	PeriodStart time.Time
	PeriodEnd   time.Time

	RowCount   int
	IngestedAt time.Time

	Rows []Row
}

// Covers reports whether an instant falls inside the file's window.
func (f File) Covers(at time.Time) bool {
	return !at.Before(f.PeriodStart) && at.Before(f.PeriodEnd)
}
