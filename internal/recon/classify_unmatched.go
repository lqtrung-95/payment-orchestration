package recon

import (
	"fmt"

	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
)

// ClassifyDuplicate reports a reference the provider settled more than once.
//
// Always a break, never absorbed. A duplicate settlement is money arriving that
// we did not earn, and the one category where doing nothing is itself the
// problem: keeping it silently leaves the provider's books and ours permanently
// apart, and it will be reclaimed later without warning.
func (c *Classifier) ClassifyDuplicate(row Row) *Break {
	rowID := row.ID
	return &Break{
		Category: breaks.DuplicateSettlement,
		// Keyed by line as well as reference: a file settling the same charge
		// three times raises two distinct breaks rather than one that
		// overwrites itself.
		MatchKey:        fmt.Sprintf("%s#%d", row.ProviderReference, row.LineNumber),
		SettlementRowID: &rowID,
		Actual:          &row.Gross,
		Detail:          fmt.Sprintf("reference %s settled again on line %d", row.ProviderReference, row.LineNumber),
	}
}

// ClassifyUnmatchedRow reports a settlement row with nothing on our side.
//
// The urgent category. The provider moved money for something this system has
// no record of, which means either an ingestion gap or a payment created
// outside the system — and until it is explained, the books understate what we
// are holding.
func (c *Classifier) ClassifyUnmatchedRow(row Row) *Break {
	rowID := row.ID
	return &Break{
		Category:        breaks.MissingInternally,
		MatchKey:        row.ProviderReference,
		SettlementRowID: &rowID,
		Actual:          &row.Gross,
		Detail:          fmt.Sprintf("settled %s with no matching capture", row.Gross),
	}
}

// ClassifyUnmatchedRecord reports a capture the file does not mention.
//
// Split by timing rather than lumped together. A capture that happened after
// the file's window closed is not missing — it settles in the next one, and
// reporting it as lost trains operators to ignore the category. Only a capture
// that falls inside the window and still is not there is a real absence.
func (c *Classifier) ClassifyUnmatchedRecord(record LedgerRecord, file File) *Break {
	txID := record.TransactionID

	if !file.Covers(record.CapturedAt) {
		return &Break{
			Category:      breaks.TimingCutoff,
			MatchKey:      record.TransactionID.String(),
			TransactionID: &txID,
			Expected:      &record.Captured,
			Detail: fmt.Sprintf("captured %s, outside the file window %s to %s",
				record.CapturedAt.Format("2006-01-02T15:04:05Z"),
				file.PeriodStart.Format("2006-01-02T15:04:05Z"),
				file.PeriodEnd.Format("2006-01-02T15:04:05Z")),
		}
	}

	return &Break{
		Category:      breaks.MissingAtPSP,
		MatchKey:      record.TransactionID.String(),
		TransactionID: &txID,
		Expected:      &record.Captured,
		Detail:        fmt.Sprintf("captured %s inside the window with no settlement row", record.Captured),
	}
}
