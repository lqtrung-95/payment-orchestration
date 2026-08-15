package recon

import (
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// LedgerRecord is what this system believes about one captured payment.
//
// Assembled from the transaction and its ledger postings, so reconciliation
// compares the provider against the books rather than against the transaction
// table. The distinction matters: the transaction says what we intended, the
// ledger says what we accounted for, and it is the second that has to agree
// with the provider's money.
type LedgerRecord struct {
	TransactionID     uuid.UUID
	MerchantID        string
	ProviderReference string

	// Captured is the gross the ledger recorded moving.
	Captured money.Money

	// Fee is what we booked as our own revenue on it.
	Fee money.Money

	// CapturedAt is when the money moved economically.
	CapturedAt time.Time
}

// Net is what we expect the provider to actually pay us.
func (r LedgerRecord) Net() (money.Money, error) { return r.Captured.Sub(r.Fee) }

// Pair is a settlement row matched to a ledger record. Either side may be
// absent, and which one is absent is itself the finding.
type Pair struct {
	Row    *Row
	Record *LedgerRecord
}

// MatchResult is the outcome of matching a file against the ledger.
type MatchResult struct {
	// Matched holds pairs where both sides are present.
	Matched []Pair

	// UnmatchedRows settled at the provider with nothing on our side.
	UnmatchedRows []Row

	// UnmatchedRecords are captured on our side with nothing in the file.
	UnmatchedRecords []LedgerRecord

	// DuplicateRows are second and subsequent settlements of one reference.
	DuplicateRows []Row
}

// Match pairs settlement rows to ledger records by provider reference.
//
// Exact on the reference only. Fuzzy matching on amount and date is a
// deliberate non-feature here: the reference is authoritative, and pairing two
// records that merely have the same amount on the same day produces a
// confident, wrong answer — which is worse than an unmatched pair a human looks
// at, because nobody ever looks at it again.
func Match(rows []Row, records []LedgerRecord) MatchResult {
	byReference := make(map[string]*LedgerRecord, len(records))
	for i := range records {
		if ref := records[i].ProviderReference; ref != "" {
			byReference[ref] = &records[i]
		}
	}

	var result MatchResult
	// seen tracks references already settled in this file, so the second
	// occurrence is reported as a duplicate rather than silently overwriting
	// the first match.
	seen := make(map[string]bool, len(rows))
	matchedRefs := make(map[string]bool, len(rows))

	for _, row := range rows {
		if seen[row.ProviderReference] {
			result.DuplicateRows = append(result.DuplicateRows, row)
			continue
		}
		seen[row.ProviderReference] = true

		record, ok := byReference[row.ProviderReference]
		if !ok {
			result.UnmatchedRows = append(result.UnmatchedRows, row)
			continue
		}

		matchedRefs[row.ProviderReference] = true

		settled := row // an explicit copy, so the pair does not alias the loop
		result.Matched = append(result.Matched, Pair{Row: &settled, Record: record})
	}

	for i := range records {
		if ref := records[i].ProviderReference; ref == "" || !matchedRefs[ref] {
			result.UnmatchedRecords = append(result.UnmatchedRecords, records[i])
		}
	}

	return result
}
