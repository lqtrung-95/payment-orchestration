package recon

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
)

// Defect is a deliberate flaw seeded into a generated settlement file.
//
// Detection is only provable against known defects. A generator that produces
// plausible-looking noise proves that the classifier produces plausible-looking
// output; a generator that plants one of each category, by count, proves the
// classifier finds them.
type Defect struct {
	Category breaks.Category

	// Reference identifies which record the defect is applied to. Empty for
	// categories that invent a record rather than corrupting one.
	Reference string
}

// GeneratorInput describes the file to build.
type GeneratorInput struct {
	Provider string

	// Records are the ledger's view; the generator writes the provider's
	// counterpart to each, defect by defect.
	Records []LedgerRecord

	// Defects to seed, applied in order.
	Defects []Defect

	// SettledAt is the nominal settlement instant for clean rows.
	SettledAt time.Time
}

// Generate writes a settlement file in the simulator's CSV format.
//
// Returns the file contents and the defects actually applied, so a test can
// assert on exactly what was planted rather than on what it hoped would be.
func Generate(in GeneratorInput) (string, []Defect, error) {
	byReference := make(map[string][]Defect, len(in.Defects))
	var freestanding []Defect

	for _, d := range in.Defects {
		if d.Reference == "" {
			freestanding = append(freestanding, d)
			continue
		}
		byReference[d.Reference] = append(byReference[d.Reference], d)
	}

	var (
		lines   []string
		applied []Defect
	)
	lines = append(lines, "reference,gross_minor,fee_minor,net_minor,currency,settled_at,settlement_currency,settlement_rate_nano,settled_minor")

	for _, record := range in.Records {
		defects := byReference[record.ProviderReference]

		// Two categories are the *absence* of a row rather than a corrupted
		// one, and they differ only in when the capture happened: inside the
		// file's window it is genuinely missing, outside it the payment simply
		// settles in the next file. The caller controls which by choosing the
		// capture time; the generator just omits the line.
		if omitted, ok := omittedCategory(defects); ok {
			applied = append(applied, Defect{Category: omitted, Reference: record.ProviderReference})
			continue
		}

		line, extra, err := renderRow(record, defects, in.SettledAt)
		if err != nil {
			return "", nil, err
		}
		lines = append(lines, line)
		applied = append(applied, extra...)

		// duplicate_settlement writes the same charge a second time.
		if hasCategory(defects, breaks.DuplicateSettlement) {
			lines = append(lines, line)
			applied = append(applied, Defect{Category: breaks.DuplicateSettlement, Reference: record.ProviderReference})
		}
	}

	// missing_internally is a row with no counterpart, so it is invented rather
	// than derived from any record.
	for i, d := range freestanding {
		if d.Category != breaks.MissingInternally {
			return "", nil, fmt.Errorf("defect %s requires a reference", d.Category)
		}
		reference := fmt.Sprintf("ch_phantom_%d", i)
		lines = append(lines, fmt.Sprintf("%s,%d,%d,%d,%s,%s,,,",
			reference, 4200, 130, 4070, "USD", in.SettledAt.UTC().Format(time.RFC3339)))
		applied = append(applied, Defect{Category: breaks.MissingInternally, Reference: reference})
	}

	sort.SliceStable(applied, func(i, j int) bool { return applied[i].Category < applied[j].Category })
	return strings.Join(lines, "\n") + "\n", applied, nil
}

// renderRow writes the provider's version of one record, corrupted per defect.
func renderRow(record LedgerRecord, defects []Defect, settledAt time.Time) (string, []Defect, error) {
	gross := record.Captured.Amount()
	fee := record.Fee.Amount()
	currency := record.Captured.Currency()
	settlementCurrency := ""
	var rateNano, settled int64
	when := settledAt

	var applied []Defect

	for _, d := range defects {
		switch d.Category {
		case breaks.AmountMismatch:
			// A difference no rate or fee explains.
			gross += 777
			applied = append(applied, d)

		case breaks.FeeMismatch:
			// The provider kept more than the schedule we booked.
			fee += 45
			applied = append(applied, d)

		case breaks.CurrencyMismatch:
			// Settled in a currency the payment was never in.
			currency = "GBP"
			applied = append(applied, d)

		case breaks.FXDrift:
			// Converted at a rate close to, but not exactly, the locked one.
			// Gross stays in the charge currency; the settled amount is what the
			// provider's own rate produces, so its figures are self-consistent
			// and the only disagreement is with the rate we promised.
			settlementCurrency = "USD"
			rateNano = 1_090_000_000
			converted, err := convertForSettlement(record, rateNano)
			if err != nil {
				return "", nil, err
			}
			settled = converted
			applied = append(applied, d)

		case breaks.MissingAtPSP, breaks.TimingCutoff, breaks.DuplicateSettlement, breaks.MissingInternally:
			// Handled by Generate, which adds or omits whole lines.

		default:
			return "", nil, fmt.Errorf("generator does not implement defect %s", d.Category)
		}
	}

	// The three FX columns are written together or not at all: an amount with
	// no currency cannot be compared to anything, and the parser refuses a
	// partial set.
	renderedRate, renderedSettled := "", ""
	if settlementCurrency != "" {
		renderedRate = strconv.FormatInt(rateNano, 10)
		renderedSettled = strconv.FormatInt(settled, 10)
	}

	net := gross - fee
	return fmt.Sprintf("%s,%d,%d,%d,%s,%s,%s,%s,%s",
		record.ProviderReference, gross, fee, net, currency,
		when.UTC().Format(time.RFC3339), settlementCurrency, renderedRate, renderedSettled), applied, nil
}

// convertForSettlement restates a captured amount at the provider's rate.
func convertForSettlement(record LedgerRecord, rateNano int64) (int64, error) {
	rate, err := fx.NewRate(record.Captured.Currency(), "USD", rateNano, "generated", time.Now())
	if err != nil {
		return 0, err
	}
	converted, err := rate.Convert(record.Captured)
	if err != nil {
		return 0, err
	}
	return converted.Amount(), nil
}

// omittedCategory reports whether a record should have no settlement row.
func omittedCategory(defects []Defect) (breaks.Category, bool) {
	for _, d := range defects {
		if d.Category == breaks.MissingAtPSP || d.Category == breaks.TimingCutoff {
			return d.Category, true
		}
	}
	return "", false
}

func hasCategory(defects []Defect, want breaks.Category) bool {
	for _, d := range defects {
		if d.Category == want {
			return true
		}
	}
	return false
}
