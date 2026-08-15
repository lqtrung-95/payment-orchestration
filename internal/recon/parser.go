package recon

import (
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

var (
	ErrUnknownProvider = errors.New("no settlement parser for that provider")
	ErrMalformedFile   = errors.New("settlement file is malformed")
)

// Parser turns one provider's settlement format into normalized rows.
//
// Per provider, with no shared "generic CSV" implementation. Real settlement
// formats differ in column order, date format, sign convention, and whether the
// fee is stated or implied — a shared parser accumulates flags until nobody can
// say what any single provider actually sends.
type Parser interface {
	Provider() string
	Parse(r io.Reader) (File, error)
}

// Registry resolves a provider name to its parser.
type Registry struct {
	parsers map[string]Parser
}

func NewRegistry(parsers ...Parser) *Registry {
	reg := &Registry{parsers: make(map[string]Parser, len(parsers))}
	for _, p := range parsers {
		reg.parsers[p.Provider()] = p
	}
	return reg
}

func (r *Registry) Get(provider string) (Parser, error) {
	p, ok := r.parsers[provider]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownProvider, provider)
	}
	return p, nil
}

// SimulatorParser reads the fault-injecting simulator's settlement format.
//
//	reference,gross_minor,fee_minor,net_minor,currency,settled_at,settlement_currency,settlement_rate_nano,settled_minor
type SimulatorParser struct {
	name string
}

func NewSimulatorParser(name string) *SimulatorParser { return &SimulatorParser{name: name} }

func (p *SimulatorParser) Provider() string { return p.name }

const simulatorColumns = 9

func (p *SimulatorParser) Parse(r io.Reader) (File, error) {
	// The bytes are hashed as they are read, so the identity recorded for the
	// file is of exactly what was parsed rather than of a second read that
	// might differ.
	hasher := sha256.New()
	reader := csv.NewReader(io.TeeReader(r, hasher))
	reader.FieldsPerRecord = simulatorColumns

	records, err := reader.ReadAll()
	if err != nil {
		return File{}, fmt.Errorf("%w: %w", ErrMalformedFile, err)
	}
	if len(records) == 0 {
		return File{}, fmt.Errorf("%w: file is empty", ErrMalformedFile)
	}

	file := File{Provider: p.name, ContentSHA256: hasher.Sum(nil)}

	for i, record := range records {
		if i == 0 && strings.EqualFold(strings.TrimSpace(record[0]), "reference") {
			continue // header
		}

		row, err := p.parseRow(record, i+1)
		if err != nil {
			return File{}, err
		}
		file.Rows = append(file.Rows, row)
	}

	file.RowCount = len(file.Rows)
	file.PeriodStart, file.PeriodEnd = windowFor(file.Rows)
	return file, nil
}

func (p *SimulatorParser) parseRow(record []string, line int) (Row, error) {
	fail := func(format string, args ...any) (Row, error) {
		return Row{}, fmt.Errorf("%w: line %d: %s", ErrMalformedFile, line, fmt.Sprintf(format, args...))
	}

	currency := money.Currency(strings.TrimSpace(record[4]))
	if err := currency.Validate(); err != nil {
		return fail("currency: %v", err)
	}

	amounts := make([]money.Money, 3)
	for i, raw := range record[1:4] {
		minor, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fail("amount %q is not an integer", raw)
		}
		if amounts[i], err = money.New(minor, currency); err != nil {
			return fail("amount: %v", err)
		}
	}

	settledAt, err := time.Parse(time.RFC3339, strings.TrimSpace(record[5]))
	if err != nil {
		return fail("settled_at %q is not RFC3339", record[5])
	}

	row := Row{
		LineNumber:        line,
		ProviderReference: strings.TrimSpace(record[0]),
		Gross:             amounts[0],
		Fee:               amounts[1],
		Net:               amounts[2],
		SettledAt:         settledAt.UTC(),
		Raw:               strings.Join(record, ","),
	}
	if row.ProviderReference == "" {
		return fail("reference is empty")
	}

	// The three FX fields are meaningful only together: an amount with no
	// currency cannot be compared to anything, and a rate with neither cannot
	// be applied. The database enforces the same rule.
	settlementCurrency := strings.TrimSpace(record[6])
	rawRate := strings.TrimSpace(record[7])
	rawSettled := strings.TrimSpace(record[8])

	present := 0
	for _, field := range []string{settlementCurrency, rawRate, rawSettled} {
		if field != "" {
			present++
		}
	}
	if present != 0 && present != 3 {
		return fail("settlement currency, rate and amount must all be present or all absent")
	}

	if present == 3 {
		cur := money.Currency(settlementCurrency)
		if err := cur.Validate(); err != nil {
			return fail("settlement currency: %v", err)
		}
		rate, err := strconv.ParseInt(rawRate, 10, 64)
		if err != nil || rate <= 0 {
			return fail("settlement rate %q is not a positive integer", rawRate)
		}
		settledMinor, err := strconv.ParseInt(rawSettled, 10, 64)
		if err != nil {
			return fail("settled amount %q is not an integer", rawSettled)
		}
		if row.Settled, err = money.New(settledMinor, cur); err != nil {
			return fail("settled amount: %v", err)
		}
		row.SettlementCurrency = cur
		row.SettlementRateNano = rate
	}

	return row, nil
}

// windowFor derives the file's period from the rows it contains.
//
// Derived rather than declared because the simulator's format carries no header
// row for it. The end is exclusive and pushed to the next whole second, so a row
// settled at the last instant still falls inside the window it belongs to.
func windowFor(rows []Row) (start, end time.Time) {
	if len(rows) == 0 {
		now := time.Now().UTC()
		return now, now.Add(time.Second)
	}

	start, end = rows[0].SettledAt, rows[0].SettledAt
	for _, row := range rows[1:] {
		if row.SettledAt.Before(start) {
			start = row.SettledAt
		}
		if row.SettledAt.After(end) {
			end = row.SettledAt
		}
	}
	return start, end.Add(time.Second)
}
