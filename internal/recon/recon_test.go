package recon_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/recon"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
	fxstore "github.com/lequoctrung/payment-orchestrator/internal/store/fx"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

const testProvider = "psp-test"

type harness struct {
	db      *postgres.DB
	service *recon.Service
	repo    *recon.Repository
	seeder  *seeder
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := testsupport.FreshDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return &harness{
		db:   db,
		repo: recon.NewRepository(),
		service: recon.NewService(db,
			recon.NewRegistry(recon.NewSimulatorParser(testProvider)),
			recon.NewRepository(),
			recon.NewLedgerReader(),
			fxstore.NewRepository(),
			ledgerstore.NewRepository(),
			recon.DefaultTolerances(),
			logger),
		seeder: newSeeder(db),
	}
}

// ledgerTotals sums every posting by direction, across all accounts.
func (h *harness) ledgerTotals(t *testing.T) (debits, credits int64) {
	t.Helper()

	err := h.db.Pool().QueryRow(context.Background(), `
		SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'debit'), 0),
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'credit'), 0)
		FROM postings`).Scan(&debits, &credits)
	if err != nil {
		t.Fatalf("sum postings: %v", err)
	}
	return debits, credits
}

// settledAt is the nominal settlement instant every fixture hangs off.
var settledAt = time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)

// The headline claim of this phase: every seeded defect is detected and lands
// in the right category. One of each, planted deliberately, counted exactly.
func TestEverySeededDefectIsDetectedAndClassified(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// One clean payment per defect, plus one that should reconcile silently.
	wanted := []breaks.Category{
		breaks.AmountMismatch,
		breaks.FeeMismatch,
		breaks.CurrencyMismatch,
		breaks.TimingCutoff,
		breaks.DuplicateSettlement,
		breaks.MissingAtPSP,
	}

	var (
		records []recon.LedgerRecord
		defects []recon.Defect
	)
	for i, category := range wanted {
		// timing_cutoff is the one category defined by *when* the capture
		// happened rather than by what the row says: the payment is absent from
		// this file because it settles in the next one. Seeding it inside the
		// window would make it indistinguishable from missing_at_psp.
		occurredAt := settledAt
		if category == breaks.TimingCutoff {
			occurredAt = settledAt.Add(24 * time.Hour)
		}

		record := h.seeder.capture(t, i, money.MustNew(100_00, "USD"), occurredAt)
		records = append(records, record)
		defects = append(defects, recon.Defect{Category: category, Reference: record.ProviderReference})
	}

	// A clean payment, to prove a correct file does not manufacture breaks.
	clean := h.seeder.capture(t, len(wanted), money.MustNew(55_00, "USD"), settledAt)
	records = append(records, clean)

	// And a settlement row with no counterpart at all.
	defects = append(defects, recon.Defect{Category: breaks.MissingInternally})

	content, applied, err := recon.Generate(recon.GeneratorInput{
		Provider: testProvider, Records: records, Defects: defects, SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(applied) != len(defects) {
		t.Fatalf("generator applied %d defects, planted %d", len(applied), len(defects))
	}

	file, _, err := h.service.Ingest(ctx, testProvider, "settlement.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Exactly one break per planted defect, in the category it was planted as.
	for _, d := range applied {
		if got := report.ByCategory[d.Category]; got != 1 {
			t.Errorf("%s: %d breaks, want exactly 1", d.Category, got)
		}
	}
	if report.Total() != len(applied) {
		t.Errorf("total breaks = %d, want %d — an unplanted break was raised",
			report.Total(), len(applied))
		for category, n := range report.ByCategory {
			t.Logf("  %-22s %d", category, n)
		}
	}

	// The clean payment reconciled silently.
	if report.Matched == 0 {
		t.Error("nothing matched at all; the reference join is broken")
	}
}

// FX drift needs a rate lock to be explainable, so it gets its own fixture.
//
// The classifier's test is not "is the difference small" but "does the rate the
// provider says it used reproduce the figure it sent" — so this asserts drift
// is recognised as explained rather than reported as an unexplained mismatch.
func TestSettlementRateMovementIsClassifiedAsDriftNotMismatch(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(200_00, "EUR"), settledAt)
	h.seeder.lockRate(t, record.TransactionID, "EUR", "USD", 1_085_000_000)

	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record},
		Defects:   []recon.Defect{{Category: breaks.FXDrift, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	file, _, err := h.service.Ingest(ctx, testProvider, "fx.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := report.ByCategory[breaks.FXDrift]; got != 1 {
		t.Errorf("fx_drift breaks = %d, want 1", got)
	}
	if got := report.ByCategory[breaks.AmountMismatch]; got != 0 {
		t.Errorf("amount_mismatch breaks = %d, want 0 — drift was reported as unexplained", got)
	}
}

// Re-running a reconciliation must not raise the same break twice. Otherwise
// every re-run after fixing an ingestion gap doubles the operator's workload.
func TestReconcilingTheSameFileTwiceRaisesNoNewBreaks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(100_00, "USD"), settledAt)
	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record},
		Defects:   []recon.Defect{{Category: breaks.AmountMismatch, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	file, _, err := h.service.Ingest(ctx, testProvider, "twice.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	first, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if first.NewBreaks != 1 {
		t.Fatalf("first run raised %d new breaks, want 1", first.NewBreaks)
	}

	second, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if second.NewBreaks != 0 {
		t.Errorf("second run raised %d new breaks, want 0 — reconciliation is not idempotent", second.NewBreaks)
	}
	if second.Total() != first.Total() {
		t.Errorf("second run found %d breaks, first found %d", second.Total(), first.Total())
	}

	var stored int
	if err := h.db.Pool().QueryRow(ctx, `SELECT count(*) FROM recon_breaks`).Scan(&stored); err != nil {
		t.Fatalf("count breaks: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored breaks = %d, want 1", stored)
	}
}

// Re-ingesting identical bytes recognises the file rather than storing a copy.
func TestReingestingTheSameFileIsRecognised(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(10_00, "USD"), settledAt)
	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider: testProvider, Records: []recon.LedgerRecord{record}, SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	first, isNew, err := h.service.Ingest(ctx, testProvider, "a.csv", strings.NewReader(content))
	if err != nil || !isNew {
		t.Fatalf("first ingest: new=%v err=%v", isNew, err)
	}

	// Same bytes, different filename — providers rename files routinely.
	second, isNew, err := h.service.Ingest(ctx, testProvider, "a-resent.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if isNew {
		t.Error("identical content was ingested as a new file")
	}
	if second.ID != first.ID {
		t.Errorf("re-ingest produced id %s, want the original %s", second.ID, first.ID)
	}
}

// Only two categories may close without a human, and both because the
// difference is explained rather than merely small.
func TestOnlyExplainedCategoriesAreAutoResolvable(t *testing.T) {
	for _, c := range breaks.All {
		want := c == breaks.FXDrift || c == breaks.TimingCutoff
		if got := c.AutoResolvable(); got != want {
			t.Errorf("%s.AutoResolvable() = %v, want %v", c, got, want)
		}
	}
}

// Resolving requires attribution, and the database enforces it independently of
// the Go check.
func TestResolvingABreakRequiresAnActorAndReason(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(100_00, "USD"), settledAt)
	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record},
		Defects:   []recon.Defect{{Category: breaks.AmountMismatch, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	file, _, err := h.service.Ingest(ctx, testProvider, "resolve.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := h.service.Reconcile(ctx, file.ID, "test"); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var breakID uuid.UUID
	if err := h.db.Pool().QueryRow(ctx, `SELECT id FROM recon_breaks LIMIT 1`).Scan(&breakID); err != nil {
		t.Fatalf("read break: %v", err)
	}

	if err := h.repo.Resolve(ctx, h.db.Pool(), breakID,
		breaks.Resolution{Status: breaks.StatusResolved}, nil); err == nil {
		t.Error("resolved a break with no actor and no reason")
	}

	if err := h.repo.Resolve(ctx, h.db.Pool(), breakID, breaks.Resolution{
		Status: breaks.StatusResolved, Actor: "ops:alice", Note: "provider confirmed adjustment",
		At: time.Now().UTC(),
	}, nil); err != nil {
		t.Fatalf("Resolve with attribution: %v", err)
	}

	// A decided break is not reopened; a new break is the correct move.
	if err := h.repo.Resolve(ctx, h.db.Pool(), breakID, breaks.Resolution{
		Status: breaks.StatusWrittenOff, Actor: "ops:bob", Note: "changed my mind",
		At: time.Now().UTC(),
	}, nil); err == nil {
		t.Error("re-resolved an already decided break")
	}
}

// A rate lock is not required for the common case, and its absence must not be
// treated as a fault.
func TestFXRateHelpersRemainConsistent(t *testing.T) {
	rate, err := fx.NewRate("EUR", "USD", 1_085_000_000, "test", settledAt)
	if err != nil {
		t.Fatalf("NewRate: %v", err)
	}
	if _, err := rate.Convert(money.MustNew(100_00, "EUR")); err != nil {
		t.Fatalf("Convert: %v", err)
	}
}

// Detecting drift and accounting for it are different things. Until an entry
// exists the books still say we are owed what we originally booked, and the
// discrepancy lives only in a report — which is the state reconciliation is
// supposed to end.
func TestFXDriftIsAutoResolvedAndPostedToTheLedger(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(200_00, "EUR"), settledAt)
	h.seeder.lockRate(t, record.TransactionID, "EUR", "USD", 1_085_000_000)

	debitsBefore, creditsBefore := h.ledgerTotals(t)

	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record},
		Defects:   []recon.Defect{{Category: breaks.FXDrift, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	file, _, err := h.service.Ingest(ctx, testProvider, "drift.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ByCategory[breaks.FXDrift] != 1 {
		t.Fatalf("fx_drift breaks = %d, want 1", report.ByCategory[breaks.FXDrift])
	}
	if report.AutoResolved != 1 {
		t.Errorf("auto-resolved = %d, want 1", report.AutoResolved)
	}

	// Closed, attributed, and carrying the entry that justifies it.
	var (
		status, actor, note string
		entryID             *uuid.UUID
	)
	if err := h.db.Pool().QueryRow(ctx, `
		SELECT status, COALESCE(resolved_by,''), COALESCE(resolution_note,''), adjustment_entry_id
		FROM recon_breaks WHERE category = 'fx_drift'`).Scan(&status, &actor, &note, &entryID); err != nil {
		t.Fatalf("read break: %v", err)
	}
	if status != string(breaks.StatusResolved) {
		t.Errorf("status = %s, want resolved", status)
	}
	if actor == "" || note == "" {
		t.Errorf("auto-resolution left actor %q and note %q; both must be recorded", actor, note)
	}
	if entryID == nil {
		t.Fatal("no adjustment entry linked; the drift was detected but never accounted")
	}

	// The adjustment balances, like every other entry.
	debitsAfter, creditsAfter := h.ledgerTotals(t)
	if debitsAfter == debitsBefore {
		t.Error("no postings were written for the drift")
	}
	if debitsAfter != creditsAfter {
		t.Errorf("ledger does not balance after the adjustment: debits %d, credits %d",
			debitsAfter, creditsAfter)
	}
	if (debitsAfter - debitsBefore) != (creditsAfter - creditsBefore) {
		t.Error("the adjustment itself does not balance")
	}

	// And it landed in the FX gain/loss account, not somewhere convenient.
	var gainLossPostings int
	if err := h.db.Pool().QueryRow(ctx, `
		SELECT count(*) FROM postings p
		JOIN ledger_accounts a ON a.id = p.account_id
		WHERE a.purpose = 'fx_gain_loss'`).Scan(&gainLossPostings); err != nil {
		t.Fatalf("count fx postings: %v", err)
	}
	if gainLossPostings != 1 {
		t.Errorf("fx_gain_loss postings = %d, want 1", gainLossPostings)
	}
}

// A timing cutoff is not a discrepancy — the payment settles in the next file —
// so it closes with no entry. Posting one would invent a movement.
func TestTimingCutoffAutoResolvesWithoutPostingAnything(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(80_00, "USD"), settledAt.Add(24*time.Hour))
	// A second payment inside the window, so the file has rows and a period.
	inWindow := h.seeder.capture(t, 1, money.MustNew(10_00, "USD"), settledAt)

	debitsBefore, _ := h.ledgerTotals(t)

	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record, inWindow},
		Defects:   []recon.Defect{{Category: breaks.TimingCutoff, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	file, _, err := h.service.Ingest(ctx, testProvider, "cutoff.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.ByCategory[breaks.TimingCutoff] != 1 {
		t.Fatalf("timing_cutoff breaks = %d, want 1", report.ByCategory[breaks.TimingCutoff])
	}
	if report.AutoResolved != 1 {
		t.Errorf("auto-resolved = %d, want 1", report.AutoResolved)
	}

	if debitsAfter, _ := h.ledgerTotals(t); debitsAfter != debitsBefore {
		t.Errorf("postings changed from %d to %d; a timing cutoff moves no money",
			debitsBefore, debitsAfter)
	}
}

// Everything else waits for a person. An amount mismatch might be the
// provider's error or ours, and closing it automatically is how a real
// discrepancy disappears as noise.
func TestUnexplainedBreaksAreLeftOpen(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(100_00, "USD"), settledAt)
	content, _, err := recon.Generate(recon.GeneratorInput{
		Provider:  testProvider,
		Records:   []recon.LedgerRecord{record},
		Defects:   []recon.Defect{{Category: breaks.AmountMismatch, Reference: record.ProviderReference}},
		SettledAt: settledAt,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	file, _, err := h.service.Ingest(ctx, testProvider, "open.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.AutoResolved != 0 {
		t.Errorf("auto-resolved = %d, want 0 — an unexplained break needs a human", report.AutoResolved)
	}

	var status string
	if err := h.db.Pool().QueryRow(ctx,
		`SELECT status FROM recon_breaks WHERE category = 'amount_mismatch'`).Scan(&status); err != nil {
		t.Fatalf("read break: %v", err)
	}
	if status != string(breaks.StatusOpen) {
		t.Errorf("status = %s, want open", status)
	}
}

// A provider whose own numbers do not add up is not drifting — its figures are
// wrong in a way no rate explains, and calling that drift would close a real
// error as expected noise.
func TestSelfInconsistentConversionIsNotCalledDrift(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	record := h.seeder.capture(t, 0, money.MustNew(200_00, "EUR"), settledAt)
	h.seeder.lockRate(t, record.TransactionID, "EUR", "USD", 1_085_000_000)

	// A rate of 1.09 on 200.00 EUR implies 218.00 USD; this file claims 300.00.
	content := "reference,gross_minor,fee_minor,net_minor,currency,settled_at,settlement_currency,settlement_rate_nano,settled_minor\n" +
		record.ProviderReference + ",20000,605,19395,EUR," +
		settledAt.UTC().Format(time.RFC3339) + ",USD,1090000000,30000\n"

	file, _, err := h.service.Ingest(ctx, testProvider, "inconsistent.csv", strings.NewReader(content))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	report, err := h.service.Reconcile(ctx, file.ID, "test")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := report.ByCategory[breaks.AmountMismatch]; got != 1 {
		t.Errorf("amount_mismatch = %d, want 1", got)
	}
	if got := report.ByCategory[breaks.FXDrift]; got != 0 {
		t.Errorf("fx_drift = %d, want 0 — inconsistent figures were excused as drift", got)
	}
	if report.AutoResolved != 0 {
		t.Errorf("auto-resolved = %d, want 0", report.AutoResolved)
	}
}
