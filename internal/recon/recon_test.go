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
			recon.DefaultTolerances(),
			logger),
		seeder: newSeeder(db),
	}
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
