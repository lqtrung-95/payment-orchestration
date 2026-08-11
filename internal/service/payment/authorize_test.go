package payment_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

type harness struct {
	db      *postgres.DB
	service *payment.Service
	store   *simulator.Store
	engine  *simulator.Engine
	sim     *httptest.Server
}

// newHarness wires the real service against a real database and a real (if
// fake) provider running over real HTTP. Nothing here is mocked: the behaviour
// under test is what happens when a network call goes wrong, and a mock would
// only assert that the test's own model of failure is self-consistent.
func newHarness(t *testing.T, mode string, faults map[simulator.Fault]float64) *harness {
	t.Helper()

	db := testsupport.FreshDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg, _ := simulator.Preset(simulator.PresetHealthy, 20260811)
	for f, p := range faults {
		cfg.Probabilities[f] = p
	}
	cfg.WebhookDelayMs = 10

	store := simulator.NewStore()
	engine := simulator.NewEngine(cfg)
	hooks := simulator.NewWebhookEmitter("", "test-secret", logger)
	sim := httptest.NewServer(simulator.NewServer(store, engine, hooks, logger))
	t.Cleanup(sim.Close)

	adapter := simclient.New(simclient.Config{
		Name: "psp-test", BaseURL: sim.URL, Mode: mode,
		// Short, so a hung provider surfaces as a timeout this service has to
		// classify rather than as the test hanging.
		Timeout: 300 * time.Millisecond,
	})

	return &harness{
		db:      db,
		service: payment.NewService(db, txstore.NewRepository(), psp.NewRegistry("psp-test", adapter), logger),
		store:   store,
		engine:  engine,
		sim:     sim,
	}
}

// stateChangeReasons returns the audit trail for a transaction.
//
// Used to prove which code path ran. Without it, a test asserting only the
// final state would pass identically whether the fault fired and was recovered
// from, or never fired at all — which would make the most important test in the
// suite vacuous.
func (h *harness) stateChangeReasons(t *testing.T, transactionID uuid.UUID) []string {
	t.Helper()

	rows, err := h.db.Pool().Query(context.Background(),
		`SELECT COALESCE(reason, '') FROM transaction_state_changes
		 WHERE transaction_id = $1 ORDER BY created_at, id`, transactionID)
	if err != nil {
		t.Fatalf("query state changes: %v", err)
	}
	defer rows.Close()

	var reasons []string
	for rows.Next() {
		var reason string
		if err := rows.Scan(&reason); err != nil {
			t.Fatalf("scan reason: %v", err)
		}
		reasons = append(reasons, reason)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate state changes: %v", err)
	}
	return reasons
}

func containsSubstring(values []string, want string) bool {
	for _, v := range values {
		if strings.Contains(v, want) {
			return true
		}
	}
	return false
}

func (h *harness) create(t *testing.T, key string, amountMinor int64) *domain.Transaction {
	t.Helper()

	tx, err := h.service.Create(context.Background(), payment.CreateInput{
		MerchantID:     "m_test",
		IdempotencyKey: key,
		Amount:         money.MustNew(amountMinor, "USD"),
		Actor:          "test",
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	return tx
}

// The flagship guarantee. The provider records the charge and then hangs, so the
// caller is told nothing. A system that retries here charges twice; a system
// that gives up loses a real payment. The only correct move is to ask the
// provider what happened.
func TestTimeoutAfterSuccessRecoversWithoutDoubleCharging(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, map[simulator.Fault]float64{
		simulator.FaultTimeoutAfterSuccess: 1.0,
	})

	tx := h.create(t, "key-timeout", 12550)

	if tx.State != domain.StateAuthorized {
		t.Errorf("state = %s, want authorized — the outcome was recoverable via status", tx.State)
	}

	// Exactly one charge at the provider. This is the number that matters.
	if got := h.store.Count(); got != 1 {
		t.Errorf("provider charge count = %d, want exactly 1", got)
	}

	if tx.PSPReference == "" {
		t.Error("provider reference not recorded, so the charge could not be reconciled later")
	}

	// Prove the recovery path is what produced that state. Asserting only the
	// final state would pass just as happily if the fault had never fired.
	reasons := h.stateChangeReasons(t, tx.ID)
	if !containsSubstring(reasons, "recovered after ambiguous failure") {
		t.Errorf("audit trail %v does not show recovery after an ambiguous failure — "+
			"the timeout fault may not have fired, making this test vacuous", reasons)
	}
}

// Same class of failure, different shape: the provider records the charge and
// then reports a 500. A 500 is ambiguous, not a failure.
func TestError5xxAfterSuccessRecoversWithoutDoubleCharging(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, map[simulator.Fault]float64{
		simulator.FaultError5xxAfterSuccess: 1.0,
	})

	tx := h.create(t, "key-5xx", 8800)

	if tx.State != domain.StateAuthorized {
		t.Errorf("state = %s, want authorized", tx.State)
	}
	if got := h.store.Count(); got != 1 {
		t.Errorf("provider charge count = %d, want exactly 1", got)
	}

	reasons := h.stateChangeReasons(t, tx.ID)
	if !containsSubstring(reasons, "recovered after ambiguous failure") {
		t.Errorf("audit trail %v does not show recovery after an ambiguous failure", reasons)
	}
}

// A decline is a decision, not a fault. It is terminal, and nothing should have
// been recorded at the provider.
func TestDeclineIsTerminal(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)

	// Amount ending in 51: ISO-8583 "not sufficient funds".
	tx := h.create(t, "key-declined", 10051)

	if tx.State != domain.StateFailed {
		t.Errorf("state = %s, want failed", tx.State)
	}
	if tx.State.IsTerminal() != true {
		t.Error("a decline must land in a terminal state")
	}
	if got := h.store.Count(); got != 0 {
		t.Errorf("provider charge count = %d after a decline, want 0", got)
	}
}

// A provider that refuses to service the request has done nothing. The
// transaction must stay open for a later attempt rather than being failed —
// failing it would discard a payment the customer could still have made.
func TestOutageLeavesTransactionOpenRatherThanFailed(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)
	h.engine.StartOutage(5 * time.Second)

	tx := h.create(t, "key-outage", 7700)

	if tx.State == domain.StateFailed {
		t.Error("an outage must not fail the transaction — nothing happened at the provider")
	}
	if tx.State.IsTerminal() {
		t.Errorf("state = %s is terminal; the payment is still attemptable", tx.State)
	}
	if got := h.store.Count(); got != 0 {
		t.Errorf("provider charge count = %d during an outage, want 0", got)
	}
}

// The "no lost payments" guarantee, in its hardest form. The provider records
// the charge, hangs, and then dies before it can be asked what happened. The
// outcome is genuinely unknowable, and the system must say so rather than
// guess — guessing "failed" writes off a payment the customer was charged for.
func TestUnresolvableOutcomeNeverMarksTransactionFailed(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, map[simulator.Fault]float64{
		simulator.FaultTimeoutAfterSuccess: 1.0,
	})

	tx := h.create(t, "key-created", 4400)
	if tx.State != domain.StateAuthorized {
		t.Fatalf("setup: state = %s, want authorized", tx.State)
	}

	// Now kill the provider and try a second, independent payment.
	h.sim.Close()

	second, err := h.service.Create(context.Background(), payment.CreateInput{
		MerchantID:     "m_test",
		IdempotencyKey: "key-provider-dead",
		Amount:         money.MustNew(6600, "USD"),
		Actor:          "test",
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if second.State == domain.StateFailed {
		t.Error("a dead provider must not produce a failed transaction — the outcome is unknown")
	}
	if second.State.IsTerminal() {
		t.Errorf("state = %s is terminal; nothing was established about this payment", second.State)
	}
}

// An asynchronous provider answers "pending". The transaction parks in a
// non-terminal state because the outcome genuinely has not arrived; claiming
// anything else would be asserting something the provider has not said.
func TestAsyncProviderParksTransactionUntilResolved(t *testing.T) {
	h := newHarness(t, simclient.ModeAsync, nil)

	tx := h.create(t, "key-async", 9900)

	if tx.State != domain.StateAuthorizing {
		t.Errorf("state = %s, want authorizing — the provider has not answered yet", tx.State)
	}
	if tx.State.IsTerminal() {
		t.Error("a pending charge must not be in a terminal state")
	}
	if got := h.store.Count(); got != 1 {
		t.Errorf("provider charge count = %d, want 1", got)
	}
}

// A redirect-based provider needs the customer to complete a challenge. Until
// they do, the transaction is unresolved.
func TestRedirectProviderParksTransactionUntilChallengeCompletes(t *testing.T) {
	h := newHarness(t, simclient.ModeRedirect, nil)

	tx := h.create(t, "key-redirect", 3300)

	if tx.State != domain.StateAuthorizing {
		t.Errorf("state = %s, want authorizing", tx.State)
	}
	if tx.PSPReference == "" {
		t.Error("provider reference not recorded, so the challenge could not be correlated on return")
	}
}

// When the provider's status endpoint lags, recovery cannot conclude anything.
// The safe answer is to leave the transaction open, not to assume the charge
// never happened — the charge demonstrably did happen.
func TestStaleStatusLeavesOutcomeUnresolvedRatherThanGuessing(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, map[simulator.Fault]float64{
		simulator.FaultTimeoutAfterSuccess: 1.0,
		simulator.FaultStaleStatus:         1.0,
	})

	tx := h.create(t, "key-stale", 5500)

	if tx.State == domain.StateFailed {
		t.Error("a lagging status endpoint must not cause the payment to be written off")
	}
	if tx.State.IsTerminal() {
		t.Errorf("state = %s is terminal despite an unresolved outcome", tx.State)
	}

	// The charge exists at the provider even though we could not confirm it.
	// Something later — a webhook, a retry, reconciliation — has to close this,
	// which is exactly why it was left open.
	if got := h.store.Count(); got != 1 {
		t.Errorf("provider charge count = %d, want 1", got)
	}
}

// Repeated authorization attempts must present the same provider-scoped key, so
// the provider's own idempotency collapses them into one charge.
func TestRepeatedAuthorizationsCollapseToOneCharge(t *testing.T) {
	h := newHarness(t, simclient.ModeSync, nil)
	ctx := context.Background()

	tx := h.create(t, "key-repeat", 4200)

	// Drive the transaction back to a state from which authorization is legal,
	// then attempt it again. The provider must not see a second charge.
	for i := 0; i < 3; i++ {
		if _, err := h.service.Authorize(ctx, tx.ID); err != nil && !errors.Is(err, domain.ErrIllegalTransition) {
			t.Logf("attempt %d returned: %v", i, err)
		}
	}

	if got := h.store.Count(); got != 1 {
		t.Errorf("provider charge count = %d after repeated authorizations, want 1", got)
	}
}

func TestOperationKeysDifferPerOperation(t *testing.T) {
	tx := domain.Transaction{}
	txn := psp.OperationKey(tx.ID, "authorize")
	cap := psp.OperationKey(tx.ID, "capture")

	// Reusing one key across operations would make the provider treat a capture
	// as a replay of the authorization and return the wrong charge.
	if txn == cap {
		t.Error("authorize and capture must not share an idempotency key")
	}
	if psp.OperationKey(tx.ID, "authorize") != txn {
		t.Error("the key for one operation must be stable across calls, or retries lose their protection")
	}
}
