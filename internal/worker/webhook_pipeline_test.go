package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// newAsyncPipeline talks to the provider shape whose outcome only ever arrives
// as a callback. Authorization answers "pending", so the transaction parks in
// authorizing and nothing but a webhook can move it — which makes every
// assertion below about the webhook path rather than about the API path.
func newAsyncPipeline(t *testing.T, faults map[simulator.Fault]float64) *pipeline {
	t.Helper()
	return newPipelineWith(t, pipelineOpts{mode: simclient.ModeAsync, faults: faults})
}

// countEvents reports how many stored events reached a given outcome.
func (p *pipeline) countEvents(t *testing.T, status webhook.Outcome) int {
	t.Helper()

	var n int
	if err := p.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM webhook_events_raw WHERE status = $1`, string(status)).Scan(&n); err != nil {
		t.Fatalf("count webhook events: %v", err)
	}
	return n
}

func (p *pipeline) countTransitions(t *testing.T, id uuid.UUID, to domain.State) int {
	t.Helper()

	var n int
	if err := p.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM transaction_state_changes WHERE transaction_id = $1 AND to_state = $2`,
		id, string(to)).Scan(&n); err != nil {
		t.Fatalf("count transitions: %v", err)
	}
	return n
}

// The full asynchronous shape: the API returns before the provider has decided,
// the provider decides out of band, and a signed callback carries the outcome
// back through ingestion, Kafka, and the guarded processor.
func TestAsyncPaymentIsResolvedByItsWebhook(t *testing.T) {
	p := newAsyncPipeline(t, nil)

	id := p.create(t, "hook-happy", 7700)

	// Parked first: the provider said pending, and nothing may claim otherwise
	// until the callback lands.
	p.awaitState(t, id, 20*time.Second, domain.StateAuthorizing)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)

	if got := p.countTransitions(t, id, domain.StateAuthorized); got != 1 {
		t.Errorf("transitions to authorized = %d, want exactly 1", got)
	}
	if got := p.countEvents(t, webhook.OutcomeApplied); got != 1 {
		t.Errorf("applied events = %d, want 1", got)
	}
	if got := p.store.Count(); got != 1 {
		t.Errorf("provider charges = %d, want 1", got)
	}
}

// A provider that redelivers the same event, and also delivers an older one
// after a newer one, must still produce one transition and the right final
// state. Both faults at once, which is how they actually show up.
func TestDuplicateAndOutOfOrderWebhooksProduceOneTransition(t *testing.T) {
	p := newAsyncPipeline(t, map[simulator.Fault]float64{
		simulator.FaultDuplicateWebhook:  1.0,
		simulator.FaultOutOfOrderWebhook: 1.0,
	})

	id := p.create(t, "hook-chaos", 6600)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)

	// Long enough for the duplicates and the stale event to have been processed.
	time.Sleep(3 * time.Second)

	tx := p.awaitState(t, id, 5*time.Second, domain.StateAuthorized)
	if tx.State != domain.StateAuthorized {
		t.Fatalf("state = %s, want authorized", tx.State)
	}
	if got := p.countTransitions(t, id, domain.StateAuthorized); got != 1 {
		t.Errorf("transitions to authorized = %d, want exactly 1 despite duplicates", got)
	}

	// The stale event is kept and marked, not silently discarded: "we saw this
	// and deliberately did not act on it" is the answer an investigation needs.
	if got := p.countEvents(t, webhook.OutcomeSuperseded); got == 0 {
		t.Error("no event was recorded as superseded; the out-of-order delivery left no trace")
	}
}

// The webhook-before-response race. The provider's callback and its HTTP reply
// are sent concurrently, so the callback regularly arrives before this service
// has recorded the reference that same reply was about to deliver.
//
// The event must not be dropped, and — the part that actually matters — no
// transaction may be conjured from it. A payment nobody asked for is not a
// payment.
func TestWebhookArrivingBeforeTheResponseIsCorrelatedNotInvented(t *testing.T) {
	p := newAsyncPipeline(t, map[simulator.Fault]float64{
		simulator.FaultWebhookBeforeResponse: 1.0,
	})

	id := p.create(t, "hook-race", 5500)

	// It resolves, but only after the retry ladder gives correlation a second
	// chance — the first attempt genuinely cannot find the transaction.
	p.awaitState(t, id, 40*time.Second, domain.StateAuthorized)

	if got := p.countEvents(t, webhook.OutcomeApplied); got != 1 {
		t.Errorf("applied events = %d, want 1", got)
	}

	var transactions int
	if err := p.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM payment_transactions`).Scan(&transactions); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if transactions != 1 {
		t.Errorf("transactions = %d, want exactly 1 — a webhook must never create one", transactions)
	}
}

// A message waiting out a slow retry tier must not stall live traffic.
//
// This is a regression test for a real stall: records from every assigned
// partition are handed to one goroutine, so a handler that slept until its
// message was due held all of them — one deferred retry stopped every payment
// the consumer owned. Worse, a wait longer than the rebalance timeout would get
// the consumer evicted from its group entirely.
//
// The fix is to pause and rewind the partition instead of sleeping, so the
// deferred message waits alone.
func TestDeferredRetryDoesNotStallLiveWork(t *testing.T) {
	// Long enough that a sleeping consumer could not possibly finish in time.
	slow := messaging.PrefixedTopics("test-stall-"+uuid.NewString()[:8]+".", 90*time.Second)
	p := newPipelineWith(t, pipelineOpts{mode: simclient.ModeSync, topics: &slow})

	// Park a message on a retry tier by hand: it becomes due in 90 seconds, and
	// nothing may wait for it.
	p.publishRaw(t, slow.Retry[0].Topic, "m_async", uuid.NewString(),
		[]byte(`{"transaction_id":"00000000-0000-0000-0000-000000000000","merchant_id":"m_async"}`))

	// Give the consumer time to fetch the deferred message and defer it.
	time.Sleep(3 * time.Second)

	id := p.create(t, "stall-live", 9900)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)
}

// Replaying the whole stored log against current state must change nothing. A
// log that quietly re-applies itself is worse than no log, because it invites
// exactly the recovery procedure that corrupts state.
func TestReplayingTheLogAfterAFullRunChangesNothing(t *testing.T) {
	p := newAsyncPipeline(t, map[simulator.Fault]float64{
		simulator.FaultDuplicateWebhook:  1.0,
		simulator.FaultOutOfOrderWebhook: 1.0,
	})

	id := p.create(t, "hook-replay", 4400)
	p.awaitState(t, id, 30*time.Second, domain.StateAuthorized)
	time.Sleep(3 * time.Second)

	report, err := p.proc.Replay(context.Background(), p.db.Pool())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(report.Entries) == 0 {
		t.Fatal("no events were stored; the replay assertion would be vacuous")
	}
	if report.Changed != 0 {
		for _, e := range report.Entries {
			if e.Changed() {
				t.Errorf("event %s would re-apply on replay", e.ProviderEventID)
			}
		}
		t.Fatalf("%d of %d events would change state on replay", report.Changed, len(report.Entries))
	}
}
