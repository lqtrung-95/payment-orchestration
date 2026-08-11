package webhook_test

import (
	"context"
	"testing"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

func TestAuthorizedEventResolvesTheTransaction(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransaction(t, "ch_apply")

	if got := h.deliverAndProcess(t, "evt_1", "ch_apply", "AUTH_OK", 5); got != webhook.OutcomeApplied {
		t.Fatalf("outcome = %s, want applied", got)
	}

	reloaded := h.reload(t, tx.ID)
	if reloaded.State != domain.StateAuthorized {
		t.Errorf("state = %s, want authorized", reloaded.State)
	}
	if reloaded.LastAppliedEventSeq != 5 {
		t.Errorf("high-water mark = %d, want 5", reloaded.LastAppliedEventSeq)
	}
}

// Arrival order carries no authority. An event older than what has already been
// applied is stale by its own sequence, whenever it turns up — and it is
// recorded as superseded rather than dropped, because "we saw this and did not
// act on it" is the answer an investigation needs.
func TestStaleEventIsSupersededNotApplied(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransaction(t, "ch_stale")

	h.deliverAndProcess(t, "evt_new", "ch_stale", "AUTH_OK", 9)

	result := h.deliver(t, "evt_old", "ch_stale", "AUTH_PENDING_STALE", 4)
	outcome, err := h.processor.Process(context.Background(), result.RawEventID)
	if err != nil {
		t.Fatalf("process stale event: %v", err)
	}
	if outcome != webhook.OutcomeSuperseded {
		t.Fatalf("outcome = %s, want superseded", outcome)
	}

	if got := h.rawEvent(t, result.RawEventID).Status; got != webhook.OutcomeSuperseded {
		t.Errorf("recorded status = %s, want superseded — a stale event must be kept, not dropped", got)
	}
	reloaded := h.reload(t, tx.ID)
	if reloaded.State != domain.StateAuthorized {
		t.Errorf("state = %s, want authorized to be unchanged by a stale event", reloaded.State)
	}
	if reloaded.LastAppliedEventSeq != 9 {
		t.Errorf("high-water mark = %d, want it to stay at 9", reloaded.LastAppliedEventSeq)
	}
}

// The same three events in either order must land in the same place. This is
// the property that lets the receiver stop caring about delivery order at all.
func TestFullyReversedDeliveryReachesTheSameState(t *testing.T) {
	h := newHarness(t)

	forward := h.seedTransaction(t, "ch_forward")
	h.deliverAndProcess(t, "f1", "ch_forward", "PENDING_ASYNC", 1)
	h.deliverAndProcess(t, "f2", "ch_forward", "AUTH_PENDING_STALE", 2)
	h.deliverAndProcess(t, "f3", "ch_forward", "AUTH_OK", 3)

	reversed := h.seedTransaction(t, "ch_reversed")
	h.deliverAndProcess(t, "r3", "ch_reversed", "AUTH_OK", 3)
	h.deliverAndProcess(t, "r2", "ch_reversed", "AUTH_PENDING_STALE", 2)
	h.deliverAndProcess(t, "r1", "ch_reversed", "PENDING_ASYNC", 1)

	a, b := h.reload(t, forward.ID), h.reload(t, reversed.ID)
	if a.State != b.State {
		t.Errorf("forward order ended at %s, reversed at %s — delivery order changed the outcome",
			a.State, b.State)
	}
	if a.State != domain.StateAuthorized {
		t.Errorf("state = %s, want authorized", a.State)
	}
	if got := h.countStateChanges(t, reversed.ID, "authorized"); got != 1 {
		t.Errorf("transitions to authorized under reversed delivery = %d, want 1", got)
	}
}

// A late event implying a move the state machine does not allow is refused
// structurally, not by a check somebody remembered to write for this case.
func TestEventImplyingAnIllegalTransitionIsRejected(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransactionInState(t, "ch_captured", domain.StateCaptured)

	result := h.deliver(t, "evt_late_auth", "ch_captured", "AUTH_OK", 99)
	outcome, err := h.processor.Process(context.Background(), result.RawEventID)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if outcome != webhook.OutcomeRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}

	if got := h.reload(t, tx.ID).State; got != domain.StateCaptured {
		t.Errorf("state = %s, want captured to be untouched", got)
	}
	if note := h.rawEvent(t, result.RawEventID).Note; note == "" {
		t.Error("rejection recorded without a reason")
	}
}

// An event confirming a state the transaction already reached — which happens
// whenever the ambiguous-outcome recovery path asks the provider before the
// callback lands — must not be written into the audit trail as a transition.
// A trail showing a payment authorized twice sends an incident responder
// looking for a second authorization that never happened.
func TestEventConfirmingTheCurrentStateWritesNoTransition(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransactionInState(t, "ch_confirm", domain.StateAuthorized)

	// Compared before and after rather than against a fixed number, because the
	// fixture reaches its state without writing audit rows.
	before := h.countStateChanges(t, tx.ID, "authorized")

	result := h.deliver(t, "evt_confirm", "ch_confirm", "AUTH_OK", 12)
	outcome, err := h.processor.Process(context.Background(), result.RawEventID)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if outcome != webhook.OutcomeIgnored {
		t.Fatalf("outcome = %s, want ignored — nothing changed", outcome)
	}

	// It still moves the high-water mark: an older event arriving afterwards is
	// stale relative to this one too.
	if got := h.reload(t, tx.ID).LastAppliedEventSeq; got != 12 {
		t.Errorf("high-water mark = %d, want 12", got)
	}
	if got := h.countStateChanges(t, tx.ID, "authorized"); got != before {
		t.Errorf("transitions to authorized = %d, want %d — the confirmation added a row", got, before)
	}
}

// Reprocessing the same stored event cannot transition twice, even if the
// consumer's own deduplication is bypassed entirely.
func TestReprocessingAStoredEventIsANoOp(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransaction(t, "ch_reprocess")

	result := h.deliver(t, "evt_once", "ch_reprocess", "AUTH_OK", 5)
	for range 5 {
		if _, err := h.processor.Process(context.Background(), result.RawEventID); err != nil {
			t.Fatalf("process: %v", err)
		}
	}

	if got := h.countStateChanges(t, tx.ID, "authorized"); got != 1 {
		t.Errorf("transitions to authorized = %d, want exactly 1", got)
	}
}

// The log is convergent: replaying every stored event against current state
// changes nothing. A log that quietly re-applies itself is worse than no log,
// because it invites exactly the recovery procedure that corrupts state.
func TestReplayingTheWholeLogChangesNothing(t *testing.T) {
	h := newHarness(t)

	h.seedTransaction(t, "ch_a")
	h.seedTransaction(t, "ch_b")

	h.deliverAndProcess(t, "a1", "ch_a", "PENDING_ASYNC", 1)
	h.deliverAndProcess(t, "a2", "ch_a", "AUTH_OK", 2)
	h.deliverAndProcess(t, "b1", "ch_b", "AUTH_OK", 4)
	h.deliverAndProcess(t, "b2", "ch_b", "AUTH_PENDING_STALE", 3)

	report, err := h.processor.Replay(context.Background(), h.db.Pool())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(report.Entries) != 4 {
		t.Fatalf("replayed %d events, want 4", len(report.Entries))
	}
	if report.Changed != 0 {
		for _, e := range report.Entries {
			if e.Changed() {
				t.Errorf("event %s would re-apply on replay", e.ProviderEventID)
			}
		}
		t.Fatalf("%d events would change state on replay; the log is not convergent", report.Changed)
	}
}
