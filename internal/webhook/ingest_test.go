package webhook_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// The headline guarantee: a provider that redelivers the same event — which is
// ordinary behaviour, not an anomaly — gets a success every time and causes
// exactly one of everything.
func TestSameDeliveryHundredTimesIsStoredOnce(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransaction(t, "ch_dedup")

	const deliveries = 100
	var (
		firstID    int64
		duplicates int
	)

	for i := range deliveries {
		result := h.deliver(t, "evt_dedup", "ch_dedup", "AUTH_OK", 5)
		if i == 0 {
			if result.Duplicate {
				t.Fatal("first delivery reported as a duplicate")
			}
			firstID = result.RawEventID
			continue
		}
		if !result.Duplicate {
			t.Fatalf("delivery %d was accepted as new; deduplication is not holding", i)
		}
		duplicates++
	}

	if duplicates != deliveries-1 {
		t.Errorf("duplicates = %d, want %d", duplicates, deliveries-1)
	}

	// One message queued, so the work is done once even before the consumer's
	// own deduplication gets involved.
	if got := h.countOutboxFor(t, messaging.TopicWebhook); got != 1 {
		t.Errorf("queued messages = %d, want 1", got)
	}

	if _, err := h.processor.Process(context.Background(), firstID); err != nil {
		t.Fatalf("process: %v", err)
	}

	if got := h.countStateChanges(t, tx.ID, "authorized"); got != 1 {
		t.Errorf("transitions to authorized = %d, want exactly 1", got)
	}
}

// Verification happens before anything is written. A public endpoint that
// persists whatever it is sent is a free write amplifier for anyone who finds
// the URL, so a forged delivery must leave no trace at all.
func TestUnauthenticatedDeliveryIsNotPersisted(t *testing.T) {
	h := newHarness(t)
	h.seedTransaction(t, "ch_forged")

	body, _ := event(t, "evt_forged", "ch_forged", "AUTH_OK", 5)
	forged := webhook.Headers(func(key string) string {
		switch key {
		case "X-Sim-Timestamp":
			return strconv.FormatInt(time.Now().Unix(), 10)
		case "X-Sim-Signature":
			return "0000000000000000000000000000000000000000000000000000000000000000"
		default:
			return ""
		}
	})

	_, err := h.ingestor.Ingest(context.Background(), testProvider, forged, body)
	if !errors.Is(err, webhook.ErrInvalidSignature) {
		t.Fatalf("Ingest = %v, want ErrInvalidSignature", err)
	}

	var stored int
	if err := h.db.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM webhook_events_raw`).Scan(&stored); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if stored != 0 {
		t.Errorf("stored events = %d, want 0 — an unverified payload must not be persisted", stored)
	}
	if got := h.countOutboxFor(t, messaging.TopicWebhook); got != 0 {
		t.Errorf("queued messages = %d, want 0", got)
	}
}

// The log keeps the exact bytes, not a re-encoding of them.
//
// The signature was computed over those bytes: normalising the payload into
// JSONB reorders keys and rewrites whitespace, and the stored event can then
// never be verified against the signature that arrived with it — which would
// make the log useless as evidence, its entire reason for existing.
func TestStoredPayloadRemainsVerifiable(t *testing.T) {
	h := newHarness(t)
	h.seedTransaction(t, "ch_bytes")

	body, hdr := event(t, "evt_bytes", "ch_bytes", "AUTH_OK", 5)
	result, err := h.ingestor.Ingest(context.Background(), testProvider, hdr, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	stored := h.rawEvent(t, result.RawEventID)
	if !bytes.Equal(stored.Payload, body) {
		t.Fatalf("stored payload differs from what was received:\n got %s\nwant %s",
			stored.Payload, body)
	}

	// Re-derived from the stored bytes and the stored timestamp, and it still
	// matches the signature that came with the delivery.
	replayed := simulator.Sign(testSecret, hdr("X-Sim-Timestamp"), stored.Payload)
	if replayed != hdr("X-Sim-Signature") {
		t.Error("signature no longer verifies against the stored payload")
	}
}

// A delivery whose transaction does not exist yet is stored and queued anyway.
// The event is authentic and the correlation is what is missing, so refusing it
// here would discard evidence over a race that resolves itself in seconds.
func TestUncorrelatedDeliveryIsStillAccepted(t *testing.T) {
	h := newHarness(t)

	result := h.deliver(t, "evt_early", "ch_unknown", "AUTH_OK", 1)
	if result.Duplicate || result.RawEventID == 0 {
		t.Fatalf("ingest result = %+v, want a stored event", result)
	}

	_, err := h.processor.Process(context.Background(), result.RawEventID)
	if !errors.Is(err, webhook.ErrNotCorrelated) {
		t.Fatalf("Process = %v, want ErrNotCorrelated", err)
	}

	// Left as received, so a retry re-runs the whole decision rather than
	// resuming a half-made one.
	if got := h.rawEvent(t, result.RawEventID).Status; got != webhook.OutcomeReceived {
		t.Errorf("status = %s, want received", got)
	}
}

// A sequence of zero is a real sequence — the oldest one — not a missing value.
//
// Treating it as absent and deriving something from the event's timestamp
// instead rewrites the oldest event in a batch into the newest, which defeats
// the staleness guard for exactly the delivery it exists to catch. The failure
// is silent: the event is accepted, applied, and looks entirely normal.
func TestZeroSequenceIsPreservedNotSubstituted(t *testing.T) {
	h := newHarness(t)
	tx := h.seedTransaction(t, "ch_zero")

	h.deliverAndProcess(t, "evt_current", "ch_zero", "AUTH_OK", 1)

	result := h.deliver(t, "evt_oldest", "ch_zero", "AUTH_OK", 0)
	stored := h.rawEvent(t, result.RawEventID)
	if stored.Sequence != 0 {
		t.Fatalf("stored sequence = %d, want 0 — the provider's own ordering token was rewritten",
			stored.Sequence)
	}

	outcome, err := h.processor.Process(context.Background(), result.RawEventID)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if outcome != webhook.OutcomeSuperseded {
		t.Errorf("outcome = %s, want superseded", outcome)
	}
	if got := h.countStateChanges(t, tx.ID, "authorized"); got != 1 {
		t.Errorf("transitions to authorized = %d, want 1", got)
	}
}

func TestUnknownProviderIsRejected(t *testing.T) {
	h := newHarness(t)

	body, hdr := event(t, "evt_x", "ch_x", "AUTH_OK", 1)
	_, err := h.ingestor.Ingest(context.Background(), "not-a-provider", hdr, body)
	if !errors.Is(err, webhook.ErrUnknownProvider) {
		t.Errorf("Ingest = %v, want ErrUnknownProvider", err)
	}
}
