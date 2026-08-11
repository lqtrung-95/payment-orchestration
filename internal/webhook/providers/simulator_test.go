package providers_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook/providers"
)

const secret = "test-webhook-secret"

// signed builds a delivery exactly as the simulator's emitter would, so the
// verifier is tested against the real scheme rather than against the test's own
// idea of it.
func signed(t *testing.T, at time.Time, ev simulator.Event) ([]byte, webhook.Headers) {
	t.Helper()

	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	ts := strconv.FormatInt(at.Unix(), 10)
	signature := simulator.Sign(secret, ts, body)

	return body, func(key string) string {
		switch key {
		case "X-Sim-Timestamp":
			return ts
		case "X-Sim-Signature":
			return signature
		default:
			return ""
		}
	}
}

func sampleEvent() simulator.Event {
	return simulator.Event{
		ID:        "evt_1",
		Type:      "charge.updated",
		Reference: "ch_abc",
		Status:    "AUTH_OK",
		CreatedAt: time.Now().UTC(),
		Sequence:  7,
	}
}

func TestValidDeliveryIsAccepted(t *testing.T) {
	now := time.Now()
	body, hdr := signed(t, now, sampleEvent())

	v := providers.NewSimulator("psp-sim", secret)
	if err := v.Verify(hdr, body, now); err != nil {
		t.Fatalf("Verify rejected a valid delivery: %v", err)
	}

	event, err := v.Parse(body)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if event.ProviderEventID != "evt_1" || event.Reference != "ch_abc" || event.Sequence != 7 {
		t.Errorf("parsed event = %+v, want the fields carried through unchanged", event)
	}
	if event.Status != psp.StatusAuthorized {
		t.Errorf("status = %s, want authorized", event.Status)
	}
}

// The signature covers the body, so any edit to it must invalidate the
// delivery — otherwise a signed event could be rewritten in transit into one
// about a different charge.
func TestTamperedBodyIsRejected(t *testing.T) {
	now := time.Now()
	body, hdr := signed(t, now, sampleEvent())

	tampered := append([]byte(nil), body...)
	tampered = []byte(string(tampered[:len(tampered)-1]) + " }")

	v := providers.NewSimulator("psp-sim", secret)
	if err := v.Verify(hdr, tampered, now); !errors.Is(err, webhook.ErrInvalidSignature) {
		t.Errorf("Verify on a tampered body = %v, want ErrInvalidSignature", err)
	}
}

func TestWrongSecretIsRejected(t *testing.T) {
	now := time.Now()
	body, hdr := signed(t, now, sampleEvent())

	v := providers.NewSimulator("psp-sim", "a-different-secret")
	if err := v.Verify(hdr, body, now); !errors.Is(err, webhook.ErrInvalidSignature) {
		t.Errorf("Verify with the wrong secret = %v, want ErrInvalidSignature", err)
	}
}

// A signature alone does not make a delivery current. Without a bounded window,
// one captured request stays replayable for as long as the secret lives, and
// replaying an old `authorized` event is a way to move a transaction backwards.
func TestGenuineDeliveryOutsideTheWindowIsRejected(t *testing.T) {
	captured := time.Now().Add(-2 * providers.DefaultTolerance)
	body, hdr := signed(t, captured, sampleEvent())

	v := providers.NewSimulator("psp-sim", secret)

	// Authentic at the time it was sent...
	if err := v.Verify(hdr, body, captured); err != nil {
		t.Fatalf("Verify at the time of sending: %v", err)
	}
	// ...and refused when replayed later, with the signature still valid.
	if err := v.Verify(hdr, body, time.Now()); !errors.Is(err, webhook.ErrTimestampOutsideWindow) {
		t.Errorf("Verify on a replayed delivery = %v, want ErrTimestampOutsideWindow", err)
	}
}

func TestMissingSignatureHeadersAreRejected(t *testing.T) {
	body, _ := signed(t, time.Now(), sampleEvent())
	empty := webhook.Headers(func(string) string { return "" })

	v := providers.NewSimulator("psp-sim", secret)
	if err := v.Verify(empty, body, time.Now()); !errors.Is(err, webhook.ErrInvalidSignature) {
		t.Errorf("Verify with no headers = %v, want ErrInvalidSignature", err)
	}
}

// Without the provider's own event id there is no deduplication key, and
// generating one would make every redelivery look new. It has to be fatal.
func TestPayloadWithoutIdentityIsRejected(t *testing.T) {
	v := providers.NewSimulator("psp-sim", secret)

	cases := map[string]simulator.Event{
		"no event id":  {Reference: "ch_abc", Status: "AUTH_OK"},
		"no reference": {ID: "evt_1", Status: "AUTH_OK"},
	}

	for name, ev := range cases {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if _, err := v.Parse(body); !errors.Is(err, webhook.ErrMalformedPayload) {
				t.Errorf("Parse = %v, want ErrMalformedPayload", err)
			}
		})
	}
}

// An unrecognised status maps to pending, never to a terminal state. A provider
// using a word we have not seen is saying something is still in motion, and
// treating that as settled is the more dangerous guess.
func TestUnknownStatusMapsToPending(t *testing.T) {
	ev := sampleEvent()
	ev.Status = "SOMETHING_NEW"
	body, _ := signed(t, time.Now(), ev)

	event, err := providers.NewSimulator("psp-sim", secret).Parse(body)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if event.Status != psp.StatusPending {
		t.Errorf("status = %s, want pending for an unrecognised provider status", event.Status)
	}
}
