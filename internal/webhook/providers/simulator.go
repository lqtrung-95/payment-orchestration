// Package providers holds one signature scheme and payload mapping per
// provider.
//
// Each provider gets its own file and its own verifier. Real schemes differ in
// what is signed, how it is encoded, and which headers carry it, and a shared
// "generic" verifier grows conditionals until no one can state what any single
// provider requires — which is precisely the code you do not want between the
// internet and a payment ledger.
package providers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
)

// Simulator verifies the fault-injecting simulator's scheme: HMAC-SHA256 over
// "<timestamp>.<body>", hex encoded.
type Simulator struct {
	name      string
	secret    []byte
	tolerance time.Duration
}

// DefaultTolerance bounds how long a captured delivery stays replayable.
//
// Five minutes is the conventional window, and the trade is explicit: too tight
// and ordinary clock skew between the provider and this service rejects genuine
// events; too loose and a captured request keeps working for as long as the
// window lasts.
const DefaultTolerance = 5 * time.Minute

func NewSimulator(name, secret string) *Simulator {
	return &Simulator{name: name, secret: []byte(secret), tolerance: DefaultTolerance}
}

func (s *Simulator) Name() string { return s.name }

func (s *Simulator) SignatureHeader() string { return headerSignature }

// Header names the simulator signs with.
const (
	headerTimestamp = "X-Sim-Timestamp"
	headerSignature = "X-Sim-Signature"
)

func (s *Simulator) Verify(hdr webhook.Headers, body []byte, now time.Time) error {
	rawTS := hdr(headerTimestamp)
	signature := hdr(headerSignature)
	if rawTS == "" || signature == "" {
		return webhook.ErrInvalidSignature
	}

	// The timestamp is checked before the signature is compared so that a
	// stale-but-authentic replay and a forgery are both refused, and neither
	// path does more work than it has to.
	seconds, err := strconv.ParseInt(rawTS, 10, 64)
	if err != nil {
		return webhook.ErrInvalidSignature
	}
	skew := now.Sub(time.Unix(seconds, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > s.tolerance {
		return fmt.Errorf("%w: %s off", webhook.ErrTimestampOutsideWindow, skew)
	}

	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(rawTS))
	_, _ = mac.Write([]byte{'.'})
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	// Constant time. A byte-by-byte compare returns sooner the earlier it finds
	// a mismatch, and that timing difference is enough to recover a valid
	// signature one byte at a time.
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return webhook.ErrInvalidSignature
	}
	return nil
}

// simulatorEvent mirrors the simulator's published payload. Duplicated here
// rather than imported from the simulator package: this is a third party's
// contract, and sharing the type would let a change inside the simulator
// silently redefine what this service believes it received.
type simulatorEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Reference string    `json:"reference"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Sequence  int64     `json:"sequence"`
}

func (s *Simulator) Parse(body []byte) (*webhook.Event, error) {
	var raw simulatorEvent
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w: %w", webhook.ErrMalformedPayload, err)
	}
	if raw.ID == "" {
		// Without the provider's own event id there is no deduplication key.
		// Generating one here would make every redelivery look like a new event
		// and defeat the whole mechanism, so this has to be fatal.
		return nil, fmt.Errorf("%w: event has no id", webhook.ErrMalformedPayload)
	}
	if raw.Reference == "" {
		return nil, fmt.Errorf("%w: event has no charge reference", webhook.ErrMalformedPayload)
	}

	return &webhook.Event{
		ProviderEventID: raw.ID,
		Reference:       raw.Reference,
		Sequence:        raw.Sequence,
		OccurredAt:      raw.CreatedAt,
		Status:          mapSimulatorStatus(raw.Status),
		RawStatus:       raw.Status,
	}, nil
}

// mapSimulatorStatus translates the simulator's vocabulary into the normalized
// one. An unrecognised status maps to pending rather than to anything terminal:
// a provider using a word we have never seen is saying something is still in
// motion, and treating that as settled is the more dangerous guess.
func mapSimulatorStatus(raw string) psp.Status {
	switch raw {
	case "AUTH_OK":
		return psp.StatusAuthorized
	case "CAPTURED":
		return psp.StatusCaptured
	case "REFUNDED":
		return psp.StatusRefunded
	case "VOIDED":
		return psp.StatusVoided
	case "FAILED":
		return psp.StatusFailed
	case "ACTION_REQUIRED":
		return psp.StatusRequiresAction
	default:
		return psp.StatusPending
	}
}
