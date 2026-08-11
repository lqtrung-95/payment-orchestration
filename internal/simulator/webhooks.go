package simulator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// deliveryTimeout bounds a single webhook delivery attempt.
const deliveryTimeout = 5 * time.Second

// Event is what the provider delivers to the orchestrator's webhook endpoint.
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Reference string    `json:"reference"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`

	// Sequence lets a receiver detect reordering. Real providers expose
	// something equivalent, and without it out-of-order delivery is
	// indistinguishable from a genuine state change.
	Sequence int64 `json:"sequence"`
}

// WebhookEmitter delivers events, optionally badly.
//
// The receiving endpoint does not exist yet — webhook ingestion is a later
// phase. The emitter is built now because it belongs to the provider, and
// because the faults it implements are what that phase will be tested against.
type WebhookEmitter struct {
	targetURL string
	secret    string
	client    *http.Client
	logger    *slog.Logger

	mu       sync.Mutex
	sequence int64

	// delivered records every event sent, so tests can assert on duplication
	// and ordering without needing a live receiver.
	delivered []Event

	wg sync.WaitGroup
}

func NewWebhookEmitter(targetURL, secret string, logger *slog.Logger) *WebhookEmitter {
	return &WebhookEmitter{
		targetURL: targetURL,
		secret:    secret,
		client:    &http.Client{Timeout: 5 * time.Second},
		logger:    logger,
	}
}

// Delivered returns a copy of the event log.
func (e *WebhookEmitter) Delivered() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Event, len(e.delivered))
	copy(out, e.delivered)
	return out
}

// Wait blocks until in-flight deliveries finish, so tests are not racing
// background goroutines.
func (e *WebhookEmitter) Wait() { e.wg.Wait() }

func (e *WebhookEmitter) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delivered = nil
	e.sequence = 0
}

// Background runs fn as a tracked goroutine.
//
// Provider follow-up work is deliberately not tied to the originating request's
// context: a webhook is the provider's own action, and cancelling it because
// the API call returned would mean an async charge never resolves.
func (e *WebhookEmitter) Background(fn func()) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		fn()
	}()
}

// Emit builds and delivers the event, applying the delivery faults.
func (e *WebhookEmitter) Emit(reference, key, status string, engine *Engine) {
	events := []Event{e.build(reference, status)}

	// Providers retry deliveries they believe failed, so the same event arrives
	// more than once. The receiver must deduplicate; the emitter's job is to
	// make sure it has to.
	if engine.Fires(FaultDuplicateWebhook, key) {
		dup := events[0]
		events = append(events, dup, dup)
		e.logger.Warn("injecting duplicate webhook", slog.String("reference", reference))
	}

	// Out-of-order delivery: an earlier event arrives after a later one. The
	// receiver must reject the stale one on its own merits rather than assuming
	// arrival order reflects causality.
	if engine.Fires(FaultOutOfOrderWebhook, key) {
		stale := e.build(reference, "AUTH_PENDING_STALE")
		stale.Sequence = events[0].Sequence - 1
		events = append(events, stale)
		e.logger.Warn("injecting out-of-order webhook", slog.String("reference", reference))
	}

	for _, ev := range events {
		e.deliver(ev)
	}
}

func (e *WebhookEmitter) build(reference, status string) Event {
	e.mu.Lock()
	e.sequence++
	seq := e.sequence
	e.mu.Unlock()

	return Event{
		ID:        "evt_" + uuid.NewString(),
		Type:      "charge.updated",
		Reference: reference,
		Status:    status,
		CreatedAt: time.Now().UTC(),
		Sequence:  seq,
	}
}

func (e *WebhookEmitter) deliver(ev Event) {
	e.mu.Lock()
	e.delivered = append(e.delivered, ev)
	e.mu.Unlock()

	if e.targetURL == "" {
		return
	}

	body, err := json.Marshal(ev)
	if err != nil {
		e.logger.Error("marshal webhook", slog.Any("error", err))
		return
	}

	// Bounded independently of any request context: delivery is the provider's
	// own follow-up, so it must outlive the API call that triggered it, but it
	// must not be able to hang a goroutine indefinitely either.
	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.targetURL, bytes.NewReader(body))
	if err != nil {
		e.logger.Error("build webhook request", slog.Any("error", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// Signed over a timestamp and the raw body. The timestamp is inside the
	// signed material so a captured request cannot be replayed indefinitely.
	ts := fmt.Sprintf("%d", time.Now().Unix())
	req.Header.Set("X-Sim-Timestamp", ts)
	req.Header.Set("X-Sim-Signature", sign(e.secret, ts, body))

	resp, err := e.client.Do(req)
	if err != nil {
		e.logger.Warn("webhook delivery failed", slog.Any("error", err), slog.String("event", ev.ID))
		return
	}
	defer func() { _ = resp.Body.Close() }()
}

// sign computes the HMAC the receiver will verify.
func sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte{'.'})
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Sign is exported so the webhook receiver in a later phase can be tested
// against exactly the scheme the emitter uses.
func Sign(secret, timestamp string, body []byte) string { return sign(secret, timestamp, body) }
