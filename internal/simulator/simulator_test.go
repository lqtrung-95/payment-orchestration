package simulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/simulator"
)

func newHarness(t *testing.T, cfg simulator.Config) (*httptest.Server, *simulator.Store, *simulator.Engine, *simulator.WebhookEmitter) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := simulator.NewStore()
	engine := simulator.NewEngine(cfg)
	hooks := simulator.NewWebhookEmitter("", "test-secret", logger)

	srv := httptest.NewServer(simulator.NewServer(store, engine, hooks, logger))
	t.Cleanup(srv.Close)

	return srv, store, engine, hooks
}

func authorize(t *testing.T, client *http.Client, base, key, mode string, amount int64) (*http.Response, []byte) {
	t.Helper()

	body, _ := json.Marshal(map[string]any{
		"transaction_id": "tx-" + key,
		"amount_minor":   amount,
		"currency":       "USD",
	})
	req, err := http.NewRequest(http.MethodPost, base+"/v1/charges/authorize", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Sim-Mode", mode)

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)
	return resp, raw
}

// The whole recovery story depends on this: the charge is recorded before the
// caller's view of it is destroyed. If the simulator only recorded on a
// successful reply, timeout-after-success would be indistinguishable from a
// clean failure and there would be nothing to recover.
func TestTimeoutAfterSuccessStillRecordsTheCharge(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	cfg.Probabilities[simulator.FaultTimeoutAfterSuccess] = 1.0

	srv, store, _, _ := newHarness(t, cfg)
	client := &http.Client{Timeout: 250 * time.Millisecond}

	resp, _ := authorize(t, client, srv.URL, "key-timeout", simulator.ModeSync, 5000)
	if resp != nil {
		t.Fatalf("expected the client to time out, got status %d", resp.StatusCode)
	}

	if got := store.Count(); got != 1 {
		t.Fatalf("charge count = %d, want 1 — the charge must exist despite the hang", got)
	}

	charge, ok := store.Get("key-timeout")
	if !ok {
		t.Fatal("charge not retrievable by idempotency key, so recovery would be impossible")
	}
	if charge.Status != "AUTH_OK" {
		t.Errorf("charge status = %q, want AUTH_OK", charge.Status)
	}
}

// A retry after an ambiguous failure must not create a second charge. This is
// the provider-side half of the double-charge guarantee.
func TestRetryWithSameKeyDoesNotCreateSecondCharge(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	srv, store, _, _ := newHarness(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}

	var references []string
	for i := 0; i < 5; i++ {
		resp, raw := authorize(t, client, srv.URL, "key-repeat", simulator.ModeSync, 5000)
		if resp == nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("attempt %d failed", i)
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		references = append(references, body["reference"].(string))
	}

	if got := store.Count(); got != 1 {
		t.Errorf("charge count = %d after 5 attempts with one key, want 1", got)
	}
	for i, ref := range references {
		if ref != references[0] {
			t.Errorf("attempt %d returned reference %q, want %q — replays must be identical",
				i, ref, references[0])
		}
	}
}

// Reproducibility is what makes a chaos run evidence rather than an anecdote.
// The same seed and the same request must always produce the same verdict,
// regardless of how many other requests are in flight.
func TestFaultVerdictsAreDeterministicUnderConcurrency(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetChaos, 424242)
	engine := simulator.NewEngine(cfg)

	keys := make([]string, 200)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String()
	}

	first := make([]bool, len(keys))
	for i, k := range keys {
		first[i] = engine.Fires(simulator.FaultTimeoutAfterSuccess, k)
	}

	// Same engine, keys visited in reverse: a shared sequential generator would
	// hand out different draws here.
	for i := len(keys) - 1; i >= 0; i-- {
		if got := engine.Fires(simulator.FaultTimeoutAfterSuccess, keys[i]); got != first[i] {
			t.Fatalf("verdict for %q changed with call order: %t then %t", keys[i], first[i], got)
		}
	}

	// A fresh engine with the same seed must agree entirely.
	replay := simulator.NewEngine(cfg)
	for i, k := range keys {
		if got := replay.Fires(simulator.FaultTimeoutAfterSuccess, k); got != first[i] {
			t.Fatalf("replay with the same seed disagreed for %q: %t vs %t", k, first[i], got)
		}
	}

	// Sanity: a chaos preset must actually fire sometimes, or the test proves
	// only that "never" is reproducible.
	fired := 0
	for _, v := range first {
		if v {
			fired++
		}
	}
	if fired == 0 {
		t.Fatal("no faults fired under the chaos preset")
	}
}

// A different seed must produce a different pattern, otherwise the seed is
// decorative and reruns cannot explore anything new.
func TestDifferentSeedsProduceDifferentVerdicts(t *testing.T) {
	cfgA, _ := simulator.Preset(simulator.PresetChaos, 1)
	cfgB, _ := simulator.Preset(simulator.PresetChaos, 2)
	a, b := simulator.NewEngine(cfgA), simulator.NewEngine(cfgB)

	differences := 0
	for i := 0; i < 200; i++ {
		key := "key-" + time.Duration(i).String()
		if a.Fires(simulator.FaultTimeoutAfterSuccess, key) != b.Fires(simulator.FaultTimeoutAfterSuccess, key) {
			differences++
		}
	}
	if differences == 0 {
		t.Error("two seeds produced identical verdicts across 200 keys")
	}
}

func TestOutageRejectsEveryRequest(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	srv, store, engine, _ := newHarness(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}

	engine.StartOutage(2 * time.Second)

	resp, _ := authorize(t, client, srv.URL, "key-outage", simulator.ModeSync, 5000)
	if resp == nil {
		t.Fatal("expected a response during an outage")
	}
	// 503 rather than a hang or a 500: the provider is explicitly refusing, so
	// the caller can classify this as retryable rather than ambiguous.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	if got := store.Count(); got != 0 {
		t.Errorf("charge count = %d during an outage, want 0 — nothing should have happened", got)
	}
}

// The three provider shapes must genuinely differ, or the abstraction over them
// is proving nothing.
func TestProviderModesHaveDistinctSemantics(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	cfg.WebhookDelayMs = 10
	srv, _, _, hooks := newHarness(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}

	tests := []struct {
		mode         string
		wantStatus   string
		wantRedirect bool
	}{
		{simulator.ModeSync, "AUTH_OK", false},
		{simulator.ModeAsync, "PENDING_ASYNC", false},
		{simulator.ModeRedirect, "ACTION_REQUIRED", true},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			resp, raw := authorize(t, client, srv.URL, "key-"+tt.mode, tt.mode, 5000)
			if resp == nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("authorize failed for mode %s", tt.mode)
			}

			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body["status"] != tt.wantStatus {
				t.Errorf("status = %v, want %v", body["status"], tt.wantStatus)
			}
			if hasRedirect := body["redirect_url"] != nil; hasRedirect != tt.wantRedirect {
				t.Errorf("redirect present = %t, want %t", hasRedirect, tt.wantRedirect)
			}
		})
	}

	// The async charge resolves out of band, which is the whole point of that
	// shape: the API response was never the answer.
	hooks.Wait()
	if len(hooks.Delivered()) == 0 {
		t.Error("async mode delivered no webhook, so the outcome would never arrive")
	}
}

func TestDuplicateWebhookFaultDeliversTheSameEventMoreThanOnce(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	cfg.Probabilities[simulator.FaultDuplicateWebhook] = 1.0
	cfg.WebhookDelayMs = 5

	srv, _, _, hooks := newHarness(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}

	authorize(t, client, srv.URL, "key-dup", simulator.ModeAsync, 5000)
	hooks.Wait()

	delivered := hooks.Delivered()
	if len(delivered) < 2 {
		t.Fatalf("delivered %d events, want more than one", len(delivered))
	}

	// Duplicates carry the same event id — which is exactly what the receiver
	// will deduplicate on.
	seen := map[string]int{}
	for _, ev := range delivered {
		seen[ev.ID]++
	}
	repeated := false
	for _, n := range seen {
		if n > 1 {
			repeated = true
		}
	}
	if !repeated {
		t.Error("no event id was delivered more than once")
	}
}

func TestOutOfOrderWebhookFaultDeliversAStaleEventLast(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 1)
	cfg.Probabilities[simulator.FaultOutOfOrderWebhook] = 1.0
	cfg.WebhookDelayMs = 5

	srv, _, _, hooks := newHarness(t, cfg)
	client := &http.Client{Timeout: 2 * time.Second}

	authorize(t, client, srv.URL, "key-order", simulator.ModeAsync, 5000)
	hooks.Wait()

	delivered := hooks.Delivered()
	if len(delivered) < 2 {
		t.Fatalf("delivered %d events, want at least 2", len(delivered))
	}

	// Arrival order does not reflect causality: the last event to arrive carries
	// a lower sequence than one already delivered. A receiver that trusts
	// arrival order will apply a stale state.
	last := delivered[len(delivered)-1]
	var maxEarlier int64
	for _, ev := range delivered[:len(delivered)-1] {
		if ev.Sequence > maxEarlier {
			maxEarlier = ev.Sequence
		}
	}
	if last.Sequence >= maxEarlier {
		t.Errorf("last delivered sequence %d is not lower than an earlier %d", last.Sequence, maxEarlier)
	}
}

// The admin charge list is what makes "the customer was charged exactly once"
// checkable from outside this process. A claim that can only be verified by an
// in-process test is one nobody watching a demo has any reason to believe.
func TestAdminChargesReportsWhatTheProviderDid(t *testing.T) {
	cfg, _ := simulator.Preset(simulator.PresetHealthy, 7)
	srv, _, _, _ := newHarness(t, cfg)
	client := srv.Client()

	count := func() int {
		resp, err := client.Get(srv.URL + "/admin/charges")
		if err != nil {
			t.Fatalf("get charges: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		var body struct {
			Count   int                `json:"count"`
			Charges []simulator.Charge `json:"charges"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode charges: %v", err)
		}
		if body.Count != len(body.Charges) {
			t.Fatalf("count %d disagrees with %d listed charges", body.Count, len(body.Charges))
		}
		return body.Count
	}

	if got := count(); got != 0 {
		t.Fatalf("charges before any request = %d, want 0", got)
	}

	authorize(t, client, srv.URL, "admin-key-1", simulator.ModeSync, 5000)
	if got := count(); got != 1 {
		t.Errorf("charges after one authorize = %d, want 1", got)
	}

	// The same key again is a replay, not a second charge — which is the exact
	// property the demo asserts against this endpoint.
	authorize(t, client, srv.URL, "admin-key-1", simulator.ModeSync, 5000)
	if got := count(); got != 1 {
		t.Errorf("charges after a replayed key = %d, want 1", got)
	}

	authorize(t, client, srv.URL, "admin-key-2", simulator.ModeSync, 5000)
	if got := count(); got != 2 {
		t.Errorf("charges after a second key = %d, want 2", got)
	}
}
