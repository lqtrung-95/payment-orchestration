package metrics_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
)

// exercised names every metric this package registers, alongside the call that
// writes it.
//
// The list is the point of the test. Adding a metric to the registry without
// adding it here fails TestEveryRegisteredMetricIsWritten, and the only way to
// add it here is to write the call that produces it. That closes the gap this
// package shipped with: seven instruments were registered and never written,
// and Prometheus reported them as a confident zero rather than as absent.
//
// A metric nothing writes is worse than a missing one. `psp_errors_total 0`
// reads as "the provider never failed"; a metric that is not there at all
// cannot be misread.
func exercised() (*metrics.Metrics, []string) {
	m := metrics.New()

	m.DoubleCharges.Set(0)
	m.LostPayments.Set(0)
	m.LedgerImbalance.Set(0)
	m.DLQDepth.Set(0)
	m.OutboxPending.Set(0)

	m.PaymentRequests.WithLabelValues("/v1/payments", "POST", "2xx").Inc()
	m.PaymentDuration.WithLabelValues("/v1/payments", "POST").Observe(0.01)
	m.ConsumerLag.WithLabelValues("payment.authorize").Set(0)

	m.ObserveCall("psp-sync-sim", "authorize", 25*time.Millisecond)
	m.ObserveError("psp-sync-sim", "declined", true)
	m.RecordRetry("payment.retry.5s", 1)
	m.SetBreakerState("psp-sync-sim", "open")
	m.RecordWebhook("psp-sim", "duplicate")

	return m, []string{
		"payment_consumer_lag",
		"payment_declines_total",
		"payment_dlq_depth",
		"payment_double_charges",
		"payment_ledger_imbalance",
		"payment_lost_payments",
		"payment_outbox_pending",
		"payment_request_duration_seconds",
		"payment_requests_total",
		"payment_retry_attempts_total",
		"psp_call_duration_seconds",
		"psp_circuit_breaker_state",
		"psp_errors_total",
		"webhooks_received_total",
	}
}

// Every instrument declared on Metrics must be written by a call in this
// package.
//
// Enumerated by reflection over the struct rather than by gathering the
// registry, because gathering cannot see the failure. An unwritten CounterVec
// or GaugeVec has no children, so it produces no metric family at all — it is
// absent from a scrape rather than present and zero, and a test that walks the
// gathered families therefore skips exactly the metrics it is looking for.
// The first version of this test did that and passed against a deliberately
// unwired counter.
//
// Reflection over the fields is the only view that sees a declared instrument
// nothing has touched.
func TestEveryDeclaredInstrumentIsWritten(t *testing.T) {
	m, _ := exercised()

	value := reflect.ValueOf(m).Elem()
	structType := value.Type()

	found := 0
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}

		collector, ok := value.Field(i).Interface().(prometheus.Collector)
		if !ok {
			continue
		}
		found++

		if samples := collect(collector); samples == 0 {
			t.Errorf("Metrics.%s is declared but nothing in this package writes it; "+
				"wire it up or remove it — a metric that always reads zero is a lie, "+
				"and an unwritten vec does not even appear in a scrape", field.Name)
		}
	}

	if found == 0 {
		t.Fatal("no collectors found on Metrics; the reflection walk is broken")
	}
}

// The exercise list and the registry must agree, so a metric cannot be renamed
// out from under the callers that write it.
func TestExercisedNamesMatchTheRegistry(t *testing.T) {
	m, expected := exercised()

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	gathered := make([]string, 0, len(families))
	for _, f := range families {
		if isRuntimeCollector(f.GetName()) {
			continue
		}
		gathered = append(gathered, f.GetName())
	}

	sort.Strings(gathered)
	sort.Strings(expected)

	for _, name := range difference(expected, gathered) {
		t.Errorf("%s is expected but was not gathered — was it renamed or removed?", name)
	}
	for _, name := range difference(gathered, expected) {
		t.Errorf("%s was gathered but is not in the exercise list", name)
	}
}

// collect counts the samples a collector currently produces.
func collect(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()

	n := 0
	for range ch {
		n++
	}
	return n
}

// The breaker's state is the gauge's value, not a label. Encoded wrongly it
// would leave the previous state's series behind at 1, so a breaker that had
// ever opened would look permanently open.
func TestBreakerStateIsEncodedAsAValue(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  float64
	}{
		{"closed", 0},
		{"half_open", 1},
		{"open", 2},
		{"something-new", 0},
	} {
		m := metrics.New()
		m.SetBreakerState("psp-sync-sim", tc.state)

		got, err := gaugeValue(m, "psp_circuit_breaker_state")
		if err != nil {
			t.Fatalf("%s: %v", tc.state, err)
		}
		if got != tc.want {
			t.Errorf("state %q reported as %v, want %v", tc.state, got, tc.want)
		}
	}
}

// A declined call counts against both the operational series and the business
// one; a timeout counts only against the operational one. Summing them would
// make a bad decline rate look like a provider outage.
func TestOnlyDefinitiveRefusalsCountAsDeclines(t *testing.T) {
	m := metrics.New()
	m.ObserveError("psp-sync-sim", "timeout", false)
	m.ObserveError("psp-sync-sim", "declined", true)

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for _, f := range families {
		switch f.GetName() {
		case "psp_errors_total", "payment_declines_total":
			counts[f.GetName()] = len(f.GetMetric())
		}
	}

	if counts["psp_errors_total"] != 2 {
		t.Errorf("psp_errors_total has %d series, want 2", counts["psp_errors_total"])
	}
	if counts["payment_declines_total"] != 1 {
		t.Errorf("payment_declines_total has %d series, want 1 — a timeout was counted as a decline",
			counts["payment_declines_total"])
	}
}

// isRuntimeCollector reports whether a family comes from the Go runtime or
// process collectors rather than from this package. Those are filled in by the
// runtime at scrape time, so "nothing writes it" does not apply to them.
func isRuntimeCollector(name string) bool {
	return strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_")
}

func gaugeValue(m *metrics.Metrics, name string) (float64, error) {
	families, err := m.Registry().Gather()
	if err != nil {
		return 0, err
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			return metric.GetGauge().GetValue(), nil
		}
	}
	return 0, nil
}

// difference returns the elements of a that are absent from b.
func difference(a, b []string) []string {
	inB := make(map[string]bool, len(b))
	for _, s := range b {
		inB[s] = true
	}

	var out []string
	for _, s := range a {
		if !inB[s] {
			out = append(out, s)
		}
	}
	return out
}
