// Package metrics defines what this system reports about itself.
//
// The set is deliberately payment-specific rather than generic. Request rate
// and p99 latency describe any web service; they say nothing about whether a
// customer was charged twice. The metrics that matter here are the ones whose
// correct value is zero — a counter that must never move is a far better alarm
// than a latency percentile, because there is no threshold to argue about.
//
// Cardinality is bounded on purpose. Nothing is labelled by transaction id,
// merchant, or provider reference: those are unbounded, they make a metrics
// backend expensive, and they leak which identifiers exist to anyone who can
// read the endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the registered instrument set.
type Metrics struct {
	registry *prometheus.Registry

	// --- The must-be-zero family -------------------------------------------
	//
	// Each of these counts a state the system claims cannot happen. They are
	// exported even while zero, because a metric that only appears once it is
	// non-zero cannot be alerted on — there is nothing to compare against.

	// DoubleCharges counts transactions the books show as captured more than
	// once. The headline number: the whole design exists to keep it at zero.
	DoubleCharges prometheus.Gauge

	// LostPayments counts transactions in a captured state with no ledger
	// entry behind them — money taken and never accounted for.
	LostPayments prometheus.Gauge

	// LedgerImbalance counts journal entries whose postings do not net to zero
	// within a currency. A deferred constraint trigger makes this impossible;
	// the metric exists to prove the trigger is actually doing its job.
	LedgerImbalance prometheus.Gauge

	// --- Traffic and latency -----------------------------------------------

	PaymentRequests  *prometheus.CounterVec
	PaymentDuration  *prometheus.HistogramVec
	PSPErrors        *prometheus.CounterVec
	PSPCallDuration  *prometheus.HistogramVec
	DeclinedPayments *prometheus.CounterVec

	// --- Asynchronous pipeline ---------------------------------------------

	RetryAttempts    *prometheus.CounterVec
	DLQDepth         prometheus.Gauge
	OutboxPending    prometheus.Gauge
	ConsumerLag      *prometheus.GaugeVec
	CircuitBreaker   *prometheus.GaugeVec
	WebhooksReceived *prometheus.CounterVec

	// --- Reconciliation ----------------------------------------------------

	ReconBreaks *prometheus.CounterVec
}

// New registers the instrument set against its own registry.
//
// A dedicated registry rather than the global default: the default is written
// to by any imported package that feels like it, which makes the exported set
// depend on the import graph rather than on a decision.
func New() *Metrics {
	r := prometheus.NewRegistry()

	// Go runtime and process collectors, for the soak test's leak hunt.
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: r,

		DoubleCharges: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "payment_double_charges",
			Help: "Transactions the ledger shows as captured more than once. Must be 0.",
		}),
		LostPayments: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "payment_lost_payments",
			Help: "Captured transactions with no ledger entry. Must be 0.",
		}),
		LedgerImbalance: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "payment_ledger_imbalance",
			Help: "Journal entries whose postings do not net to zero per currency. Must be 0.",
		}),

		PaymentRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_requests_total",
			Help: "Payment API requests by route, method, and response status class.",
		}, []string{"route", "method", "status"}),

		PaymentDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "payment_request_duration_seconds",
			Help: "Payment API request latency.",
			// Buckets chosen around this system's own shape: creation returns in
			// tens of milliseconds because the provider is off the request path,
			// so the interesting resolution is below 250ms.
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"route", "method"}),

		PSPErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "psp_errors_total",
			Help: "Provider failures by normalized error class — the taxonomy in action.",
		}, []string{"provider", "class"}),

		PSPCallDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "psp_call_duration_seconds",
			Help:    "Time spent in provider calls, by operation.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"provider", "operation"}),

		DeclinedPayments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_declines_total",
			Help: "Declines by issuer reason. Domain-specific: a rising decline rate is a business signal, not an error rate.",
		}, []string{"class"}),

		RetryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_retry_attempts_total",
			Help: "Messages scheduled onto a retry tier, by tier and attempt number.",
		}, []string{"tier", "attempt"}),

		DLQDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "payment_dlq_depth",
			Help: "Messages parked in the dead letter queue awaiting a human.",
		}),

		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "payment_outbox_pending",
			Help: "Outbox rows not yet relayed to the broker. Sustained growth means the relay is behind.",
		}),

		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "payment_consumer_lag",
			Help: "Uncommitted messages per topic. A message stuck on a retry tier is invisible without this.",
		}, []string{"topic"}),

		CircuitBreaker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "psp_circuit_breaker_state",
			Help: "Breaker state per provider: 0 closed, 1 half-open, 2 open.",
		}, []string{"provider"}),

		WebhooksReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "webhooks_received_total",
			Help: "Provider callbacks by outcome — duplicates prove deduplication is firing.",
		}, []string{"provider", "outcome"}),

		ReconBreaks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_breaks_total",
			Help: "Reconciliation breaks raised, by category.",
		}, []string{"category"}),
	}

	r.MustRegister(
		m.DoubleCharges, m.LostPayments, m.LedgerImbalance,
		m.PaymentRequests, m.PaymentDuration,
		m.PSPErrors, m.PSPCallDuration, m.DeclinedPayments,
		m.RetryAttempts, m.DLQDepth, m.OutboxPending, m.ConsumerLag,
		m.CircuitBreaker, m.WebhooksReceived, m.ReconBreaks,
	)

	return m
}

// Registry exposes the registry for the scrape handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }
