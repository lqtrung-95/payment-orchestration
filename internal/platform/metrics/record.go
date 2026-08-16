package metrics

import (
	"strconv"
	"time"
)

// Recording helpers.
//
// These exist so the label decisions live in one place rather than at each call
// site. A label set chosen inline is a label set that drifts: two callers pick
// slightly different values for the same dimension, and the resulting series
// cannot be summed.
//
// They also let the packages that do the work depend on a two-method interface
// rather than on Prometheus. `internal/psp` should not know what a histogram is.

// ObserveCall records the duration of a provider call.
//
// Operation, not endpoint: "authorize" and "capture" are the units anyone
// reasons about, and they are a closed set. A URL path would grow a series per
// provider reference.
func (m *Metrics) ObserveCall(provider, operation string, d time.Duration) {
	m.PSPCallDuration.WithLabelValues(provider, operation).Observe(d.Seconds())
}

// ObserveError records a provider failure by its normalized class, and counts
// it separately as a decline when the provider gave a definitive refusal.
//
// The two are deliberately not the same series. `psp_errors_total` is an
// operational signal — it rises when a provider is unwell. Declines are a
// business signal that rises when customers are being refused, which is not a
// fault and must not page anyone. Summing them would make a bad marketing
// campaign look like an outage.
func (m *Metrics) ObserveError(provider, class string, declined bool) {
	m.PSPErrors.WithLabelValues(provider, class).Inc()
	if declined {
		m.DeclinedPayments.WithLabelValues(class).Inc()
	}
}

// RecordRetry counts a message scheduled onto a retry tier.
//
// Attempt is a label because the distribution across attempts is the useful
// signal: load concentrated on the first rung is a system absorbing transient
// blips, while load on the last rung is a system about to start dead-lettering.
// It is bounded by the ladder's length, so the cardinality is fixed.
func (m *Metrics) RecordRetry(tier string, attempt int) {
	m.RetryAttempts.WithLabelValues(tier, strconv.Itoa(attempt)).Inc()
}

// SetBreakerState publishes a breaker's position as a number, because a gauge
// cannot hold a string: 0 closed, 1 half-open, 2 open.
//
// Ordered by severity on purpose, so `max_over_time` answers "was this provider
// ever cut off during the window" without needing to know the encoding.
func (m *Metrics) SetBreakerState(provider string, state string) {
	var value float64
	switch state {
	case "half_open":
		value = 1
	case "open":
		value = 2
	}
	// The state is the value, not a label. As a label it would leave the
	// previous state's series behind at 1 forever, so a breaker that had ever
	// opened would look permanently open.
	m.CircuitBreaker.WithLabelValues(provider).Set(value)
}

// RecordWebhook counts a provider callback by what happened to it.
//
// The outcome label is the point. A rising `duplicate` count is deduplication
// working, not a fault, and it is the only external evidence that the unique
// index is doing anything at all.
func (m *Metrics) RecordWebhook(provider, outcome string) {
	m.WebhooksReceived.WithLabelValues(provider, outcome).Inc()
}
