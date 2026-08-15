package middleware

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
)

// Metrics records request rate and latency.
//
// Labelled by the route *pattern* rather than the path. Using the path would
// put every transaction id into a label, which turns an unbounded set loose on
// the metrics backend and publishes which identifiers exist to anyone who can
// read the endpoint.
//
// Status is recorded as a class — 2xx, 4xx, 5xx — for the same reason: the
// difference between 401 and 403 is not worth a separate time series here, and
// the class is what an alert actually keys on.
func Metrics(m *metrics.Metrics) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		started := time.Now()
		c.Next(ctx)

		route := c.FullPath()
		if route == "" {
			// No matched route: a 404. Recorded under a constant so unmatched
			// traffic is visible without every probed URL becoming a label.
			route = "unmatched"
		}
		method := string(c.Request.Method())

		m.PaymentDuration.WithLabelValues(route, method).Observe(time.Since(started).Seconds())
		m.PaymentRequests.WithLabelValues(route, method, statusClass(c.Response.StatusCode())).Inc()
	}
}

// statusClass reduces a status code to its family.
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return strconv.Itoa(code)
	}
}
