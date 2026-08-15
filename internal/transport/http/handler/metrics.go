package handler

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/metrics"
)

// Prometheus serves the scrape endpoint.
//
// The Prometheus client only speaks net/http, and Hertz is not net/http, so the
// handler is run against a recorder and its result copied across. That is a
// little indirect, but the alternative is reimplementing the exposition format
// — which is a specification, not a detail, and getting it subtly wrong would
// produce a metrics endpoint that scrapes cleanly and reports nonsense.
func Prometheus(m *metrics.Metrics) app.HandlerFunc {
	promHandler := promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{
		// A failure to gather is worth surfacing rather than silently serving a
		// partial scrape that looks like a healthy system with fewer metrics.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})

	return func(ctx context.Context, c *app.RequestContext) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/metrics", nil)
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		recorder := httptest.NewRecorder()
		promHandler.ServeHTTP(recorder, req)

		for key, values := range recorder.Header() {
			for _, v := range values {
				c.Response.Header.Add(key, v)
			}
		}
		c.Data(recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.Bytes())
	}
}
