package simulator

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func (s *Server) handleGetFaults(w http.ResponseWriter, _ *http.Request) {
	cfg := s.engine.Config()

	// Faults absent from the map are reported as zero rather than omitted, so
	// the response doubles as the catalogue of what can be tuned.
	probabilities := make(map[string]float64, len(AllFaults))
	for _, f := range AllFaults {
		probabilities[string(f)] = cfg.Probabilities[f]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"seed":                 cfg.Seed,
		"probabilities":        probabilities,
		"outage_until":         cfg.OutageUntil,
		"in_outage":            s.engine.InOutage(time.Now()),
		"slow_response_min_ms": cfg.SlowResponseMinMs,
		"slow_response_max_ms": cfg.SlowResponseMaxMs,
		"webhook_delay_ms":     cfg.WebhookDelayMs,
	})
}

type setFaultsRequest struct {
	Seed              *int64             `json:"seed,omitempty"`
	Probabilities     map[string]float64 `json:"probabilities,omitempty"`
	SlowResponseMinMs *int               `json:"slow_response_min_ms,omitempty"`
	SlowResponseMaxMs *int               `json:"slow_response_max_ms,omitempty"`
	WebhookDelayMs    *int               `json:"webhook_delay_ms,omitempty"`
}

func (s *Server) handleSetFaults(w http.ResponseWriter, r *http.Request) {
	var req setFaultsRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	cfg := s.engine.Config()

	for name, p := range req.Probabilities {
		fault := Fault(name)
		if !knownFault(fault) {
			writeError(w, http.StatusBadRequest, "unknown_fault",
				fmt.Sprintf("%q is not a known fault", name))
			return
		}
		// Out-of-range probabilities are rejected rather than clamped: silently
		// turning 50 into 1.0 would make a mistyped config look like it worked.
		if p < 0 || p > 1 {
			writeError(w, http.StatusBadRequest, "invalid_probability",
				fmt.Sprintf("%s must be between 0 and 1, got %v", name, p))
			return
		}
		cfg.Probabilities[fault] = p
	}

	if req.Seed != nil {
		cfg.Seed = *req.Seed
	}
	if req.SlowResponseMinMs != nil {
		cfg.SlowResponseMinMs = *req.SlowResponseMinMs
	}
	if req.SlowResponseMaxMs != nil {
		cfg.SlowResponseMaxMs = *req.SlowResponseMaxMs
	}
	if req.WebhookDelayMs != nil {
		cfg.WebhookDelayMs = *req.WebhookDelayMs
	}

	s.engine.SetConfig(cfg)
	s.logger.WarnContext(r.Context(), "fault configuration changed",
		slog.Any("probabilities", cfg.Probabilities))

	s.handleGetFaults(w, r)
}

func (s *Server) handleSetPreset(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	seed := s.engine.Config().Seed
	if raw := r.URL.Query().Get("seed"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_seed", err.Error())
			return
		}
		seed = parsed
	}

	cfg, ok := Preset(name, seed)
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown_preset",
			fmt.Sprintf("%q is not a known preset; expected healthy, degraded, or chaos", name))
		return
	}

	s.engine.SetConfig(cfg)
	s.logger.WarnContext(r.Context(), "fault preset applied", slog.String("preset", name))
	s.handleGetFaults(w, r)
}

func (s *Server) handleOutage(w http.ResponseWriter, r *http.Request) {
	seconds := 30
	if raw := r.URL.Query().Get("seconds"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "invalid_seconds", "seconds must be a non-negative integer")
			return
		}
		seconds = parsed
	}

	until := s.engine.StartOutage(time.Duration(seconds) * time.Second)
	s.logger.WarnContext(r.Context(), "provider outage started",
		slog.Int("seconds", seconds), slog.Time("until", until))

	writeJSON(w, http.StatusOK, map[string]any{"outage_until": until})
}

// handleListCharges exposes what the provider believes it did.
//
// A real provider has a dashboard; this is that. Without it, "the customer was
// charged exactly once" is only checkable from inside a Go test, which means
// nobody watching a demo has any reason to believe it.
func (s *Server) handleListCharges(w http.ResponseWriter, _ *http.Request) {
	charges := s.store.List()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(charges),
		"charges": charges,
	})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.store.Reset()
	s.hooks.Reset()
	s.logger.WarnContext(r.Context(), "simulator state reset")
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

// handleComplete drives a pending or redirect-based authorization to its
// outcome, standing in for the customer finishing a challenge.
func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reference string `json:"reference"`
		Outcome   string `json:"outcome"`
	}
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	status := simAuthorized
	if req.Outcome == "failed" {
		status = simFailed
	}

	charge, ok := s.store.SetStatus(req.Reference, status)
	if !ok {
		writeError(w, http.StatusNotFound, "charge_not_found", "no charge with that reference")
		return
	}

	// Deliberately not carrying the request context: the webhook is the
	// provider's own follow-up and must survive this request returning.
	//nolint:contextcheck // delivery must outlive the triggering request
	s.hooks.Emit(charge.Reference, charge.IdempotencyKey, status, s.engine)

	writeJSON(w, http.StatusOK, chargeResponse{
		Reference:   charge.Reference,
		Status:      charge.Status,
		AmountMinor: charge.AmountMinor,
		Currency:    charge.Currency,
		RawStatus:   charge.Status,
	})
}

func knownFault(f Fault) bool {
	for _, known := range AllFaults {
		if known == f {
			return true
		}
	}
	return false
}
