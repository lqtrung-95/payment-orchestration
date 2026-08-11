package simulator

import (
	"log/slog"
	"net/http"
	"time"
)

func (s *Server) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	key := idempotencyKeyFrom(r)
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}

	var req chargeRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if s.applyPreFaults(w, r, key) {
		return
	}

	// Declines are triggered by the amount, the way real provider sandboxes use
	// magic values. Nothing is recorded: the issuer refused, so no charge
	// exists and there is nothing to recover.
	if code, declined := declineCodeFor(req.AmountMinor); declined {
		s.logger.InfoContext(r.Context(), "declining by magic amount",
			slog.String("key", key), slog.String("code", code))
		writeError(w, http.StatusUnprocessableEntity, code, "the issuer declined this charge")
		return
	}

	mode := modeFrom(r)
	status := simAuthorized
	switch mode {
	case ModeAsync:
		status = simPending
	case ModeRedirect:
		status = simRequiresAction
	}

	// The charge is recorded before any post-success fault fires. That ordering
	// is the entire point: the money moves, and only then does the caller's
	// view of it break.
	charge, created := s.store.GetOrCreate(key, Charge{
		TransactionID: req.TransactionID,
		Operation:     "authorize",
		AmountMinor:   req.AmountMinor,
		Currency:      req.Currency,
		Status:        status,
		Mode:          mode,
	})

	resp := chargeResponse{
		Reference:   charge.Reference,
		Status:      charge.Status,
		AmountMinor: charge.AmountMinor,
		Currency:    charge.Currency,
		RawStatus:   charge.Status,
	}
	if charge.Status == simRequiresAction {
		resp.RedirectURL = "/v1/simulate/challenge?reference=" + charge.Reference
	}

	// An async charge resolves out of band. A webhook is the only way the caller
	// ever learns the outcome, which is what parks the transaction in a
	// non-terminal state until it lands. The store is updated first, so that a
	// caller polling GetStatus and a caller waiting on the webhook cannot
	// disagree about what happened.
	if created && mode == ModeAsync {
		reference := charge.Reference
		//nolint:contextcheck // resolution must outlive the triggering request
		s.hooks.Background(func() {
			time.Sleep(s.engine.WebhookDelay())
			s.store.SetStatus(reference, simAuthorized)
			s.hooks.Emit(reference, key, simAuthorized, s.engine)
		})
	}

	if s.applyPostSuccessFaults(w, r, key, created) {
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCapture(w http.ResponseWriter, r *http.Request) {
	s.handleFollowOn(w, r, "capture", simCaptured)
}

func (s *Server) handleRefund(w http.ResponseWriter, r *http.Request) {
	s.handleFollowOn(w, r, "refund", simRefunded)
}

func (s *Server) handleVoid(w http.ResponseWriter, r *http.Request) {
	s.handleFollowOn(w, r, "void", simVoided)
}

// handleFollowOn serves the operations that act on an existing authorization.
func (s *Server) handleFollowOn(w http.ResponseWriter, r *http.Request, operation, successStatus string) {
	key := idempotencyKeyFrom(r)
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}

	var req chargeRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if s.applyPreFaults(w, r, key) {
		return
	}

	if req.Reference == "" {
		writeError(w, http.StatusBadRequest, "reference_required", "reference is required")
		return
	}
	original, ok := s.store.GetByReference(req.Reference)
	if !ok {
		writeError(w, http.StatusNotFound, "charge_not_found", "no charge with that reference")
		return
	}

	amount := req.AmountMinor

	// A provider that acts on a different amount than requested does not report
	// an error — the discrepancy only surfaces when the settlement file is
	// compared against the ledger, which is exactly what makes it worth
	// simulating.
	if operation == "capture" && s.engine.Fires(FaultPartialCaptureDrift, key) && amount > 1 {
		amount--
		s.logger.Warn("injecting partial capture drift",
			slog.String("key", key), slog.Int64("requested", req.AmountMinor), slog.Int64("captured", amount))
	}

	charge, created := s.store.GetOrCreate(key, Charge{
		TransactionID: original.TransactionID,
		Operation:     operation,
		AmountMinor:   amount,
		Currency:      original.Currency,
		Status:        successStatus,
		Mode:          original.Mode,
	})

	if s.applyPostSuccessFaults(w, r, key, created) {
		return
	}

	writeJSON(w, http.StatusOK, chargeResponse{
		Reference:   charge.Reference,
		Status:      charge.Status,
		AmountMinor: charge.AmountMinor,
		Currency:    charge.Currency,
		RawStatus:   charge.Status,
	})
}

// handleStatus is the recovery path. It must be safe to call at any time, and
// it is the only thing that can tell a caller whether an ambiguous failure left
// money moved.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("idempotency_key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "idempotency_key query parameter is required")
		return
	}

	// Outage applies, because a provider that is down cannot answer questions
	// about what it did. Post-success faults do not: corrupting the recovery
	// path itself would leave no way out of an ambiguous state.
	if s.engine.InOutage(time.Now()) {
		writeError(w, http.StatusServiceUnavailable, "provider_unavailable", "provider is in an outage window")
		return
	}

	charge, ok := s.store.Get(key)
	if !ok {
		writeJSON(w, http.StatusOK, chargeResponse{Status: "NOT_FOUND", RawStatus: "NOT_FOUND"})
		return
	}

	// A provider's status endpoint is often served from a replica that lags the
	// write path. Recovery logic must tolerate being told "no such charge" by a
	// provider that does in fact hold one, and must not read that as licence to
	// retry.
	if s.engine.Fires(FaultStaleStatus, key) && time.Since(charge.CreatedAt) < 2*time.Second {
		s.logger.Warn("serving stale status", slog.String("key", key))
		writeJSON(w, http.StatusOK, chargeResponse{Status: "NOT_FOUND", RawStatus: "NOT_FOUND_STALE"})
		return
	}

	writeJSON(w, http.StatusOK, chargeResponse{
		Reference:   charge.Reference,
		Status:      charge.Status,
		AmountMinor: charge.AmountMinor,
		Currency:    charge.Currency,
		RawStatus:   charge.Status,
	})
}
