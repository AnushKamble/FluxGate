package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/crdt"
	"fluxgate/internal/policy"
	"fluxgate/internal/telemetry"
)

// Server encapsulates the HTTP router and services for a FluxGate Node.
type Server struct {
	nodeID     string
	engine     *policy.Engine
	clock      clock.Clock
	logger     *telemetry.Logger
	metrics    *telemetry.MetricsRegistry
	telemetry  *crdt.TelemetryStore
	mux        *http.ServeMux
	handler    http.Handler
	isReady    bool
	isDegraded bool
}

func NewServer(nodeID string, engine *policy.Engine, clk clock.Clock, logger *telemetry.Logger, metrics *telemetry.MetricsRegistry) *Server {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	s := &Server{
		nodeID:  nodeID,
		engine:  engine,
		clock:   clk,
		logger:  logger,
		metrics: metrics,
		mux:     http.NewServeMux(),
		isReady: true,
	}

	s.routes()
	s.handler = LoggingAndMetricsMiddleware(logger, metrics, s.mux)
	return s
}

func (s *Server) SetTelemetryStore(t *crdt.TelemetryStore) {
	s.telemetry = t
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) SetReady(ready bool) {
	s.isReady = ready
}

func (s *Server) SetDegraded(degraded bool) {
	s.isDegraded = degraded
	s.metrics.SetDegradedMode(degraded)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /api/v1/check", s.handleCheck)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /debug/state", s.handleDebugState)
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	w.Header().Set("Content-Type", "application/json")

	var req CheckRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "invalid_request",
			Message: "Malformed or unrecognized JSON body: " + err.Error(),
		})
		return
	}

	if err := req.Validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "validation_error",
			Message: err.Error(),
		})
		return
	}

	now := s.clock.Now()
	key := policy.MatchKey{
		ClientID: req.ClientID,
		Route:    req.Route,
		Method:   req.Method,
	}

	decision, pol, err := s.engine.Evaluate(now, key, req.Cost)
	evalDurationUs := uint64(time.Since(t0).Microseconds())
	s.metrics.ObserveDecisionDuration(evalDurationUs)

	if err != nil {
		if errors.Is(err, policy.ErrPolicyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(ErrorResponse{
				Error:   "policy_not_found",
				Message: "No matching policy configured for client/route",
			})
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "eval_error",
			Message: err.Error(),
		})
		return
	}

	ApplyRateLimitHeaders(w, decision)

	mode := "normal"
	if s.isDegraded {
		mode = "degraded"
	}

	if decision.Allowed {
		s.metrics.IncDecision(true, pol.ID, "allowed")
		if s.telemetry != nil {
			s.telemetry.IncAllowed(1)
			s.telemetry.IncConsumed(req.Cost)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CheckResponse{
			Allowed:   true,
			ClientID:  req.ClientID,
			Remaining: decision.Remaining,
			Limit:     decision.Limit,
			NodeID:    s.nodeID,
			Mode:      mode,
		})
		return
	}

	// Rate Limited -> HTTP 429
	retrySec := int64(decision.RetryAfter.Round(time.Second).Seconds())
	if retrySec < 1 {
		retrySec = 1
	}

	s.metrics.IncDecision(false, pol.ID, "rate_limited")
	if s.telemetry != nil {
		s.telemetry.IncDenied(1)
	}
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(CheckResponse{
		Allowed:           false,
		Reason:            "rate_limited",
		ClientID:          req.ClientID,
		Remaining:         decision.Remaining,
		Limit:             decision.Limit,
		RetryAfterSeconds: retrySec,
		NodeID:            s.nodeID,
		Mode:              mode,
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"node_id":   s.nodeID,
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !s.isReady {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "not_ready",
			"node_id": s.nodeID,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ready",
		"node_id": s.nodeID,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.metrics.WritePrometheus(w)
}

func (s *Server) handleDebugState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":     s.nodeID,
		"is_ready":    s.isReady,
		"is_degraded": s.isDegraded,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	})
}
