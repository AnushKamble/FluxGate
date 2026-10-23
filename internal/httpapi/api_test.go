package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/policy"
	"fluxgate/internal/telemetry"
)

func setupTestServer(t *testing.T) (*Server, *clock.FakeClock) {
	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := telemetry.NewLogger(nil, telemetry.LevelError, 1, nil)
	metrics := telemetry.NewMetricsRegistry()

	policies := []config.PolicyDefinition{
		{
			ID:          "policy-test",
			Version:     1,
			ClientID:    "client1",
			Route:       "/api/pay",
			Method:      "POST",
			Capacity:    2,
			RefillRate:  1,
			DefaultCost: 1,
			Enabled:     true,
		},
		{
			ID:          "policy-default",
			Version:     1,
			ClientID:    "*",
			Route:       "*",
			Method:      "*",
			Capacity:    10,
			RefillRate:  2,
			DefaultCost: 1,
			Enabled:     true,
		},
	}

	engine, err := policy.NewEngine(policies, fc, false, 1000)
	if err != nil {
		t.Fatalf("failed to create policy engine: %v", err)
	}

	srv := NewServer("node-test-1", engine, fc, logger, metrics)
	return srv, fc
}

func TestCheckAllowAndHeaders(t *testing.T) {
	srv, _ := setupTestServer(t)

	reqBody := `{"client_id":"client1","route":"/api/pay","method":"POST","cost":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify headers
	if rec.Header().Get(HeaderRateLimitLimit) != "2" {
		t.Errorf("expected limit header 2, got %s", rec.Header().Get(HeaderRateLimitLimit))
	}
	if rec.Header().Get(HeaderRateLimitRemaining) != "1" {
		t.Errorf("expected remaining header 1, got %s", rec.Header().Get(HeaderRateLimitRemaining))
	}

	var resp CheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}
	if !resp.Allowed || resp.Remaining != 1 || resp.NodeID != "node-test-1" {
		t.Errorf("unexpected response body: %+v", resp)
	}
}

func TestCheckDenyAndRetryAfter(t *testing.T) {
	srv, _ := setupTestServer(t)

	reqBody := `{"client_id":"client1","route":"/api/pay","method":"POST","cost":1}`

	// Consume 2 allowed
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(reqBody))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected allowed on request %d, got %d", i, rec.Code)
		}
	}

	// 3rd request must be denied with 429
	req := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(reqBody))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d: %s", rec.Code, rec.Body.String())
	}

	if rec.Header().Get(HeaderRetryAfter) == "" {
		t.Errorf("expected Retry-After header to be present")
	}

	var resp CheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected allowed=false")
	}
	if resp.RetryAfterSeconds <= 0 {
		t.Errorf("expected positive retry_after_seconds, got %d", resp.RetryAfterSeconds)
	}
}

func TestValidationErrors(t *testing.T) {
	srv, _ := setupTestServer(t)

	// 1. Empty client ID
	req := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(`{"client_id":""}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty client_id, got %d", rec.Code)
	}

	// 2. Overlong client ID
	longID := strings.Repeat("x", 200)
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(`{"client_id":"`+longID+`"}`))
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for long client_id, got %d", rec2.Code)
	}

	// 3. Malformed JSON
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/check", bytes.NewBufferString(`{not-json}`))
	rec3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed json, got %d", rec3.Code)
	}
}

func TestHealthzReadyzMetrics(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recHealth := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Errorf("expected 200 for /healthz, got %d", recHealth.Code)
	}

	// Readyz (initially ready)
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Errorf("expected 200 for /readyz, got %d", recReady.Code)
	}

	// Readyz (not ready)
	srv.SetReady(false)
	recReady2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recReady2, reqReady)
	if recReady2.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for /readyz when not ready, got %d", recReady2.Code)
	}

	// Metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK {
		t.Errorf("expected 200 for /metrics, got %d", recMetrics.Code)
	}
	if !strings.Contains(recMetrics.Body.String(), "fluxgate_http_requests_total") {
		t.Errorf("expected prometheus output in /metrics, got: %s", recMetrics.Body.String())
	}
}
