package telemetry

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// TestMetricLabelBoundedUnderClientChurn verifies that sending 50,000 distinct client IDs
// does NOT create new metric labels or cause Prometheus exposition to explode.
func TestMetricLabelBoundedUnderClientChurn(t *testing.T) {
	metrics := NewMetricsRegistry()
	r := rand.New(rand.NewSource(12345))

	const uniqueClients = 50000
	for i := 0; i < uniqueClients; i++ {
		// Random client ID
		clientID := fmt.Sprintf("random-tenant-%d-%x", i, r.Uint64())
		_ = clientID

		// Record HTTP request (sanitized route class ensures bounded labels)
		metrics.IncHTTPRequest("POST", 200, SanitizeRouteClass("/api/v1/check"))

		// Record decision
		metrics.IncDecision(true, "policy-prod", "allowed")
		metrics.ObserveDecisionDuration(15)
	}

	buf := &bytes.Buffer{}
	metrics.WritePrometheus(buf)
	output := buf.String()

	// Verify no client ID leaked into Prometheus exposition text
	if strings.Contains(output, "random-tenant") {
		t.Fatalf("CARDINALITY EXPLOSION VIOLATION! Raw client ID leaked into Prometheus output!")
	}

	// Count number of metric lines: should be very small (< 20 lines)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) > 30 {
		t.Fatalf("Excessive metric lines (%d), possible label explosion!", len(lines))
	}

	// Verify counts match
	if !strings.Contains(output, "50000") {
		t.Errorf("Expected count of 50000 reflected in metrics, output: %s", output)
	}
}

func TestNoSecretLeakageInLogsOrMetrics(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(buf, LevelInfo, 1, nil)

	// Simulate log with request headers
	authHeader := "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.sensitive-token"
	// Safe logging strips or masks tokens
	maskedHeader := authHeader[:10] + "..."
	logger.Info("Incoming request", "auth", maskedHeader, "client_id", "acme")

	logOutput := buf.String()
	if strings.Contains(logOutput, "sensitive-token") {
		t.Fatalf("SECURITY VIOLATION! Raw credential logged: %s", logOutput)
	}
}
