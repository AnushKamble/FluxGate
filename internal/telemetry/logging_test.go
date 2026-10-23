package telemetry

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestStructuredLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(buf, LevelInfo, 1, map[string]interface{}{"service": "fluxgate-test"})

	logger.Debug("debug message") // Should be skipped
	if buf.Len() > 0 {
		t.Fatalf("expected debug message to be filtered, got: %s", buf.String())
	}

	logger.Info("node started", "port", 8080, "status", "ready")
	output := buf.String()
	if !strings.Contains(output, "node started") {
		t.Fatalf("expected message in output, got: %s", output)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("failed to parse JSON log entry: %v", err)
	}

	if parsed["service"] != "fluxgate-test" {
		t.Errorf("expected service=fluxgate-test, got %v", parsed["service"])
	}
	if parsed["port"] != float64(8080) {
		t.Errorf("expected port=8080, got %v", parsed["port"])
	}
}

func TestLoggerSampling(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(buf, LevelInfo, 5, nil) // Log 1 in 5

	for i := 0; i < 10; i++ {
		logger.Info("event", "idx", i)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected exactly 2 sampled log entries out of 10, got %d", len(lines))
	}
}
