package main

import (
	"flag"
	"os"

	"fluxgate/internal/telemetry"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:8081", "Target node HTTP address")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent client workers")
	durationSec := flag.Int("duration", 10, "Test duration in seconds")
	flag.Parse()

	logger := telemetry.NewLogger(os.Stdout, telemetry.LevelInfo, 1, map[string]interface{}{
		"component": "load-generator",
	})
	logger.Info("FluxGate Load Generator Initialized", "target", *target, "concurrency", *concurrency, "duration_sec", *durationSec)
}
