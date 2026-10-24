package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"fluxgate/internal/telemetry"
)

type BenchmarkSummary struct {
	TargetURL       string        `json:"target_url"`
	Concurrency     int           `json:"concurrency"`
	Duration        string        `json:"duration"`
	TotalRequests   int64         `json:"total_requests"`
	ThroughputRPS   float64       `json:"throughput_rps"`
	AllowedRequests int64         `json:"allowed_requests"`
	DeniedRequests  int64         `json:"denied_requests"`
	ErrorRequests   int64         `json:"error_requests"`
	P50Latency      time.Duration `json:"p50_latency"`
	P90Latency      time.Duration `json:"p90_latency"`
	P95Latency      time.Duration `json:"p95_latency"`
	P99Latency      time.Duration `json:"p99_latency"`
	P999Latency     time.Duration `json:"p999_latency"`
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8081/api/v1/check", "Target HTTP check endpoint")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent client workers")
	durationSec := flag.Int("duration", 5, "Duration of load test in seconds")
	clientID := flag.String("client-id", "acme", "Client ID to use for checks")
	route := flag.String("route", "/api/v1/charge", "Route to specify in check")
	cost := flag.Uint64("cost", 1, "Token cost per check")
	outputJSON := flag.Bool("json", false, "Output results in JSON format")
	flag.Parse()

	logger := telemetry.NewLogger(os.Stdout, telemetry.LevelInfo, 1, map[string]interface{}{
		"component": "load-generator",
	})

	if !*outputJSON {
		logger.Info("Starting FluxGate Load Generator",
			"target", *target,
			"concurrency", *concurrency,
			"duration_sec", *durationSec,
		)
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"client_id": *clientID,
		"route":     *route,
		"method":    "POST",
		"cost":      *cost,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*durationSec)*time.Second)
	defer cancel()

	var totalAllowed int64
	var totalDenied int64
	var totalErrors int64
	var totalRequests int64

	// Thread-safe latency collector
	var mu sync.Mutex
	latencies := make([]int64, 0, 100000)

	var wg sync.WaitGroup
	startTime := time.Now()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLatencies := make([]int64, 0, 5000)

			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					latencies = append(latencies, localLatencies...)
					mu.Unlock()
					return
				default:
					t0 := time.Now()
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, *target, bytes.NewReader(payload))
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}
					req.Header.Set("Content-Type", "application/json")

					resp, err := client.Do(req)
					elapsedNs := time.Since(t0).Nanoseconds()
					localLatencies = append(localLatencies, elapsedNs)
					atomic.AddInt64(&totalRequests, 1)

					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}

					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()

					if resp.StatusCode == http.StatusOK {
						atomic.AddInt64(&totalAllowed, 1)
					} else if resp.StatusCode == http.StatusTooManyRequests {
						atomic.AddInt64(&totalDenied, 1)
					} else {
						atomic.AddInt64(&totalErrors, 1)
					}
				}
			}
		}()
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	// Calculate percentiles
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := float64(len(latencies))
	var p50, p90, p95, p99, p999 time.Duration
	if n > 0 {
		p50 = time.Duration(latencies[int(n*0.50)])
		p90 = time.Duration(latencies[int(n*0.90)])
		p95 = time.Duration(latencies[int(n*0.95)])
		p99 = time.Duration(latencies[int(n*0.99)])
		p999 = time.Duration(latencies[int(n*0.999)])
	}

	rps := float64(totalRequests) / actualDuration.Seconds()

	summary := BenchmarkSummary{
		TargetURL:       *target,
		Concurrency:     *concurrency,
		Duration:        actualDuration.String(),
		TotalRequests:   totalRequests,
		ThroughputRPS:   rps,
		AllowedRequests: totalAllowed,
		DeniedRequests:  totalDenied,
		ErrorRequests:   totalErrors,
		P50Latency:      p50,
		P90Latency:      p90,
		P95Latency:      p95,
		P99Latency:      p99,
		P999Latency:     p999,
	}

	if *outputJSON {
		data, _ := json.MarshalIndent(summary, "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Println("\n================================================================================")
		fmt.Println("                       FluxGate Load Test Summary")
		fmt.Println("================================================================================")
		fmt.Printf("Target:           %s\n", summary.TargetURL)
		fmt.Printf("Duration:         %s\n", summary.Duration)
		fmt.Printf("Concurrency:      %d workers\n", summary.Concurrency)
		fmt.Printf("Total Requests:   %d\n", summary.TotalRequests)
		fmt.Printf("Throughput:       %.2f req/sec\n", summary.ThroughputRPS)
		fmt.Printf("Allowed (200):    %d\n", summary.AllowedRequests)
		fmt.Printf("Denied (429):     %d\n", summary.DeniedRequests)
		fmt.Printf("Errors:           %d\n", summary.ErrorRequests)
		fmt.Println("--------------------------------------------------------------------------------")
		fmt.Println("Latency Percentiles:")
		fmt.Printf("  p50:   %v\n", summary.P50Latency)
		fmt.Printf("  p90:   %v\n", summary.P90Latency)
		fmt.Printf("  p95:   %v\n", summary.P95Latency)
		fmt.Printf("  p99:   %v\n", summary.P99Latency)
		fmt.Printf("  p99.9: %v\n", summary.P999Latency)
		fmt.Println("================================================================================")
	}
}
