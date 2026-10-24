package hardening

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/allocator"
	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/node"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc"
)

// TestHTTPGatewayPartitionDenial verifies that a real running Gateway Node receiving HTTP
// requests strictly denies with HTTP 429 once its lease is exhausted during an allocator
// partition, and NEVER refills locally.
func TestHTTPGatewayPartitionDenial(t *testing.T) {
	// Policy path relative to test execution
	policyPath := "../../configs/policies.example.yaml"
	if _, err := os.Stat(policyPath); err != nil {
		policyPath = "configs/policies.example.yaml"
	}

	clk := clock.NewRealClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start temporary Allocator on random port
	policies, err := config.LoadPoliciesFile(policyPath)
	if err != nil {
		t.Fatalf("load policies error: %v", err)
	}

	coord, _ := allocator.NewCoordinator(policies, clk, 30*time.Second)
	allocLis, _ := net.Listen("tcp", "127.0.0.1:0")
	allocAddr := allocLis.Addr().String()

	allocServer := grpc.NewServer()
	logger := telemetry.NewLogger(nil, telemetry.LevelError, 1, nil)
	fluxgatev1.RegisterQuotaAllocatorServiceServer(allocServer, allocator.NewGRPCServer(coord, logger))
	go func() { _ = allocServer.Serve(allocLis) }()

	// 2. Start Gateway Node with batch size of 5 credits
	nodeCfg := &config.NodeConfig{
		NodeID:               "node-adv-1",
		HTTPPort:             19081,
		GRPCPort:             16051,
		AllocatorAddr:        allocAddr,
		Peers:                []string{},
		LeaseRefillBatchSize: 5,
		LeaseLowWaterRatio:   0.2,
		GossipIntervalMs:     100,
		PolicyFilePath:       policyPath,
		LogLevel:             "ERROR",
	}

	gw, err := node.NewGatewayNode(nodeCfg, clk)
	if err != nil {
		t.Fatalf("create node error: %v", err)
	}
	if err := gw.Start(ctx); err != nil {
		t.Fatalf("start node error: %v", err)
	}
	defer gw.Stop(context.Background())

	// Wait for node to acquire initial lease of 5
	time.Sleep(200 * time.Millisecond)

	// 3. SEVER ALLOCATOR CONNECTION (PARTITION)
	allocServer.Stop()
	allocLis.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	reqBody := []byte(`{"client_id":"acme","route":"/api/v1/charge","method":"POST","cost":1}`)

	// 4. Consume the 5 valid credits via real HTTP POST requests
	for i := 0; i < 5; i++ {
		resp, err := client.Post("http://127.0.0.1:19081/api/v1/check", "application/json", bytes.NewReader(reqBody))
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d returned status %d (expected 200)", i+1, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// 5. 6th request MUST be rejected with HTTP 429 (Too Many Requests)
	resp, err := client.Post("http://127.0.0.1:19081/api/v1/check", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("6th request network failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("CRITICAL INVARIANT VIOLATION: Partitioned node returned status %d on depleted lease! Expected 429", resp.StatusCode)
	}

	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter == "" {
		t.Errorf("expected Retry-After header on 429 response")
	}

	// 6. Wait 1 full second (policy refill rate is 20 tokens/sec)
	// If the node had local refill, it would have minted 20 tokens!
	time.Sleep(1 * time.Second)

	// 7th request MUST STILL be rejected with HTTP 429!
	resp2, err := client.Post("http://127.0.0.1:19081/api/v1/check", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("7th request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("LOCAL REFILL LEAK VIOLATION: Partitioned node allowed traffic after wait! Status: %d", resp2.StatusCode)
	}
}

// TestGrantPruningPreventsMemoryLeak verifies that fully consumed and acknowledged grants
// are pruned from local memory.
func TestGrantPruningPreventsMemoryLeak(t *testing.T) {
	clk := clock.NewRealClock()
	bucket := node.NewLeaseBucket("p1", 1, 100, 10.0, 10, 0.3, clk)

	// Ingest 50 sequential grants
	for i := 0; i < 50; i++ {
		grantID := fmt.Sprintf("grant-%d", i)
		bucket.IngestGrant(grantID, 2, 10*time.Second)
	}

	// Spend all 100 credits
	for i := 0; i < 100; i++ {
		if !bucket.Allow() {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}

	// Before pruning, pending reports has 50 entries
	if len(bucket.PendingReports()) != 50 {
		t.Fatalf("expected 50 pending before prune, got %d", len(bucket.PendingReports()))
	}

	// Acknowledge the first 40 grants
	ackMap := make(map[string]uint64)
	for i := 0; i < 40; i++ {
		grantID := fmt.Sprintf("grant-%d", i)
		ackMap[grantID] = 2
	}
	bucket.AcknowledgeReports(ackMap)

	// Exactly 10 active grants should remain (the unacknowledged ones)
	remaining := len(bucket.PendingReports())
	if remaining != 10 {
		t.Fatalf("expected 10 grants remaining after prune, got %d", remaining)
	}
}
