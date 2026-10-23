package integration

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestClusterIntegration(t *testing.T) {
	// Resolve policy file path relative to repo root
	policyPath := "../../configs/policies.example.yaml"
	if _, err := os.Stat(policyPath); err != nil {
		policyPath = "configs/policies.example.yaml"
	}

	clk := clock.NewRealClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start Quota Allocator on port 15050
	policies, err := config.LoadPoliciesFile(policyPath)
	if err != nil {
		t.Fatalf("failed to load policies for allocator: %v", err)
	}

	coord, err := allocator.NewCoordinator(policies, clk, 30*time.Second)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	allocLis, err := net.Listen("tcp", "127.0.0.1:15050")
	if err != nil {
		t.Fatalf("failed to listen allocator port 15050: %v", err)
	}
	defer allocLis.Close()

	allocServer := grpc.NewServer()
	logger := telemetry.NewLogger(nil, telemetry.LevelError, 1, nil)
	fluxgatev1.RegisterQuotaAllocatorServiceServer(allocServer, allocator.NewGRPCServer(coord, logger))
	go func() {
		_ = allocServer.Serve(allocLis)
	}()
	defer allocServer.Stop()

	// 2. Setup 3 Gateway Nodes
	node1Cfg := &config.NodeConfig{
		NodeID:               "node-1",
		HTTPPort:             18081,
		GRPCPort:             15051,
		AllocatorAddr:        "127.0.0.1:15050",
		Peers:                []string{"127.0.0.1:15052", "127.0.0.1:15053"},
		LeaseRefillBatchSize: 10,
		LeaseLowWaterRatio:   0.3,
		GossipIntervalMs:     100,
		PolicyFilePath:       policyPath,
		LogLevel:             "ERROR",
	}

	node2Cfg := &config.NodeConfig{
		NodeID:               "node-2",
		HTTPPort:             18082,
		GRPCPort:             15052,
		AllocatorAddr:        "127.0.0.1:15050",
		Peers:                []string{"127.0.0.1:15051", "127.0.0.1:15053"},
		LeaseRefillBatchSize: 10,
		LeaseLowWaterRatio:   0.3,
		GossipIntervalMs:     100,
		PolicyFilePath:       policyPath,
		LogLevel:             "ERROR",
	}

	node3Cfg := &config.NodeConfig{
		NodeID:               "node-3",
		HTTPPort:             18083,
		GRPCPort:             15053,
		AllocatorAddr:        "127.0.0.1:15050",
		Peers:                []string{"127.0.0.1:15051", "127.0.0.1:15052"},
		LeaseRefillBatchSize: 10,
		LeaseLowWaterRatio:   0.3,
		GossipIntervalMs:     100,
		PolicyFilePath:       policyPath,
		LogLevel:             "ERROR",
	}

	n1, err := node.NewGatewayNode(node1Cfg, clk)
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}
	n2, err := node.NewGatewayNode(node2Cfg, clk)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}
	n3, err := node.NewGatewayNode(node3Cfg, clk)
	if err != nil {
		t.Fatalf("failed to create node 3: %v", err)
	}

	if err := n1.Start(ctx); err != nil {
		t.Fatalf("failed to start node 1: %v", err)
	}
	defer n1.Stop(context.Background())

	if err := n2.Start(ctx); err != nil {
		t.Fatalf("failed to start node 2: %v", err)
	}
	defer n2.Stop(context.Background())

	if err := n3.Start(ctx); err != nil {
		t.Fatalf("failed to start node 3: %v", err)
	}
	defer n3.Stop(context.Background())

	// Give servers a moment to bind and acquire initial leases
	time.Sleep(200 * time.Millisecond)

	// Verify readiness on all 3 nodes
	ports := []int{18081, 18082, 18083}
	client := &http.Client{Timeout: 2 * time.Second}

	for _, port := range ports {
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
		if err != nil {
			t.Fatalf("port %d readyz failed: %v", port, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("port %d readyz returned status %d", port, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Send requests to all 3 nodes
	for i, port := range ports {
		reqPayload := []byte(fmt.Sprintf(`{"client_id":"acme","route":"/api/v1/charge","method":"POST","cost":1}`))
		resp, err := client.Post(fmt.Sprintf("http://127.0.0.1:%d/api/v1/check", port), "application/json", bytes.NewBuffer(reqPayload))
		if err != nil {
			t.Fatalf("node %d check failed: %v", i+1, err)
		}

		var checkResp map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&checkResp)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("node %d check returned %d: %+v", i+1, resp.StatusCode, checkResp)
		}
		if checkResp["allowed"] != true {
			t.Fatalf("expected allowed=true for node %d, got %+v", i+1, checkResp)
		}
	}
}
