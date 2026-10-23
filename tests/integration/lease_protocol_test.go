package integration

import (
	"context"
	"net"
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

func TestLeaseProtocolOverGRPC(t *testing.T) {
	// 1. Setup real Allocator gRPC server
	policies := []config.PolicyDefinition{
		{
			ID:          "policy-integ",
			Version:     1,
			Capacity:    100,
			RefillRate:  20.0,
			DefaultCost: 1,
			Enabled:     true,
		},
	}

	realClock := clock.NewRealClock()
	coord, err := allocator.NewCoordinator(policies, realClock, 10*time.Second)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}

	logger := telemetry.NewLogger(nil, telemetry.LevelError, 1, nil)
	metrics := telemetry.NewMetricsRegistry()
	grpcServer := allocator.NewGRPCServer(coord, logger)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on random port: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	fluxgatev1.RegisterQuotaAllocatorServiceServer(server, grpcServer)
	go func() {
		_ = server.Serve(lis)
	}()
	defer server.Stop()

	allocatorAddr := lis.Addr().String()

	// 2. Setup Node LeaseBucket and LeaseWorker
	bucket := node.NewLeaseBucket("policy-integ", 1, 100, 20.0, 25, 0.3, realClock)
	worker := node.NewLeaseWorker("node-test", "inc-test", allocatorAddr, bucket, logger, metrics, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.Start(ctx); err != nil {
		t.Fatalf("failed to start lease worker: %v", err)
	}
	defer worker.Stop()

	// 3. Wait for initial grant to arrive in background
	deadline := time.Now().Add(2 * time.Second)
	for bucket.SpendableCredits() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	credits := bucket.SpendableCredits()
	if credits != 25 {
		t.Fatalf("expected 25 spendable credits after initial lease acquisition, got %d", credits)
	}

	// 4. Spend credits locally and verify hot path speed (should be microseconds)
	t0 := time.Now()
	for i := 0; i < 20; i++ {
		if !bucket.Allow() {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}
	elapsed := time.Since(t0)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("local hot path took too long (%v), synchronous RPC suspected!", elapsed)
	}

	// Spend the remaining 5 to deplete
	for i := 0; i < 5; i++ {
		bucket.Allow()
	}

	// 5. Verify low-water renewal happens in background
	deadline2 := time.Now().Add(2 * time.Second)
	for bucket.SpendableCredits() == 0 && time.Now().Before(deadline2) {
		time.Sleep(20 * time.Millisecond)
	}

	if bucket.SpendableCredits() == 0 {
		t.Fatalf("expected background replenishment to grant more credits")
	}

	// 6. Test Partition Behavior: stop server abruptly
	server.Stop()

	// Drain whatever credits the node currently holds
	for bucket.Allow() {
		// Draining
	}

	// Once drained, the partitioned node MUST deny and NOT mint tokens locally!
	if bucket.Allow() {
		t.Fatalf("PARTITION SAFETY VIOLATION! Node allowed request without valid credits")
	}
}
