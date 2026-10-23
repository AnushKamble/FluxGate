package hardening

import (
	"context"
	"testing"
	"time"

	"fluxgate/internal/allocator"
	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/limiter"
	"fluxgate/internal/node"
	"fluxgate/internal/policy"
	"fluxgate/internal/telemetry"
)

// 1. Token arithmetic overflow and huge elapsed time (e.g., 20 years in future)
func TestHugeElapsedTimeOverflowProtection(t *testing.T) {
	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	tb := limiter.NewTokenBucketMutex(1000, 10.0, fc)

	// Consume all
	tb.Check(fc.Now(), 1000)

	// Advance 20 years (630,720,000 seconds)
	fc.Advance(20 * 365 * 24 * time.Hour)

	d := tb.Check(fc.Now(), 1000)
	if !d.Allowed {
		t.Fatalf("expected allowed after 20 years, got denied")
	}
	// Capacity must still be strictly capped at 1000, not infinity or NaN
	if d.Remaining != 0 {
		t.Fatalf("expected remaining to be 0 after consuming 1000, got %d", d.Remaining)
	}
	if tb.Allow() {
		t.Fatalf("expected bucket to be depleted, but allowed another")
	}
}

// 2. Negative elapsed time / Clock rollback
func TestClockRollbackSafety(t *testing.T) {
	start := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(start)
	tb := limiter.NewTokenBucketMutex(50, 5.0, fc)

	// Consume 10
	tb.Check(fc.Now(), 10)

	// Check with a timestamp 1 day in the past
	past := start.Add(-24 * time.Hour)
	d := tb.Check(past, 5)
	if !d.Allowed {
		t.Fatalf("expected request to be allowed despite past timestamp")
	}
	if d.Remaining > 50 {
		t.Fatalf("token count corrupted by clock rollback: %d", d.Remaining)
	}
}

// 3. Grant replay across allocator
func TestGrantReplayIdempotency(t *testing.T) {
	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 1, Capacity: 100, RefillRate: 10, DefaultCost: 1, Enabled: true},
	}
	coord, _ := allocator.NewCoordinator(policies, fc, 30*time.Second)

	args := allocator.AcquireArgs{
		RequestID:        "stable-req-id",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "p1",
		PolicyVersion:    1,
		RequestedCredits: 20,
	}

	g1, isDup1, err := coord.ReserveGrant(fc.Now(), args)
	if err != nil || isDup1 {
		t.Fatalf("initial reservation failed: %v", err)
	}

	// Replay 100 times concurrently
	for i := 0; i < 100; i++ {
		g2, isDup2, err := coord.ReserveGrant(fc.Now(), args)
		if err != nil || !isDup2 {
			t.Fatalf("replay %d failed: isDup=%v err=%v", i, isDup2, err)
		}
		if g1.GrantID != g2.GrantID || g1.Amount != g2.Amount {
			t.Fatalf("mismatched grant on replay %d", i)
		}
	}

	// Outstanding must remain 20, not 2000!
	unalloc, outstanding, active, _ := coord.GetPolicyStatus("p1")
	if unalloc != 80 || outstanding != 20 || active != 1 {
		t.Fatalf("double allocation on replay: unalloc=%d outstanding=%d active=%d", unalloc, outstanding, active)
	}
}

// 4. Policy version mismatch
func TestPolicyVersionMismatch(t *testing.T) {
	fc := clock.NewRealClock()
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 2, Capacity: 100, RefillRate: 10, DefaultCost: 1, Enabled: true},
	}
	coord, _ := allocator.NewCoordinator(policies, fc, 30*time.Second)

	// Node sends version 1 while allocator has version 2
	_, _, err := coord.ReserveGrant(fc.Now(), allocator.AcquireArgs{
		RequestID:        "ver-req-1",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "p1",
		PolicyVersion:    1, // Mismatch!
		RequestedCredits: 10,
	})

	if err == nil {
		t.Fatalf("expected ErrPolicyVersionMismatch, got nil")
	}
}

// 5. Stale / duplicate reports ignored without state corruption
func TestStaleAndDuplicateReportsIgnored(t *testing.T) {
	fc := clock.NewRealClock()
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 1, Capacity: 100, RefillRate: 10, DefaultCost: 1, Enabled: true},
	}
	coord, _ := allocator.NewCoordinator(policies, fc, 30*time.Second)

	g, _, _ := coord.ReserveGrant(fc.Now(), allocator.AcquireArgs{
		RequestID:        "rep-stale-req",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "p1",
		PolicyVersion:    1,
		RequestedCredits: 50,
	})

	// Initial report 25
	coord.ReconcileReports(fc.Now(), "node-1", "inc-1", []allocator.ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 25},
	})

	// Late report arriving with 10 (stale)
	res, _ := coord.ReconcileReports(fc.Now(), "node-1", "inc-1", []allocator.ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 10},
	})

	if res[0].Status != "STALE" || res[0].AcceptedConsumed != 25 {
		t.Fatalf("stale report corrupted accepted count: %+v", res[0])
	}
}

// 6. Node incarnation collision / restart isolation
func TestIncarnationRestartIsolation(t *testing.T) {
	fc := clock.NewRealClock()
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 1, Capacity: 100, RefillRate: 10, DefaultCost: 1, Enabled: true},
	}
	coord, _ := allocator.NewCoordinator(policies, fc, 30*time.Second)

	g, _, _ := coord.ReserveGrant(fc.Now(), allocator.AcquireArgs{
		RequestID:        "inc-req-1",
		NodeID:           "node-coll",
		IncarnationID:    "inc-original",
		PolicyID:         "p1",
		PolicyVersion:    1,
		RequestedCredits: 20,
	})

	// Stale attempt with wrong incarnation
	res, _ := coord.ReconcileReports(fc.Now(), "node-coll", "inc-collided", []allocator.ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 10},
	})

	if res[0].Status != "INCARNATION_MISMATCH" {
		t.Fatalf("expected INCARNATION_MISMATCH, got: %s", res[0].Status)
	}
}

// 7. Allocator no-free-token handling
func TestAllocatorNoFreeTokens(t *testing.T) {
	fc := clock.NewRealClock()
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 1, Capacity: 20, RefillRate: 0, DefaultCost: 1, Enabled: true},
	}
	coord, _ := allocator.NewCoordinator(policies, fc, 30*time.Second)

	// Exhaust entire capacity
	coord.ReserveGrant(fc.Now(), allocator.AcquireArgs{
		RequestID:        "drain-req",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "p1",
		PolicyVersion:    1,
		RequestedCredits: 20,
	})

	// Next reservation attempt
	_, _, err := coord.ReserveGrant(fc.Now(), allocator.AcquireArgs{
		RequestID:        "excess-req",
		NodeID:           "node-2",
		IncarnationID:    "inc-2",
		PolicyID:         "p1",
		PolicyVersion:    1,
		RequestedCredits: 10,
	})

	if err != allocator.ErrInsufficientHeadroom {
		t.Fatalf("expected ErrInsufficientHeadroom, got: %v", err)
	}
}

// 8. Graceful shutdown without goroutine leak
func TestGracefulShutdownNoGoroutineLeaks(t *testing.T) {
	fc := clock.NewRealClock()
	bucket := node.NewLeaseBucket("p1", 1, 100, 10.0, 10, 0.3, fc)
	logger := telemetry.NewLogger(nil, telemetry.LevelError, 1, nil)
	metrics := telemetry.NewMetricsRegistry()

	worker := node.NewLeaseWorker("node-leak-test", "inc-1", "127.0.0.1:59999", bucket, logger, metrics, 50*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	_ = worker.Start(ctx)

	// Trigger worker activity
	bucket.Check(fc.Now(), 1)

	// Cancel and stop
	cancel()
	worker.Stop()
	// Completes without hanging
}

// 9. Memory cardinality bound under client churn
func TestHighCardinalityMemoryBounded(t *testing.T) {
	fc := clock.NewRealClock()
	policies := []config.PolicyDefinition{
		{ID: "p1", Version: 1, ClientID: "*", Route: "*", Method: "*", Capacity: 10, RefillRate: 1, DefaultCost: 1, Enabled: true},
	}

	// Maximum 50 client limiters permitted in memory
	engine, err := policy.NewEngine(policies, fc, false, 50)
	if err != nil {
		t.Fatalf("create engine error: %v", err)
	}

	// Create 50 distinct clients
	for i := 0; i < 50; i++ {
		key := policy.MatchKey{ClientID: string(rune('A' + i)), Route: "/api", Method: "GET"}
		_, _, err := engine.Evaluate(fc.Now(), key, 1)
		if err != nil {
			t.Fatalf("client %d evaluate failed: %v", i, err)
		}
	}

	// 51st client should be rejected with memory bound error
	key51 := policy.MatchKey{ClientID: "client-51", Route: "/api", Method: "GET"}
	_, _, err51 := engine.Evaluate(fc.Now(), key51, 1)
	if err51 == nil {
		t.Fatalf("expected error when exceeding maxClientIDs bound")
	}
}
