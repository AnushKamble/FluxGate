package allocator

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
)

func createTestCoordinator(t *testing.T, capacity uint64, refillRate float64) (*Coordinator, *clock.FakeClock) {
	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	policies := []config.PolicyDefinition{
		{
			ID:          "policy-global",
			Version:     1,
			Capacity:    capacity,
			RefillRate:  refillRate,
			DefaultCost: 1,
			Enabled:     true,
		},
	}
	c, err := NewCoordinator(policies, fc, 30*time.Second)
	if err != nil {
		t.Fatalf("failed to create coordinator: %v", err)
	}
	return c, fc
}

func TestGrantReservationAndConservation(t *testing.T) {
	c, fc := createTestCoordinator(t, 100, 10.0)

	// Reserve 40 credits
	args := AcquireArgs{
		RequestID:        "req-1",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "policy-global",
		PolicyVersion:    1,
		RequestedCredits: 40,
	}

	g, isDup, err := c.ReserveGrant(fc.Now(), args)
	if err != nil || isDup {
		t.Fatalf("reserve grant failed: %v", err)
	}
	if g.Amount != 40 {
		t.Fatalf("expected 40 credits, got %d", g.Amount)
	}

	unalloc, outstanding, active, err := c.GetPolicyStatus("policy-global")
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if unalloc != 60 || outstanding != 40 || active != 1 {
		t.Fatalf("unexpected state: unalloc=%d outstanding=%d active=%d", unalloc, outstanding, active)
	}

	// Verify invariant holds
	ok, err := c.CheckConservationStatus()
	if !ok || err != nil {
		t.Fatalf("conservation invariant check failed: %v", err)
	}
}

func TestGrantIdempotency(t *testing.T) {
	c, fc := createTestCoordinator(t, 100, 10.0)

	args := AcquireArgs{
		RequestID:        "req-idempotent",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "policy-global",
		PolicyVersion:    1,
		RequestedCredits: 30,
	}

	// 1st request
	g1, isDup1, err := c.ReserveGrant(fc.Now(), args)
	if err != nil || isDup1 {
		t.Fatalf("first call failed: %v", err)
	}

	// 2nd identical request (e.g. network retry)
	g2, isDup2, err := c.ReserveGrant(fc.Now(), args)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if !isDup2 {
		t.Fatalf("expected isDuplicate=true on retry")
	}
	if g1.GrantID != g2.GrantID || g1.Amount != g2.Amount {
		t.Fatalf("mismatched grant returned on retry: g1=%+v, g2=%+v", g1, g2)
	}

	// Outstanding must remain 30, NOT 60!
	unalloc, outstanding, active, _ := c.GetPolicyStatus("policy-global")
	if unalloc != 70 || outstanding != 30 || active != 1 {
		t.Fatalf("double-allocation detected: unalloc=%d outstanding=%d active=%d", unalloc, outstanding, active)
	}
}

func TestCumulativeReportingAndMonotonicity(t *testing.T) {
	c, fc := createTestCoordinator(t, 100, 10.0)

	g, _, _ := c.ReserveGrant(fc.Now(), AcquireArgs{
		RequestID:        "req-rep",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "policy-global",
		PolicyVersion:    1,
		RequestedCredits: 50,
	})

	// 1. Report 20 consumed
	res, err := c.ReconcileReports(fc.Now(), "node-1", "inc-1", []ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 20},
	})
	if err != nil || len(res) != 1 || res[0].Status != "OK" {
		t.Fatalf("report 20 failed: res=%+v, err=%v", res, err)
	}
	if res[0].AcceptedConsumed != 20 {
		t.Errorf("expected 20 accepted, got %d", res[0].AcceptedConsumed)
	}

	// Outstanding should now be 50 - 20 = 30
	_, outstanding, _, _ := c.GetPolicyStatus("policy-global")
	if outstanding != 30 {
		t.Fatalf("expected outstanding 30, got %d", outstanding)
	}

	// 2. Out-of-order / duplicate report of 15 (should be acknowledged as STALE without regression)
	res2, err := c.ReconcileReports(fc.Now(), "node-1", "inc-1", []ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 15},
	})
	if err != nil || len(res2) != 1 || res2[0].Status != "STALE" {
		t.Fatalf("expected STALE status on lower report, got: %+v", res2)
	}
	if res2[0].AcceptedConsumed != 20 {
		t.Errorf("expected accepted to remain 20, got %d", res2[0].AcceptedConsumed)
	}

	// 3. Invalid report exceeding grant (60 > 50)
	res3, err := c.ReconcileReports(fc.Now(), "node-1", "inc-1", []ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 60},
	})
	if err != nil || len(res3) != 1 || res3[0].Status != "EXCEEDS_GRANT" {
		t.Fatalf("expected EXCEEDS_GRANT, got: %+v", res3)
	}

	// 4. Incarnation mismatch
	res4, err := c.ReconcileReports(fc.Now(), "node-1", "inc-wrong", []ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 30},
	})
	if err != nil || len(res4) != 1 || res4[0].Status != "INCARNATION_MISMATCH" {
		t.Fatalf("expected INCARNATION_MISMATCH, got: %+v", res4)
	}
}

// TestConservativeRefillNoRetroactiveMinting proves that if a grant remains unreported
// for a long time, the allocator does NOT mint retroactive refill for the unobserved window.
func TestConservativeRefillNoRetroactiveMinting(t *testing.T) {
	// Capacity 50, refill rate 10 tokens/sec
	c, fc := createTestCoordinator(t, 50, 10.0)

	// Node-1 reserves all 50 credits -> unallocated becomes 0, outstanding = 50
	g, _, err := c.ReserveGrant(fc.Now(), AcquireArgs{
		RequestID:        "req-full",
		NodeID:           "node-1",
		IncarnationID:    "inc-1",
		PolicyID:         "policy-global",
		PolicyVersion:    1,
		RequestedCredits: 50,
	})
	if err != nil {
		t.Fatalf("full grant failed: %v", err)
	}

	// Advance time by 100 seconds!
	// If the allocator naively refilled without conservative headroom,
	// 100s * 10 = 1000 tokens would have accumulated in the background.
	fc.Advance(100 * time.Second)

	// Now node-1 finally reports it consumed 50 tokens
	res, err := c.ReconcileReports(fc.Now(), "node-1", "inc-1", []ReportItem{
		{GrantID: g.GrantID, CumulativeConsumed: 50},
	})
	if err != nil || res[0].Status != "OK" {
		t.Fatalf("reconcile failed: %+v", res)
	}

	// Invariant Check immediately after reporting at observation time:
	// Unallocated tokens must be 0 (no retroactive tokens minted for the 100s when capacity was reserved!)
	// Headroom is now 50, but refill starts from THIS observation time forward.
	unalloc, outstanding, _, _ := c.GetPolicyStatus("policy-global")
	if unalloc != 0 {
		t.Fatalf("RETROACTIVE REFILL VIOLATION! Unallocated tokens minted retroactively: %d (expected 0)", unalloc)
	}
	if outstanding != 0 {
		t.Fatalf("expected outstanding 0, got %d", outstanding)
	}

	// Now advance 2 seconds: 2s * 10 = 20 tokens should properly refill
	fc.Advance(2 * time.Second)

	// Request 20 tokens from the newly earned refill
	g2, _, err := c.ReserveGrant(fc.Now(), AcquireArgs{
		RequestID:        "req-post-refill",
		NodeID:           "node-2",
		IncarnationID:    "inc-2",
		PolicyID:         "policy-global",
		PolicyVersion:    1,
		RequestedCredits: 20,
	})
	if err != nil {
		t.Fatalf("expected 20 tokens to be available after 2 seconds: %v", err)
	}
	if g2.Amount != 20 {
		t.Fatalf("expected 20 credits, got %d", g2.Amount)
	}
}

// TestRandomizedEventSchedulesProperty runs randomized grant, report, and refill schedules
// to stress-test the conservation invariant.
func TestRandomizedEventSchedulesProperty(t *testing.T) {
	c, fc := createTestCoordinator(t, 200, 20.0)
	r := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility

	type activeGrant struct {
		grantID       string
		amount        uint64
		consumed      uint64
		nodeID        string
		incarnationID string
	}
	var grants []activeGrant

	for step := 0; step < 500; step++ {
		// Random time progression: 0 to 500ms
		advanceMs := r.Intn(500)
		fc.Advance(time.Duration(advanceMs) * time.Millisecond)

		action := r.Intn(3)
		switch action {
		case 0, 1: // Acquire grant
			nodeIdx := r.Intn(3) + 1
			nodeID := fmt.Sprintf("node-%d", nodeIdx)
			incID := fmt.Sprintf("inc-%d", nodeIdx)
			reqCredits := uint64(r.Intn(30) + 1)
			reqID := fmt.Sprintf("rand-req-%d", step)

			g, _, err := c.ReserveGrant(fc.Now(), AcquireArgs{
				RequestID:        reqID,
				NodeID:           nodeID,
				IncarnationID:    incID,
				PolicyID:         "policy-global",
				PolicyVersion:    1,
				RequestedCredits: reqCredits,
			})
			if err == nil {
				grants = append(grants, activeGrant{
					grantID:       g.GrantID,
					amount:        g.Amount,
					consumed:      0,
					nodeID:        nodeID,
					incarnationID: incID,
				})
			}

		case 2: // Report consumption on a random existing grant
			if len(grants) > 0 {
				idx := r.Intn(len(grants))
				ag := &grants[idx]
				if ag.consumed < ag.amount {
					inc := uint64(r.Intn(int(ag.amount-ag.consumed)) + 1)
					ag.consumed += inc
					_, _ = c.ReconcileReports(fc.Now(), ag.nodeID, ag.incarnationID, []ReportItem{
						{GrantID: ag.grantID, CumulativeConsumed: ag.consumed},
					})
				}
			}
		}

		// Conservation Invariant MUST hold on every single step
		ok, err := c.CheckConservationStatus()
		if !ok || err != nil {
			t.Fatalf("Invariant violated at step %d: %v", step, err)
		}
	}
}
