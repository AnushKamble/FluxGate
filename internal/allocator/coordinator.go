package allocator

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
)

// AcquireArgs defines parameters for reserving a grant.
type AcquireArgs struct {
	RequestID        string
	NodeID           string
	IncarnationID    string
	PolicyID         string
	PolicyVersion    uint32
	RequestedCredits uint64
}

// ReportItem represents a single grant consumption update.
type ReportItem struct {
	GrantID            string
	CumulativeConsumed uint64
}

// ReconcileResult reports the outcome for an individual grant consumption report.
type ReconcileResult struct {
	GrantID          string
	AcceptedConsumed uint64
	Status           string // "OK", "STALE", "EXCEEDS_GRANT", "INCARNATION_MISMATCH", "NOT_FOUND"
	ErrorMessage     string
}

// Coordinator is the thread-safe central Quota Allocator authority.
type Coordinator struct {
	mu          sync.Mutex
	clock       clock.Clock
	epochID     string
	ledger      *Ledger
	pools       map[string]*PolicyPool
	leaseWindow time.Duration
}

func NewCoordinator(policies []config.PolicyDefinition, clk clock.Clock, leaseWindow time.Duration) (*Coordinator, error) {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	if leaseWindow <= 0 {
		leaseWindow = 30 * time.Second
	}

	// Generate random ClusterEpochID
	epochBytes := make([]byte, 8)
	_, _ = rand.Read(epochBytes)
	epochID := hex.EncodeToString(epochBytes)

	now := clk.Now()
	c := &Coordinator{
		clock:       clk,
		epochID:     epochID,
		ledger:      NewLedger(),
		pools:       make(map[string]*PolicyPool),
		leaseWindow: leaseWindow,
	}

	for _, p := range policies {
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("invalid policy %s: %w", p.ID, err)
		}
		c.pools[p.ID] = NewPolicyPool(p, now, epochID)
	}

	return c, nil
}

func (c *Coordinator) EpochID() string {
	return c.epochID
}

// ReserveGrant issues a finite quota lease to a specific node incarnation.
// This operation is strictly idempotent: replaying the same RequestID returns the
// recorded grant without deducting further tokens.
func (c *Coordinator) ReserveGrant(now time.Time, args AcquireArgs) (*Grant, bool, error) {
	if args.RequestID == "" {
		return nil, false, errors.New("request_id cannot be empty")
	}
	if args.NodeID == "" {
		return nil, false, errors.New("node_id cannot be empty")
	}
	if args.IncarnationID == "" {
		return nil, false, errors.New("incarnation_id cannot be empty")
	}
	if args.RequestedCredits == 0 {
		return nil, false, errors.New("requested_credits must be > 0")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Idempotency Check
	if existing, found := c.ledger.LookupByRequestID(args.RequestID); found {
		return existing, true, nil
	}

	// 2. Policy Lookup & Validation
	pool, exists := c.pools[args.PolicyID]
	if !exists {
		return nil, false, fmt.Errorf("policy not found: %s", args.PolicyID)
	}
	if args.PolicyVersion != pool.Policy.Version {
		return nil, false, fmt.Errorf("%w: client version %d vs allocator version %d",
			ErrPolicyVersionMismatch, args.PolicyVersion, pool.Policy.Version)
	}

	// 3. Conservative Refill
	outstanding := c.ledger.SumOutstandingUpperBounds(args.PolicyID)
	pool.Refill(now, outstanding)

	// 4. Calculate Granted Credits
	available := pool.AvailableCredits()
	grantedCredits := args.RequestedCredits
	if grantedCredits > available {
		grantedCredits = available
	}

	if grantedCredits == 0 {
		return nil, false, ErrInsufficientHeadroom
	}

	// 5. Create Grant and Deduct from Pool
	grantID := fmt.Sprintf("grant-%s-%s-%d", args.NodeID, args.RequestID, now.UnixNano())
	grant := &Grant{
		GrantID:            grantID,
		RequestID:          args.RequestID,
		NodeID:             args.NodeID,
		IncarnationID:      args.IncarnationID,
		PolicyID:           args.PolicyID,
		PolicyVersion:      args.PolicyVersion,
		Amount:             grantedCredits,
		CumulativeConsumed: 0,
		CreatedAt:          now,
		ValidDuration:      c.leaseWindow,
	}

	pool.Deduct(grantedCredits)
	if err := c.ledger.AddGrant(grant); err != nil {
		// Rollback deduction if grant insertion fails
		pool.UnallocatedTokens += float64(grantedCredits)
		return nil, false, err
	}

	// 6. Conservation Invariant Check
	if err := c.verifyConservationInvariant(args.PolicyID); err != nil {
		panic(fmt.Sprintf("FATAL INVARIANT VIOLATION on grant creation: %v", err))
	}

	return grant, false, nil
}

// ReconcileReports processes cumulative consumption reports for active grants.
func (c *Coordinator) ReconcileReports(now time.Time, nodeID, incarnationID string, reports []ReportItem) ([]ReconcileResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	results := make([]ReconcileResult, 0, len(reports))

	for _, rep := range reports {
		g, found := c.ledger.LookupByGrantID(rep.GrantID)
		if !found {
			results = append(results, ReconcileResult{
				GrantID:      rep.GrantID,
				Status:       "NOT_FOUND",
				ErrorMessage: "grant ID not found in ledger",
			})
			continue
		}

		// Verify Incarnation and Node
		if g.NodeID != nodeID || g.IncarnationID != incarnationID {
			results = append(results, ReconcileResult{
				GrantID:          rep.GrantID,
				AcceptedConsumed: g.CumulativeConsumed,
				Status:           "INCARNATION_MISMATCH",
				ErrorMessage:     fmt.Sprintf("node/incarnation mismatch: expected %s#%s, got %s#%s", g.NodeID, g.IncarnationID, nodeID, incarnationID),
			})
			continue
		}

		// Verify consumption does not exceed grant
		if rep.CumulativeConsumed > g.Amount {
			results = append(results, ReconcileResult{
				GrantID:          rep.GrantID,
				AcceptedConsumed: g.CumulativeConsumed,
				Status:           "EXCEEDS_GRANT",
				ErrorMessage:     fmt.Sprintf("reported usage %d exceeds granted credits %d", rep.CumulativeConsumed, g.Amount),
			})
			continue
		}

		// Monotonic verification
		if rep.CumulativeConsumed < g.CumulativeConsumed {
			// Stale or out-of-order report: acknowledge without lowering accepted usage
			results = append(results, ReconcileResult{
				GrantID:          rep.GrantID,
				AcceptedConsumed: g.CumulativeConsumed,
				Status:           "STALE",
				ErrorMessage:     "reported usage is lower than previously acknowledged cumulative count",
			})
			continue
		}

		// 1. First, refill pool up to 'now' under the PRE-REPORT outstanding upper bounds.
		// This ensures elapsed time during the unobserved window is evaluated against
		// the old headroom, preventing retroactive refill minting.
		pool := c.pools[g.PolicyID]
		if pool != nil {
			preReportOutstanding := c.ledger.SumOutstandingUpperBounds(g.PolicyID)
			pool.Refill(now, preReportOutstanding)
		}

		// 2. Now update accepted cumulative consumed, which safely frees headroom
		// starting from 'now' forward.
		g.CumulativeConsumed = rep.CumulativeConsumed

		results = append(results, ReconcileResult{
			GrantID:          rep.GrantID,
			AcceptedConsumed: g.CumulativeConsumed,
			Status:           "OK",
		})
	}

	return results, nil
}

// verifyConservationInvariant enforces unallocated + sum(unconsumed) <= capacity.
func (c *Coordinator) verifyConservationInvariant(policyID string) error {
	pool := c.pools[policyID]
	if pool == nil {
		return nil
	}

	outstanding := c.ledger.SumOutstandingUpperBounds(policyID)
	unallocated := pool.AvailableCredits()
	total := outstanding + unallocated

	if total > pool.Policy.Capacity {
		return fmt.Errorf("conservation violation for policy %s: unallocated(%d) + outstanding(%d) = %d > capacity(%d)",
			policyID, unallocated, outstanding, total, pool.Policy.Capacity)
	}
	return nil
}

// CheckConservationStatus inspects the conservation invariant for all policies.
func (c *Coordinator) CheckConservationStatus() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for polID := range c.pools {
		if err := c.verifyConservationInvariant(polID); err != nil {
			return false, err
		}
	}
	return true, nil
}

// GetPolicyStatus returns snapshot statistics for monitoring.
func (c *Coordinator) GetPolicyStatus(policyID string) (unallocated uint64, outstanding uint64, activeGrants uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pool, exists := c.pools[policyID]
	if !exists {
		return 0, 0, 0, fmt.Errorf("policy not found: %s", policyID)
	}

	outstanding = c.ledger.SumOutstandingUpperBounds(policyID)
	unallocated = pool.AvailableCredits()
	activeGrants = c.ledger.ActiveGrantCount(policyID)
	return unallocated, outstanding, activeGrants, nil
}
