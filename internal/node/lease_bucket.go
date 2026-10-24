package node

import (
	"math"
	"sync"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/limiter"
)

// ActiveLeaseGrant tracks an active grant locally on the gateway node.
type ActiveLeaseGrant struct {
	GrantID            string
	Amount             uint64
	CumulativeConsumed uint64
	ExpiresAt          time.Time
}

// LeaseBucket implements limiter.Limiter backed strictly by finite quota leases.
// A LeaseBucket NEVER mints credits locally. It spends only credits explicitly
// granted by the central Quota Allocator.
type LeaseBucket struct {
	mu               sync.Mutex
	clock            clock.Clock
	policyID         string
	policyVersion    uint32
	capacity         uint64
	refillRate       float64
	spendableCredits uint64
	grants           []*ActiveLeaseGrant
	lowWaterChan     chan struct{} // Notifies lease worker to request refill
	batchSize        uint64
	lowWaterRatio    float64
}

func NewLeaseBucket(policyID string, policyVersion uint32, capacity uint64, refillRate float64, batchSize uint64, lowWaterRatio float64, clk clock.Clock) *LeaseBucket {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	if batchSize == 0 {
		batchSize = 10
	}
	if lowWaterRatio <= 0 || lowWaterRatio >= 1.0 {
		lowWaterRatio = 0.3
	}

	return &LeaseBucket{
		clock:            clk,
		policyID:         policyID,
		policyVersion:    policyVersion,
		capacity:         capacity,
		refillRate:       refillRate,
		spendableCredits: 0, // Starts with 0 credits until safely granted by allocator
		grants:           make([]*ActiveLeaseGrant, 0),
		lowWaterChan:     make(chan struct{}, 1),
		batchSize:        batchSize,
		lowWaterRatio:    lowWaterRatio,
	}
}

func (lb *LeaseBucket) Allow() bool {
	return lb.AllowCost(1)
}

func (lb *LeaseBucket) AllowCost(cost uint64) bool {
	d := lb.Check(lb.clock.Now(), cost)
	return d.Allowed
}

// Check evaluates a cost-bearing request against locally available lease credits.
// NEVER calls a remote RPC.
func (lb *LeaseBucket) Check(now time.Time, cost uint64) limiter.Decision {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if cost == 0 {
		cost = 1
	}

	// Trigger low-water check
	lowWaterMark := uint64(math.Ceil(float64(lb.batchSize) * lb.lowWaterRatio))
	if lb.spendableCredits <= lowWaterMark {
		select {
		case lb.lowWaterChan <- struct{}{}:
		default:
		}
	}

	if lb.spendableCredits >= cost {
		lb.spendableCredits -= cost

		// Record consumption against active grants (FIFO order)
		remainingToAccount := cost
		for _, g := range lb.grants {
			unconsumedInGrant := g.Amount - g.CumulativeConsumed
			if unconsumedInGrant > 0 {
				toConsume := remainingToAccount
				if toConsume > unconsumedInGrant {
					toConsume = unconsumedInGrant
				}
				g.CumulativeConsumed += toConsume
				remainingToAccount -= toConsume
				if remainingToAccount == 0 {
					break
				}
			}
		}

		return limiter.Decision{
			Allowed:    true,
			Remaining:  lb.spendableCredits,
			Limit:      lb.capacity,
			RetryAfter: 0,
			ResetTime:  now,
		}
	}

	// Insufficient spendable credits -> strictly DENY in bounded mode
	var retrySec float64
	if lb.refillRate > 0 {
		retrySec = float64(cost) / lb.refillRate
	} else {
		retrySec = 5.0
	}
	retryAfter := time.Duration(math.Ceil(retrySec * float64(time.Second)))

	return limiter.Decision{
		Allowed:    false,
		Remaining:  lb.spendableCredits,
		Limit:      lb.capacity,
		RetryAfter: retryAfter,
		ResetTime:  now.Add(retryAfter),
	}
}

// IngestGrant adds newly granted credits from the allocator.
func (lb *LeaseBucket) IngestGrant(grantID string, amount uint64, validDuration time.Duration) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if amount == 0 {
		return
	}

	now := lb.clock.Now()
	lb.grants = append(lb.grants, &ActiveLeaseGrant{
		GrantID:            grantID,
		Amount:             amount,
		CumulativeConsumed: 0,
		ExpiresAt:          now.Add(validDuration),
	})
	lb.spendableCredits += amount
}

// ResetCredits clears all spendable credits (e.g. after cluster epoch reset or restart).
func (lb *LeaseBucket) ResetCredits() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.spendableCredits = 0
	lb.grants = make([]*ActiveLeaseGrant, 0)
}

// SpendableCredits returns the current available lease credits.
func (lb *LeaseBucket) SpendableCredits() uint64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.spendableCredits
}

// PendingReports gathers cumulative consumption reports for all active grants.
func (lb *LeaseBucket) PendingReports() []ActiveLeaseGrant {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	reports := make([]ActiveLeaseGrant, len(lb.grants))
	for i, g := range lb.grants {
		reports[i] = *g
	}
	return reports
}

// AcknowledgeReports prunes fully consumed grants that have been accepted by the allocator,
// preventing unbounded growth of historical grant state.
func (lb *LeaseBucket) AcknowledgeReports(reconciled map[string]uint64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	active := make([]*ActiveLeaseGrant, 0, len(lb.grants))
	for _, g := range lb.grants {
		accepted, wasReported := reconciled[g.GrantID]
		if wasReported && accepted >= g.Amount && g.CumulativeConsumed >= g.Amount {
			// Grant is completely spent and acknowledged by allocator; safe to prune
			continue
		}
		active = append(active, g)
	}
	lb.grants = active
}

// LowWaterChannel exposes the channel for background worker notifications.
func (lb *LeaseBucket) LowWaterChannel() <-chan struct{} {
	return lb.lowWaterChan
}
