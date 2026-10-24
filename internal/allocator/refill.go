package allocator

import (
	"math"
	"time"

	"fluxgate/internal/config"
)

// PolicyPool manages the global token inventory and conservative refill for a single policy.
type PolicyPool struct {
	Policy            config.PolicyDefinition
	UnallocatedTokens float64   // Available tokens in global pool
	LastRefillTime    time.Time // Monotonic timestamp of last refill evaluation
	EpochID           string
}

func NewPolicyPool(pol config.PolicyDefinition, initialTime time.Time, epochID string) *PolicyPool {
	return &PolicyPool{
		Policy:            pol,
		UnallocatedTokens: float64(pol.Capacity), // Initialized to full capacity
		LastRefillTime:    initialTime,
		EpochID:           epochID,
	}
}

// Refill applies conservative token replenishment up to currently provable headroom.
//
// CONSERVATIVE REFILL INVARIANT:
// If outstanding unconfirmed grants make the total accounted quota equal to capacity,
// headroom is 0 and no refill accumulates. When a cumulative consumption report later
// proves that tokens were consumed, that headroom becomes eligible for refill only from
// that observation timestamp forward. No retroactive refill is minted.
func (p *PolicyPool) Refill(now time.Time, outstandingUpperBounds uint64) {
	// Calculate provable headroom first
	var headroom float64
	if outstandingUpperBounds >= p.Policy.Capacity {
		headroom = 0.0
	} else {
		headroom = float64(p.Policy.Capacity - outstandingUpperBounds)
	}

	if now.Before(p.LastRefillTime) {
		// Clock rollback protection: do not advance time, clamp to headroom
		p.UnallocatedTokens = math.Max(0.0, math.Min(headroom, p.UnallocatedTokens))
		return
	}

	elapsedSec := now.Sub(p.LastRefillTime).Seconds()
	p.LastRefillTime = now

	// Calculate unallocated tokens with rate refill
	potentialTokens := p.UnallocatedTokens + (elapsedSec * p.Policy.RefillRate)

	// Clamp to available headroom and capacity
	p.UnallocatedTokens = math.Max(0.0, math.Min(headroom, potentialTokens))
}

// AvailableCredits returns the integer floor of unallocated tokens.
func (p *PolicyPool) AvailableCredits() uint64 {
	return uint64(math.Floor(p.UnallocatedTokens))
}

// Deduct removes granted credits from the unallocated pool.
func (p *PolicyPool) Deduct(amount uint64) {
	p.UnallocatedTokens = math.Max(0.0, p.UnallocatedTokens-float64(amount))
}
