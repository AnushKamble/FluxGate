package limiter

import (
	"math"
	"sync"
	"time"

	"fluxgate/internal/clock"
)

// TokenBucketMutex implements the reference token bucket algorithm protected by a mutex.
// Fractional token refills are precisely retained as float64, while capacities and costs
// are exact integer credits.
type TokenBucketMutex struct {
	mu         sync.Mutex
	clock      clock.Clock
	capacity   uint64
	refillRate float64 // Tokens refilled per second
	tokens     float64 // Current available tokens (fractional retention)
	lastRefill time.Time
}

// NewTokenBucketMutex creates a reference token bucket initialized to full capacity.
func NewTokenBucketMutex(capacity uint64, refillRate float64, clk clock.Clock) *TokenBucketMutex {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	now := clk.Now()
	return &TokenBucketMutex{
		clock:      clk,
		capacity:   capacity,
		refillRate: refillRate,
		tokens:     float64(capacity),
		lastRefill: now,
	}
}

// Allow attempts to consume 1 token.
func (tb *TokenBucketMutex) Allow() bool {
	return tb.AllowCost(1)
}

// AllowCost attempts to consume 'cost' tokens.
func (tb *TokenBucketMutex) AllowCost(cost uint64) bool {
	d := tb.Check(tb.clock.Now(), cost)
	return d.Allowed
}

// Check evaluates a request for 'cost' tokens at timestamp 'now'.
func (tb *TokenBucketMutex) Check(now time.Time, cost uint64) Decision {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Handle time progression
	if now.After(tb.lastRefill) {
		elapsed := now.Sub(tb.lastRefill).Seconds()
		if elapsed > 0 {
			refilled := elapsed * tb.refillRate
			tb.tokens = math.Min(float64(tb.capacity), tb.tokens+refilled)
			tb.lastRefill = now
		}
	} else if now.Before(tb.lastRefill) {
		// Monotonic safety guard: clock went backwards; clamp to lastRefill
		// Do not allow token regression, keep tokens and lastRefill intact.
	}

	if cost == 0 {
		cost = 1
	}

	costF := float64(cost)
	if tb.tokens >= costF {
		tb.tokens -= costF
		remaining := uint64(math.Floor(tb.tokens))
		return Decision{
			Allowed:    true,
			Remaining:  remaining,
			Limit:      tb.capacity,
			RetryAfter: 0,
			ResetTime:  now,
		}
	}

	// Insufficient tokens: calculate required wait duration for retry
	missing := costF - tb.tokens
	var retrySec float64
	if tb.refillRate > 0 {
		retrySec = missing / tb.refillRate
	} else {
		retrySec = 60.0 // Fallback if refillRate is 0
	}

	retryAfter := time.Duration(math.Ceil(retrySec * float64(time.Second)))
	remaining := uint64(math.Floor(tb.tokens))

	return Decision{
		Allowed:    false,
		Remaining:  remaining,
		Limit:      tb.capacity,
		RetryAfter: retryAfter,
		ResetTime:  now.Add(retryAfter),
	}
}

// Tokens returns the current floor of available tokens without consuming.
func (tb *TokenBucketMutex) Tokens() uint64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return uint64(math.Floor(tb.tokens))
}

// Capacity returns the maximum token capacity.
func (tb *TokenBucketMutex) Capacity() uint64 {
	return tb.capacity
}
