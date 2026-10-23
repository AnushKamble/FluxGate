package limiter

import (
	"math"
	"sync/atomic"
	"time"

	"fluxgate/internal/clock"
)

// tbState encapsulates the immutable snapshot of the atomic token bucket.
type tbState struct {
	tokens          float64
	lastRefillNanos int64
}

// TokenBucketAtomic implements a lock-free token bucket using atomic pointer Compare-And-Swap (CAS).
// It maintains exact fractional token accounting and monotonic time progression without mutex contention.
type TokenBucketAtomic struct {
	clock      clock.Clock
	capacity   uint64
	refillRate float64
	state      atomic.Pointer[tbState]
}

// NewTokenBucketAtomic creates a new lock-free atomic token bucket.
func NewTokenBucketAtomic(capacity uint64, refillRate float64, clk clock.Clock) *TokenBucketAtomic {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	nowNanos := clk.Now().UnixNano()
	tb := &TokenBucketAtomic{
		clock:      clk,
		capacity:   capacity,
		refillRate: refillRate,
	}
	initialState := &tbState{
		tokens:          float64(capacity),
		lastRefillNanos: nowNanos,
	}
	tb.state.Store(initialState)
	return tb
}

func (tb *TokenBucketAtomic) Allow() bool {
	return tb.AllowCost(1)
}

func (tb *TokenBucketAtomic) AllowCost(cost uint64) bool {
	d := tb.Check(tb.clock.Now(), cost)
	return d.Allowed
}

// Check performs a lock-free CAS decision for 'cost' tokens at 'now'.
func (tb *TokenBucketAtomic) Check(now time.Time, cost uint64) Decision {
	if cost == 0 {
		cost = 1
	}
	costF := float64(cost)
	nowNanos := now.UnixNano()

	for {
		current := tb.state.Load()
		curTokens := current.tokens
		curNanos := current.lastRefillNanos

		// Refill calculation
		var newTokens float64
		var newNanos int64

		if nowNanos > curNanos {
			elapsedSec := float64(nowNanos-curNanos) / 1e9
			refilled := elapsedSec * tb.refillRate
			newTokens = math.Min(float64(tb.capacity), curTokens+refilled)
			newNanos = nowNanos
		} else {
			// Clock rollback or identical timestamp
			newTokens = curTokens
			newNanos = curNanos
		}

		if newTokens >= costF {
			nextState := &tbState{
				tokens:          newTokens - costF,
				lastRefillNanos: newNanos,
			}
			if tb.state.CompareAndSwap(current, nextState) {
				remaining := uint64(math.Floor(nextState.tokens))
				return Decision{
					Allowed:    true,
					Remaining:  remaining,
					Limit:      tb.capacity,
					RetryAfter: 0,
					ResetTime:  now,
				}
			}
			// CAS conflict: another goroutine updated state concurrently, retry loop
			continue
		}

		// Insufficient tokens: return denial without altering state
		missing := costF - newTokens
		var retrySec float64
		if tb.refillRate > 0 {
			retrySec = missing / tb.refillRate
		} else {
			retrySec = 60.0
		}
		retryAfter := time.Duration(math.Ceil(retrySec * float64(time.Second)))
		remaining := uint64(math.Floor(newTokens))

		return Decision{
			Allowed:    false,
			Remaining:  remaining,
			Limit:      tb.capacity,
			RetryAfter: retryAfter,
			ResetTime:  now.Add(retryAfter),
		}
	}
}

// Tokens returns the floor of currently available tokens.
func (tb *TokenBucketAtomic) Tokens() uint64 {
	s := tb.state.Load()
	return uint64(math.Floor(s.tokens))
}

// Capacity returns the maximum token capacity.
func (tb *TokenBucketAtomic) Capacity() uint64 {
	return tb.capacity
}
