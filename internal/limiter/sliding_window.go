package limiter

import (
	"math"
	"sync"
	"time"

	"fluxgate/internal/clock"
)

// SlidingWindowCounter implements an approximate sliding-window counter.
//
// APPROXIMATION NOTICE:
// Unlike a sliding-log algorithm that stores exact event timestamps, this implementation
// computes an estimated count across a moving window of duration W by weighting the previous
// fixed window's count by (1 - elapsed_fraction) and adding the current window's count:
//
//	EstimatedCount = PreviousCount * (1 - f) + CurrentCount
//
// where f = (now - currentWindowStart) / W.
//
// This approximation assumes an even distribution of requests across the previous window.
// In burst scenarios where all requests in the previous window occurred at the very end of
// that window, the approximation under-estimates true past load. Conversely, if all requests
// occurred at the beginning of the previous window, it over-estimates.
type SlidingWindowCounter struct {
	mu                 sync.Mutex
	clock              clock.Clock
	windowSize         time.Duration
	limit              uint64
	currentWindowStart time.Time
	currentCount       uint64
	previousCount      uint64
}

// NewSlidingWindowCounter creates a new sliding-window counter.
func NewSlidingWindowCounter(limit uint64, windowSize time.Duration, clk clock.Clock) *SlidingWindowCounter {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	now := clk.Now()
	return &SlidingWindowCounter{
		clock:              clk,
		windowSize:         windowSize,
		limit:              limit,
		currentWindowStart: now.Truncate(windowSize),
		currentCount:       0,
		previousCount:      0,
	}
}

func (sw *SlidingWindowCounter) Allow() bool {
	return sw.AllowCost(1)
}

func (sw *SlidingWindowCounter) AllowCost(cost uint64) bool {
	d := sw.Check(sw.clock.Now(), cost)
	return d.Allowed
}

// Check evaluates the sliding window approximation at timestamp 'now'.
func (sw *SlidingWindowCounter) Check(now time.Time, cost uint64) Decision {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if cost == 0 {
		cost = 1
	}

	// Advance windows if time has progressed
	if now.After(sw.currentWindowStart.Add(sw.windowSize)) {
		elapsedWindows := int64(now.Sub(sw.currentWindowStart) / sw.windowSize)
		if elapsedWindows == 1 {
			sw.previousCount = sw.currentCount
			sw.currentCount = 0
			sw.currentWindowStart = sw.currentWindowStart.Add(sw.windowSize)
		} else {
			// Two or more windows have elapsed: prior history is irrelevant
			sw.previousCount = 0
			sw.currentCount = 0
			sw.currentWindowStart = now.Truncate(sw.windowSize)
		}
	}

	// Calculate fraction of current window elapsed: f in [0.0, 1.0)
	var fraction float64
	elapsedInWindow := now.Sub(sw.currentWindowStart)
	if elapsedInWindow > 0 {
		fraction = float64(elapsedInWindow) / float64(sw.windowSize)
		if fraction > 1.0 {
			fraction = 1.0
		}
	}

	// Estimate current load
	estimatedPrev := float64(sw.previousCount) * (1.0 - fraction)
	estimatedTotal := estimatedPrev + float64(sw.currentCount)

	costF := float64(cost)
	limitF := float64(sw.limit)

	if estimatedTotal+costF <= limitF {
		sw.currentCount += cost
		newTotal := estimatedTotal + costF
		remaining := uint64(math.Max(0, math.Floor(limitF-newTotal)))
		return Decision{
			Allowed:    true,
			Remaining:  remaining,
			Limit:      sw.limit,
			RetryAfter: 0,
			ResetTime:  sw.currentWindowStart.Add(sw.windowSize),
		}
	}

	// Denied
	windowReset := sw.currentWindowStart.Add(sw.windowSize)
	retryAfter := windowReset.Sub(now)
	if retryAfter < 0 {
		retryAfter = time.Millisecond * 100
	}

	remaining := uint64(math.Max(0, math.Floor(limitF-estimatedTotal)))
	return Decision{
		Allowed:    false,
		Remaining:  remaining,
		Limit:      sw.limit,
		RetryAfter: retryAfter,
		ResetTime:  windowReset,
	}
}
