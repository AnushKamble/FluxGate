package limiter

import (
	"time"
)

// Decision captures the outcome of a rate-limit check.
type Decision struct {
	Allowed    bool
	Remaining  uint64
	Limit      uint64
	RetryAfter time.Duration
	ResetTime  time.Time
}

// Limiter defines the standard interface for rate limiters.
type Limiter interface {
	// Allow checks if 1 token is available and consumes it.
	Allow() bool

	// AllowCost checks if 'cost' tokens are available and consumes them.
	AllowCost(cost uint64) bool

	// Check evaluates an attempt for 'cost' tokens at time 'now'.
	Check(now time.Time, cost uint64) Decision
}
