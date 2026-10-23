package crdt

import (
	"sync"
)

// GCounter implements a state-based Grow-Only Counter (G-Counter) CRDT.
// Keys are compound identifiers formatted as "NodeID#IncarnationID".
// Values are monotonically increasing uint64 counters.
type GCounter struct {
	mu    sync.RWMutex
	state map[string]uint64
}

func NewGCounter() *GCounter {
	return &GCounter{
		state: make(map[string]uint64),
	}
}

// Increment adds 'amount' to the local component counter.
func (c *GCounter) Increment(componentKey string, amount uint64) {
	if amount == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state[componentKey] += amount
}

// Get returns the value of a specific component key.
func (c *GCounter) Get(componentKey string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state[componentKey]
}

// Sum returns the aggregate sum across all known components.
func (c *GCounter) Sum() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total uint64
	for _, v := range c.state {
		total += v
	}
	return total
}

// Snapshot returns a copy of the component map.
func (c *GCounter) Snapshot() map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cp := make(map[string]uint64, len(c.state))
	for k, v := range c.state {
		cp[k] = v
	}
	return cp
}

// Merge combines another counter state using component-wise maximum.
// This operation is associative, commutative, and idempotent:
//
//	state[k] = max(state[k], other[k])
func (c *GCounter) Merge(other map[string]uint64) {
	if other == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range other {
		if cur, exists := c.state[k]; !exists || v > cur {
			c.state[k] = v
		}
	}
}

// MergeCounter merges another GCounter directly.
func (c *GCounter) MergeCounter(other *GCounter) {
	c.Merge(other.Snapshot())
}
