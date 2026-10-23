package clock

import (
	"sync"
	"time"
)

// Clock provides an interface for time measurement and manipulation.
// It allows deterministic testing with a mock/fake clock while using
// the standard monotonic time source in production.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// RealClock implements Clock using standard library time.
type RealClock struct{}

func NewRealClock() *RealClock {
	return &RealClock{}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

func (c *RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

// FakeClock implements Clock with deterministic manual time progression.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewFakeClock(initial time.Time) *FakeClock {
	return &FakeClock{
		now: initial,
	}
}

func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *FakeClock) Sleep(d time.Duration) {
	c.Advance(d)
}
