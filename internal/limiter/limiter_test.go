package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fluxgate/internal/clock"
)

func runCommonTokenBucketTests(t *testing.T, name string, factory func(capacity uint64, refillRate float64, clk clock.Clock) Limiter) {
	t.Run(name+"/InitialFullCapacity", func(t *testing.T) {
		fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		tb := factory(10, 1.0, fc)

		d := tb.Check(fc.Now(), 1)
		if !d.Allowed || d.Remaining != 9 {
			t.Fatalf("expected allowed with remaining 9, got allowed=%v remaining=%d", d.Allowed, d.Remaining)
		}
	})

	t.Run(name+"/DepleteToZeroAndDeny", func(t *testing.T) {
		fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		tb := factory(5, 1.0, fc)

		for i := 0; i < 5; i++ {
			if !tb.Allow() {
				t.Fatalf("expected request %d to be allowed", i)
			}
		}

		// 6th request should be denied
		d := tb.Check(fc.Now(), 1)
		if d.Allowed {
			t.Fatalf("expected 6th request to be denied")
		}
		if d.Remaining != 0 {
			t.Fatalf("expected 0 remaining, got %d", d.Remaining)
		}
		if d.RetryAfter <= 0 {
			t.Fatalf("expected positive RetryAfter, got %v", d.RetryAfter)
		}
	})

	t.Run(name+"/RefillCappedAtCapacity", func(t *testing.T) {
		fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		tb := factory(10, 2.0, fc)

		// Consume all 10
		for i := 0; i < 10; i++ {
			tb.Allow()
		}

		// Advance 100 seconds: 100 * 2 = 200 tokens refilled, capped at 10
		fc.Advance(100 * time.Second)

		d := tb.Check(fc.Now(), 10)
		if !d.Allowed || d.Remaining != 0 {
			t.Fatalf("expected allowed 10 with remaining 0, got allowed=%v, remaining=%d", d.Allowed, d.Remaining)
		}

		// Bucket must be empty now
		if tb.Allow() {
			t.Fatalf("expected empty bucket after consuming 10")
		}
	})

	t.Run(name+"/FractionalRefillRetention", func(t *testing.T) {
		fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		tb := factory(10, 1.0, fc) // 1 token per second

		// Drain bucket
		for i := 0; i < 10; i++ {
			tb.Allow()
		}

		// Advance 500ms (0.5 token)
		fc.Advance(500 * time.Millisecond)
		if tb.Allow() {
			t.Fatalf("expected denial for 0.5 tokens")
		}

		// Advance another 500ms (total 1.0 token)
		fc.Advance(500 * time.Millisecond)
		if !tb.Allow() {
			t.Fatalf("expected allow after accumulating 1.0 token")
		}

		// Next immediate should fail
		if tb.Allow() {
			t.Fatalf("expected immediate next to fail")
		}
	})

	t.Run(name+"/NegativeTimeRollbackProtection", func(t *testing.T) {
		start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		fc := clock.NewFakeClock(start)
		tb := factory(10, 1.0, fc)

		// Consume 5
		for i := 0; i < 5; i++ {
			tb.Allow()
		}

		// Check with a timestamp in the past (clock rollback)
		past := start.Add(-10 * time.Minute)
		d := tb.Check(past, 1)
		// Should safely allow without panic or token corruption
		if !d.Allowed {
			t.Fatalf("expected allowed despite past timestamp")
		}
	})

	t.Run(name+"/ConcurrentConsumers", func(t *testing.T) {
		fc := clock.NewRealClock()
		// Bucket capacity 500, refill 0
		tb := factory(500, 0.0, fc)

		var allowedCount int64
		var deniedCount int64
		var wg sync.WaitGroup

		workers := 50
		requestsPerWorker := 20 // 1000 total requests against 500 capacity

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < requestsPerWorker; j++ {
					if tb.Allow() {
						atomic.AddInt64(&allowedCount, 1)
					} else {
						atomic.AddInt64(&deniedCount, 1)
					}
				}
			}()
		}
		wg.Wait()

		if allowedCount != 500 {
			t.Fatalf("expected exactly 500 allowed, got %d", allowedCount)
		}
		if deniedCount != 500 {
			t.Fatalf("expected exactly 500 denied, got %d", deniedCount)
		}
	})
}

func TestTokenBucketMutex(t *testing.T) {
	runCommonTokenBucketTests(t, "Mutex", func(capacity uint64, refillRate float64, clk clock.Clock) Limiter {
		return NewTokenBucketMutex(capacity, refillRate, clk)
	})
}

func TestTokenBucketAtomic(t *testing.T) {
	runCommonTokenBucketTests(t, "Atomic", func(capacity uint64, refillRate float64, clk clock.Clock) Limiter {
		return NewTokenBucketAtomic(capacity, refillRate, clk)
	})
}

func TestSlidingWindowCounter(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := clock.NewFakeClock(start)
	window := time.Minute
	sw := NewSlidingWindowCounter(10, window, fc)

	// Consume 6 in first window
	for i := 0; i < 6; i++ {
		if !sw.Allow() {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// Move into second window by 30 seconds (halfway: f = 0.5)
	fc.Advance(90 * time.Second) // 1.5 windows elapsed from start
	// Previous count = 6, weight = 1 - 0.5 = 0.5 -> estimated prev = 3.0
	// Current count = 0 -> total estimated = 3.0
	// Remaining = 10 - 3 = 7

	d := sw.Check(fc.Now(), 1)
	if !d.Allowed {
		t.Fatalf("expected allowed halfway through second window")
	}
	// Consumed 1 in current window -> current count = 1, total estimated = 4.0
	if d.Remaining != 6 {
		t.Errorf("expected remaining 6, got %d", d.Remaining)
	}

	// Consume remaining 6
	for i := 0; i < 6; i++ {
		if !sw.Allow() {
			t.Fatalf("expected request %d in current window to be allowed", i)
		}
	}

	// Next should be denied
	if sw.Allow() {
		t.Fatalf("expected denial after hitting limit")
	}

	// Advance 2 full windows (no previous activity retained)
	fc.Advance(3 * time.Minute)
	dClean := sw.Check(fc.Now(), 10)
	if !dClean.Allowed || dClean.Remaining != 0 {
		t.Fatalf("expected full limit of 10 available after 2 idle windows")
	}
}
