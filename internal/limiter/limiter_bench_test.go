package limiter

import (
	"sort"
	"sync"
	"testing"
	"time"

	"fluxgate/internal/clock"
)

func runLimiterBenchmark(b *testing.B, l Limiter) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow()
	}
}

func BenchmarkTokenBucketMutexSequential(b *testing.B) {
	tb := NewTokenBucketMutex(10000000, 10000000, clock.NewRealClock())
	runLimiterBenchmark(b, tb)
}

func BenchmarkTokenBucketAtomicSequential(b *testing.B) {
	tb := NewTokenBucketAtomic(10000000, 10000000, clock.NewRealClock())
	runLimiterBenchmark(b, tb)
}

func BenchmarkTokenBucketMutexParallel(b *testing.B) {
	tb := NewTokenBucketMutex(10000000, 10000000, clock.NewRealClock())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

func BenchmarkTokenBucketAtomicParallel(b *testing.B) {
	tb := NewTokenBucketAtomic(10000000, 10000000, clock.NewRealClock())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

func BenchmarkSlidingWindowParallel(b *testing.B) {
	sw := NewSlidingWindowCounter(10000000, time.Minute, clock.NewRealClock())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sw.Allow()
		}
	})
}

// TestLatencyDistributions measures and reports p50, p95, p99, and p99.9 latencies.
func TestLatencyDistributions(t *testing.T) {
	const iterations = 50000
	const workers = 8

	measure := func(name string, factory func() Limiter) {
		l := factory()
		latencies := make([]int64, iterations)
		var wg sync.WaitGroup

		chunk := iterations / workers
		for w := 0; w < workers; w++ {
			startIdx := w * chunk
			endIdx := startIdx + chunk
			if w == workers-1 {
				endIdx = iterations
			}

			wg.Add(1)
			go func(s, e int) {
				defer wg.Done()
				for i := s; i < e; i++ {
					t0 := time.Now()
					l.Allow()
					latencies[i] = time.Since(t0).Nanoseconds()
				}
			}(startIdx, endIdx)
		}
		wg.Wait()

		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		p50 := time.Duration(latencies[int(float64(iterations)*0.50)])
		p95 := time.Duration(latencies[int(float64(iterations)*0.95)])
		p99 := time.Duration(latencies[int(float64(iterations)*0.99)])
		p999 := time.Duration(latencies[int(float64(iterations)*0.999)])

		t.Logf("[%s] %d ops (%d workers) -> p50: %v, p95: %v, p99: %v, p99.9: %v",
			name, iterations, workers, p50, p95, p99, p999)
	}

	measure("MutexTokenBucket", func() Limiter {
		return NewTokenBucketMutex(10000000, 10000000, clock.NewRealClock())
	})
	measure("AtomicTokenBucket", func() Limiter {
		return NewTokenBucketAtomic(10000000, 10000000, clock.NewRealClock())
	})
	measure("SlidingWindowCounter", func() Limiter {
		return NewSlidingWindowCounter(10000000, time.Minute, clock.NewRealClock())
	})
}
