package clock

import (
	"sync"
	"testing"
	"time"
)

func TestRealClock(t *testing.T) {
	c := NewRealClock()
	t1 := c.Now()
	time.Sleep(10 * time.Millisecond)
	t2 := c.Now()
	if !t2.After(t1) {
		t.Fatalf("expected t2 > t1, got t1=%v, t2=%v", t1, t2)
	}
}

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := NewFakeClock(start)

	if !fc.Now().Equal(start) {
		t.Fatalf("expected initial time %v, got %v", start, fc.Now())
	}

	fc.Advance(5 * time.Second)
	expected := start.Add(5 * time.Second)
	if !fc.Now().Equal(expected) {
		t.Fatalf("expected advanced time %v, got %v", expected, fc.Now())
	}

	// Concurrent read and advance
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fc.Advance(10 * time.Millisecond)
			_ = fc.Now()
		}()
	}
	wg.Wait()
}
