package policy

import (
	"testing"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
)

func TestPolicyMatchingPrecedence(t *testing.T) {
	policies := []config.PolicyDefinition{
		{
			ID:          "p-catchall",
			Version:     1,
			ClientID:    "*",
			Route:       "*",
			Method:      "*",
			Capacity:    10,
			RefillRate:  1,
			DefaultCost: 1,
			Enabled:     true,
		},
		{
			ID:          "p-client-wildcard",
			Version:     1,
			ClientID:    "acme",
			Route:       "*",
			Method:      "*",
			Capacity:    50,
			RefillRate:  5,
			DefaultCost: 1,
			Enabled:     true,
		},
		{
			ID:          "p-client-exact-route",
			Version:     1,
			ClientID:    "acme",
			Route:       "/api/charge",
			Method:      "POST",
			Capacity:    100,
			RefillRate:  10,
			DefaultCost: 1,
			Enabled:     true,
		},
	}

	fc := clock.NewFakeClock(time.Now())
	engine, err := NewEngine(policies, fc, false, 1000)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// 1. Exact match should pick p-client-exact-route
	p1, err := engine.Match(MatchKey{ClientID: "acme", Route: "/api/charge", Method: "POST"})
	if err != nil {
		t.Fatalf("unexpected match error: %v", err)
	}
	if p1.ID != "p-client-exact-route" {
		t.Errorf("expected p-client-exact-route, got %s", p1.ID)
	}

	// 2. Client match with other route should pick p-client-wildcard
	p2, err := engine.Match(MatchKey{ClientID: "acme", Route: "/api/other", Method: "GET"})
	if err != nil {
		t.Fatalf("unexpected match error: %v", err)
	}
	if p2.ID != "p-client-wildcard" {
		t.Errorf("expected p-client-wildcard, got %s", p2.ID)
	}

	// 3. Other client should pick p-catchall
	p3, err := engine.Match(MatchKey{ClientID: "unknown", Route: "/api/test", Method: "GET"})
	if err != nil {
		t.Fatalf("unexpected match error: %v", err)
	}
	if p3.ID != "p-catchall" {
		t.Errorf("expected p-catchall, got %s", p3.ID)
	}
}

func TestPolicyEvaluateAndDeplete(t *testing.T) {
	policies := []config.PolicyDefinition{
		{
			ID:          "p1",
			Version:     1,
			ClientID:    "clientA",
			Route:       "*",
			Method:      "*",
			Capacity:    2,
			RefillRate:  1,
			DefaultCost: 1,
			Enabled:     true,
		},
	}

	fc := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	engine, err := NewEngine(policies, fc, false, 1000)
	if err != nil {
		t.Fatalf("create engine error: %v", err)
	}

	key := MatchKey{ClientID: "clientA", Route: "/test", Method: "GET"}

	// Req 1: Allowed
	d1, _, err := engine.Evaluate(fc.Now(), key, 1)
	if err != nil || !d1.Allowed || d1.Remaining != 1 {
		t.Fatalf("req 1 failed: err=%v, allowed=%v, remaining=%d", err, d1.Allowed, d1.Remaining)
	}

	// Req 2: Allowed
	d2, _, err := engine.Evaluate(fc.Now(), key, 1)
	if err != nil || !d2.Allowed || d2.Remaining != 0 {
		t.Fatalf("req 2 failed: err=%v, allowed=%v, remaining=%d", err, d2.Allowed, d2.Remaining)
	}

	// Req 3: Denied
	d3, _, err := engine.Evaluate(fc.Now(), key, 1)
	if err != nil || d3.Allowed {
		t.Fatalf("req 3 should be denied: err=%v, allowed=%v", err, d3.Allowed)
	}
}
