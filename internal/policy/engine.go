package policy

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"fluxgate/internal/clock"
	"fluxgate/internal/config"
	"fluxgate/internal/limiter"
)

var (
	ErrPolicyNotFound = errors.New("no matching rate limit policy found")
	ErrInvalidPolicy  = errors.New("invalid policy definition")
)

// MatchKey identifies the lookup parameters for a request.
type MatchKey struct {
	ClientID string
	Route    string
	Method   string
}

// Engine evaluates incoming requests against configured policies and maintains local limiters.
type Engine struct {
	mu             sync.RWMutex
	clock          clock.Clock
	policies       []config.PolicyDefinition
	limiters       map[string]limiter.Limiter // key: policy_id + "#" + client_id
	policyLimiters map[string]limiter.Limiter // key: policy_id -> distributed LeaseBucket
	useAtomic      bool
	maxClientIDs   int // Bound memory against high-cardinality attacks
}

// NewEngine creates a new policy engine.
func NewEngine(policies []config.PolicyDefinition, clk clock.Clock, useAtomic bool, maxClientIDs int) (*Engine, error) {
	if clk == nil {
		clk = clock.NewRealClock()
	}
	if maxClientIDs <= 0 {
		maxClientIDs = 100000
	}
	e := &Engine{
		clock:          clk,
		policies:       make([]config.PolicyDefinition, len(policies)),
		limiters:       make(map[string]limiter.Limiter),
		policyLimiters: make(map[string]limiter.Limiter),
		useAtomic:      useAtomic,
		maxClientIDs:   maxClientIDs,
	}
	copy(e.policies, policies)

	// Validate all policies
	for _, p := range e.policies {
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidPolicy, err.Error())
		}
	}

	return e, nil
}

// Match evaluates precedence to find the winning policy:
// 1. Exact ClientID + Exact Route + Exact Method
// 2. Exact ClientID + Prefix Route + Method
// 3. Exact ClientID + Wildcard Route + Method
// 4. Wildcard ClientID + Exact Route + Exact Method
// 5. Wildcard ClientID + Prefix Route + Method
// 6. Wildcard ClientID + Wildcard Route + Wildcard Method
func (e *Engine) Match(key MatchKey) (*config.PolicyDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	client := key.ClientID
	route := key.Route
	method := strings.ToUpper(key.Method)

	var bestMatch *config.PolicyDefinition
	highestScore := -1

	for i := range e.policies {
		p := &e.policies[i]
		if !p.Enabled {
			continue
		}

		score := 0

		// Client match
		if p.ClientID == client {
			score += 100
		} else if p.ClientID == "*" {
			score += 10
		} else {
			continue
		}

		// Route match
		if p.Route == route {
			score += 40
		} else if strings.HasSuffix(p.Route, "*") && strings.HasPrefix(route, strings.TrimSuffix(p.Route, "*")) {
			score += 20
		} else if p.Route == "*" {
			score += 5
		} else {
			continue
		}

		// Method match
		if strings.ToUpper(p.Method) == method {
			score += 10
		} else if p.Method == "*" {
			score += 2
		} else {
			continue
		}

		if score > highestScore {
			highestScore = score
			bestMatch = p
		}
	}

	if bestMatch == nil {
		return nil, ErrPolicyNotFound
	}

	return bestMatch, nil
}

// GetOrCreateLimiter retrieves or creates a limiter for the given policy and client.
func (e *Engine) GetOrCreateLimiter(p *config.PolicyDefinition, clientID string) (limiter.Limiter, error) {
	limiterKey := p.ID + "#" + clientID

	e.mu.RLock()
	l, exists := e.limiters[limiterKey]
	e.mu.RUnlock()
	if exists {
		return l, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Double check
	if l, exists := e.limiters[limiterKey]; exists {
		return l, nil
	}

	// Memory cardinality protection: if limiters map exceeds threshold, reject or evict
	if len(e.limiters) >= e.maxClientIDs {
		return nil, errors.New("maximum client cardinality reached for in-memory limiters")
	}

	if e.useAtomic {
		l = limiter.NewTokenBucketAtomic(p.Capacity, p.RefillRate, e.clock)
	} else {
		l = limiter.NewTokenBucketMutex(p.Capacity, p.RefillRate, e.clock)
	}

	e.limiters[limiterKey] = l
	return l, nil
}

// RegisterPolicyLimiter sets a policy-level distributed limiter (e.g., LeaseBucket).
func (e *Engine) RegisterPolicyLimiter(policyID string, l limiter.Limiter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policyLimiters[policyID] = l
}

// Evaluate performs a complete match and rate check for an incoming request.
func (e *Engine) Evaluate(now time.Time, key MatchKey, cost uint64) (limiter.Decision, *config.PolicyDefinition, error) {
	p, err := e.Match(key)
	if err != nil {
		return limiter.Decision{}, nil, err
	}

	if cost == 0 {
		cost = p.DefaultCost
	}

	// 1. Check if a distributed policy limiter (LeaseBucket) is registered
	e.mu.RLock()
	polLimiter, hasPolLimiter := e.policyLimiters[p.ID]
	e.mu.RUnlock()

	if hasPolLimiter {
		decision := polLimiter.Check(now, cost)
		return decision, p, nil
	}

	// 2. Fall back to local standalone limiter per client
	l, err := e.GetOrCreateLimiter(p, key.ClientID)
	if err != nil {
		return limiter.Decision{}, p, err
	}

	decision := l.Check(now, cost)
	return decision, p, nil
}

// UpdatePolicies updates policies atomically.
func (e *Engine) UpdatePolicies(newPolicies []config.PolicyDefinition) error {
	for _, p := range newPolicies {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("%w: %s", ErrInvalidPolicy, err.Error())
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies = make([]config.PolicyDefinition, len(newPolicies))
	copy(e.policies, newPolicies)
	return nil
}
