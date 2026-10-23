package allocator

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrDuplicateRequestID     = errors.New("request id already exists")
	ErrGrantNotFound          = errors.New("grant not found")
	ErrIncarnationMismatch    = errors.New("grant incarnation does not match node incarnation")
	ErrReportExceedsGrant     = errors.New("reported cumulative consumption exceeds grant amount")
	ErrReportNonMonotonic     = errors.New("reported consumption is lower than previously accepted consumption")
	ErrPolicyVersionMismatch  = errors.New("policy version mismatch")
	ErrInsufficientHeadroom   = errors.New("insufficient global quota available")
	ErrNegativeConsumption    = errors.New("reported consumption cannot be negative")
)

// Grant represents an escrowed credit allocation to a specific node incarnation.
type Grant struct {
	GrantID            string
	RequestID          string
	NodeID             string
	IncarnationID      string
	PolicyID           string
	PolicyVersion      uint32
	Amount             uint64
	CumulativeConsumed uint64
	CreatedAt          time.Time
	ValidDuration      time.Duration
}

// UnconsumedUpperBound returns the maximum unconsumed tokens that could still be spent.
func (g *Grant) UnconsumedUpperBound() uint64 {
	if g.CumulativeConsumed >= g.Amount {
		return 0
	}
	return g.Amount - g.CumulativeConsumed
}

// Ledger maintains active grants and provides idempotency indexing.
type Ledger struct {
	mu            sync.RWMutex
	grantsByID    map[string]*Grant
	grantsByReqID map[string]*Grant // Idempotency map
	grantsByPol   map[string]map[string]*Grant
}

func NewLedger() *Ledger {
	return &Ledger{
		grantsByID:    make(map[string]*Grant),
		grantsByReqID: make(map[string]*Grant),
		grantsByPol:   make(map[string]map[string]*Grant),
	}
}

// LookupByRequestID retrieves a previous grant result for idempotency.
func (l *Ledger) LookupByRequestID(requestID string) (*Grant, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	g, ok := l.grantsByReqID[requestID]
	return g, ok
}

// LookupByGrantID retrieves a grant by its unique ID.
func (l *Ledger) LookupByGrantID(grantID string) (*Grant, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	g, ok := l.grantsByID[grantID]
	return g, ok
}

// AddGrant inserts a newly issued grant into the ledger.
func (l *Ledger) AddGrant(g *Grant) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.grantsByReqID[g.RequestID]; exists {
		return ErrDuplicateRequestID
	}
	if _, exists := l.grantsByID[g.GrantID]; exists {
		return fmt.Errorf("grant ID collision: %s", g.GrantID)
	}

	l.grantsByID[g.GrantID] = g
	l.grantsByReqID[g.RequestID] = g

	if l.grantsByPol[g.PolicyID] == nil {
		l.grantsByPol[g.PolicyID] = make(map[string]*Grant)
	}
	l.grantsByPol[g.PolicyID][g.GrantID] = g
	return nil
}

// SumOutstandingUpperBounds calculates the sum of all unconsumed upper bounds for a policy.
func (l *Ledger) SumOutstandingUpperBounds(policyID string) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	grants := l.grantsByPol[policyID]
	var sum uint64
	for _, g := range grants {
		sum += g.UnconsumedUpperBound()
	}
	return sum
}

// ActiveGrantCount returns the total number of grants tracked.
func (l *Ledger) ActiveGrantCount(policyID string) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.grantsByPol[policyID]))
}
