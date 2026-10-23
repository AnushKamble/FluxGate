package crdt

import (
	"sync"
)

// TelemetryStore maintains distinct G-Counters for different event types.
// It enforces the normative requirement that allowed requests, denied requests,
// and consumed credits are separate CRDT counters and not conflated.
type TelemetryStore struct {
	mu              sync.RWMutex
	componentKey    string
	allowedRequests *GCounter
	deniedRequests  *GCounter
	consumedCredits *GCounter
}

func NewTelemetryStore(nodeID, incarnationID string) *TelemetryStore {
	key := nodeID + "#" + incarnationID
	return &TelemetryStore{
		componentKey:    key,
		allowedRequests: NewGCounter(),
		deniedRequests:  NewGCounter(),
		consumedCredits: NewGCounter(),
	}
}

func (s *TelemetryStore) ComponentKey() string {
	return s.componentKey
}

func (s *TelemetryStore) IncAllowed(amount uint64) {
	s.allowedRequests.Increment(s.componentKey, amount)
}

func (s *TelemetryStore) IncDenied(amount uint64) {
	s.deniedRequests.Increment(s.componentKey, amount)
}

func (s *TelemetryStore) IncConsumed(amount uint64) {
	s.consumedCredits.Increment(s.componentKey, amount)
}

func (s *TelemetryStore) AllowedTotal() uint64 {
	return s.allowedRequests.Sum()
}

func (s *TelemetryStore) DeniedTotal() uint64 {
	return s.deniedRequests.Sum()
}

func (s *TelemetryStore) ConsumedTotal() uint64 {
	return s.consumedCredits.Sum()
}

func (s *TelemetryStore) Snapshot() (allowed, denied, consumed map[string]uint64) {
	return s.allowedRequests.Snapshot(), s.deniedRequests.Snapshot(), s.consumedCredits.Snapshot()
}

func (s *TelemetryStore) MergeRemote(allowed, denied, consumed map[string]uint64) {
	s.allowedRequests.Merge(allowed)
	s.deniedRequests.Merge(denied)
	s.consumedCredits.Merge(consumed)
}
