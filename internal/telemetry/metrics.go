package telemetry

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// MetricsRegistry provides bounded-cardinality Prometheus-compatible metrics tracking.
type MetricsRegistry struct {
	mu sync.RWMutex

	// Counters
	httpRequestsTotal map[string]*uint64 // key: method#status_code#route_class
	decisionsTotal    map[string]*uint64 // key: decision#policy_class#reason
	grantsTotal       map[string]*uint64 // key: status
	evictionsTotal    uint64

	// Gauges
	localCreditsTotal uint64
	degradedMode      uint64 // 0 for normal, 1 for degraded
	activeGrants      uint64

	// Histograms / Latency aggregates (count and sum in microseconds)
	decisionDurationCount uint64
	decisionDurationSumUs uint64
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		httpRequestsTotal: make(map[string]*uint64),
		decisionsTotal:    make(map[string]*uint64),
		grantsTotal:       make(map[string]*uint64),
	}
}

// sanitizeRouteClass collapses arbitrary URLs into bounded route categories
// to prevent cardinality explosion.
func SanitizeRouteClass(route string) string {
	if route == "" || route == "/" {
		return "root"
	}
	if route == "/api/v1/check" {
		return "check"
	}
	if route == "/healthz" {
		return "healthz"
	}
	if route == "/readyz" {
		return "readyz"
	}
	if route == "/metrics" {
		return "metrics"
	}
	if route == "/debug/state" {
		return "debug"
	}
	return "other"
}

func (m *MetricsRegistry) IncHTTPRequest(method string, statusCode int, routeClass string) {
	key := fmt.Sprintf("%s#%d#%s", method, statusCode, routeClass)
	m.mu.RLock()
	ptr, exists := m.httpRequestsTotal[key]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		ptr, exists = m.httpRequestsTotal[key]
		if !exists {
			var val uint64
			ptr = &val
			m.httpRequestsTotal[key] = ptr
		}
		m.mu.Unlock()
	}

	atomic.AddUint64(ptr, 1)
}

func (m *MetricsRegistry) IncDecision(allowed bool, policyClass string, reason string) {
	decision := "allow"
	if !allowed {
		decision = "deny"
	}
	if policyClass == "" {
		policyClass = "default"
	}
	if reason == "" {
		reason = "none"
	}

	key := fmt.Sprintf("%s#%s#%s", decision, policyClass, reason)
	m.mu.RLock()
	ptr, exists := m.decisionsTotal[key]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		ptr, exists = m.decisionsTotal[key]
		if !exists {
			var val uint64
			ptr = &val
			m.decisionsTotal[key] = ptr
		}
		m.mu.Unlock()
	}

	atomic.AddUint64(ptr, 1)
}

func (m *MetricsRegistry) ObserveDecisionDuration(durationUs uint64) {
	atomic.AddUint64(&m.decisionDurationCount, 1)
	atomic.AddUint64(&m.decisionDurationSumUs, durationUs)
}

func (m *MetricsRegistry) SetLocalCredits(credits uint64) {
	atomic.StoreUint64(&m.localCreditsTotal, credits)
}

func (m *MetricsRegistry) SetDegradedMode(degraded bool) {
	if degraded {
		atomic.StoreUint64(&m.degradedMode, 1)
	} else {
		atomic.StoreUint64(&m.degradedMode, 0)
	}
}

func (m *MetricsRegistry) IncEvictions() {
	atomic.AddUint64(&m.evictionsTotal, 1)
}

// WritePrometheus formats all collected metrics into standard Prometheus exposition text.
func (m *MetricsRegistry) WritePrometheus(w io.Writer) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Fprintln(w, "# HELP fluxgate_http_requests_total Total HTTP requests handled.")
	fmt.Fprintln(w, "# TYPE fluxgate_http_requests_total counter")
	for k, ptr := range m.httpRequestsTotal {
		val := atomic.LoadUint64(ptr)
		var method, route string
		var status int
		fmt.Sscanf(k, "%s#%d#%s", &method, &status, &route)
		fmt.Fprintf(w, "fluxgate_http_requests_total{method=\"%s\",status=\"%d\",route_class=\"%s\"} %d\n", method, status, route, val)
	}

	fmt.Fprintln(w, "# HELP fluxgate_decisions_total Total rate limit decisions.")
	fmt.Fprintln(w, "# TYPE fluxgate_decisions_total counter")
	for k, ptr := range m.decisionsTotal {
		val := atomic.LoadUint64(ptr)
		var decision, policyClass, reason string
		fmt.Sscanf(k, "%s#%s#%s", &decision, &policyClass, &reason)
		fmt.Fprintf(w, "fluxgate_decisions_total{decision=\"%s\",policy_class=\"%s\",reason=\"%s\"} %d\n", decision, policyClass, reason, val)
	}

	fmt.Fprintln(w, "# HELP fluxgate_decision_duration_seconds Latency of rate limit evaluation.")
	fmt.Fprintln(w, "# TYPE fluxgate_decision_duration_seconds summary")
	count := atomic.LoadUint64(&m.decisionDurationCount)
	sumUs := atomic.LoadUint64(&m.decisionDurationSumUs)
	sumSec := float64(sumUs) / 1e6
	fmt.Fprintf(w, "fluxgate_decision_duration_seconds_count %d\n", count)
	fmt.Fprintf(w, "fluxgate_decision_duration_seconds_sum %f\n", sumSec)

	fmt.Fprintln(w, "# HELP fluxgate_local_credits Current spendable local credits.")
	fmt.Fprintln(w, "# TYPE fluxgate_local_credits gauge")
	fmt.Fprintf(w, "fluxgate_local_credits %d\n", atomic.LoadUint64(&m.localCreditsTotal))

	fmt.Fprintln(w, "# HELP fluxgate_node_degraded_mode 1 if node is in degraded/partitioned mode, 0 otherwise.")
	fmt.Fprintln(w, "# TYPE fluxgate_node_degraded_mode gauge")
	fmt.Fprintf(w, "fluxgate_node_degraded_mode %d\n", atomic.LoadUint64(&m.degradedMode))

	fmt.Fprintln(w, "# HELP fluxgate_evictions_total Total client limiters evicted.")
	fmt.Fprintln(w, "# TYPE fluxgate_evictions_total counter")
	fmt.Fprintf(w, "fluxgate_evictions_total %d\n", atomic.LoadUint64(&m.evictionsTotal))
}
