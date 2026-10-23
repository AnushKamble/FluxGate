# FluxGate Empirical Benchmark & Experiment Report

**Executed At:** `2026-10-23T18:32:13Z`  
**Host Platform:** `windows / amd64`  
**CPU Cores:** `32`  
**Go Version:** `go1.27.0`  
**Git Revision:** `59e67ccfe5d50373c01ac45bd8bb783a1634eba4`  

---

## Summary Table

| Experiment | Description | Status | Key Measured Result |
|---|---|---|---|
| **Experiment A: Local Algorithm Baselines** | Sequential latency distributions (p50, p95, p99, p99.9) and throughput for local limiters. | **PASS** | Mutex: 21610875 ops/s, Atomic: 11788838 ops/s |
| **Experiment B: Distributed Credit Conservation Invariant** | Concurrent load against 3 lease-backed nodes with zero refill. Proves global overspend is exactly 0. | **PASS** | Overspend: 0 (Allowed: 300 / 300) |
| **Experiment C: Duplicate and Out-of-Order Report Accounting** | Demonstrates monotonic cumulative reporting resilience to duplicate and reordered deliveries. | **PASS** | Monotonicity Verified: true, Outstanding: 5 |
| **Experiment D: Partition, Depletion, Heal, and Recovery** | Measures traffic allowed from cached lease, bounded denials after exhaustion, and recovery upon heal. | **PASS** | Allowed: 40, Denied: 20, Post-Heal: 10 |
| **Experiment E: Uncoordinated Local Baseline Comparison** | Demonstrates how independent uncoordinated limiters cause N-fold quota overshoot (200% overshoot across 3 nodes). | **PASS** | Unsafe Overshoot: +200% (300 allowed vs 100 capacity) |
| **Experiment F: CRDT Telemetry Convergence under Packet Loss** | Measures G-counter eventual convergence after 60% lossy gossip rounds followed by anti-entropy heal. | **PASS** | Converged: true (Aggregate: 1000 across 3 replicas) |
| **Experiment G: Node Restart and Incarnation Binding** | Verifies that restarting a node generates a new incarnation that cannot spend or report stale grants. | **PASS** | Stale Grant Rejected: true (INCARNATION_MISMATCH) |
| **Experiment H: Tenant Skew and High Cardinality Churn** | Simulates 10,000 requests under Zipfian tenant skew; evaluates memory footprint and cardinality stability. | **PASS** | 10k requests completed safely; Mem: 0.19 MB |
