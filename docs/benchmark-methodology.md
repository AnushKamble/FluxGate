# FluxGate Benchmark Methodology & Evaluation Framework

## 1. Principles of Statistical Honesty
Every performance metric reported in FluxGate is measured empirically on the local host. In accordance with the project specification:
1. No synthetic target numbers or proposal goals are reported as measured output.
2. Latency percentiles (p50, p95, p99, p99.9) are calculated from full sorted sample distributions across fixed iteration counts.
3. Every experiment records complete host metadata: CPU model, core count, OS, Go runtime version, Git commit SHA, fixed random seeds, and measurement duration.
4. Measurements clearly distinguish between data-plane in-memory evaluation latency and end-to-end HTTP network round-trip time.

---

## 2. Mandatory Experiments Matrix

### Experiment A — Local Limiter Algorithm Comparison
- **Objective**: Measure raw throughput, allocation profile, and latency percentiles (p50, p95, p99, p99.9) comparing Mutex Token Bucket, Atomic CAS Token Bucket, and Sliding Window Counter under single-threaded and 32-way parallel contention.
- **Metric**: Throughput (ops/sec), ns/op, B/op, allocs/op, latency percentiles.

### Experiment B — Distributed Leased Quota Invariant
- **Objective**: Verify that 3 nodes concurrently serving requests against finite leases never collectively exceed the global capacity and refill limits.
- **Metric**: Global credited overspend (target: 0). Total allowed requests vs global budget.

### Experiment C — Duplicate and Reordered Report Accounting
- **Objective**: Transmit duplicate, out-of-order, and delayed consumption reports over network transport; verify allocator monotonicity.
- **Metric**: Ledger consumption monotonicity errors (target: 0).

### Experiment D — Network Partition, Depletion, and Recovery
- **Objective**: Isolate one node from the allocator while high traffic arrives. Quantify traffic allowed from valid lease, traffic denied after depletion, stranded quota, and recovery latency.
- **Metric**: Allowed count, denied count, local refill violations (target: 0).

### Experiment E — Uncoordinated Local Baseline Comparison
- **Objective**: Run an intentionally uncoordinated baseline where each node gives itself full local capacity without central lease escrow. Prove and quantify global quota overshoot.
- **Metric**: Percentage overshoot over target global quota ($N \times \text{capacity}$).

### Experiment F — CRDT Telemetry Convergence under Packet Loss
- **Objective**: Measure G-counter convergence rounds and time after 50% simulated packet loss and message reordering across 3 nodes.
- **Metric**: Convergence delta (target: 0 divergence after heal), sync rounds.

### Experiment G — Node Restart & Incarnation Binding
- **Objective**: Abruptly restart a node holding active credits; verify new incarnation cannot spend or report old credits.
- **Metric**: Stale grant reuse attempts rejected (100%).

### Experiment H — Tenant Skew & High Cardinality Churn
- **Objective**: Stress the limiter with 100,000 distinct client IDs under a Zipf distribution (skewed hot tenant) and verify memory bounds and label cardinality safety.
- **Metric**: Memory growth (MB), metric label count, cache eviction rate.
