# FluxGate Failure Modes, Degraded States, and Recovery Matrix

This document provides a comprehensive analysis of failure modes in FluxGate, defining the system's exact operational behavior, invariant guarantees, and recovery procedures for each scenario.

---

## 1. Summary Matrix

| Failure Scenario | Enforcement Impact | Telemetry Impact | Invariant Protected | Recovery Action |
|---|---|---|---|---|
| **Allocator Partition** (Node isolated from Allocator) | Node spends remaining valid credits; when exhausted, **strictly denies (HTTP 429)** | None | Global Quota Conservation ($T_{\text{unallocated}} + \sum U(g) \le C$) | On reconnect, node sends monotonic cumulative consumption reports; renews leases |
| **Peer Gossip Partition** (Node isolated from Peers) | **Zero impact** on request allow/deny decisions | Replicated telemetry diverges during partition | Telemetry Monotonicity & Observation Barrier | When partition heals, anti-entropy merge converges component-wise max |
| **Node Crash & Restart** | Stale in-memory credits lost; node starts with 0 credits and new Incarnation ID | New CRDT component key created (`NodeID#NewIncarnation`) | Incarnation Isolation & No Counter Inflation | Node requests fresh grant with new Incarnation ID; stale credits stranded until epoch reset |
| **Allocator Crash / State Loss** | Allocator loses in-memory ledger and pool state | None | Requires coordinated cluster epoch reset to prevent double-minting | All nodes notified to discard existing grants and reconnect under new Allocator Epoch |
| **Duplicate / Retried Grant Request** | Zero duplicate tokens minted; returns existing grant result | None | Grant Idempotency ($K_{\text{req}} \to g$) | Safe automatic retry by client |
| **Out-of-Order / Duplicate Report** | Zero double-counting; acknowledged via monotonic max | None | Monotonic Cumulative Reporting | Allocator accepts $\max(R_{\text{prev}}, u_{\text{new}})$ |
| **Client ID Churn / High Cardinality** | Bounded local memory; cache eviction starts evicted keys with 0 credits | Telemetry bounded | Memory Safety & Denial of Service Resistance | Eviction counter increments; metric labels remain low-cardinality |

---

## 2. Detailed Scenario Analysis

### 2.1 Allocator Partition (Isolated Node)
- **Condition**: Network failure severs communication between Node $A$ and the Quota Allocator. Peer-to-peer and client traffic remain operational.
- **Behavior**:
  1. Node $A$ continues serving client requests using credits remaining in its local lease bucket.
  2. Background `LeaseWorker` attempts replenishment when credits fall below the low-water mark. Requests time out and back off exponentially.
  3. Node $A$ **strictly forbids local credit creation or uncoordinated refill**.
  4. When local credits reach 0, all subsequent cost-bearing requests are denied with `HTTP 429 Too Many Requests` and a `Retry-After` header.
  5. The node sets its health status to degraded and increments `fluxgate_node_degraded_mode_total`.
- **Healing**:
  1. Allocator connectivity is restored.
  2. `LeaseWorker` transmits pending cumulative consumption reports for active grants.
  3. Allocator acknowledges reports and frees headroom.
  4. Node $A$ acquires a fresh grant and resumes normal traffic processing.
- **Trade-off Analysis**: Safety is strictly preserved at the expense of availability (stranded quota utilization loss).

### 2.2 Peer Gossip Partition
- **Condition**: Communication between Node $A$ and Node $B$ is severed. Both can reach the Allocator and serve clients.
- **Behavior**:
  1. Node $A$ and Node $B$ continue making local rate-limiting decisions against their respective lease grants without hesitation.
  2. Anti-entropy gossip rounds fail with transport errors, triggering retries.
  3. G-counter states on Node $A$ and Node $B$ reflect only locally observed traffic increments; cluster-wide aggregate views on each node temporarily diverge.
- **Healing**:
  1. Network partition heals.
  2. The next anti-entropy sync round exchanges complete vector maps.
  3. Component-wise maximum merge ($\max(S_A[k], S_B[k])$) instantaneously converges both replicas to the exact true aggregate.
  4. No counter resets, lost counts, or double-counting occurs.

### 2.3 Node Process Crash and Restart
- **Condition**: Node process terminates abruptly (OOM, panic, host reboot) and restarts.
- **Behavior**:
  1. The node process initialises a new `IncarnationID` (random UUID).
  2. Local memory is clean; spendable credit balance is 0.
  3. The node **cannot reuse old grants** because its new `IncarnationID` does not match the `IncarnationID` registered with the allocator for previous grants.
  4. Unspent credits belonging to the crashed incarnation remain stranded in the allocator ledger as outstanding upper-bound reserves ($U(g)$).
  5. The restarted node issues an `AcquireGrant` with its new `IncarnationID`.
  6. In CRDT telemetry, the node writes to key `NodeID#NewIncarnationID`. Previous increments in `NodeID#OldIncarnationID` remain immutable in the semilattice, preventing regression of cluster telemetry.

### 2.4 Allocator Crash / State Loss
- **Condition**: The Allocator process crashes and restarts with clean in-memory state.
- **Risk**: If the Allocator reboots with initial capacity $C$ and immediately begins issuing grants while existing nodes still hold active grants issued prior to the crash, total issued credits would exceed $C$ (double-minting violation).
- **Mitigation & Epoch Protocol**:
  1. In-memory Allocator generates a unique `ClusterEpochID` on startup.
  2. Every response from the Allocator includes the current `ClusterEpochID`.
  3. Nodes detect an epoch change when communicating with the Allocator.
  4. Upon observing a new epoch, nodes immediately invalidate all existing local lease grants, drop outstanding credit balances to 0, reset their reporting state, and request fresh grants under the new epoch.
  5. This guarantees that un-accounted pre-crash credits are never spent concurrently with post-crash grants.
