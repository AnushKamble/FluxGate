# FluxGate Limitations, Trade-offs, and Non-Goals

This document provides a candid engineering analysis of the trade-offs, constraints, and non-goals of FluxGate.

---

## 1. Safety vs. Availability Trade-off in Partitions (Stranded Quota)

In distributed rate limiting, the CAP theorem and PACELC trade-off dictate that one cannot simultaneously have:
1. Exact global quota enforcement (zero overspend)
2. Unlimited availability during network partitions
3. Zero per-request network coordination

FluxGate explicitly chooses **Safety over Availability**:
- When a node is partitioned from the Quota Allocator, it consumes only the credits previously escrowed to its local lease bucket.
- Once these credits are depleted, the node **strictly denies (HTTP 429)** subsequent cost-bearing requests rather than uncoordinatedly minting credits.
- **Consequence**: Legitimate client requests may be rejected even if unallocated global capacity remains in the central allocator, or unused credits are stranded on other unreachable nodes.

---

## 2. In-Memory Control Plane & Epoch Reset Requirement

For educational and demonstration clarity on a developer laptop:
- The Quota Allocator maintains its Grant Ledger and unallocated token pool in memory.
- **Limitation**: If the Allocator process terminates abruptly, its in-memory record of active grants is lost.
- **Protocol Guarantee**: The Allocator generates a unique `ClusterEpochID` on boot. Gateway nodes detect this epoch transition and immediately reset local lease credit balances to 0 before acquiring fresh grants under the new epoch.
- **Non-Goal**: Production fault-tolerant distributed consensus (e.g. Raft) or durable WAL persistence for the control plane.

---

## 3. Telemetry Lag vs. Authorization

- CRDT G-counters converge asymptotically via periodic anti-entropy gossip over gRPC.
- During high network latency or gossip partitions, the aggregate count viewed on a single node reflects only observed increments.
- **Critical Guarantee**: The system NEVER uses CRDT state to authorize fresh quota tokens. Telemetry convergence provides cluster-wide operational visibility, drift detection, and traffic analysis, not distributed locking.

---

## 4. Single Policy Precedence Matching

- FluxGate evaluates requests against configured policies using deterministic precedence (exact client + exact route before prefix or wildcard).
- **Limitation**: A single incoming request matches the single highest-scoring winning policy. Layered multi-rule evaluation (e.g., checking both per-client and per-global-tier simultaneously) is not enabled by default to prevent partial credit spend on rejection.

---

## 5. Host Scope

- Tested and benchmarked for local processes and Docker Compose environments on modern multicore laptops/workstations.
- Not intended as a multi-region hyperscale edge gateway deployment.
