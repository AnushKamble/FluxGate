# FluxGate Architecture & Engineering Decision Log

## ADR-001: Separation of Enforcement (Escrowed Leases) and Telemetry (CRDT G-Counter)
- **Status**: Accepted
- **Context**: In a distributed rate-limiting system across multiple gateway nodes, making a remote RPC on every incoming API request causes latency inflation and tight availability coupling. Conversely, allowing uncoordinated local refill or relying on asynchronous gossip counters for authorization causes dangerous quota overspending during partitions or replication lag.
- **Decision**: 
  1. Quota enforcement is strictly based on finite, centrally accounted leases (escrowed token credits). Nodes can only spend credits explicitly transferred to their unique incarnation ID.
  2. Telemetry and observability use a grow-only counter (G-counter) CRDT replicated via asynchronous anti-entropy gRPC rounds.
  3. CRDT state is NEVER used as an authorization oracle to mint or extend quota credits.
- **Consequences**:
  - Positive: Guarantees global quota conservation invariant ($T_{\text{unallocated}} + \sum T_{\text{outstanding}} \le C$). Fast local request decision (no cross-node RPC on hot path).
  - Negative: During network partitions or communication delay, isolated nodes may exhaust their granted credits and deny legitimate traffic (stranded quota), trading availability for safety.

---

## ADR-002: Monotonic Cumulative Reporting Keyed by Grant ID
- **Status**: Accepted
- **Context**: Network packets can be duplicated, delayed, or reordered. If nodes report incremental deltas (e.g., "consumed 5 tokens"), packet retries or duplicates would lead to double-decrementing the allocator's outstanding balance, causing premature and illegal quota expansion.
- **Decision**: 
  - Nodes maintain a monotonic cumulative consumption counter per grant ID (`cumulative_consumed`).
  - Consumption reports send the current total consumed for that grant ID.
  - The allocator validates $0 \le \text{cumulative\_consumed} \le \text{grant\_amount}$ and updates the accepted consumption to $\max(\text{previous\_accepted}, \text{cumulative\_consumed})$.
  - Out-of-order or duplicate reports are strictly idempotent and cannot decrease or double-count consumption.
- **Consequences**:
  - Positive: Resilient to network reordering, duplication, and retries.
  - Negative: Allocator must track per-grant state until the grant expires, is fully consumed, or is reconciled.

---

## ADR-003: Node Incarnation Identity for Epoch Isolation
- **Status**: Accepted
- **Context**: If a node crashes and restarts, reusing the same node ID could allow stale or in-flight grants from the previous process incarnation to be spent or double-reported. Additionally, CRDT components would suffer collision if counter components restarted from 0.
- **Decision**:
  - Each node generates a cryptographically random / unique `IncarnationID` (UUID or high-entropy token) at process startup.
  - Grants issued by the allocator are bound to `(NodeID, IncarnationID)`. A restarted node cannot spend or report against grants issued to an older incarnation.
  - CRDT G-counter keys are compound: `NodeID#IncarnationID`.
- **Consequences**:
  - Positive: Eliminates ghost grants and stale component regressions after node restarts.
  - Negative: Unused credits remaining on a crashed node are stranded until allocator lease expiry/reset.

---

## ADR-004: Conservative Allocator Refill on Stale Reports
- **Status**: Accepted
- **Context**: If an isolated node does not report consumption, the allocator cannot prove whether those credits were spent or remain in the node's local bucket. If the allocator retroactively refilled tokens assuming the capacity was free, reconnecting the node could cause global capacity to be exceeded.
- **Decision**:
  - Outstanding grants are conservatively assumed to be fully unconsumed until a monotonic report proves otherwise: $\text{unconsumed} = \text{grant\_amount} - \text{cumulative\_consumed}$.
  - Global refill accumulates tokens only up to available headroom: $C - \sum \text{unconsumed}$. If outstanding grants saturate the global bucket, token accumulation halts.
  - When a late consumption report arrives, newly freed capacity is eligible for refill only from that observation instant forward. No retroactive refill is minted.
- **Consequences**:
  - Positive: Mathematically impossible to double-mint quota.
  - Negative: Reporting delays reduce overall cluster token utilization compared to an ideal instantaneous oracle.

---

## ADR-005: Deterministic In-Process Transport Wrapper for Fault Injection
- **Status**: Accepted
- **Context**: Docker-based traffic control (`tc`, `iptables`, `pumba`) requires root/privileged access and Docker daemon availability, which may not exist on all developer or CI environments.
- **Decision**:
  - Provide an injectable in-process gRPC transport wrapper / interceptor (`FaultTransport`) capable of simulating:
    - Symmetric and asymmetric link partitions.
    - Configurable artificial delay and jitter.
    - Packet drops (probability or deterministic sequence).
    - Message duplication and reordering.
  - Maintain a clean Docker Compose file for full multi-container environments.
- **Consequences**:
  - Positive: 100% deterministic, zero-privilege, cross-platform testability in unit and integration test suites.
