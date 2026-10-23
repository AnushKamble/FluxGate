# FluxGate System Architecture

## 1. Overview and High-Level Architecture

FluxGate addresses the fundamental trade-off in distributed rate limiting: providing low-latency request decisions without cross-node network dependencies, while preventing independent gateway nodes from violating a shared global quota during network partitions.

The core architectural principle separates:
1. **Enforcement Plane**: Finite, centrally escrowed quota leases granted to specific node incarnations.
2. **Observation Plane**: Eventually consistent G-counter CRDT telemetry replicated asynchronously over gRPC.

```mermaid
flowchart TD
    Client["API Client / Consumer"] -->|POST /api/v1/check| NodeHTTP["Node HTTP Gateway (/api/v1/check)"]
    
    subgraph FluxGateNode["FluxGate Node (Data Plane)"]
        NodeHTTP -->|1. Match Policy| PolicyEngine["Policy Engine"]
        PolicyEngine -->|2. Check/Spend Lease Token| LocalLimiter["Local Limiter (Token Bucket / Lease)"]
        LocalLimiter -->|3. Record Allowed/Denied| TelemetryStore["CRDT Telemetry Store (G-Counter)"]
        LocalLimiter -->|4. Allow / Deny (429)| NodeHTTP
        
        subgraph BackgroundWorkers["Background Async Workers"]
            LeaseWorker["Lease Manager Worker"]
            GossipWorker["Anti-Entropy Gossip Worker"]
        end
        
        LocalLimiter -.->|Low-water trigger| LeaseWorker
        TelemetryStore -.->|Periodic anti-entropy| GossipWorker
    end
    
    subgraph ControlPlane["Quota Allocator (Control Plane)"]
        AllocatorServer["Allocator gRPC Service"]
        EscrowLedger["Escrow Ledger & Invariant Guard"]
        GlobalBucket["Global Refill Token Bucket"]
        
        AllocatorServer --> EscrowLedger
        EscrowLedger --> GlobalBucket
    end
    
    subgraph PeerNodes["Peer Cluster Nodes"]
        PeerNode1["Peer Node B"]
        PeerNode2["Peer Node C"]
    end
    
    LeaseWorker ==="Async Lease Request / Monotonic Report (gRPC)"===> AllocatorServer
    GossipWorker <.-."Asynchronous CRDT State Sync (gRPC)".-> PeerNode1
    GossipWorker <.-."Asynchronous CRDT State Sync (gRPC)".-> PeerNode2
```

---

## 2. Component Breakdown

### 2.1 FluxGate Gateway Node
Each node runs an HTTP server exposing:
- `POST /api/v1/check`: Rate limiting decision endpoint.
- `GET /healthz`: Liveness check.
- `GET /readyz`: Readiness check (verifies policy loaded and local limiter initialized).
- `GET /metrics`: Prometheus-compatible metrics endpoint.
- `GET /debug/state`: Constrained internal debugging snapshot.

#### Fast Data Path (Request Decision)
When an incoming request arrives at `POST /api/v1/check`:
1. **Validation & Normalization**: The payload is validated (length bounds, positive cost, structured fields).
2. **Policy Lookup**: The request matches a tenant key / route rule to determine capacity, refill rate, and burst limits.
3. **Local Credit Evaluation**: The local limiter checks if sufficient spendable lease tokens exist in the local bucket.
   - If available: decrements local spendable credits and returns `ALLOWED`.
   - If unavailable: returns `DENIED` with HTTP 429 and `Retry-After` headers.
4. **Local Telemetry Update**: Monotonically increments local G-counter metrics (`allowed`, `denied`).
5. **Zero Synchronous Network Calls**: The handler NEVER waits on an RPC to the allocator or peer nodes.

### 2.2 Quota Allocator (Control Plane)
The allocator is the central logical authority managing global quota:
- Maintains the authoritative global token bucket ($C$, $r$) per policy key.
- Manages an in-memory **Grant Ledger** tracking all active leases:
  - `GrantID`: Unique cryptographic identifier for each lease batch.
  - `RequestID`: Client-supplied idempotency key.
  - `NodeID` and `IncarnationID`: Identity of the recipient node.
  - `GrantedCredits`: Total credits transferred to the lease.
  - `CumulativeConsumed`: Highest acknowledged monotonic usage reported.
- Enforces the global escrow invariant before granting any credits.
- Reconciles cumulative usage reports and safely frees capacity without double-minting.

### 2.3 Lease Protocol (Background Worker)
Each node runs a background `LeaseWorker`:
- **Low-Water Mark Refill**: When local spendable credits drop below a configured threshold (e.g., 30% of target lease size), the worker asynchronously requests a new grant from the allocator over gRPC.
- **Monotonic Usage Reporting**: Periodically batches and transmits the cumulative credits consumed for each active grant.
- **Idempotency**: Retries with the same `RequestID` on network timeout or transient disconnection.

### 2.4 CRDT Telemetry (Anti-Entropy)
Each node maintains a Grow-Only Counter (G-counter) for cluster-wide observability:
- Keyed by compound component `NodeID#IncarnationID`.
- Periodically exchanges full state with peers over gRPC anti-entropy rounds.
- Merges states using component-wise maximum:
  $$\text{merged}[k] = \max(S_1[k], S_2[k])$$
- Aggregates cluster-wide total by summing all component values.
- **Critical Invariant**: Telemetry counts are strictly used for monitoring and drift detection; they are never used as authorization tokens.
