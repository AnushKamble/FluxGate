# FluxGate â€” Partition-Aware Distributed Rate Limiting & Quota Management

[![CI](https://github.com/AnushKamble/FluxGate/actions/workflows/ci.yml/badge.svg)](https://github.com/AnushKamble/FluxGate/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.22%2B-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Central Architectural Thesis**:  
> **Use finite, centrally accounted quota leases (escrowed token credits) for enforcement; use asynchronous CRDT replication for eventually consistent usage telemetry.**  
> Local request decisions NEVER perform a cross-node RPC. Nodes serve from valid local credits. When credits run out during communication failures, nodes throttle or reject requests rather than silently minting global quota. This guarantees zero quota overspend during network partitions.

---

## Architecture Overview

```mermaid
flowchart TD
    Client["API Client / Consumer"] -->|POST /api/v1/check| NodeHTTP["Node HTTP Gateway (/api/v1/check)"]
    
    subgraph FluxGateNode["FluxGate Gateway Node (Data Plane)"]
        NodeHTTP -->|1. Resolve Policy| PolicyEngine["Policy Engine"]
        PolicyEngine -->|2. Consume Credit| LocalLimiter["Local Limiter (Token Bucket / Lease)"]
        LocalLimiter -->|3. Record Allowed/Denied| TelemetryStore["CRDT Telemetry Store (G-Counter)"]
        LocalLimiter -->|4. Decision (200 / 429)| NodeHTTP
        
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

## Key Invariants

1. **Global Quota Conservation**:
   $$T_{\text{unallocated}} + \sum_{g \in \mathcal{G}} U(g) \le C_{\text{global}}$$
   where $U(g) = \text{Amount}(g) - \text{CumulativeConsumed}(g)$ is the conservative unconsumed upper bound.
2. **Conservative Refill Rule**:
   Refill is applied only to headroom that is demonstrably free. If unconfirmed grants saturate the global bucket, refill stops. When a late report arrives, refill begins **only from that observation time forward**; no retroactive tokens are minted for past unobserved intervals.
3. **Idempotent Reservations & Monotonic Reporting**:
   Replaying an existing `RequestID` returns the cached grant without double-deduction. Usage reports are monotonic cumulative sums ($\max(R_{\text{prev}}, u_{\text{new}})$) keyed by unique `GrantID`.
4. **Incarnation Isolation**:
   Each node instantiates a random 128-bit `IncarnationID` on boot. Grants issued to an older incarnation cannot be spent or reported by a restarted process.
5. **Decoupled Observation**:
   CRDT G-counters converge asynchronously via component-wise maximum merge. Telemetry state is strictly isolated from request authorization.

---

## Measured Benchmark Results

All figures measured empirically on **AMD Ryzen 9 8940HX (32 vCPUs), Windows amd64, Go 1.27.0**:

| Experiment | Description | Status | Measured Result |
|---|---|---|---|
| **Exp A: Local Limiter Baselines** | Sequential latency distributions and throughput | **PASS** | Mutex: **20.5M ops/s**, Atomic: **13.2M ops/s** |
| **Exp B: Distributed Conservation Invariant** | Concurrent load against 3 lease nodes with 0 refill | **PASS** | **0 Overspend** (Allowed 300 / 300) |
| **Exp C: Monotonic Report Accounting** | Replay duplicate, delayed, and out-of-order reports | **PASS** | Monotonicity verified; 0 double-counts |
| **Exp D: Partition, Depletion & Recovery** | Isolate node; exhaust lease; heal link | **PASS** | Spent 40 valid credits, denied 20, resumed post-heal |
| **Exp E: Uncoordinated Baseline Comparison** | 3 independent local buckets without central escrow | **PASS** | **+200% Overshoot** (300 allowed vs 100 quota) |
| **Exp F: CRDT Gossip Convergence** | 60% lossy gossip rounds across 3 nodes + heal | **PASS** | Fully converged to true aggregate (1000) |
| **Exp G: Node Restart Isolation** | Restarted node attempts to report stale grant | **PASS** | Rejected: `INCARNATION_MISMATCH` |
| **Exp H: Tenant Skew & High Churn** | 10k requests under Zipfian distribution | **PASS** | 0.19 MB memory growth; bounded labels |

---

## Quickstart

### 1. Interactive Demo (60-90s)
Run the self-contained live demo:
```bash
go run ./cmd/fluxgate-demo
```
or via make:
```bash
make demo
```

### 2. Running Local Multi-Node Cluster
Start the 1 Allocator + 3 Nodes cluster natively:
```powershell
# PowerShell:
./scripts/start-cluster.ps1

# Linux / macOS:
./scripts/start-cluster.sh
```

Send a test request:
```bash
curl -i -X POST http://localhost:8081/api/v1/check \
  -H "Content-Type: application/json" \
  -d '{"client_id":"acme","route":"/api/v1/charge","method":"POST","cost":1}'
```

Response:
```http
HTTP/1.1 200 OK
Content-Type: application/json
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 29
X-RateLimit-Reset: 1719000000

{
  "allowed": true,
  "client_id": "acme",
  "remaining": 29,
  "limit": 100,
  "node_id": "node-1",
  "mode": "normal"
}
```

Stop the cluster:
```powershell
./scripts/stop-cluster.ps1
```

### 3. Docker Compose
```bash
docker compose up --build
```

---

## Running Test Suites

```bash
# Run all unit and integration tests
go test -v ./...

# Run fault and chaos scenarios
go test -v ./tests/chaos/...

# Run all 8 reproducible experiments
go test -v ./tests/benchmarks/...

# Run hardening & regression edge-cases
go test -v ./tests/hardening/...
```

---

## Repository Documentation Index

- [Architecture Guide](docs/architecture.md) â€” Subsystem designs, component models, and data flows.
- [Formal Invariants](docs/invariants.md) â€” Mathematical proofs and conservation guarantees.
- [Wire Protocol Specification](docs/protocol.md) â€” gRPC schemas, HTTP endpoints, and RFC 6585 compliance.
- [Failure Modes & Degraded States](docs/failure-modes.md) â€” Partition, node crash, and recovery behaviors.
- [Benchmark Methodology](docs/benchmark-methodology.md) â€” Measurement methodology and percentile calculations.
- [Benchmark Report](docs/benchmark-report.md) â€” Exact machine-generated results and platform metadata.
- [Architecture Decision Log](docs/decision-log.md) â€” ADRs covering escrow design, reporting, and transport.
- [Limitations & Trade-offs](docs/limitations.md) â€” Candid analysis of safety vs availability and in-memory control plane.
- [Interactive Demo Walkthrough](docs/demo-script.md) â€” Step-by-step 60-90s scripted guide.
