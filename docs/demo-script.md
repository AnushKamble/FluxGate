# FluxGate Interactive Demo Script (60–90 Seconds)

This script guides a reviewer through an interactive or automated demonstration of FluxGate.

---

## Quick Run

Run the automated standalone demo:
```bash
go run ./cmd/fluxgate-demo
```
or via Makefile:
```bash
make demo
```

---

## Step-by-Step Walkthrough

### Step 1: Cluster Initialization (0:00 - 0:15)
Start the Quota Allocator and 3 Gateway Nodes:
```bash
# In PowerShell:
./scripts/start-cluster.ps1

# In Bash:
./scripts/start-cluster.sh
```
Verify all nodes are healthy and ready:
```bash
curl http://localhost:8081/readyz
curl http://localhost:8082/readyz
curl http://localhost:8083/readyz
```

---

### Step 2: Normal Traffic Load Across All Nodes (0:15 - 0:35)
Send 30 concurrent API requests across Node 1, Node 2, and Node 3:
```bash
curl -X POST http://localhost:8081/api/v1/check -H "Content-Type: application/json" -d '{"client_id":"acme","route":"/api/v1/charge","cost":1}'
```
**Observation**:
- Every request decision is made locally in sub-millisecond time ($< 100\,\mu\text{s}$) with zero per-request remote RPCs.
- `X-RateLimit-*` headers reflect decremented local lease balances.
- Background lease workers automatically renew quota batches when low-water marks are crossed.

---

### Step 3: Network Partition & Safe Denial (0:35 - 0:55)
Isolate Node 1 from the Allocator and peers.
Send requests to Node 1 until its cached lease is depleted:
- Requests $1 \dots \text{Credits}$: Return `HTTP 200 OK`.
- Request $\text{Credits} + 1$: Returns `HTTP 429 Too Many Requests` with `Retry-After`.
**Key Distributed Systems Invariant**:
- The isolated node strictly denies traffic.
- It **never** mints quota locally or falls back to an uncoordinated refill rate.

---

### Step 4: Partition Healing & Telemetry Convergence (0:55 - 1:15)
Restore network connectivity to Node 1.
- Node 1 flushes its monotonic cumulative consumption report to the Allocator over gRPC.
- Allocator acknowledges the report, safely freeing headroom from the observation time forward.
- Anti-entropy gossip rounds merge G-counter telemetry maps across all 3 nodes.
- All replicas converge to identical aggregate allowed and denied request counts.

---

### Step 5: Counterexample from Uncoordinated Baseline (1:15 - 1:30)
Compare with the uncoordinated baseline:
- If 3 nodes operate without central escrow, each allowing up to a local capacity of 100, the cluster permits **300 requests** against an intended global target of 100 (a **200% quota overshoot**).
- FluxGate's finite lease escrow eliminates this overshoot entirely (**0 overspend**).

---

### Step 6: Cluster Teardown
```bash
./scripts/stop-cluster.ps1
```
