# FluxGate Protocols & Interface Specifications

This document specifies the wire protocols, message schemas, and state transition semantics for FluxGate.

---

## 1. gRPC Quota Lease Protocol

The Lease Protocol operates between FluxGate gateway nodes and the central Quota Allocator over gRPC.

### 1.1 Service Definition
```protobuf
syntax = "proto3";

package fluxgate.v1;

option go_package = "fluxgate/api/proto/fluxgate/v1;fluxgatev1";

service QuotaAllocatorService {
  rpc AcquireGrant(AcquireGrantRequest) returns (AcquireGrantResponse);
  rpc ReportConsumption(ReportConsumptionRequest) returns (ReportConsumptionResponse);
  rpc GetAllocatorStatus(GetAllocatorStatusRequest) returns (GetAllocatorStatusResponse);
}
```

### 1.2 Message Schemas

#### `AcquireGrant`
```protobuf
message AcquireGrantRequest {
  string request_id = 1;         // Client-generated UUID for idempotency
  string node_id = 2;            // Unique node identifier (e.g., "node-1")
  string incarnation_id = 3;     // Random UUID generated at node startup
  string policy_id = 4;          // Target policy identifier
  uint32 policy_version = 5;     // Node's current policy version
  uint64 requested_credits = 6;  // Number of credits requested (batch size)
}

enum GrantStatus {
  GRANT_STATUS_UNSPECIFIED = 0;
  GRANT_STATUS_OK = 1;
  GRANT_STATUS_EXHAUSTED = 2;
  GRANT_STATUS_POLICY_MISMATCH = 3;
  GRANT_STATUS_INVALID_REQUEST = 4;
}

message AcquireGrantResponse {
  string request_id = 1;
  string grant_id = 2;           // Allocator-assigned unique grant ID
  uint64 granted_credits = 3;    // Number of credits allocated (0 <= granted <= requested)
  GrantStatus status = 4;
  string message = 5;
  uint64 valid_duration_ms = 6;  // Lease validity window in milliseconds
}
```

#### `ReportConsumption`
```protobuf
message GrantConsumptionReport {
  string grant_id = 1;
  uint64 cumulative_consumed = 2; // Monotonic cumulative count of tokens spent from this grant
}

message ReportConsumptionRequest {
  string node_id = 1;
  string incarnation_id = 2;
  repeated GrantConsumptionReport reports = 3;
}

enum ReportStatus {
  REPORT_STATUS_UNSPECIFIED = 0;
  REPORT_STATUS_OK = 1;
  REPORT_STATUS_STALE = 2;
  REPORT_STATUS_GRANT_NOT_FOUND = 3;
  REPORT_STATUS_EXCEEDS_GRANT = 4;
  REPORT_STATUS_INCARNATION_MISMATCH = 5;
}

message ReconciledGrant {
  string grant_id = 1;
  uint64 accepted_cumulative_consumed = 2;
  ReportStatus status = 3;
  string error_message = 4;
}

message ReportConsumptionResponse {
  repeated ReconciledGrant results = 1;
}
```

#### `GetAllocatorStatus`
```protobuf
message PolicyStatus {
  string policy_id = 1;
  uint32 policy_version = 2;
  uint64 capacity = 3;
  double refill_rate = 4;
  uint64 unallocated_tokens = 5;
  uint64 outstanding_credits = 6;
  uint64 total_grants_active = 7;
}

message GetAllocatorStatusRequest {}

message GetAllocatorStatusResponse {
  string status = 1;
  repeated PolicyStatus policies = 2;
  bool conservation_invariant_satisfied = 3;
}
```

---

## 2. gRPC CRDT Anti-Entropy Gossip Protocol

The Telemetry Protocol operates peer-to-peer among FluxGate nodes to replicate observation state.

### 2.1 Service Definition
```protobuf
syntax = "proto3";

package fluxgate.v1;

option go_package = "fluxgate/api/proto/fluxgate/v1;fluxgatev1";

service GossipService {
  rpc SyncTelemetry(SyncTelemetryRequest) returns (SyncTelemetryResponse);
}

message GCounterState {
  // Key: NodeID#IncarnationID, Value: cumulative count
  map<string, uint64> components = 1;
}

message SyncTelemetryRequest {
  string sender_node_id = 1;
  string sender_incarnation_id = 2;
  GCounterState allowed_requests = 3;
  GCounterState denied_requests = 4;
  GCounterState consumed_credits = 5;
}

message SyncTelemetryResponse {
  string receiver_node_id = 1;
  GCounterState allowed_requests = 2;
  GCounterState denied_requests = 3;
  GCounterState consumed_credits = 4;
}
```

---

## 3. HTTP Data-Plane Protocol

Client applications communicate with gateway nodes via HTTP/JSON.

### 3.1 `POST /api/v1/check`

#### Request
```http
POST /api/v1/check HTTP/1.1
Host: node-1:8080
Content-Type: application/json

{
  "client_id": "customer-abc",
  "route": "/api/payments/charge",
  "method": "POST",
  "cost": 1
}
```

#### Allow Response (HTTP 200 OK)
```http
HTTP/1.1 200 OK
Content-Type: application/json
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 42
X-RateLimit-Reset: 1719000060

{
  "allowed": true,
  "client_id": "customer-abc",
  "remaining": 42,
  "limit": 100,
  "node_id": "node-1"
}
```

#### Deny Response (HTTP 429 Too Many Requests)
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 3
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1719000063

{
  "allowed": false,
  "reason": "rate_limited",
  "client_id": "customer-abc",
  "remaining": 0,
  "limit": 100,
  "retry_after_seconds": 3,
  "node_id": "node-1"
}
```

---

## 4. State Transition Walkthroughs

### 4.1 Grant Acquisition
1. Node detects local spendable credits for Policy $P$ fall below Low-Water Mark (e.g. $\le 30\%$ batch target).
2. Node generates a new UUID `RequestID`.
3. Node invokes `AcquireGrant(RequestID, NodeID, IncarnationID, PolicyID, Version, Requested=20)`.
4. Allocator locks policy state, checks idempotency map for `RequestID`.
5. Allocator calculates provable headroom: $\text{Headroom} = C - \sum U(g)$.
6. Allocator refills $T_{\text{unallocated}}$ up to $\text{Headroom}$ based on monotonic elapsed clock.
7. Allocator grants $A = \min(\text{Requested}, T_{\text{unallocated}})$.
8. Allocator deducts $A$ from $T_{\text{unallocated}}$, registers grant $g$ in ledger with $R(g)=0$.
9. Allocator returns `GrantID`, $A$.
10. Node receives grant and adds $A$ tokens to its local spendable pool.

### 4.2 Consumption and Cumulative Reporting
1. Client requests arrive at Node and consume local credits ($u \leftarrow u + 1$).
2. Background lease reporter gathers active grants and cumulative consumed counts.
3. Node transmits `ReportConsumption` with `{grant_id: g1, cumulative_consumed: 15}`.
4. Allocator verifies $15 \le A(g_1)$ and $15 \ge R(g_1)$.
5. Allocator updates $R(g_1) \leftarrow 15$. This decreases $U(g_1) = A(g_1) - 15$, instantly freeing 15 units of global headroom for subsequent refill from current time forward.
6. Allocator responds with status `REPORT_STATUS_OK`.
