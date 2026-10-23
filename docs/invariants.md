# FluxGate Formal Invariants and State Transitions

This document formalizes the mathematical and operational invariants enforced across the FluxGate system.

---

## 1. Local Token Bucket Invariant

For any local token bucket instance with capacity $C \in \mathbb{N}^+$, sustained refill rate $r \in \mathbb{R}^+$, current tokens $T$, and last refill timestamp $t_{\text{last}}$:

### Invariant 1.1: Bounded Token Range
$$\forall t \ge 0, \quad 0 \le T(t) \le C$$

### Invariant 1.2: Monotonic Elapsed Refill
For elapsed duration $\Delta t = t - t_{\text{last}}$ where $t \ge t_{\text{last}}$ measured via an injected monotonic clock:
$$T_{\text{refilled}} = \min\left(C, T + \Delta t \times r\right)$$
If an attempt with cost $w \in \mathbb{N}^+$ arrives:
$$\text{Decision} = \begin{cases}
\text{ALLOW}, & \text{if } T_{\text{refilled}} \ge w \implies T \leftarrow T_{\text{refilled}} - w \\
\text{DENY},  & \text{if } T_{\text{refilled}} < w \implies T \leftarrow T_{\text{refilled}}
\end{cases}$$
Cost $w \le 0$ is rejected at the API validation layer. Time delta $\Delta t < 0$ (clock rollback) is handled safely by clamping $\Delta t \leftarrow 0$.

---

## 2. Global Quota Conservation Invariant

The Quota Allocator guarantees that under all operational schedules, network partitions, message drops, and reorderings, the total global token budget is strictly conserved.

Let:
- $C_{\text{global}}$: Global bucket capacity for policy $P$.
- $r_{\text{global}}$: Global refill rate for policy $P$.
- $T_{\text{unallocated}}$: Authoritative unallocated tokens held in the allocator's central pool.
- $\mathcal{G}$: The set of all outstanding active grants for policy $P$.
- For each grant $g \in \mathcal{G}$:
  - $A(g)$: Granted credit amount.
  - $R(g)$: Highest verified cumulative consumption reported by the node.
  - $U(g) = A(g) - R(g)$: Conservative upper bound of unconsumed credits still spendable by the node.

### Invariant 2.1: Conservation of Issued and Unallocated Quota
$$T_{\text{unallocated}} + \sum_{g \in \mathcal{G}} U(g) \le C_{\text{global}}$$

### Invariant 2.2: Conservative Headroom Refill
The allocator replenishes $T_{\text{unallocated}}$ at rate $r_{\text{global}}$ only into currently provable headroom:
$$\text{MaxHeadroom} = C_{\text{global}} - \sum_{g \in \mathcal{G}} U(g)$$
$$T_{\text{unallocated}}(t) = \min\left(\text{MaxHeadroom}, T_{\text{unallocated}}(t_{\text{last}}) + \Delta t \times r_{\text{global}}\right)$$
> **Conservative Refill Rule**: If outstanding unconfirmed grants saturate the global bucket ($\sum U(g) = C_{\text{global}}$), headroom is 0 and no refill tokens accumulate. When a delayed consumption report arrives at time $t_{\text{obs}}$ proving that $k$ tokens were consumed, that headroom becomes eligible for refill **only from $t_{\text{obs}}$ forward**. The allocator NEVER mints retroactive refill for past unobserved intervals.

---

## 3. Idempotency & Monotonicity Invariants

### Invariant 3.1: One-Time Grant Reservation
For any grant request with client-generated idempotency key $K_{\text{req}}$:
$$\text{ReserveQuota}(K_{\text{req}}, \dots) = \begin{cases}
\text{NewGrant}(g), & \text{if } K_{\text{req}} \notin \text{Ledger} \\
\text{ExistingGrant}(g), & \text{if } K_{\text{req}} \in \text{Ledger}
\end{cases}$$
Replaying an existing $K_{\text{req}}$ returns the recorded response without allocating additional credits from $T_{\text{unallocated}}$.

### Invariant 3.2: Monotonic Cumulative Reporting
Let $R_k(g)$ be the $k$-th report received for grant $g$ with reported usage $u_k$:
$$R(g)_{k+1} = \max\left(R(g)_k, u_k\right), \quad \text{subject to } 0 \le u_k \le A(g)$$
- Any report with $u_k < 0$ or $u_k > A(g)$ is rejected as an invariant violation.
- Out-of-order reports ($u_k \le R(g)_k$) are acknowledged idempotently without altering $R(g)$.
- Duplicate reports cannot decrement outstanding balances twice.

---

## 4. Node Incarnation Isolation Invariant

Each node process instantiates a unique $\text{IncarnationID} \in \{0, 1\}^{128}$ at boot.

### Invariant 4.1: Incarnation Binding
A grant $g$ is spendable by and reportable by node $N$ if and only if:
$$\text{NodeID}(N) = \text{NodeID}(g) \quad \land \quad \text{IncarnationID}(N) = \text{IncarnationID}(g)$$
If node $N$ crashes and restarts with $\text{IncarnationID}' \ne \text{IncarnationID}(g)$:
- $N$ cannot spend remaining credits of $g$.
- $N$ cannot report usage for $g$.
- Unconsumed credits $U(g)$ remain safely accounted in the allocator until explicit cluster epoch reset or lease expiration.

---

## 5. CRDT Telemetry Invariants

FluxGate uses a State-based Grow-Only Counter (G-counter) for distributed telemetry across nodes.

Let $S \in \mathbb{N}^{\mathcal{K}}$ be the counter state map where keys $k = \text{NodeID} \# \text{IncarnationID}$.

### Invariant 5.1: Join-Semilattice Properties of Merge ($\oplus$)
1. **Commutativity**: $A \oplus B = B \oplus A$
2. **Associativity**: $(A \oplus B) \oplus C = A \oplus (B \oplus C)$
3. **Idempotence**: $A \oplus A = A$

Where $(A \oplus B)[k] = \max(A[k], B[k])$.

### Invariant 5.2: State Monotonicity
For any replica with state $S(t)$:
$$\forall t_2 \ge t_1, \quad \forall k \in \mathcal{K}, \quad S(t_2)[k] \ge S(t_1)[k]$$

### Invariant 5.3: Eventual Convergence
Under any network fault schedule with finite message delay once links heal:
$$\lim_{t \to \infty} S_{N_i}(t) = S^* \quad \forall i, j: S_{N_i}^* = S_{N_j}^*$$

### Invariant 5.4: Observation-Enforcement Barrier
$$\text{SpendableCredits}(N) \perp S_{\text{CRDT}}$$
CRDT state is strictly an observability artifact and is never read by the local rate limiter to authorize request decisions.
