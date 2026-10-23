package crdt

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestGCounterSemilatticeLaws(t *testing.T) {
	// Generate 3 random states
	genState := func(seed int64) map[string]uint64 {
		r := rand.New(rand.NewSource(seed))
		m := make(map[string]uint64)
		keys := []string{"n1#inc1", "n2#inc1", "n3#inc1"}
		for _, k := range keys {
			m[k] = uint64(r.Intn(1000))
		}
		return m
	}

	A := genState(10)
	B := genState(20)
	C := genState(30)

	// 1. Commutativity: merge(A, B) == merge(B, A)
	cAB := NewGCounter()
	cAB.Merge(A)
	cAB.Merge(B)

	cBA := NewGCounter()
	cBA.Merge(B)
	cBA.Merge(A)

	if !reflect.DeepEqual(cAB.Snapshot(), cBA.Snapshot()) {
		t.Fatalf("Commutativity violated! merge(A,B) != merge(B,A)")
	}

	// 2. Associativity: merge(merge(A, B), C) == merge(A, merge(B, C))
	cAB_C := NewGCounter()
	cAB_C.Merge(cAB.Snapshot())
	cAB_C.Merge(C)

	cBC := NewGCounter()
	cBC.Merge(B)
	cBC.Merge(C)

	cA_BC := NewGCounter()
	cA_BC.Merge(A)
	cA_BC.Merge(cBC.Snapshot())

	if !reflect.DeepEqual(cAB_C.Snapshot(), cA_BC.Snapshot()) {
		t.Fatalf("Associativity violated! (A+B)+C != A+(B+C)")
	}

	// 3. Idempotence: merge(A, A) == A
	cAA := NewGCounter()
	cAA.Merge(A)
	cAA.Merge(A)

	if !reflect.DeepEqual(cAA.Snapshot(), A) {
		t.Fatalf("Idempotence violated! merge(A,A) != A")
	}

	// 4. Monotonicity: merge with stale state never decreases components
	stale := map[string]uint64{"n1#inc1": 1, "n2#inc1": 1}
	cCurrent := NewGCounter()
	cCurrent.Merge(A)
	sumBefore := cCurrent.Sum()
	cCurrent.Merge(stale)
	sumAfter := cCurrent.Sum()
	if sumAfter < sumBefore {
		t.Fatalf("Monotonicity violated! Sum decreased after merge: %d < %d", sumAfter, sumBefore)
	}
}

func TestIncarnationRestartIsolation(t *testing.T) {
	// Node 1 incarnation 1 produces counts
	store1 := NewTelemetryStore("node-1", "inc-1")
	store1.IncAllowed(50)

	// Node 1 crashes and restarts with incarnation 2
	store2 := NewTelemetryStore("node-1", "inc-2")
	store2.IncAllowed(20)

	// Peer merges both states
	peerStore := NewTelemetryStore("node-2", "inc-1")
	a1, d1, c1 := store1.Snapshot()
	peerStore.MergeRemote(a1, d1, c1)

	a2, d2, c2 := store2.Snapshot()
	peerStore.MergeRemote(a2, d2, c2)

	// Total allowed must be 50 + 20 = 70 without component collision
	if peerStore.AllowedTotal() != 70 {
		t.Fatalf("expected 70 allowed across incarnations, got %d", peerStore.AllowedTotal())
	}
}

func TestGossipConvergenceUnderPacketLoss(t *testing.T) {
	// 3 replicas
	n1 := NewTelemetryStore("node-1", "inc-1")
	n2 := NewTelemetryStore("node-2", "inc-1")
	n3 := NewTelemetryStore("node-3", "inc-1")

	// Each node records traffic
	n1.IncAllowed(100)
	n2.IncAllowed(250)
	n3.IncAllowed(400)

	nodes := []*TelemetryStore{n1, n2, n3}
	r := rand.New(rand.NewSource(99))

	// Simulate chaotic gossip rounds: 50% packet drop
	for round := 0; round < 30; round++ {
		senderIdx := r.Intn(3)
		receiverIdx := r.Intn(3)
		if senderIdx == receiverIdx {
			continue
		}

		// 50% packet drop
		if r.Float32() < 0.5 {
			continue
		}

		a, d, c := nodes[senderIdx].Snapshot()
		nodes[receiverIdx].MergeRemote(a, d, c)
	}

	// Final heal round: full state exchanged across all pairs
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i != j {
				a, d, c := nodes[i].Snapshot()
				nodes[j].MergeRemote(a, d, c)
			}
		}
	}

	// All 3 replicas must converge to exactly 100 + 250 + 400 = 750
	const expected = uint64(750)
	for i, n := range nodes {
		if n.AllowedTotal() != expected {
			t.Fatalf("Replica %d did not converge! Got %d, expected %d", i+1, n.AllowedTotal(), expected)
		}
	}
}
