#!/usr/bin/env bash
set -e

mkdir -p bin
go build -o bin/fluxgate-allocator ./cmd/fluxgate-allocator
go build -o bin/fluxgate-node ./cmd/fluxgate-node

./bin/fluxgate-allocator -config configs/allocator.yaml &
ALLOCATOR_PID=$!
sleep 1

./bin/fluxgate-node -config configs/node1.yaml &
NODE1_PID=$!

./bin/fluxgate-node -config configs/node2.yaml &
NODE2_PID=$!

./bin/fluxgate-node -config configs/node3.yaml &
NODE3_PID=$!

echo "{\"allocator\":$ALLOCATOR_PID,\"node1\":$NODE1_PID,\"node2\":$NODE2_PID,\"node3\":$NODE3_PID}" > .cluster-pids.json
echo "Cluster started: Allocator ($ALLOCATOR_PID), Node1 ($NODE1_PID), Node2 ($NODE2_PID), Node3 ($NODE3_PID)"
