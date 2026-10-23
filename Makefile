# FluxGate Makefile

.PHONY: all build fmt vet test race run-allocator run-node integration bench-local bench-cluster demo clean proto

all: fmt vet test build

build:
	go build -v -o bin/fluxgate-node ./cmd/fluxgate-node
	go build -v -o bin/fluxgate-allocator ./cmd/fluxgate-allocator
	go build -v -o bin/fluxgate-load ./cmd/fluxgate-load

fmt:
	gofmt -s -w .

vet:
	go vet ./...

test:
	go test -v ./...

race:
	go test -v -race ./...

run-allocator:
	go run ./cmd/fluxgate-allocator -config configs/allocator.yaml

run-node:
	go run ./cmd/fluxgate-node -config configs/node1.yaml

integration:
	go test -v -race -run TestClusterIntegration ./tests/integration/...

bench-local:
	go test -v -benchmem -bench . ./internal/limiter/...

bench-cluster:
	go test -v -benchmem -bench . ./tests/benchmarks/...

demo:
	go run ./cmd/fluxgate-demo

clean:
	go clean
	rm -rf bin/
