package fault

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FaultController provides zero-privilege in-process network fault injection.
type FaultController struct {
	mu            sync.RWMutex
	dropRate      float64
	delay         time.Duration
	duplicateRate float64
	partitions    map[string]bool // key: from + "->" + to
	rand          *rand.Rand
}

func NewFaultController(seed int64) *FaultController {
	return &FaultController{
		partitions: make(map[string]bool),
		rand:       rand.New(rand.NewSource(seed)),
	}
}

func (fc *FaultController) Partition(from, to string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.partitions[fmt.Sprintf("%s->%s", from, to)] = true
}

func (fc *FaultController) PartitionBidirectional(a, b string) {
	fc.Partition(a, b)
	fc.Partition(b, a)
}

func (fc *FaultController) Heal(from, to string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	delete(fc.partitions, fmt.Sprintf("%s->%s", from, to))
}

func (fc *FaultController) HealBidirectional(a, b string) {
	fc.Heal(a, b)
	fc.Heal(b, a)
}

func (fc *FaultController) HealAll() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.partitions = make(map[string]bool)
	fc.dropRate = 0
	fc.delay = 0
	fc.duplicateRate = 0
}

func (fc *FaultController) SetDelay(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.delay = d
}

func (fc *FaultController) SetDropRate(rate float64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.dropRate = rate
}

func (fc *FaultController) isPartitioned(from, to string) bool {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.partitions[fmt.Sprintf("%s->%s", from, to)]
}

// UnaryClientInterceptor returns a gRPC client interceptor that simulates network faults.
func (fc *FaultController) UnaryClientInterceptor(senderID, targetID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		fc.mu.RLock()
		delay := fc.delay
		dropRate := fc.dropRate
		isPart := fc.partitions[fmt.Sprintf("%s->%s", senderID, targetID)]
		fc.mu.RUnlock()

		if isPart {
			return status.Error(codes.Unavailable, fmt.Sprintf("simulated partition: %s -> %s severed", senderID, targetID))
		}

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if dropRate > 0 {
			fc.mu.Lock()
			r := fc.rand.Float64()
			fc.mu.Unlock()
			if r < dropRate {
				return status.Error(codes.DeadlineExceeded, "simulated packet drop")
			}
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
