package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LeaseWorker manages background replenishment and cumulative usage reporting over gRPC.
type LeaseWorker struct {
	nodeID        string
	incarnationID string
	allocatorAddr string
	bucket        *LeaseBucket
	logger        *telemetry.Logger
	metrics       *telemetry.MetricsRegistry
	syncInterval  time.Duration
	client        fluxgatev1.QuotaAllocatorServiceClient
	conn          *grpc.ClientConn
	lastEpochID   string
	stopChan      chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	onDegraded    func(bool)
}

func NewLeaseWorker(
	nodeID string,
	incarnationID string,
	allocatorAddr string,
	bucket *LeaseBucket,
	logger *telemetry.Logger,
	metrics *telemetry.MetricsRegistry,
	syncInterval time.Duration,
	onDegraded func(bool),
) *LeaseWorker {
	if syncInterval <= 0 {
		syncInterval = 500 * time.Millisecond
	}
	return &LeaseWorker{
		nodeID:        nodeID,
		incarnationID: incarnationID,
		allocatorAddr: allocatorAddr,
		bucket:        bucket,
		logger:        logger,
		metrics:       metrics,
		syncInterval:  syncInterval,
		stopChan:      make(chan struct{}),
		onDegraded:    onDegraded,
	}
}

func (w *LeaseWorker) Start(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, w.allocatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		// Log warning, allow degraded mode
		w.logger.Warn("Initial connection to allocator failed; starting in degraded mode", "addr", w.allocatorAddr, "error", err)
		if w.onDegraded != nil {
			w.onDegraded(true)
		}
	} else {
		w.conn = conn
		w.client = fluxgatev1.NewQuotaAllocatorServiceClient(conn)
	}

	w.wg.Add(1)
	go w.runLoop(ctx)
	return nil
}

func (w *LeaseWorker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopChan)
		w.wg.Wait()
		if w.conn != nil {
			_ = w.conn.Close()
		}
	})
}

func (w *LeaseWorker) runLoop(ctx context.Context) {
	defer w.wg.Done()

	// Initial grant request
	w.renewLease(ctx)

	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.flushReports(context.Background())
			return
		case <-w.stopChan:
			w.flushReports(context.Background())
			return
		case <-w.bucket.LowWaterChannel():
			w.renewLease(ctx)
		case <-ticker.C:
			w.flushReports(ctx)
			if w.bucket.SpendableCredits() <= uint64(float64(w.bucket.batchSize)*w.bucket.lowWaterRatio) {
				w.renewLease(ctx)
			}
		}
	}
}

func (w *LeaseWorker) ensureConnection(ctx context.Context) bool {
	if w.client != nil && w.conn != nil {
		return true
	}
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, w.allocatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		if w.onDegraded != nil {
			w.onDegraded(true)
		}
		return false
	}
	w.conn = conn
	w.client = fluxgatev1.NewQuotaAllocatorServiceClient(conn)
	return true
}

func (w *LeaseWorker) renewLease(ctx context.Context) {
	if !w.ensureConnection(ctx) {
		return
	}

	// First flush reports so allocator can observe recent consumption and free headroom
	w.flushReports(ctx)

	// Generate random idempotency RequestID
	reqBytes := make([]byte, 16)
	_, _ = rand.Read(reqBytes)
	reqID := hex.EncodeToString(reqBytes)

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := w.client.AcquireGrant(reqCtx, &fluxgatev1.AcquireGrantRequest{
		RequestId:        reqID,
		NodeId:           w.nodeID,
		IncarnationId:    w.incarnationID,
		PolicyId:         w.bucket.policyID,
		PolicyVersion:    w.bucket.policyVersion,
		RequestedCredits: w.bucket.batchSize,
	})

	if err != nil {
		w.logger.Warn("Failed to acquire grant from allocator", "error", err)
		if w.onDegraded != nil {
			w.onDegraded(true)
		}
		return
	}

	// Check for epoch reset
	if w.lastEpochID != "" && resp.EpochId != "" && resp.EpochId != w.lastEpochID {
		w.logger.Warn("Cluster epoch changed; resetting local lease credits", "old_epoch", w.lastEpochID, "new_epoch", resp.EpochId)
		w.bucket.ResetCredits()
	}
	w.lastEpochID = resp.EpochId

	if resp.Status == fluxgatev1.GrantStatus_GRANT_STATUS_OK && resp.GrantedCredits > 0 {
		duration := time.Duration(resp.ValidDurationMs) * time.Millisecond
		w.bucket.IngestGrant(resp.GrantId, resp.GrantedCredits, duration)
		w.metrics.SetLocalCredits(w.bucket.SpendableCredits())
		if w.onDegraded != nil {
			w.onDegraded(false)
		}
		w.logger.Debug("Ingested lease grant", "grant_id", resp.GrantId, "credits", resp.GrantedCredits)
	} else if resp.Status == fluxgatev1.GrantStatus_GRANT_STATUS_EXHAUSTED {
		w.logger.Debug("Allocator pool exhausted; no credits granted")
	}
}

func (w *LeaseWorker) flushReports(ctx context.Context) {
	if !w.ensureConnection(ctx) {
		return
	}

	pending := w.bucket.PendingReports()
	if len(pending) == 0 {
		return
	}

	reqReports := make([]*fluxgatev1.GrantConsumptionReport, len(pending))
	for i, p := range pending {
		reqReports[i] = &fluxgatev1.GrantConsumptionReport{
			GrantId:            p.GrantID,
			CumulativeConsumed: p.CumulativeConsumed,
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := w.client.ReportConsumption(reqCtx, &fluxgatev1.ReportConsumptionRequest{
		NodeId:        w.nodeID,
		IncarnationId: w.incarnationID,
		Reports:       reqReports,
	})

	if err != nil {
		w.logger.Warn("Failed to report consumption to allocator", "error", err)
		if w.onDegraded != nil {
			w.onDegraded(true)
		}
	} else {
		if w.onDegraded != nil {
			w.onDegraded(false)
		}
		// Prune fully spent and acknowledged grants
		if resp != nil && len(resp.Results) > 0 {
			ackMap := make(map[string]uint64, len(resp.Results))
			for _, res := range resp.Results {
				if res.Status == fluxgatev1.ReportStatus_REPORT_STATUS_OK || res.Status == fluxgatev1.ReportStatus_REPORT_STATUS_STALE {
					ackMap[res.GrantId] = res.AcceptedCumulativeConsumed
				}
			}
			w.bucket.AcknowledgeReports(ackMap)
		}
	}
}
