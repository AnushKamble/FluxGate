package gossip

import (
	"context"
	"sync"
	"time"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/crdt"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GossipWorker handles asynchronous periodic peer-to-peer anti-entropy rounds.
type GossipWorker struct {
	nodeID        string
	incarnationID string
	peers         []string
	store         *crdt.TelemetryStore
	interval      time.Duration
	logger        *telemetry.Logger
	stopChan      chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	peerConns     map[string]*grpc.ClientConn
	mu            sync.Mutex
}

func NewGossipWorker(
	nodeID string,
	incarnationID string,
	peers []string,
	store *crdt.TelemetryStore,
	interval time.Duration,
	logger *telemetry.Logger,
) *GossipWorker {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &GossipWorker{
		nodeID:        nodeID,
		incarnationID: incarnationID,
		peers:         peers,
		store:         store,
		interval:      interval,
		logger:        logger,
		stopChan:      make(chan struct{}),
		peerConns:     make(map[string]*grpc.ClientConn),
	}
}

func (gw *GossipWorker) Start(ctx context.Context) {
	gw.wg.Add(1)
	go gw.run(ctx)
}

func (gw *GossipWorker) Stop() {
	gw.stopOnce.Do(func() {
		close(gw.stopChan)
		gw.wg.Wait()

		gw.mu.Lock()
		defer gw.mu.Unlock()
		for _, conn := range gw.peerConns {
			_ = conn.Close()
		}
	})
}

func (gw *GossipWorker) run(ctx context.Context) {
	defer gw.wg.Done()
	ticker := time.NewTicker(gw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gw.stopChan:
			return
		case <-ticker.C:
			gw.syncAllPeers(ctx)
		}
	}
}

func (gw *GossipWorker) getPeerClient(peerAddr string) (fluxgatev1.GossipServiceClient, error) {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	if conn, exists := gw.peerConns[peerAddr]; exists {
		return fluxgatev1.NewGossipServiceClient(conn), nil
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, peerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	gw.peerConns[peerAddr] = conn
	return fluxgatev1.NewGossipServiceClient(conn), nil
}

func (gw *GossipWorker) syncAllPeers(ctx context.Context) {
	allowed, denied, consumed := gw.store.Snapshot()

	for _, peer := range gw.peers {
		client, err := gw.getPeerClient(peer)
		if err != nil {
			gw.logger.Debug("Peer unreachable for gossip", "peer", peer, "error", err)
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		resp, err := client.SyncTelemetry(reqCtx, &fluxgatev1.SyncTelemetryRequest{
			SenderNodeId:        gw.nodeID,
			SenderIncarnationId: gw.incarnationID,
			AllowedRequests:     &fluxgatev1.GCounterState{Components: allowed},
			DeniedRequests:      &fluxgatev1.GCounterState{Components: denied},
			ConsumedCredits:     &fluxgatev1.GCounterState{Components: consumed},
		})
		cancel()

		if err != nil {
			gw.logger.Debug("Gossip sync failed with peer", "peer", peer, "error", err)
			continue
		}

		// Merge peer's state into local store
		var pAllowed, pDenied, pConsumed map[string]uint64
		if resp.AllowedRequests != nil {
			pAllowed = resp.AllowedRequests.Components
		}
		if resp.DeniedRequests != nil {
			pDenied = resp.DeniedRequests.Components
		}
		if resp.ConsumedCredits != nil {
			pConsumed = resp.ConsumedCredits.Components
		}

		gw.store.MergeRemote(pAllowed, pDenied, pConsumed)
	}
}
