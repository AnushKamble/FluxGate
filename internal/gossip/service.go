package gossip

import (
	"context"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/crdt"
	"fluxgate/internal/telemetry"
)

// Service implements the gRPC GossipServiceServer for CRDT state exchange.
type Service struct {
	fluxgatev1.UnimplementedGossipServiceServer
	nodeID        string
	incarnationID string
	store         *crdt.TelemetryStore
	logger        *telemetry.Logger
}

func NewService(nodeID, incarnationID string, store *crdt.TelemetryStore, logger *telemetry.Logger) *Service {
	return &Service{
		nodeID:        nodeID,
		incarnationID: incarnationID,
		store:         store,
		logger:        logger,
	}
}

func (s *Service) SyncTelemetry(ctx context.Context, req *fluxgatev1.SyncTelemetryRequest) (*fluxgatev1.SyncTelemetryResponse, error) {
	// Merge incoming state into local store
	var remoteAllowed, remoteDenied, remoteConsumed map[string]uint64
	if req.AllowedRequests != nil {
		remoteAllowed = req.AllowedRequests.Components
	}
	if req.DeniedRequests != nil {
		remoteDenied = req.DeniedRequests.Components
	}
	if req.ConsumedCredits != nil {
		remoteConsumed = req.ConsumedCredits.Components
	}

	s.store.MergeRemote(remoteAllowed, remoteDenied, remoteConsumed)

	// Snapshot local state to return in response (bi-directional sync)
	localAllowed, localDenied, localConsumed := s.store.Snapshot()

	return &fluxgatev1.SyncTelemetryResponse{
		ReceiverNodeId:        s.nodeID,
		ReceiverIncarnationId: s.incarnationID,
		AllowedRequests:       &fluxgatev1.GCounterState{Components: localAllowed},
		DeniedRequests:        &fluxgatev1.GCounterState{Components: localDenied},
		ConsumedCredits:       &fluxgatev1.GCounterState{Components: localConsumed},
	}, nil
}
