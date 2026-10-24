package allocator

import (
	"context"
	"errors"

	fluxgatev1 "fluxgate/api/proto/fluxgate/v1"
	"fluxgate/internal/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	fluxgatev1.UnimplementedQuotaAllocatorServiceServer
	coordinator *Coordinator
	logger      *telemetry.Logger
}

func NewGRPCServer(coordinator *Coordinator, logger *telemetry.Logger) *GRPCServer {
	return &GRPCServer{
		coordinator: coordinator,
		logger:      logger,
	}
}

func (s *GRPCServer) AcquireGrant(ctx context.Context, req *fluxgatev1.AcquireGrantRequest) (*fluxgatev1.AcquireGrantResponse, error) {
	if req.RequestId == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.IncarnationId == "" {
		return nil, status.Error(codes.InvalidArgument, "incarnation_id is required")
	}
	if req.PolicyId == "" {
		return nil, status.Error(codes.InvalidArgument, "policy_id is required")
	}
	if req.RequestedCredits == 0 {
		return nil, status.Error(codes.InvalidArgument, "requested_credits must be > 0")
	}

	args := AcquireArgs{
		RequestID:        req.RequestId,
		NodeID:           req.NodeId,
		IncarnationID:    req.IncarnationId,
		PolicyID:         req.PolicyId,
		PolicyVersion:    req.PolicyVersion,
		RequestedCredits: req.RequestedCredits,
	}

	now := s.coordinator.clock.Now()
	grant, isDup, err := s.coordinator.ReserveGrant(now, args)
	if err != nil {
		if errors.Is(err, ErrInsufficientHeadroom) {
			return &fluxgatev1.AcquireGrantResponse{
				RequestId:      req.RequestId,
				Status:         fluxgatev1.GrantStatus_GRANT_STATUS_EXHAUSTED,
				Message:        "No quota headroom available in allocator pool",
				GrantedCredits: 0,
				EpochId:        s.coordinator.EpochID(),
			}, nil
		}
		if errors.Is(err, ErrPolicyVersionMismatch) {
			return &fluxgatev1.AcquireGrantResponse{
				RequestId:      req.RequestId,
				Status:         fluxgatev1.GrantStatus_GRANT_STATUS_POLICY_MISMATCH,
				Message:        err.Error(),
				GrantedCredits: 0,
				EpochId:        s.coordinator.EpochID(),
			}, nil
		}
		return nil, status.Errorf(codes.Internal, "reservation error: %v", err)
	}

	s.logger.Debug("Issued Quota Lease",
		"grant_id", grant.GrantID,
		"node_id", grant.NodeID,
		"amount", grant.Amount,
		"is_duplicate", isDup,
	)

	return &fluxgatev1.AcquireGrantResponse{
		RequestId:       req.RequestId,
		GrantId:         grant.GrantID,
		GrantedCredits:  grant.Amount,
		Status:          fluxgatev1.GrantStatus_GRANT_STATUS_OK,
		ValidDurationMs: uint64(grant.ValidDuration.Milliseconds()),
		EpochId:         s.coordinator.EpochID(),
	}, nil
}

func (s *GRPCServer) ReportConsumption(ctx context.Context, req *fluxgatev1.ReportConsumptionRequest) (*fluxgatev1.ReportConsumptionResponse, error) {
	if req.NodeId == "" || req.IncarnationId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and incarnation_id are required")
	}

	items := make([]ReportItem, len(req.Reports))
	for i, r := range req.Reports {
		items[i] = ReportItem{
			GrantID:            r.GrantId,
			CumulativeConsumed: r.CumulativeConsumed,
		}
	}

	now := s.coordinator.clock.Now()
	results, err := s.coordinator.ReconcileReports(now, req.NodeId, req.IncarnationId, items)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reconciliation error: %v", err)
	}

	pbResults := make([]*fluxgatev1.ReconciledGrant, len(results))
	for i, res := range results {
		var pbStatus fluxgatev1.ReportStatus
		switch res.Status {
		case "OK":
			pbStatus = fluxgatev1.ReportStatus_REPORT_STATUS_OK
		case "STALE":
			pbStatus = fluxgatev1.ReportStatus_REPORT_STATUS_STALE
		case "EXCEEDS_GRANT":
			pbStatus = fluxgatev1.ReportStatus_REPORT_STATUS_EXCEEDS_GRANT
		case "INCARNATION_MISMATCH":
			pbStatus = fluxgatev1.ReportStatus_REPORT_STATUS_INCARNATION_MISMATCH
		default:
			pbStatus = fluxgatev1.ReportStatus_REPORT_STATUS_GRANT_NOT_FOUND
		}

		pbResults[i] = &fluxgatev1.ReconciledGrant{
			GrantId:                    res.GrantID,
			AcceptedCumulativeConsumed: res.AcceptedConsumed,
			Status:                     pbStatus,
			ErrorMessage:               res.ErrorMessage,
		}
	}

	return &fluxgatev1.ReportConsumptionResponse{
		Results: pbResults,
	}, nil
}

func (s *GRPCServer) GetAllocatorStatus(ctx context.Context, req *fluxgatev1.GetAllocatorStatusRequest) (*fluxgatev1.GetAllocatorStatusResponse, error) {
	ok, _ := s.coordinator.CheckConservationStatus()

	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()

	policies := make([]*fluxgatev1.PolicyStatusInfo, 0, len(s.coordinator.pools))
	for polID, pool := range s.coordinator.pools {
		outstanding := s.coordinator.ledger.SumOutstandingUpperBounds(polID)
		unallocated := pool.AvailableCredits()
		active := s.coordinator.ledger.ActiveGrantCount(polID)

		policies = append(policies, &fluxgatev1.PolicyStatusInfo{
			PolicyId:           polID,
			PolicyVersion:      pool.Policy.Version,
			Capacity:           pool.Policy.Capacity,
			RefillRate:         pool.Policy.RefillRate,
			UnallocatedTokens:  unallocated,
			OutstandingCredits: outstanding,
			TotalGrantsActive:  active,
		})
	}

	return &fluxgatev1.GetAllocatorStatusResponse{
		Status:                         "OPERATIONAL",
		EpochId:                        s.coordinator.EpochID(),
		Policies:                       policies,
		ConservationInvariantSatisfied: ok,
	}, nil
}
